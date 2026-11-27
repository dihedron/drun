// Package supervisor runs the runnables on the leader
// (resurgo_specification.md §7.2): one incarnation controller per
// configured runnable while the node is leader, nothing on a follower.
package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/fsm"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/dihedron/resurgo/internal/store"
	"github.com/dihedron/resurgo/pkg/runnable"
	"github.com/hashicorp/raft"
)

// applyTimeout bounds a command's wait for its commit
const applyTimeout = 5 * time.Second

// ErrNotLeader: a planned stop or handover on a node that is not leader.
var ErrNotLeader = errors.New("supervisor: not leader")

// Spec is a runnable as this node runs it.
type Spec struct {
	config.Runnable
	// TakeoverDelay is the resolved takeover_delay (§8.4).
	TakeoverDelay time.Duration
	// ConfigJSON is the runnable's configuration, as given to it.
	ConfigJSON []byte
	// BinarySHA256 and ConfigSHA256 are the digests recorded by every
	// incarnation (FR-15).
	BinarySHA256, ConfigSHA256 []byte
}

// Options are what a supervisor needs.
type Options struct {
	Raft  *raft.Raft
	FSM   *fsm.FSM
	Store *store.Store
	// Node and Cluster name this node and its cluster.
	Node, Cluster string
	// Lease and RenewInterval are lease.duration and lease.renew_interval.
	Lease, RenewInterval time.Duration
	Runnables            []Spec
	// Load starts a fresh instance of a runnable for one incarnation.
	Load func(ctx context.Context, spec Spec) (runnable.Instance, error)
	// Events receives what the metrics count (§10.3); nil: nothing.
	Events Events
	Logger *slog.Logger
}

// Events are the supervisor's events that metrics count.
type Events interface {
	// Started: an incarnation started; for the first of a leadership,
	// failover is the time since the node became leader, planned whether
	// its predecessor was released, and wait the takeover delay it waited.
	Started(runnable string, first, planned bool, failover, wait time.Duration)
	// Ended: an incarnation ended, how (outcome).
	Ended(runnable, outcome string)
	// CheckpointCommitted: a checkpoint was committed after took.
	CheckpointCommitted(runnable string, took time.Duration)
	// LeaseRenewFailed: leadership was not confirmed, or the lease not
	// renewed.
	LeaseRenewFailed(runnable string)
}

type noEvents struct{}

func (noEvents) Started(string, bool, bool, time.Duration, time.Duration) {}
func (noEvents) Ended(string, string)                                     {}
func (noEvents) CheckpointCommitted(string, time.Duration)                {}
func (noEvents) LeaseRenewFailed(string)                                  {}

// Specs builds the runnables' specs from the node's configuration.
func Specs(cfg *config.Config) ([]Spec, error) {
	var specs []Spec
	for _, r := range cfg.Runnables {
		configJSON, err := json.Marshal(r.Config) // map keys sorted: a stable digest
		if err != nil {
			return nil, fmt.Errorf("runnable %s: configuration: %w", r.Name, err)
		}
		bin, err := hex.DecodeString(r.SHA256)
		if err != nil {
			return nil, fmt.Errorf("runnable %s: sha256: %w", r.Name, err)
		}
		sum := sha256.Sum256(configJSON)
		specs = append(specs, Spec{Runnable: r, TakeoverDelay: cfg.TakeoverDelay(r), ConfigJSON: configJSON, BinarySHA256: bin, ConfigSHA256: sum[:]})
	}
	return specs, nil
}

// Drift reports whether this node's digests of a runnable differ from
// those its current incarnation recorded (FR-15).
func Drift(state *fsmpb.Runnable, sp Spec) bool {
	cur := state.GetCurrent()
	if cur == nil || cur.GetOperator() != "" {
		return false // no incarnation, or an operator's (STO-17)
	}
	return !bytes.Equal(cur.GetBinarySha256(), sp.BinarySHA256) || !bytes.Equal(cur.GetConfigSha256(), sp.ConfigSHA256)
}

// Supervisor watches leadership and runs the controllers.
type Supervisor struct {
	o Options

	mu      sync.Mutex
	leading *leadership
	views   map[string]*View
}

// leadership is one term of leadership of this node
type leadership struct {
	ctx    context.Context
	cancel context.CancelFunc
	since  time.Time
	term   uint64
	ctrls  map[string]*controller
	wg     sync.WaitGroup
}

// New returns a supervisor; Run starts it.
func New(o Options) *Supervisor {
	if o.Events == nil {
		o.Events = noEvents{}
	}
	s := &Supervisor{o: o, views: map[string]*View{}}
	for _, spec := range o.Runnables {
		s.views[spec.Name] = &View{Runnable: spec.Name, State: StateIdle}
	}
	return s
}

// Run follows leadership until ctx ends: controllers run while this node
// is leader, and stop as soon as it is not (FR-11).
func (s *Supervisor) Run(ctx context.Context) {
	ch := s.o.Raft.LeaderCh()
	tick := time.NewTicker(s.o.RenewInterval)
	defer tick.Stop()
	defer s.lose()
	for {
		// a missed signal cannot leave controllers running on a follower:
		// the state is checked on every signal and every tick
		if s.o.Raft.State() == raft.Leader {
			s.lead()
		} else {
			s.lose()
		}
		select {
		case <-ctx.Done():
			return
		case <-ch:
		case <-tick.C:
		}
	}
}

// lead starts the controllers, once per leadership, after the barrier
func (s *Supervisor) lead() {
	s.mu.Lock()
	if s.leading != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	// every entry of earlier leaders is applied before anything starts
	// (INV-4); a barrier that fails means this node is not leader after all
	if err := s.o.Raft.Barrier(applyTimeout).Error(); err != nil {
		s.o.Logger.Warn("barrier after the election failed", "error", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &leadership{ctx: ctx, cancel: cancel, since: time.Now(), term: s.o.Raft.CurrentTerm(), ctrls: map[string]*controller{}}
	s.o.Logger.Info("leader: starting the runnables", "term", l.term)
	s.mu.Lock()
	s.leading = l
	for _, spec := range s.o.Runnables {
		c := newController(s, l, spec)
		l.ctrls[spec.Name] = c
		l.wg.Go(func() { c.run() })
	}
	s.mu.Unlock()
}

// lose stops every controller and waits for them: unplanned, no Release
func (s *Supervisor) lose() {
	s.mu.Lock()
	l := s.leading
	s.leading = nil
	s.mu.Unlock()
	if l == nil {
		return
	}
	s.o.Logger.Info("not leader: stopping the runnables", "term", l.term)
	l.cancel()
	l.wg.Wait()
}

// planned stops every controller's incarnation and releases it (FR-12);
// hold keeps them from starting another until leadership changes
func (s *Supervisor) planned(ctx context.Context, hold bool, names ...string) error {
	l, err := s.leadership(ctx)
	if err != nil {
		return err
	}
	var dones []chan struct{}
	for name, c := range l.ctrls {
		if len(names) > 0 && !contains(names, name) {
			continue
		}
		cmd := &command{hold: hold, done: make(chan struct{})}
		select {
		case c.cmds <- cmd:
			dones = append(dones, cmd.done)
		case <-l.ctx.Done():
		}
	}
	for _, d := range dones {
		select {
		case <-d:
		case <-ctx.Done():
			return ctx.Err()
		case <-l.ctx.Done():
		}
	}
	return nil
}

// leadership returns the current leadership; a node raft just made leader
// is waited for until its controllers run (after the barrier)
func (s *Supervisor) leadership(ctx context.Context) (*leadership, error) {
	for {
		s.mu.Lock()
		l := s.leading
		s.mu.Unlock()
		if l != nil {
			return l, nil
		}
		if s.o.Raft.State() != raft.Leader {
			return nil, ErrNotLeader
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func contains(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// Hold stops and releases the named runnables' incarnations, as a planned
// stop, and keeps them stopped until Resume or a change of leader.
func (s *Supervisor) Hold(ctx context.Context, names ...string) error {
	return s.planned(ctx, true, names...)
}

// Resume lets the named held runnables start again.
func (s *Supervisor) Resume(names ...string) { s.resume(names...) }

// resume lets held controllers start incarnations again: the named ones,
// or all
func (s *Supervisor) resume(names ...string) {
	s.mu.Lock()
	l := s.leading
	s.mu.Unlock()
	if l == nil {
		return
	}
	for name, c := range l.ctrls {
		if len(names) > 0 && !contains(names, name) {
			continue
		}
		select {
		case c.cmds <- &command{resume: true, done: make(chan struct{})}:
		case <-l.ctx.Done():
		}
	}
}

// Handover stops and releases every incarnation, then transfers
// leadership to the named voter, or to any if to is empty (FR-3, FR-12).
func (s *Supervisor) Handover(ctx context.Context, to string) error {
	if err := s.planned(ctx, true); err != nil {
		return err
	}
	var f raft.Future
	if to == "" {
		f = s.o.Raft.LeadershipTransfer()
	} else {
		addr, err := s.voterAddress(to)
		if err != nil {
			s.resume()
			return err
		}
		f = s.o.Raft.LeadershipTransferToServer(raft.ServerID(to), addr)
	}
	if err := f.Error(); err != nil {
		s.resume()
		return fmt.Errorf("supervisor: transferring leadership: %w", err)
	}
	s.o.Logger.Info("handed over", "to", to)
	return nil
}

// Shutdown hands leadership over if this node is leader (SIGTERM, FR-12).
func (s *Supervisor) Shutdown(ctx context.Context) error {
	if s.o.Raft.State() != raft.Leader {
		return nil
	}
	if !s.otherVoter() {
		// a single voter has no one to hand over to: stop and release
		return s.planned(ctx, true)
	}
	return s.Handover(ctx, "")
}

// otherVoter reports whether a voter other than this node exists
func (s *Supervisor) otherVoter() bool {
	f := s.o.Raft.GetConfiguration()
	if f.Error() != nil {
		return true // let the transfer report it
	}
	for _, srv := range f.Configuration().Servers {
		if string(srv.ID) != s.o.Node && srv.Suffrage == raft.Voter {
			return true
		}
	}
	return false
}

// Restart stops and releases the runnable's incarnation, and starts a new
// one (FR-14).
func (s *Supervisor) Restart(ctx context.Context, name string) error {
	return s.planned(ctx, false, name)
}

func (s *Supervisor) voterAddress(id string) (raft.ServerAddress, error) {
	f := s.o.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return "", err
	}
	for _, srv := range f.Configuration().Servers {
		if string(srv.ID) == id && srv.Suffrage == raft.Voter {
			return srv.Address, nil
		}
	}
	return "", fmt.Errorf("supervisor: %s is not a voter", id)
}

// apply commits a command and returns the FSM's result
func (s *Supervisor) apply(cmd *fsmpb.Command) (*fsm.Result, error) {
	data, err := fsm.Encode(cmd)
	if err != nil {
		return nil, err
	}
	f := s.o.Raft.Apply(data, applyTimeout)
	if err := f.Error(); err != nil {
		// the entry may still commit (EXT-1): not a fencing
		return nil, fmt.Errorf("not committed: %w", err)
	}
	res, ok := f.Response().(*fsm.Result)
	if !ok {
		return nil, errors.New("supervisor: no result from the FSM")
	}
	return res, nil
}
