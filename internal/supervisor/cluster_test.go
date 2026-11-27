package supervisor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/fsm"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/dihedron/resurgo/internal/store"
	"github.com/dihedron/resurgo/pkg/runnable"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// timings scaled down from §8.6, with the same constraints
const (
	heartbeat = 200 * time.Millisecond
	lease     = 150 * time.Millisecond
	renew     = 50 * time.Millisecond
	stopGrace = 200 * time.Millisecond
)

// tracker sees every incarnation of every node: it records violations of
// "one incarnation acting at a time" (NFR-4 without pauses), of epochs
// increasing (INV-1) and of checkpoints carried over (INV-4)
type tracker struct {
	mu         sync.Mutex
	active     map[string][]string // runnable: "node/epoch" acting now
	starts     []start
	lastEpoch  map[string]uint64
	acked      map[string]uint64 // runnable: highest acknowledged checkpoint value
	violations []string
}

type start struct {
	runnable, node string
	epoch          uint64
	at             time.Time
	checkpoint     uint64
}

func newTracker() *tracker {
	return &tracker{active: map[string][]string{}, lastEpoch: map[string]uint64{}, acked: map[string]uint64{}}
}

func (tr *tracker) violate(format string, args ...any) {
	tr.violations = append(tr.violations, fmt.Sprintf(format, args...))
}

func (tr *tracker) begin(inc runnable.Incarnation) uint64 {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	key := fmt.Sprintf("%s/%d", inc.Node, inc.Epoch)
	if len(tr.active[inc.Runnable]) > 0 {
		tr.violate("%s: %s started while %v acts", inc.Runnable, key, tr.active[inc.Runnable])
	}
	tr.active[inc.Runnable] = append(tr.active[inc.Runnable], key)
	if inc.Epoch <= tr.lastEpoch[inc.Runnable] {
		tr.violate("%s: epoch %d after %d", inc.Runnable, inc.Epoch, tr.lastEpoch[inc.Runnable])
	}
	tr.lastEpoch[inc.Runnable] = inc.Epoch
	var cp uint64
	if inc.Checkpoint != nil && len(inc.Checkpoint.Data) == 8 {
		cp = binary.BigEndian.Uint64(inc.Checkpoint.Data)
	}
	if cp < tr.acked[inc.Runnable] {
		tr.violate("%s: %s resumed from checkpoint %d, %d was acknowledged", inc.Runnable, key, cp, tr.acked[inc.Runnable])
	}
	tr.starts = append(tr.starts, start{runnable: inc.Runnable, node: inc.Node, epoch: inc.Epoch, at: time.Now(), checkpoint: cp})
	return cp
}

func (tr *tracker) end(inc runnable.Incarnation) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	key := fmt.Sprintf("%s/%d", inc.Node, inc.Epoch)
	tr.active[inc.Runnable] = slices.DeleteFunc(tr.active[inc.Runnable], func(k string) bool { return k == key })
}

func (tr *tracker) ack(runnable string, v uint64) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.acked[runnable] = max(tr.acked[runnable], v)
}

func (tr *tracker) startsOf(name string) []start {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var out []start
	for _, s := range tr.starts {
		if s.runnable == name {
			out = append(out, s)
		}
	}
	return out
}

func (tr *tracker) acting(name string) []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.active[name])
}

func (tr *tracker) check(t testing.TB) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, v := range tr.violations {
		t.Error(v)
	}
}

// behaviour of the test runnable, from its configuration
type behaviour struct {
	Mode     string `json:"mode"` // service (default), task, fail, unhealthy, flaky
	Interval int    `json:"interval_ms"`
	Store    bool   `json:"store"`
}

// ticker is the test runnable: it counts in its checkpoint, from where the
// previous incarnation left it
type ticker struct {
	tr *tracker
}

func (ticker) Describe() runnable.Description {
	return runnable.Description{Name: "ticker", Version: "test"}
}

func (ticker) Validate(context.Context, []byte) error { return nil }

func (k ticker) Run(ctx context.Context, inc runnable.Incarnation, host runnable.Host) error {
	var b behaviour
	_ = json.Unmarshal(inc.Config, &b)
	v := k.tr.begin(inc)
	defer k.tr.end(inc)
	switch b.Mode {
	case "fail":
		return errors.New("ticker fails")
	case "unhealthy":
		_ = host.Status(ctx, false, "sick")
		<-ctx.Done()
		return context.Cause(ctx)
	}
	interval := time.Duration(max(b.Interval, 20)) * time.Millisecond
	if b.Store {
		if _, err := host.Store().Exec(ctx, []runnable.Statement{{SQL: "CREATE TABLE IF NOT EXISTS ticks (v INTEGER, epoch INTEGER)"}}); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(interval):
		}
		v++
		data := binary.BigEndian.AppendUint64(nil, v)
		if _, err := host.Checkpoint(ctx, data); err != nil {
			return err
		}
		k.tr.ack(inc.Runnable, v)
		if b.Store {
			if _, err := host.Store().Exec(ctx, []runnable.Statement{{SQL: "INSERT INTO ticks VALUES (?, resurgo_epoch())", Args: []any{int64(v)}}}); err != nil {
				return err
			}
		}
		if b.Mode == "task" {
			return nil
		}
		if b.Mode == "flaky" && rand.IntN(50) == 0 { //nolint:gosec // test faults
			return errors.New("ticker failed at random")
		}
		_ = host.Status(ctx, true, fmt.Sprintf("v=%d", v))
	}
}

type tnode struct {
	id     string
	addr   raft.ServerAddress
	trans  *raft.InmemTransport
	raft   *raft.Raft
	fsm    *fsm.FSM
	store  *store.Store
	sup    *Supervisor
	cancel context.CancelFunc
	done   chan struct{}
}

type cluster struct {
	t     *testing.T
	tr    *tracker
	nodes []*tnode
}

// testSpec is the ticker with test timings; change modifies it
func testSpec(b behaviour, change func(*Spec)) Spec {
	cfg, _ := json.Marshal(b)
	r := config.Runnable{
		Name: "ticker", Kind: config.KindService,
		StopGrace: stopGrace, ShutdownGrace: time.Second,
		Restart:    config.Restart{Initial: 50 * time.Millisecond, Max: 400 * time.Millisecond, HandoverAfter: 3},
		Checkpoint: config.Checkpoint{MaxSize: 1024, MinInterval: 0},
		Health:     config.Health{Timeout: 300 * time.Millisecond},
		Store:      config.Store{MaxBatch: config.MiB, MaxSteps: 50_000_000, MaxSize: 64 * config.MiB, MaxResult: config.MiB, QueryTimeout: time.Second},
	}
	if b.Mode == "task" {
		r.Kind = config.KindTask
	}
	s := Spec{Runnable: r, TakeoverDelay: lease + stopGrace - heartbeat + 500*time.Millisecond, ConfigJSON: cfg, BinarySHA256: []byte{1}, ConfigSHA256: []byte{2}}
	if change != nil {
		change(&s)
	}
	return s
}

func newCluster(t *testing.T, n int, spec Spec) *cluster {
	c := &cluster{t: t, tr: newTracker()}
	var servers []raft.Server
	for i := range n {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("n%d", i)))
		c.nodes = append(c.nodes, &tnode{id: string(addr), addr: addr, trans: trans})
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(addr), Address: addr})
	}
	c.heal()
	for _, nd := range c.nodes {
		st, err := store.Open(filepath.Join(t.TempDir(), "store"), func(err error) { t.Errorf("%s: fatal store error: %v", nd.id, err) }, discard)
		if err != nil {
			t.Fatal(err)
		}
		nd.store, nd.fsm = st, fsm.New(st, discard)
		rc := raft.DefaultConfig()
		rc.LocalID = raft.ServerID(nd.id)
		rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout = heartbeat, heartbeat, heartbeat/2
		rc.CommitTimeout = 5 * time.Millisecond
		rc.Logger = hclog.New(&hclog.LoggerOptions{Level: hclog.Error, Output: io.Discard})
		r, err := raft.NewRaft(rc, nd.fsm, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), nd.trans)
		if err != nil {
			t.Fatal(err)
		}
		nd.raft = r
		nd.sup = New(Options{
			Raft: r, FSM: nd.fsm, Store: st, Node: nd.id, Cluster: "test",
			Lease: lease, RenewInterval: renew, Runnables: []Spec{spec},
			Load: func(context.Context, Spec) (runnable.Instance, error) {
				return runnable.InProcess(ticker{tr: c.tr}), nil
			},
			Logger: discard.With("node", nd.id),
		})
		ctx, cancel := context.WithCancel(context.Background())
		nd.cancel, nd.done = cancel, make(chan struct{})
		go func() { defer close(nd.done); nd.sup.Run(ctx) }()
	}
	if err := c.nodes[0].raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.cancel()
			<-nd.done
			_ = nd.raft.Shutdown().Error()
			_ = nd.store.Close()
		}
		c.tr.check(t)
	})
	return c
}

func (c *cluster) leader(timeout time.Duration, among ...*tnode) *tnode {
	c.t.Helper()
	if len(among) == 0 {
		among = c.nodes
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*tnode
		for _, n := range among {
			if n.raft.State() == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %s", timeout)
	return nil
}

func (c *cluster) others(n *tnode) []*tnode {
	return slices.DeleteFunc(slices.Clone(c.nodes), func(o *tnode) bool { return o == n })
}

func (c *cluster) isolate(n *tnode) {
	n.trans.DisconnectAll()
	for _, o := range c.others(n) {
		o.trans.Disconnect(n.addr)
	}
}

func (c *cluster) heal() {
	for _, a := range c.nodes {
		for _, b := range c.nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}
}

// eventually waits for cond
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not %s within %s", what, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *cluster) apply(n *tnode, cmd *fsmpb.Command) *fsm.Result {
	c.t.Helper()
	data, err := fsm.Encode(cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.t.Fatalf("apply: %v", err)
	}
	return f.Response().(*fsm.Result) //nolint:forcetypeassert // the FSM's
}

func (c *cluster) runnableState(n *tnode) *fsmpb.Runnable {
	r, _ := n.fsm.Runnable("ticker")
	return r
}
