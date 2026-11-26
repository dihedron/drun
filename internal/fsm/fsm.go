// Package fsm is resurgo's replicated state machine (resurgo_specification.md
// §9): the fencing epoch, the runnables' incarnations, checkpoints,
// failures and completions, and, through Store, their SQL stores.
//
// Apply is deterministic (NFR-8): no clock, no randomness, no I/O but the
// Store's; every timestamp comes from the leader, in the command. A
// command that is refused changes nothing, and is answered with an error
// in its Result, never a panic.
package fsm

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// FormatVersion is the version of the command and snapshot formats this
// node writes and the newest it reads (OI-10).
const FormatVersion uint32 = 1

// MaxCheckpointSize is the hard limit on a checkpoint (FR-9); the
// configured, smaller limit is enforced on the leader.
const MaxCheckpointSize = 1 << 20

// Result is what Apply returns for every command entry.
type Result struct {
	// Epoch is the new epoch, for Incarnate.
	Epoch uint64
	// Seq is the new checkpoint's sequence number, for Checkpoint.
	Seq uint64
	// Batch is the result of a StoreBatch.
	Batch *fsmpb.BatchResult
	// Err is why the command was refused; it wraps one of the Err values.
	Err error
}

// FSM is the state machine; it implements raft.FSM.
type FSM struct {
	store  Store
	logger *slog.Logger

	mu           sync.RWMutex
	epochCounter uint64
	runnables    map[string]*fsmpb.Runnable
	// peers are the identities recorded by SetPeer, by node ID
	peers map[string]string
}

// New returns an empty FSM over store.
func New(store Store, logger *slog.Logger) *FSM {
	return &FSM{store: store, logger: logger, runnables: map[string]*fsmpb.Runnable{}, peers: map[string]string{}}
}

// Encode stamps a command with FormatVersion and encodes it as a log entry.
func Encode(cmd *fsmpb.Command) ([]byte, error) {
	c, _ := proto.Clone(cmd).(*fsmpb.Command)
	c.FormatVersion = FormatVersion
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("encoding command: %w", err)
	}
	return data, nil
}

// Apply applies one committed entry; for a command it returns a *Result.
func (f *FSM) Apply(l *raft.Log) any {
	if l.Type != raft.LogCommand {
		return nil
	}
	var cmd fsmpb.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		return f.refused(l, nil, fmt.Errorf("%w: decoding: %w", ErrUnsupported, err))
	}
	if cmd.GetFormatVersion() == 0 || cmd.GetFormatVersion() > FormatVersion {
		return f.refused(l, nil, fmt.Errorf("%w: format version %d, this node reads up to %d", ErrUnsupported, cmd.GetFormatVersion(), FormatVersion))
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	var msg any
	var res *Result
	switch c := cmd.GetCommand().(type) {
	case *fsmpb.Command_StoreBatch:
		msg, res = c.StoreBatch, f.storeBatch(l, c.StoreBatch)
	case *fsmpb.Command_Incarnate:
		msg, res = c.Incarnate, f.incarnate(l, c.Incarnate)
	case *fsmpb.Command_Checkpoint:
		msg, res = c.Checkpoint, f.checkpoint(l, c.Checkpoint)
	case *fsmpb.Command_Release:
		msg, res = c.Release, f.release(c.Release)
	case *fsmpb.Command_Complete:
		msg, res = c.Complete, f.complete(c.Complete)
	case *fsmpb.Command_Fail:
		msg, res = c.Fail, f.fail(c.Fail)
	case *fsmpb.Command_SetDisabled:
		msg, res = c.SetDisabled, f.setDisabled(c.SetDisabled)
	case *fsmpb.Command_ResetRunnable:
		msg, res = c.ResetRunnable, f.reset(l, c.ResetRunnable)
	case *fsmpb.Command_Forget:
		msg, res = c.Forget, f.forget(l, c.Forget)
	case *fsmpb.Command_SetPeer:
		msg, res = c.SetPeer, f.setPeer(c.SetPeer)
	case *fsmpb.Command_RemovePeer:
		msg, res = c.RemovePeer, f.removePeer(c.RemovePeer)
	default:
		res = &Result{Err: fmt.Errorf("%w: no known command in the entry", ErrUnsupported)}
	}
	if res.Err != nil {
		return f.refused(l, msg, res.Err)
	}
	return res
}

// refused logs a refusal and returns it as a Result
func (f *FSM) refused(l *raft.Log, msg any, err error) *Result {
	attrs := []any{"index", l.Index, "term", l.Term, "error", err}
	if m, ok := msg.(interface{ GetRunnable() string }); ok {
		attrs = append(attrs, "runnable", m.GetRunnable())
	}
	if m, ok := msg.(interface{ GetEpoch() uint64 }); ok {
		attrs = append(attrs, "epoch", m.GetEpoch())
	}
	switch {
	case errors.Is(err, ErrStore):
		f.logger.Error("command failed in the store", attrs...)
	case errors.Is(err, ErrUnsupported):
		f.logger.Warn("command not supported", attrs...)
	default:
		f.logger.Info("command refused", attrs...)
	}
	return &Result{Err: err}
}

// current returns the runnable if epoch is its current incarnation's
func (f *FSM) current(name string, epoch uint64) (*fsmpb.Runnable, error) {
	r, ok := f.runnables[name]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownRunnable, name)
	}
	if cur := r.GetCurrent(); cur == nil || cur.GetEpoch() != epoch {
		return nil, fmt.Errorf("%w: epoch %d, current %d", ErrFenced, epoch, cur.GetEpoch())
	}
	return r, nil
}

// writable is current, for commands a released incarnation may not send
func (f *FSM) writable(name string, epoch uint64) (*fsmpb.Runnable, error) {
	r, err := f.current(name, epoch)
	if err != nil {
		return nil, err
	}
	if r.GetReleased() {
		return nil, fmt.Errorf("%w: epoch %d was released", ErrFenced, epoch)
	}
	return r, nil
}

func (f *FSM) storeBatch(l *raft.Log, c *fsmpb.StoreBatch) *Result {
	r, err := f.writable(c.GetRunnable(), c.GetEpoch())
	if err != nil {
		return &Result{Err: err}
	}
	br, err := f.store.Exec(l.Index, c, r.GetStoreLimits())
	if err != nil {
		return &Result{Err: fmt.Errorf("%w: batch: %w", ErrStore, err)}
	}
	return &Result{Batch: br}
}

func (f *FSM) incarnate(l *raft.Log, c *fsmpb.Incarnate) *Result {
	if err := config.ValidRunnableName(c.GetRunnable()); err != nil {
		return &Result{Err: fmt.Errorf("%w: %w", ErrInvalid, err)}
	}
	r, ok := f.runnables[c.GetRunnable()]
	switch {
	case !ok:
		r = &fsmpb.Runnable{Name: c.GetRunnable()}
	case r.GetDisabled():
		return &Result{Err: ErrDisabled}
	case r.GetCompleted() != nil:
		return &Result{Err: ErrCompleted}
	}
	f.epochCounter++
	r.Current = &fsmpb.Incarnation{
		Epoch:        f.epochCounter,
		Term:         l.Term,
		Node:         c.GetNode(),
		StartedAt:    c.GetAt(),
		BinarySha256: c.GetBinarySha256(),
		ConfigSha256: c.GetConfigSha256(),
	}
	r.Released = false
	r.StoreLimits = c.GetStoreLimits()
	f.runnables[r.GetName()] = r
	return &Result{Epoch: f.epochCounter}
}

func (f *FSM) checkpoint(l *raft.Log, c *fsmpb.Checkpoint) *Result {
	r, err := f.writable(c.GetRunnable(), c.GetEpoch())
	if err != nil {
		return &Result{Err: err}
	}
	if len(c.GetData()) > MaxCheckpointSize {
		return &Result{Err: fmt.Errorf("%w: %d bytes, at most %d", ErrTooLarge, len(c.GetData()), MaxCheckpointSize)}
	}
	meta := &fsmpb.CheckpointMeta{
		Seq:         r.GetCheckpoint().GetSeq() + 1,
		Epoch:       c.GetEpoch(),
		CommittedAt: c.GetAt(),
		Size:        uint64(len(c.GetData())),
	}
	if err := f.store.PutCheckpoint(l.Index, r.GetName(), meta, c.GetData()); err != nil {
		return &Result{Err: fmt.Errorf("%w: checkpoint: %w", ErrStore, err)}
	}
	r.Checkpoint = meta
	clearFailures(r, r.GetCurrent().GetNode())
	return &Result{Seq: meta.GetSeq()}
}

func (f *FSM) release(c *fsmpb.Release) *Result {
	r, err := f.current(c.GetRunnable(), c.GetEpoch())
	if err != nil {
		return &Result{Err: err}
	}
	r.Released = true
	clearFailures(r, r.GetCurrent().GetNode())
	return &Result{}
}

func (f *FSM) complete(c *fsmpb.Complete) *Result {
	r, err := f.current(c.GetRunnable(), c.GetEpoch())
	if err != nil {
		return &Result{Err: err}
	}
	r.Completed = &fsmpb.Completion{Epoch: c.GetEpoch(), At: c.GetAt()}
	r.Released = true
	return &Result{}
}

func (f *FSM) fail(c *fsmpb.Fail) *Result {
	r, err := f.current(c.GetRunnable(), c.GetEpoch())
	if err != nil {
		return &Result{Err: err}
	}
	i, found := slices.BinarySearchFunc(r.GetFailures(), c.GetNode(), func(fr *fsmpb.FailureRecord, node string) int {
		return strings.Compare(fr.GetNode(), node)
	})
	if !found {
		r.Failures = slices.Insert(r.Failures, i, &fsmpb.FailureRecord{Node: c.GetNode()})
	}
	fr := r.GetFailures()[i]
	fr.Consecutive++
	fr.LastAt = c.GetAt()
	fr.LastReason = c.GetReason()
	return &Result{}
}

func (f *FSM) setDisabled(c *fsmpb.SetDisabled) *Result {
	if err := config.ValidRunnableName(c.GetRunnable()); err != nil {
		return &Result{Err: fmt.Errorf("%w: %w", ErrInvalid, err)}
	}
	r, ok := f.runnables[c.GetRunnable()]
	if !ok {
		r = &fsmpb.Runnable{Name: c.GetRunnable()}
		f.runnables[r.GetName()] = r
	}
	r.Disabled = c.GetDisabled()
	return &Result{}
}

func (f *FSM) reset(l *raft.Log, c *fsmpb.Reset) *Result {
	r, ok := f.runnables[c.GetRunnable()]
	if !ok {
		return &Result{Err: fmt.Errorf("%w %q", ErrUnknownRunnable, c.GetRunnable())}
	}
	if c.GetCheckpoint() {
		if err := f.store.ClearCheckpoint(l.Index, r.GetName()); err != nil {
			return &Result{Err: fmt.Errorf("%w: clearing the checkpoint: %w", ErrStore, err)}
		}
		r.Checkpoint = nil
	}
	if c.GetCompletion() {
		r.Completed = nil
	}
	if c.GetFailures() {
		r.Failures = nil
	}
	return &Result{}
}

func (f *FSM) forget(l *raft.Log, c *fsmpb.Forget) *Result {
	r, ok := f.runnables[c.GetRunnable()]
	if !ok {
		return &Result{Err: fmt.Errorf("%w %q", ErrUnknownRunnable, c.GetRunnable())}
	}
	if r.GetCurrent() != nil && !r.GetReleased() {
		return &Result{Err: fmt.Errorf("%w: epoch %d was not released", ErrRunning, r.GetCurrent().GetEpoch())}
	}
	if err := f.store.Forget(l.Index, r.GetName()); err != nil {
		return &Result{Err: fmt.Errorf("%w: forgetting: %w", ErrStore, err)}
	}
	delete(f.runnables, r.GetName())
	return &Result{}
}

func (f *FSM) setPeer(c *fsmpb.SetPeer) *Result {
	if c.GetId() == "" {
		return &Result{Err: fmt.Errorf("%w: a peer without id", ErrInvalid)}
	}
	if err := config.ValidIdentity(c.GetIdentity()); err != nil {
		return &Result{Err: fmt.Errorf("%w: %w", ErrInvalid, err)}
	}
	for id, identity := range f.peers {
		if identity == c.GetIdentity() && id != c.GetId() {
			return &Result{Err: fmt.Errorf("%w: identity %q is already %q's", ErrInvalid, identity, id)}
		}
	}
	f.peers[c.GetId()] = c.GetIdentity()
	return &Result{}
}

func (f *FSM) removePeer(c *fsmpb.RemovePeer) *Result {
	if _, ok := f.peers[c.GetId()]; !ok {
		return &Result{Err: fmt.Errorf("%w %q", ErrUnknownPeer, c.GetId())}
	}
	delete(f.peers, c.GetId())
	return &Result{}
}

// clearFailures removes node's failure record: it ran successfully
func clearFailures(r *fsmpb.Runnable, node string) {
	r.Failures = slices.DeleteFunc(r.Failures, func(fr *fsmpb.FailureRecord) bool { return fr.GetNode() == node })
}

// Runnable returns a copy of the runnable's state.
func (f *FSM) Runnable(name string) (*fsmpb.Runnable, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	r, ok := f.runnables[name]
	if !ok {
		return nil, false
	}
	c, _ := proto.Clone(r).(*fsmpb.Runnable)
	return c, true
}

// Runnables returns the names of the runnables with a state, sorted.
func (f *FSM) Runnables() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return slices.Sorted(maps.Keys(f.runnables))
}

// Peers returns a copy of the recorded peer identities, by node ID.
func (f *FSM) Peers() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return maps.Clone(f.peers)
}

// EpochCounter returns the last epoch handed out.
func (f *FSM) EpochCounter() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epochCounter
}
