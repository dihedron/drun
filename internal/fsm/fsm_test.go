package fsm

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// harness applies commands to an FSM with increasing indexes, in term 1
type harness struct {
	t     testing.TB
	fsm   *FSM
	store *fakeStore
	index uint64
	term  uint64
}

func newHarness(t testing.TB) *harness {
	s := newFakeStore()
	return &harness{t: t, fsm: New(s, slog.New(slog.NewTextHandler(io.Discard, nil))), store: s, term: 1}
}

func (h *harness) raw(data []byte) *Result {
	h.t.Helper()
	h.index++
	res, ok := h.fsm.Apply(&raft.Log{Index: h.index, Term: h.term, Type: raft.LogCommand, Data: data}).(*Result)
	if !ok {
		h.t.Fatalf("Apply did not return a *Result")
	}
	return res
}

func (h *harness) apply(cmd *fsmpb.Command) *Result {
	h.t.Helper()
	data, err := Encode(cmd)
	if err != nil {
		h.t.Fatalf("encode: %v", err)
	}
	return h.raw(data)
}

func (h *harness) incarnate(runnable, node string) uint64 {
	h.t.Helper()
	res := h.apply(incarnate(runnable, node))
	if res.Err != nil {
		h.t.Fatalf("incarnate %s on %s: %v", runnable, node, res.Err)
	}
	return res.Epoch
}

func (h *harness) runnable(name string) *fsmpb.Runnable {
	h.t.Helper()
	r, ok := h.fsm.Runnable(name)
	if !ok {
		h.t.Fatalf("no runnable %s", name)
	}
	return r
}

func incarnate(runnable, node string) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_Incarnate{Incarnate: &fsmpb.Incarnate{
		Runnable: runnable, Node: node, BinarySha256: []byte{1}, ConfigSha256: []byte{2},
		StoreLimits: &fsmpb.StoreLimits{MaxBatch: 1 << 20}, At: 1000,
	}}}
}

func checkpoint(runnable string, epoch uint64, data []byte) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_Checkpoint{Checkpoint: &fsmpb.Checkpoint{Runnable: runnable, Epoch: epoch, Data: data, At: 2000}}}
}

func release(runnable string, epoch uint64) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_Release{Release: &fsmpb.Release{Runnable: runnable, Epoch: epoch, At: 3000}}}
}

func complete(runnable string, epoch uint64) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_Complete{Complete: &fsmpb.Complete{Runnable: runnable, Epoch: epoch, At: 4000}}}
}

func fail(runnable string, epoch uint64, node string) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_Fail{Fail: &fsmpb.Fail{Runnable: runnable, Epoch: epoch, Node: node, Reason: "crash", At: 5000}}}
}

func batch(runnable string, epoch uint64) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_StoreBatch{StoreBatch: &fsmpb.StoreBatch{
		Runnable: runnable, Epoch: epoch, At: 6000,
		Statements: []*fsmpb.Statement{{Sql: "INSERT INTO t VALUES (?)", Args: []*fsmpb.Value{{Value: &fsmpb.Value_Integer{Integer: 1}}}}},
	}}}
}

func setDisabled(runnable string, disabled bool) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_SetDisabled{SetDisabled: &fsmpb.SetDisabled{Runnable: runnable, Disabled: disabled}}}
}

func reset(runnable string, cp, completion, failures bool) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_ResetRunnable{ResetRunnable: &fsmpb.Reset{Runnable: runnable, Checkpoint: cp, Completion: completion, Failures: failures}}}
}

func forget(runnable string) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_Forget{Forget: &fsmpb.Forget{Runnable: runnable}}}
}

func wantErr(t *testing.T, res *Result, want error) {
	t.Helper()
	if !errors.Is(res.Err, want) {
		t.Fatalf("error %v, want %v", res.Err, want)
	}
}

func TestIncarnate(t *testing.T) {
	h := newHarness(t)
	h.term = 7
	if e := h.incarnate("svc", "n1"); e != 1 {
		t.Fatalf("first epoch %d, want 1", e)
	}
	if e := h.incarnate("other", "n1"); e != 2 {
		t.Fatalf("epochs are cluster-wide: %d, want 2", e)
	}
	h.apply(release("svc", 1))
	if e := h.incarnate("svc", "n2"); e != 3 {
		t.Fatalf("epoch %d, want 3", e)
	}
	r := h.runnable("svc")
	cur := r.GetCurrent()
	if cur.GetEpoch() != 3 || cur.GetTerm() != 7 || cur.GetNode() != "n2" || cur.GetStartedAt() != 1000 ||
		!bytes.Equal(cur.GetBinarySha256(), []byte{1}) || !bytes.Equal(cur.GetConfigSha256(), []byte{2}) {
		t.Fatalf("current incarnation %v", cur)
	}
	if r.GetReleased() {
		t.Fatal("a new incarnation is not released")
	}
	if r.GetStoreLimits().GetMaxBatch() != 1<<20 {
		t.Fatalf("store limits %v", r.GetStoreLimits())
	}
	if h.fsm.EpochCounter() != 3 {
		t.Fatalf("epoch counter %d", h.fsm.EpochCounter())
	}
}

func TestIncarnateRefused(t *testing.T) {
	h := newHarness(t)
	h.apply(setDisabled("svc", true))
	wantErr(t, h.apply(incarnate("svc", "n1")), ErrDisabled)
	if h.fsm.EpochCounter() != 0 {
		t.Fatal("a refused Incarnate used an epoch")
	}
	h.apply(setDisabled("svc", false))
	wantErr(t, h.apply(incarnate("../svc", "n1")), ErrInvalid)
	wantErr(t, h.apply(setDisabled("", true)), ErrInvalid)
	e := h.incarnate("svc", "n1")

	// INV-8: a completed task is not started again until reset
	if res := h.apply(complete("svc", e)); res.Err != nil {
		t.Fatalf("complete: %v", res.Err)
	}
	wantErr(t, h.apply(incarnate("svc", "n2")), ErrCompleted)
	h.apply(reset("svc", false, true, false))
	h.incarnate("svc", "n2")
}

func TestCheckpoint(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	for want := uint64(1); want <= 3; want++ {
		res := h.apply(checkpoint("svc", e, []byte{byte(want)}))
		if res.Err != nil || res.Seq != want {
			t.Fatalf("checkpoint: seq %d err %v, want seq %d", res.Seq, res.Err, want)
		}
	}
	meta := h.runnable("svc").GetCheckpoint()
	if meta.GetSeq() != 3 || meta.GetEpoch() != e || meta.GetCommittedAt() != 2000 || meta.GetSize() != 1 {
		t.Fatalf("checkpoint meta %v", meta)
	}
	if !bytes.Equal(h.store.checkpoints["svc"], []byte{3}) {
		t.Fatalf("store holds %v", h.store.checkpoints["svc"])
	}

	// the sequence survives incarnations
	h.apply(release("svc", e))
	e2 := h.incarnate("svc", "n2")
	if res := h.apply(checkpoint("svc", e2, nil)); res.Seq != 4 {
		t.Fatalf("seq %d after a new incarnation, want 4", res.Seq)
	}

	wantErr(t, h.apply(checkpoint("svc", e2, make([]byte, MaxCheckpointSize+1))), ErrTooLarge)
	if res := h.apply(checkpoint("svc", e2, make([]byte, MaxCheckpointSize))); res.Err != nil {
		t.Fatalf("checkpoint of the maximum size: %v", res.Err)
	}
}

// INV-3: a stale incarnation's commands change nothing
func TestFencing(t *testing.T) {
	h := newHarness(t)
	old := h.incarnate("svc", "n1")
	h.apply(checkpoint("svc", old, []byte("old")))
	cur := h.incarnate("svc", "n2")
	before := h.fsm.encodeState()

	for _, cmd := range []*fsmpb.Command{
		checkpoint("svc", old, []byte("stale")),
		release("svc", old),
		complete("svc", old),
		fail("svc", old, "n1"),
		batch("svc", old),
		checkpoint("svc", cur+1, nil),
	} {
		wantErr(t, h.apply(cmd), ErrFenced)
	}
	if !bytes.Equal(before, h.fsm.encodeState()) {
		t.Fatal("stale commands changed the state")
	}
	if h.store.batches["svc"] != 0 || !bytes.Equal(h.store.checkpoints["svc"], []byte("old")) {
		t.Fatal("stale commands reached the store")
	}

	// after a release, the released incarnation can no longer write
	h.apply(release("svc", cur))
	wantErr(t, h.apply(checkpoint("svc", cur, nil)), ErrFenced)
	wantErr(t, h.apply(batch("svc", cur)), ErrFenced)

	wantErr(t, h.apply(checkpoint("nobody", 1, nil)), ErrUnknownRunnable)
}

func TestStoreBatch(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	res := h.apply(batch("svc", e))
	if res.Err != nil || len(res.Batch.GetResults()) != 1 {
		t.Fatalf("batch: %v %v", res.Batch, res.Err)
	}
	if h.store.batches["svc"] != 1 {
		t.Fatal("batch did not reach the store")
	}
}

func TestStoreFailureLeavesStateUnchanged(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	before := h.fsm.encodeState()
	h.store.fail = errors.New("disk full")
	wantErr(t, h.apply(checkpoint("svc", e, []byte("x"))), ErrStore)
	wantErr(t, h.apply(batch("svc", e)), ErrStore)
	if !bytes.Equal(before, h.fsm.encodeState()) {
		t.Fatal("a store failure changed the state")
	}
}

func TestFailuresAndReset(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	for range 3 {
		if res := h.apply(fail("svc", e, "n1")); res.Err != nil {
			t.Fatalf("fail: %v", res.Err)
		}
	}
	h.apply(fail("svc", e, "n0"))
	failures := h.runnable("svc").GetFailures()
	if len(failures) != 2 || failures[0].GetNode() != "n0" || failures[1].GetNode() != "n1" ||
		failures[1].GetConsecutive() != 3 || failures[1].GetLastReason() != "crash" || failures[1].GetLastAt() != 5000 {
		t.Fatalf("failures %v", failures)
	}

	// a checkpoint resets the current node's count only
	h.apply(checkpoint("svc", e, nil))
	failures = h.runnable("svc").GetFailures()
	if len(failures) != 1 || failures[0].GetNode() != "n0" {
		t.Fatalf("failures after a checkpoint on n1: %v", failures)
	}
	h.apply(reset("svc", false, false, true))
	if len(h.runnable("svc").GetFailures()) != 0 {
		t.Fatal("reset kept the failures")
	}

	// a release resets them too
	h.apply(fail("svc", e, "n1"))
	h.apply(release("svc", e))
	if len(h.runnable("svc").GetFailures()) != 0 {
		t.Fatal("release kept the failures")
	}

	h.apply(reset("svc", true, false, false))
	if h.runnable("svc").GetCheckpoint() != nil || h.store.checkpoints["svc"] != nil {
		t.Fatal("reset kept the checkpoint")
	}
	wantErr(t, h.apply(reset("nobody", true, true, true)), ErrUnknownRunnable)
}

func TestForget(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	wantErr(t, h.apply(forget("svc")), ErrRunning)
	h.apply(release("svc", e))
	if res := h.apply(forget("svc")); res.Err != nil {
		t.Fatalf("forget: %v", res.Err)
	}
	if _, ok := h.fsm.Runnable("svc"); ok {
		t.Fatal("runnable still there")
	}
	if len(h.store.forgotten) != 1 {
		t.Fatal("store not told")
	}
	wantErr(t, h.apply(forget("svc")), ErrUnknownRunnable)
	// a forgotten runnable starts again from a fresh state, epochs go on
	if e2 := h.incarnate("svc", "n1"); e2 != e+1 {
		t.Fatalf("epoch %d after forget, want %d", e2, e+1)
	}
}

func TestUnsupportedEntries(t *testing.T) {
	h := newHarness(t)
	h.incarnate("svc", "n1")
	before := h.fsm.encodeState()

	newer, err := proto.Marshal(&fsmpb.Command{FormatVersion: FormatVersion + 1, Command: incarnate("svc", "n2").Command})
	if err != nil {
		t.Fatal(err)
	}
	unversioned, err := proto.Marshal(&fsmpb.Command{Command: incarnate("svc", "n2").Command})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := Encode(&fsmpb.Command{})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"garbage":     {0xff, 0xff, 0xff},
		"newer":       newer,
		"unversioned": unversioned,
		"no command":  empty,
	} {
		wantErr(t, h.raw(data), ErrUnsupported)
		if !bytes.Equal(before, h.fsm.encodeState()) {
			t.Fatalf("%s entry changed the state", name)
		}
	}

	// entries that are not commands are not the FSM's business
	h.index++
	if res := h.fsm.Apply(&raft.Log{Index: h.index, Type: raft.LogNoop}); res != nil {
		t.Fatalf("no-op entry: %v", res)
	}
}

func setPeer(id, identity string) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_SetPeer{SetPeer: &fsmpb.SetPeer{Id: id, Identity: identity}}}
}

func removePeer(id string) *fsmpb.Command {
	return &fsmpb.Command{Command: &fsmpb.Command_RemovePeer{RemovePeer: &fsmpb.RemovePeer{Id: id}}}
}

func TestPeers(t *testing.T) {
	h := newHarness(t)
	for _, cmd := range []*fsmpb.Command{
		setPeer("n4", "spiffe://example.org/resurgo/n4"),
		setPeer("n5", "n5.example.org"),
		setPeer("n4", "spiffe://example.org/resurgo/n4-renewed"), // replaced
	} {
		if res := h.apply(cmd); res.Err != nil {
			t.Fatalf("%v: %v", cmd, res.Err)
		}
	}
	want := map[string]string{"n4": "spiffe://example.org/resurgo/n4-renewed", "n5": "n5.example.org"}
	if got := h.fsm.Peers(); len(got) != 2 || got["n4"] != want["n4"] || got["n5"] != want["n5"] {
		t.Fatalf("peers %v, want %v", got, want)
	}

	before := h.fsm.encodeState()
	wantErr(t, h.apply(setPeer("", "n6.example.org")), ErrInvalid)
	wantErr(t, h.apply(setPeer("n6", "http://n6")), ErrInvalid)
	// one identity, one node
	wantErr(t, h.apply(setPeer("n6", "n5.example.org")), ErrInvalid)
	wantErr(t, h.apply(removePeer("n9")), ErrUnknownPeer)
	if !bytes.Equal(before, h.fsm.encodeState()) {
		t.Fatal("refused peer commands changed the state")
	}

	if res := h.apply(removePeer("n5")); res.Err != nil {
		t.Fatalf("remove: %v", res.Err)
	}
	if _, ok := h.fsm.Peers()["n5"]; ok {
		t.Fatal("n5 still recorded")
	}

	// peers survive a snapshot
	other := newHarness(t)
	restore(t, other.fsm, snapshotBytes(t, h.fsm))
	if got := other.fsm.Peers(); len(got) != 1 || got["n4"] != want["n4"] {
		t.Fatalf("restored peers %v", got)
	}
}

// STO-17: an operator's incarnation fences the running one, also while the
// runnable is disabled or completed, and records the operator
func TestOperatorIncarnation(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	h.apply(setDisabled("svc", true))
	op := incarnate("svc", "n1")
	op.GetIncarnate().Operator = "uid=0@n1"
	op.GetIncarnate().BinarySha256 = nil
	res := h.apply(op)
	if res.Err != nil || res.Epoch != e+1 {
		t.Fatalf("operator incarnation: %+v", res)
	}
	r := h.runnable("svc")
	if r.GetCurrent().GetOperator() != "uid=0@n1" || !r.GetDisabled() {
		t.Fatalf("state %+v", r)
	}
	wantErr(t, h.apply(checkpoint("svc", e, []byte("x"))), ErrFenced)
	if res := h.apply(batch("svc", e+1)); res.Err != nil {
		t.Fatalf("the operator's batch: %v", res.Err)
	}
	// still disabled: the runnable itself may not incarnate
	wantErr(t, h.apply(incarnate("svc", "n1")), ErrDisabled)
}

// STO-16: resetting the store drops the database, not while running
func TestResetStore(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	h.apply(checkpoint("svc", e, []byte("x")))
	cmd := reset("svc", false, false, false)
	cmd.GetResetRunnable().Store = true
	wantErr(t, h.apply(cmd), ErrRunning)
	h.apply(release("svc", e))
	if res := h.apply(cmd); res.Err != nil {
		t.Fatalf("reset store: %v", res.Err)
	}
	if len(h.store.forgotten) != 1 || h.runnable("svc").GetCheckpoint() != nil {
		t.Fatalf("store not dropped: %v %+v", h.store.forgotten, h.runnable("svc"))
	}
}
