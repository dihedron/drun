package fsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// bufferSink is a raft.SnapshotSink in memory
type bufferSink struct {
	bytes.Buffer
	cancelled bool
}

func (s *bufferSink) ID() string    { return "test" }
func (s *bufferSink) Cancel() error { s.cancelled = true; return nil }
func (s *bufferSink) Close() error  { return nil }

func snapshotBytes(t testing.TB, f *FSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Release()
	var sink bufferSink
	if err := snap.Persist(&sink); err != nil {
		t.Fatalf("persist: %v", err)
	}
	return sink.Bytes()
}

func restore(t testing.TB, f *FSM, data []byte) {
	t.Helper()
	if err := f.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

var _ raft.FSM = (*FSM)(nil)

func TestSnapshotRoundTrip(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	h.apply(checkpoint("svc", e, []byte("state")))
	h.apply(fail("svc", e, "n2"))
	h.incarnate("task", "n1")
	h.apply(setDisabled("off", true))
	data := snapshotBytes(t, h.fsm)

	other := newHarness(t)
	other.incarnate("leftover", "n9") // replaced by the restore
	restore(t, other.fsm, data)
	if !bytes.Equal(data, snapshotBytes(t, other.fsm)) {
		t.Fatal("snapshot of the restored FSM differs")
	}
	if _, ok := other.fsm.Runnable("leftover"); ok {
		t.Fatal("restore kept a runnable that was not in the snapshot")
	}
	if !bytes.Equal(other.store.checkpoints["svc"], []byte("state")) {
		t.Fatal("store part not restored")
	}
	if other.fsm.EpochCounter() != 2 {
		t.Fatalf("epoch counter %d", other.fsm.EpochCounter())
	}
}

func TestRestoreRefusesBadSnapshots(t *testing.T) {
	h := newHarness(t)
	h.incarnate("svc", "n1")
	good := snapshotBytes(t, h.fsm)

	newer := bytes.Clone(good)
	binary.BigEndian.PutUint32(newer[len(snapshotMagic):], FormatVersion+1)
	for name, data := range map[string][]byte{
		"empty":     nil,
		"bad magic": append([]byte("XXXX"), good[4:]...),
		"newer":     newer,
		"truncated": good[:len(good)-3],
	} {
		other := newHarness(t)
		other.incarnate("kept", "n1")
		before := other.fsm.encodeState()
		if err := other.fsm.Restore(io.NopCloser(bytes.NewReader(data))); err == nil {
			t.Fatalf("%s snapshot restored", name)
		}
		if !bytes.Equal(before, other.fsm.encodeState()) {
			t.Fatalf("%s snapshot changed the state", name)
		}
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	h := newHarness(t)
	e := h.incarnate("svc", "n1")
	snap, err := h.fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	h.apply(checkpoint("svc", e, []byte("later"))) // after the snapshot was taken
	var sink bufferSink
	if err := snap.Persist(&sink); err != nil {
		t.Fatal(err)
	}
	other := newHarness(t)
	restore(t, other.fsm, sink.Bytes())
	if other.runnable("svc").GetCheckpoint() != nil {
		t.Fatal("snapshot saw a later apply")
	}
}
