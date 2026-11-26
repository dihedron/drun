package fsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

// FuzzApply feeds arbitrary entries after a fixed history: Apply never
// panics, and a refused entry leaves the state unchanged
func FuzzApply(f *testing.F) {
	for _, cmd := range []*fsmpb.Command{
		incarnate("svc", "n1"), checkpoint("svc", 1, []byte("x")), release("svc", 1),
		complete("svc", 2), fail("svc", 2, "n1"), batch("svc", 2),
		setDisabled("svc", true), reset("svc", true, true, true), forget("svc"),
	} {
		data, err := Encode(cmd)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		h := newHarness(t)
		e := h.incarnate("svc", "n1")
		h.apply(checkpoint("svc", e, []byte("cp")))
		h.incarnate("svc", "n2")
		var lastEpoch = h.fsm.EpochCounter()
		before := h.fsm.encodeState()
		res := h.raw(data)
		if res.Err != nil && !bytes.Equal(before, h.fsm.encodeState()) {
			t.Fatalf("refused entry (%v) changed the state", res.Err)
		}
		if h.fsm.EpochCounter() < lastEpoch {
			t.Fatal("epoch counter went back")
		}
	})
}

// FuzzRestore feeds arbitrary snapshots: Restore never panics, and a
// refused snapshot leaves the state unchanged
func FuzzRestore(f *testing.F) {
	h := newHarness(f)
	e := h.incarnate("svc", "n1")
	h.apply(checkpoint("svc", e, []byte("cp")))
	f.Add(snapshotBytes(f, h.fsm))
	f.Add([]byte(snapshotMagic))
	f.Fuzz(func(t *testing.T, data []byte) {
		other := newHarness(t)
		other.incarnate("kept", "n1")
		before := other.fsm.encodeState()
		if err := other.fsm.Restore(io.NopCloser(bytes.NewReader(data))); err != nil &&
			!bytes.Equal(before, other.fsm.encodeState()) {
			t.Fatalf("refused snapshot (%v) changed the state", err)
		}
	})
}
