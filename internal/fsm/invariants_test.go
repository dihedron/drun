package fsm

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

var runnables = []string{"a", "b", "c"}

// randomCommand returns a command for a random runnable; epoch-carrying
// commands use the current epoch most of the time, else a random stale or
// future one
func randomCommand(rng *rand.Rand, f *FSM) *fsmpb.Command {
	name := runnables[rng.IntN(len(runnables))]
	epoch := uint64(0)
	if r, ok := f.Runnable(name); ok {
		epoch = r.GetCurrent().GetEpoch()
	}
	if rng.IntN(10) < 3 {
		epoch = rng.Uint64N(f.EpochCounter() + 2)
	}
	node := []string{"n1", "n2", "n3"}[rng.IntN(3)]
	switch rng.IntN(12) {
	case 0, 1, 2:
		return incarnate(name, node)
	case 3, 4:
		return checkpoint(name, epoch, []byte{byte(rng.IntN(256))})
	case 5:
		return release(name, epoch)
	case 6:
		return complete(name, epoch)
	case 7:
		return fail(name, epoch, node)
	case 8:
		return batch(name, epoch)
	case 9:
		return setDisabled(name, rng.IntN(3) == 0)
	case 10:
		return reset(name, rng.IntN(2) == 0, rng.IntN(2) == 0, rng.IntN(2) == 0)
	default:
		return forget(name)
	}
}

// fenced reports whether cmd carries an epoch that must be current
func fenced(cmd *fsmpb.Command) (string, uint64, bool) {
	switch c := cmd.GetCommand().(type) {
	case *fsmpb.Command_Checkpoint:
		return c.Checkpoint.GetRunnable(), c.Checkpoint.GetEpoch(), true
	case *fsmpb.Command_Release:
		return c.Release.GetRunnable(), c.Release.GetEpoch(), true
	case *fsmpb.Command_Complete:
		return c.Complete.GetRunnable(), c.Complete.GetEpoch(), true
	case *fsmpb.Command_Fail:
		return c.Fail.GetRunnable(), c.Fail.GetEpoch(), true
	case *fsmpb.Command_StoreBatch:
		return c.StoreBatch.GetRunnable(), c.StoreBatch.GetEpoch(), true
	}
	return "", 0, false
}

// checkInvariants applies cmd and checks INV-1 and INV-3 on the way
func checkInvariants(t testing.TB, h *harness, cmd *fsmpb.Command, lastEpoch *uint64) {
	t.Helper()
	before := h.fsm.encodeState()
	current := uint64(0)
	name, epoch, isFenced := fenced(cmd)
	if r, ok := h.fsm.Runnable(name); ok {
		current = r.GetCurrent().GetEpoch()
	}
	res := h.apply(cmd)

	// INV-1: epochs are unique and strictly increasing in commit order
	if _, ok := cmd.GetCommand().(*fsmpb.Command_Incarnate); ok && res.Err == nil {
		if res.Epoch <= *lastEpoch {
			t.Fatalf("epoch %d after %d", res.Epoch, *lastEpoch)
		}
		*lastEpoch = res.Epoch
	}
	if h.fsm.EpochCounter() != *lastEpoch {
		t.Fatalf("epoch counter %d, last epoch handed out %d", h.fsm.EpochCounter(), *lastEpoch)
	}

	// INV-3: a command with a stale epoch changes nothing
	if isFenced && (epoch != current || current == 0) {
		if res.Err == nil {
			t.Fatalf("command with epoch %d accepted, current %d: %v", epoch, current, cmd)
		}
		if !bytes.Equal(before, h.fsm.encodeState()) {
			t.Fatalf("command with epoch %d changed the state, current %d: %v", epoch, current, cmd)
		}
	}
	// a refused command never changes the state
	if res.Err != nil && !bytes.Equal(before, h.fsm.encodeState()) {
		t.Fatalf("refused command (%v) changed the state: %v", res.Err, cmd)
	}
}

// INV-1, INV-3 and INV-7 over random command sequences: two FSMs that
// apply the same entries hold byte-identical snapshots
func TestInvariantsOnRandomSequences(t *testing.T) {
	for seed := range uint64(50) {
		rng := rand.New(rand.NewPCG(seed, seed^0x5eed)) //nolint:gosec // reproducible test sequences
		a, b := newHarness(t), newHarness(t)
		var lastEpoch uint64
		for step := range 400 {
			cmd := randomCommand(rng, a.fsm)
			checkInvariants(t, a, cmd, &lastEpoch)
			resB := b.apply(cmd)
			_ = resB
			if step%50 == 49 {
				if !bytes.Equal(snapshotBytes(t, a.fsm), snapshotBytes(t, b.fsm)) {
					t.Fatalf("seed %d step %d: snapshots differ", seed, step)
				}
			}
		}
		if lastEpoch == 0 {
			t.Fatalf("seed %d: no incarnation in 400 commands", seed)
		}
	}
}

// INV-7 across a restore: an FSM restored from a snapshot and fed the
// rest of the log ends identical to one that applied all of it
func TestRestoreThenApplyMatchesFullApply(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // reproducible test sequences
	full := newHarness(t)
	var cmds []*fsmpb.Command
	for range 300 {
		cmd := randomCommand(rng, full.fsm)
		cmds = append(cmds, cmd)
		full.apply(cmd)
	}

	partial := newHarness(t)
	for _, cmd := range cmds[:150] {
		partial.apply(cmd)
	}
	restored := newHarness(t)
	restore(t, restored.fsm, snapshotBytes(t, partial.fsm))
	restored.index = partial.index
	for _, cmd := range cmds[150:] {
		restored.apply(cmd)
	}
	if !bytes.Equal(snapshotBytes(t, full.fsm), snapshotBytes(t, restored.fsm)) {
		t.Fatal("restored FSM differs from the one that applied the whole log")
	}
}

func TestRefusalsAreErrors(t *testing.T) {
	h := newHarness(t)
	res := h.apply(checkpoint("x", 1, nil))
	if res.Err == nil || !errors.Is(res.Err, ErrUnknownRunnable) {
		t.Fatalf("got %v", res.Err)
	}
}
