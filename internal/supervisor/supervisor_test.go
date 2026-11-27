package supervisor

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/fsm/fsmpb"
)

// AC-1, INV-2: one incarnation, on the leader; the followers run none
func TestOneIncarnationOnTheLeader(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{}, nil))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "running", func() bool { return len(c.tr.acting("ticker")) == 1 })
	time.Sleep(500 * time.Millisecond)
	acting := c.tr.acting("ticker")
	if len(acting) != 1 || !strings.HasPrefix(acting[0], l.id+"/") {
		t.Fatalf("acting %v, leader %s", acting, l.id)
	}
	for _, n := range c.others(l) {
		for _, v := range n.sup.Views() {
			if v.State == StateRunning {
				t.Fatalf("follower %s runs %v", n.id, v)
			}
		}
	}
	if v := l.sup.Views()[0]; v.State != StateRunning || !v.Healthy || !strings.HasPrefix(v.Detail, "v=") {
		t.Fatalf("leader's view %+v", v)
	}
}

// AC-2, AC-4, INV-4, INV-5: the isolated leader's incarnation ends within
// the lease and its grace; the new leader starts a newer epoch after the
// takeover delay, from the last acknowledged checkpoint
func TestFailoverAfterIsolation(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{}, func(s *Spec) { s.TakeoverDelay = time.Second }))
	old := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "checkpointing", func() bool {
		r := c.runnableState(old)
		return r.GetCheckpoint().GetSeq() >= 3
	})
	first := c.tr.startsOf("ticker")[0]

	isolated := time.Now()
	c.isolate(old)
	eventually(t, lease+stopGrace+time.Second, "the old incarnation stopped", func() bool { return len(c.tr.acting("ticker")) == 0 })
	if stopped := time.Since(isolated); stopped > lease+stopGrace+500*time.Millisecond {
		t.Fatalf("the isolated leader's incarnation ran %s after the isolation", stopped)
	}
	nl := c.leader(5*time.Second, c.others(old)...)
	eventually(t, 5*time.Second, "a successor", func() bool { return len(c.tr.startsOf("ticker")) == 2 })
	succ := c.tr.startsOf("ticker")[1]
	if succ.node != nl.id || succ.epoch <= first.epoch {
		t.Fatalf("successor %+v on leader %s after epoch %d", succ, nl.id, first.epoch)
	}
	if waited := succ.at.Sub(isolated); waited < time.Second {
		t.Fatalf("the successor started %s after the isolation: before the takeover delay", waited)
	}
	if succ.checkpoint == 0 {
		t.Fatal("the successor did not get the checkpoint")
	}
	c.heal()
}

// AC-3: a planned handover releases; the successor does not wait
func TestPlannedHandover(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{}, func(s *Spec) { s.TakeoverDelay = 3 * time.Second }))
	old := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "running", func() bool { return len(c.tr.acting("ticker")) == 1 })
	epoch := c.tr.startsOf("ticker")[0].epoch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := old.sup.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	handed := time.Now()
	c.leader(5*time.Second, c.others(old)...)
	if r := c.runnableState(old); r.GetCurrent().GetEpoch() != epoch || !r.GetReleased() {
		t.Fatal("the handed-over incarnation was not released")
	}
	eventually(t, 5*time.Second, "a successor", func() bool { return len(c.tr.startsOf("ticker")) == 2 })
	if waited := c.tr.startsOf("ticker")[1].at.Sub(handed); waited > 2*time.Second {
		t.Fatalf("the successor waited %s after a released predecessor", waited)
	}
}

// AC-7: failures restart with back-off, then move leadership; once every
// voter has failed, back-off continues
func TestFailuresHandOver(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{Mode: "fail"}, nil))
	first := c.leader(5 * time.Second)
	eventually(t, 20*time.Second, "every voter failing", func() bool {
		r := c.runnableState(first)
		if len(r.GetFailures()) < 3 {
			return false
		}
		for _, f := range r.GetFailures() {
			if f.GetConsecutive() < 3 {
				return false
			}
		}
		return true
	})
	leaders := map[string]bool{}
	for _, s := range c.tr.startsOf("ticker") {
		leaders[s.node] = true
	}
	if len(leaders) != 3 {
		t.Fatalf("the runnable ran on %v, want all three nodes", leaders)
	}
	// and it keeps trying, backing off, where it is
	n := len(c.tr.startsOf("ticker"))
	eventually(t, 5*time.Second, "another attempt", func() bool { return len(c.tr.startsOf("ticker")) > n })
}

// AC-9: an oversized checkpoint is refused; frequent ones are held back
func TestCheckpointLimits(t *testing.T) {
	c := newCluster(t, 1, testSpec(behaviour{}, func(s *Spec) { s.Checkpoint.MinInterval = 300 * time.Millisecond }))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "running", func() bool { return len(c.tr.acting("ticker")) == 1 })
	h := newHost(&controller{s: l.sup, spec: l.sup.o.Runnables[0]}, l.sup.Views()[0].Epoch)
	if _, err := h.Checkpoint(t.Context(), make([]byte, 2000)); err == nil || !strings.Contains(err.Error(), "max_size") {
		t.Fatalf("oversized checkpoint: %v", err)
	}
	if _, err := h.Checkpoint(t.Context(), binary.BigEndian.AppendUint64(nil, 1<<40)); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	began := time.Now()
	if _, err := h.Checkpoint(t.Context(), binary.BigEndian.AppendUint64(nil, 1<<41)); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if held := time.Since(began); held < 250*time.Millisecond {
		t.Fatalf("a checkpoint within min_interval was held back %s", held)
	}
}

// AC-11, INV-8: a completed task does not run again until reset
func TestTaskCompletes(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{Mode: "task"}, nil))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "completed", func() bool { return c.runnableState(l).GetCompleted() != nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.sup.Handover(ctx, ""); err != nil {
		t.Fatalf("handover: %v", err)
	}
	nl := c.leader(5*time.Second, c.others(l)...)
	time.Sleep(time.Second)
	if n := len(c.tr.startsOf("ticker")); n != 1 {
		t.Fatalf("the completed task ran %d times", n)
	}
	c.apply(nl, &fsmpb.Command{Command: &fsmpb.Command_ResetRunnable{ResetRunnable: &fsmpb.Reset{Runnable: "ticker", Completion: true}}})
	eventually(t, 5*time.Second, "run again after the reset", func() bool { return len(c.tr.startsOf("ticker")) == 2 })
}

func TestDisableAndEnable(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{}, nil))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "running", func() bool { return len(c.tr.acting("ticker")) == 1 })
	c.apply(l, &fsmpb.Command{Command: &fsmpb.Command_SetDisabled{SetDisabled: &fsmpb.SetDisabled{Runnable: "ticker", Disabled: true}}})
	eventually(t, 5*time.Second, "stopped", func() bool { return len(c.tr.acting("ticker")) == 0 })
	if r := c.runnableState(l); !r.GetReleased() {
		t.Fatal("a disabled runnable's incarnation was not released")
	}
	eventually(t, time.Second, "shown disabled", func() bool { return l.sup.Views()[0].State == StateDisabled })
	c.apply(l, &fsmpb.Command{Command: &fsmpb.Command_SetDisabled{SetDisabled: &fsmpb.SetDisabled{Runnable: "ticker", Disabled: false}}})
	eventually(t, 5*time.Second, "running again", func() bool { return len(c.tr.acting("ticker")) == 1 })
}

func TestUnhealthyFails(t *testing.T) {
	c := newCluster(t, 1, testSpec(behaviour{Mode: "unhealthy"}, func(s *Spec) { s.Restart.HandoverAfter = 0 }))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "a failure recorded", func() bool {
		f := c.runnableState(l).GetFailures()
		return len(f) == 1 && strings.Contains(f[0].GetLastReason(), "unhealthy")
	})
}

func TestRestart(t *testing.T) {
	c := newCluster(t, 1, testSpec(behaviour{}, nil))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "running", func() bool { return len(c.tr.acting("ticker")) == 1 })
	if err := l.sup.Restart(t.Context(), "ticker"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	eventually(t, 5*time.Second, "restarted", func() bool { return len(c.tr.startsOf("ticker")) == 2 })
}

// the store through the host: batches land in every node's database
func TestStoreThroughTheHost(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{Store: true}, nil))
	l := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "rows", func() bool {
		_, rows, err := l.store.Query(t.Context(), "ticker", "SELECT count(*) FROM ticks")
		return err == nil && rows[0][0].(int64) >= 5 //nolint:forcetypeassert // count(*)
	})
	for _, n := range c.others(l) {
		eventually(t, 5*time.Second, "replicated", func() bool {
			_, rows, err := n.store.Query(t.Context(), "ticker", "SELECT count(*) FROM ticks")
			return err == nil && rows[0][0].(int64) >= 5 //nolint:forcetypeassert // count(*)
		})
	}
}

// FR-13: a command during the takeover wait does not cut it short
func TestTakeoverDelayHoldsAgainstCommands(t *testing.T) {
	c := newCluster(t, 3, testSpec(behaviour{}, func(s *Spec) { s.TakeoverDelay = 1500 * time.Millisecond }))
	old := c.leader(5 * time.Second)
	eventually(t, 5*time.Second, "running", func() bool { return len(c.tr.acting("ticker")) == 1 })
	c.isolate(old)
	nl := c.leader(5*time.Second, c.others(old)...)
	became := time.Now()
	eventually(t, 5*time.Second, "waiting", func() bool { return nl.sup.Views()[0].State == StateWaiting })
	if err := nl.sup.Restart(t.Context(), "ticker"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	eventually(t, 5*time.Second, "a successor", func() bool { return len(c.tr.startsOf("ticker")) == 2 })
	if waited := c.tr.startsOf("ticker")[1].at.Sub(became); waited < 1200*time.Millisecond {
		t.Fatalf("the successor started %s after the election, before the takeover delay", waited)
	}
	c.heal()
}
