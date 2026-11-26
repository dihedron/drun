package supervisor

import (
	"context"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/dihedron/resurgo/pkg/runnable"
)

// TestStress runs a cluster under random faults for RESURGO_STRESS (a
// duration: "make stress", "make soak"): partitions and heals of random
// nodes, planned handovers, and a runnable that fails at random. The
// tracker checks throughout that no two incarnations act at once, that
// epochs increase and that no acknowledged checkpoint is lost.
func TestStress(t *testing.T) {
	d, err := time.ParseDuration(os.Getenv("RESURGO_STRESS"))
	if err != nil {
		t.Skip("set RESURGO_STRESS to a duration to run")
	}
	c := newCluster(t, 3, testSpec(behaviour{Mode: "flaky", Store: true}, func(s *Spec) { s.Restart.HandoverAfter = 4 }))
	c.leader(5 * time.Second)
	deadline := time.Now().Add(d)
	report := time.Now().Add(time.Minute)
	var faults int
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(200+rand.IntN(800)) * time.Millisecond) //nolint:gosec // test faults
		switch rand.IntN(3) {                                            //nolint:gosec // test faults
		case 0:
			n := c.nodes[rand.IntN(len(c.nodes))] //nolint:gosec // test faults
			c.isolate(n)
			time.Sleep(time.Duration(500+rand.IntN(1500)) * time.Millisecond) //nolint:gosec // test faults
			c.heal()
		case 1:
			for _, n := range c.nodes {
				if n.raft.State().String() == "Leader" {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = n.sup.Handover(ctx, "")
					cancel()
				}
			}
		default:
			// let it run
		}
		faults++
		c.tr.check(t)
		if t.Failed() {
			return
		}
		if time.Now().After(report) {
			t.Logf("%s left: %d faults, %d incarnations", time.Until(deadline).Round(time.Second), faults, len(c.tr.startsOf("ticker")))
			report = time.Now().Add(time.Minute)
		}
	}
	c.heal()
	t.Logf("%d faults, %d incarnations", faults, len(c.tr.startsOf("ticker")))
}

// the tracker itself sees what it must
func TestTrackerSeesViolations(t *testing.T) {
	tr := newTracker()
	a := runnable.Incarnation{Runnable: "r", Node: "n1", Epoch: 2}
	b := runnable.Incarnation{Runnable: "r", Node: "n2", Epoch: 1, Checkpoint: &runnable.Checkpoint{Data: make([]byte, 8)}}
	tr.begin(a)
	tr.ack("r", 5)
	tr.begin(b) // overlaps, older epoch, lost checkpoint
	if len(tr.violations) != 3 {
		t.Fatalf("violations %v, want overlap, epoch and checkpoint", tr.violations)
	}
}
