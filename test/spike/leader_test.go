package spike

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// EXT-3: a leader that cannot reach a quorum steps down within about
// LeaderLeaseTimeout and reports it on LeaderCh.
func TestEXT3IsolatedLeaderStepsDown(t *testing.T) {
	var worst time.Duration
	for range *repeat {
		c := newCluster(t, 3, nil)
		l := c.leader(5 * time.Second)
		ch := l.raft.LeaderCh()

		start := time.Now()
		c.isolate(l)
		select {
		case isLeader := <-waitFalse(ch):
			if isLeader {
				t.Fatal("LeaderCh delivered true")
			}
		case <-time.After(10 * leaderLeaseTimeout):
			t.Fatalf("no step-down on LeaderCh within %s", 10*leaderLeaseTimeout)
		}
		worst = max(worst, time.Since(start))
		c.shutdown()
	}
	// the lease is checked every LeaderLeaseTimeout: the worst case is about
	// twice that, plus scheduling
	if bound := 2*leaderLeaseTimeout + 50*time.Millisecond; worst > bound {
		t.Fatalf("step-down took %s, more than %s", worst, bound)
	}
	t.Logf("worst step-down %s with LeaderLeaseTimeout %s", worst, leaderLeaseTimeout)
}

// waitFalse forwards the first false from ch: LeaderCh may still hold the
// true of the election
func waitFalse(ch <-chan bool) <-chan bool {
	out := make(chan bool, 1)
	go func() {
		for v := range ch {
			if !v {
				out <- v
				return
			}
		}
	}()
	return out
}

// EXT-8: VerifyLeader fails once a newer leader exists or no quorum answers.
func TestEXT8VerifyLeaderFails(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	if err := l.raft.VerifyLeader().Error(); err != nil {
		t.Fatalf("VerifyLeader on a healthy leader: %v", err)
	}

	// no quorum answers: checked at once, before the lease makes it step down
	c.isolate(l)
	if err := l.raft.VerifyLeader().Error(); err == nil {
		t.Fatal("VerifyLeader succeeded on an isolated leader")
	}

	// a newer leader exists, and the old one hears of it on healing
	newLeader := c.leader(5*time.Second, c.others(l)...)
	c.heal()
	time.Sleep(2 * heartbeatTimeout)
	if l.raft.State() == raft.Leader {
		t.Fatalf("old leader %s still leader next to %s", l.id, newLeader.id)
	}
	if err := l.raft.VerifyLeader().Error(); err == nil {
		t.Fatal("VerifyLeader succeeded on a deposed leader")
	}
}
