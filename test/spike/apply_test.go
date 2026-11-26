package spike

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// EXT-1: ApplyFuture.Error() returns nil only after the entry is committed
// and applied on the leader; on leadership loss it returns an error, and the
// entry may or may not commit later.
func TestEXT1ApplyFutureOnLeadershipLoss(t *testing.T) {
	c := newCluster(t, 3, nil)

	// an isolated leader never acknowledges a write
	l := c.leader(5 * time.Second)
	c.isolate(l)
	if err := l.raft.Apply([]byte("isolated"), 2*time.Second).Error(); err == nil {
		t.Fatal("an isolated leader acknowledged a write")
	}
	c.heal()

	// a partition at a random moment around the write: whatever the outcome,
	// a nil error means the entry was applied on the leader and survives
	var acked, failedCommitted, failedLost int
	for i := range 2 * *repeat {
		l = c.leader(5 * time.Second)
		data := fmt.Appendf(nil, "entry-%d", i)
		f := l.raft.Apply(data, 2*time.Second)
		time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
		c.isolate(l)
		err := f.Error()
		if err == nil && !l.fsm.has(data) {
			t.Fatalf("%s: nil error before the entry was applied on the leader", data)
		}
		newLeader := c.leader(5*time.Second, c.others(l)...)
		if berr := newLeader.raft.Barrier(2 * time.Second).Error(); berr != nil {
			t.Fatalf("barrier on the new leader: %v", berr)
		}
		committed := newLeader.fsm.has(data)
		switch {
		case err == nil && !committed:
			t.Fatalf("%s: acknowledged but lost", data)
		case err == nil:
			acked++
		case committed:
			failedCommitted++
		default:
			failedLost++
		}
		c.heal()
		// let the old leader rejoin before the next round
		time.Sleep(2 * heartbeatTimeout)
	}
	t.Logf("acknowledged %d, failed but committed %d, failed and lost %d", acked, failedCommitted, failedLost)
}

// EXT-7: the raft.Log passed to FSM.Apply carries Term and Index.
func TestEXT7LogCarriesTermAndIndex(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	for i := range 5 {
		if err := l.raft.Apply(fmt.Appendf(nil, "%d", i), time.Second).Error(); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	term := l.raft.CurrentTerm()
	var last uint64
	for _, entry := range l.fsm.applied() {
		if entry.Index <= last || entry.Term == 0 || entry.Term > term {
			t.Fatalf("log index %d term %d after index %d, current term %d", entry.Index, entry.Term, last, term)
		}
		last = entry.Index
	}
}
