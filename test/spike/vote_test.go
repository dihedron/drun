package spike

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// EXT-4: pre-vote keeps a rejoining partitioned node from causing an
// election. The control with pre-vote disabled shows that the test can see
// the disruption.
func TestEXT4PreVoteStopsDisruptiveRejoin(t *testing.T) {
	for _, tc := range []struct {
		name     string
		preVote  bool
		disrupts bool
	}{
		{"pre-vote", true, false},
		{"control-without-pre-vote", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, func(cfg *raft.Config) { cfg.PreVoteDisabled = !tc.preVote })
			l := c.leader(5 * time.Second)
			term := l.raft.CurrentTerm()
			f := c.others(l)[0]

			c.isolate(f)
			time.Sleep(10 * electionTimeout) // f times out and campaigns, alone
			c.heal()
			time.Sleep(10 * heartbeatTimeout)

			after := c.leader(5 * time.Second)
			disrupted := after.raft.CurrentTerm() != term || after != l
			if disrupted != tc.disrupts {
				t.Fatalf("disrupted = %v (term %d to %d, leader %s to %s), want %v",
					disrupted, term, after.raft.CurrentTerm(), l.id, after.id, tc.disrupts)
			}
		})
	}
}

// EXT-9: a follower that knows a leader refuses to vote for another
// candidate, except for a leadership transfer started by that leader.
func TestEXT9FollowerRefusesVoteWhileLeaderKnown(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	if err := l.raft.Barrier(time.Second).Error(); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	others := c.others(l)
	voter, candidate := others[0], others[1]
	lastIndex := l.raft.LastIndex()

	// the forced candidate: a vote request in the candidate's name, with a
	// higher term and an up-to-date log, sent from a transport of its own
	_, forced := raft.NewInmemTransport("forced")
	forced.Connect(voter.addr, voter.trans)
	request := func(transfer bool) bool {
		req := &raft.RequestVoteRequest{
			RPCHeader:          raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte(candidate.id), Addr: []byte(candidate.addr)},
			Term:               voter.raft.CurrentTerm() + 1,
			LastLogIndex:       lastIndex + 1000,
			LastLogTerm:        voter.raft.CurrentTerm(),
			LeadershipTransfer: transfer,
		}
		var resp raft.RequestVoteResponse
		if err := forced.RequestVote(voter.id, voter.addr, req, &resp); err != nil {
			t.Fatalf("vote request: %v", err)
		}
		return resp.Granted
	}

	if request(false) {
		t.Fatal("a follower with a known leader granted its vote")
	}
	if !request(true) {
		t.Fatal("a follower refused a leadership-transfer vote")
	}
}
