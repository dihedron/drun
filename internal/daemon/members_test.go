package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func health(t *testing.T, c *tcluster, from string) map[string]*adminpb.MemberHealth {
	t.Helper()
	r, err := c.client(from).MemberShow(ctx(t), &adminpb.MemberShowRequest{})
	if err != nil {
		t.Fatalf("member show: %v", err)
	}
	out := map[string]*adminpb.MemberHealth{}
	for _, m := range r.GetMembers() {
		out[m.GetId()] = m
	}
	return out
}

func wantCode(t *testing.T, what string, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("%s: %v, want %s", what, err, code)
	}
}

// FR-17: health, gated promotion, demotion; through a follower
func TestMemberPromotion(t *testing.T) {
	c := newTCluster(t, 4)
	initial := []string{"node-0", "node-1", "node-2"}
	c.voters = map[string][]string{}
	for _, id := range c.ids {
		c.voters[id] = initial
	}
	c.startAll()
	l := c.leader()
	f := c.follower(l)
	cl := c.client(f)

	if _, err := cl.MemberAdd(ctx(t), &adminpb.MemberAddRequest{Id: "node-3", Address: c.addrs["node-3"], NonVoter: true}); err != nil {
		t.Fatalf("member add: %v", err)
	}
	eventually(t, 10*time.Second, "node-3 healthy", func() bool {
		h := health(t, c, f)["node-3"]
		return h.GetHealthy() && h.GetSuffrage() == "Nonvoter"
	})
	for _, id := range initial {
		if h := health(t, c, f)[id]; !h.GetHealthy() || h.GetSuffrage() != "Voter" || h.GetLeader() != (id == l) {
			t.Fatalf("%s: %+v", id, h)
		}
	}

	// promoted only after --stable of healthy checks
	start := time.Now()
	if _, err := cl.MemberPromote(ctx(t), &adminpb.MemberPromoteRequest{Id: "node-3", Gate: &adminpb.Gate{StableMs: 1500}}); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if took := time.Since(start); took < 1500*time.Millisecond {
		t.Fatalf("promoted after %s, before --stable", took)
	}
	if h := health(t, c, f)["node-3"]; h.GetSuffrage() != "Voter" {
		t.Fatalf("after the promotion: %+v", h)
	}
	_, err := cl.MemberPromote(ctx(t), &adminpb.MemberPromoteRequest{Id: "node-3"})
	wantCode(t, "promoting a voter", err, codes.FailedPrecondition)
	_, err = cl.MemberPromote(ctx(t), &adminpb.MemberPromoteRequest{Id: "node-9"})
	wantCode(t, "promoting a stranger", err, codes.NotFound)

	if _, err := cl.MemberDemote(ctx(t), &adminpb.MemberDemoteRequest{Id: "node-3"}); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if h := health(t, c, f)["node-3"]; h.GetSuffrage() != "Nonvoter" {
		t.Fatalf("after the demotion: %+v", h)
	}
	_, err = cl.MemberDemote(ctx(t), &adminpb.MemberDemoteRequest{Id: "node-3"})
	wantCode(t, "demoting a non-voter", err, codes.FailedPrecondition)

	// a stopped non-voter is unhealthy, and not promoted before the deadline
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = c.nodes["node-3"].Stop(stopCtx)
	cancel()
	delete(c.nodes, "node-3")
	if h := health(t, c, f)["node-3"]; h.GetHealthy() || h.GetReason() == "" {
		t.Fatalf("a stopped member: %+v", h)
	}
	short, cancel := context.WithTimeout(ctx(t), 3*time.Second)
	defer cancel()
	_, err = cl.MemberPromote(short, &adminpb.MemberPromoteRequest{Id: "node-3"})
	wantCode(t, "promoting a stopped member", err, codes.DeadlineExceeded)
	if h := health(t, c, f)["node-3"]; h.GetSuffrage() != "Nonvoter" {
		t.Fatalf("after a failed promotion: %+v", h)
	}
	// --force skips the checks
	if _, err := cl.MemberPromote(ctx(t), &adminpb.MemberPromoteRequest{Id: "node-3", Gate: &adminpb.Gate{Force: true}}); err != nil {
		t.Fatalf("forced promotion: %v", err)
	}
	if h := health(t, c, f)["node-3"]; h.GetSuffrage() != "Voter" {
		t.Fatalf("after a forced promotion: %+v", h)
	}

	// demoting the leader hands leadership over first
	if _, err := cl.MemberDemote(ctx(t), &adminpb.MemberDemoteRequest{Id: l}); err != nil {
		t.Fatalf("demoting the leader: %v", err)
	}
	nl := c.leader()
	if nl == l {
		t.Fatal("the demoted leader is still leader")
	}
	if h := health(t, c, nl)[l]; h.GetSuffrage() != "Nonvoter" {
		t.Fatalf("the demoted leader: %+v", h)
	}
}

// FR-17: member add of a new voter joins as a non-voter, promoted once
// healthy; of a voter that cannot become healthy, it stays a non-voter
func TestMemberAddGated(t *testing.T) {
	c := newTCluster(t, 5)
	initial := []string{"node-0", "node-1", "node-2"}
	c.voters = map[string][]string{}
	for _, id := range c.ids {
		c.voters[id] = initial
	}
	for _, id := range append(initial, "node-3") {
		c.start(id)
	}
	l := c.leader()
	cl := c.client(l)
	if _, err := cl.MemberAdd(ctx(t), &adminpb.MemberAddRequest{Id: "node-3", Address: c.addrs["node-3"], Gate: &adminpb.Gate{StableMs: 500}}); err != nil {
		t.Fatalf("member add: %v", err)
	}
	if h := health(t, c, l)["node-3"]; h.GetSuffrage() != "Voter" || !h.GetHealthy() {
		t.Fatalf("added: %+v", h)
	}
	// node-4 is not running: it never becomes healthy
	short, cancel := context.WithTimeout(ctx(t), 3*time.Second)
	defer cancel()
	_, err := cl.MemberAdd(short, &adminpb.MemberAddRequest{Id: "node-4", Address: c.addrs["node-4"]})
	wantCode(t, "adding a member that is down", err, codes.DeadlineExceeded)
	if h := health(t, c, l)["node-4"]; h.GetSuffrage() != "Nonvoter" || h.GetHealthy() {
		t.Fatalf("a member that is down: %+v", h)
	}
}

func TestDemoteTheLastVoter(t *testing.T) {
	c := newTCluster(t, 1)
	c.start("node-0")
	c.leader()
	_, err := c.client("node-0").MemberDemote(ctx(t), &adminpb.MemberDemoteRequest{Id: "node-0"})
	wantCode(t, "demoting the last voter", err, codes.FailedPrecondition)
}
