package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
)

// stopAndWipe stops a node and deletes its data, as a lost disk would
func (c *tcluster) stopAndWipe(id string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.nodes[id].Stop(ctx)
	delete(c.nodes, id)
	if err := os.RemoveAll(filepath.Join(c.dirs[id], "data")); err != nil {
		c.t.Fatal(err)
	}
}

// caughtUp waits until a node has the leader's epochs and checkpoints
func (c *tcluster) caughtUp(d *Daemon) {
	c.t.Helper()
	eventually(c.t, 10*time.Second, "caught up", func() bool {
		l := c.nodes[c.leader()]
		r, _ := d.FSM.Runnable("ticker")
		return r.GetCheckpoint().GetSeq() > 5 && d.FSM.EpochCounter() == l.FSM.EpochCounter()
	})
}

func (c *tcluster) checkpointing() string {
	c.t.Helper()
	l := c.leader()
	eventually(c.t, 5*time.Second, "checkpoints", func() bool {
		r, _ := c.nodes[l].FSM.Runnable("ticker")
		return r.GetCheckpoint().GetSeq() > 5
	})
	return l
}

// FR-1: the first node bootstraps; the others find it and wait for it
func TestOnlyTheFirstBootstraps(t *testing.T) {
	c := newTCluster(t, 3)
	c.startAll()
	c.checkpointing()
	if !c.nodes["node-0"].Node.Bootstrapped() || c.nodes["node-1"].Node.Bootstrapped() || c.nodes["node-2"].Node.Bootstrapped() {
		t.Fatal("a node bootstrapped after another had")
	}
}

// a wiped initial voter waits for the leader and catches up
func TestRebuildAVoter(t *testing.T) {
	for _, removeFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "wipe-restart", true: "remove-wipe-add"}[removeFirst], func(t *testing.T) {
			c := newTCluster(t, 3)
			c.startAll()
			l := c.checkpointing()
			victim := c.follower(l)
			if removeFirst {
				if _, err := c.client(l).MemberRemove(ctx(t), &adminpb.MemberRemoveRequest{Id: victim}); err != nil {
					t.Fatal(err)
				}
			}
			c.stopAndWipe(victim)
			d := c.start(victim)
			if d.Node.Bootstrapped() {
				t.Fatal("the rebuilt node bootstrapped")
			}
			if removeFirst {
				if _, err := c.client(l).MemberAdd(ctx(t), &adminpb.MemberAddRequest{Id: victim, Address: c.addrs[victim]}); err != nil {
					t.Fatal(err)
				}
			}
			c.caughtUp(d)
			// re-added through the promotion gate (FR-17): a voter again
			if h := health(t, c, l)[victim]; h.GetSuffrage() != "Voter" {
				t.Fatalf("the rebuilt member: %+v", h)
			}
		})
	}
}

// both followers wiped at once, the leader up
func TestRebuildTwoFollowers(t *testing.T) { forEachLogStore(t, testRebuildTwoFollowers) }

func testRebuildTwoFollowers(t *testing.T, logStore string) {
	c := newTCluster(t, 3)
	c.logStore = logStore
	c.startAll()
	l := c.checkpointing()
	var rebuilt []string
	for _, id := range c.ids {
		if id != l {
			c.stopAndWipe(id)
			rebuilt = append(rebuilt, id)
		}
	}
	for _, id := range rebuilt {
		if d := c.start(id); d.Node.Bootstrapped() {
			t.Fatalf("%s bootstrapped", id)
		}
	}
	for _, id := range rebuilt {
		c.caughtUp(c.nodes[id])
	}
}

// a rebuilt node whose cluster.voters changed since the cluster was
// created must not bootstrap a different first entry
func TestRebuildWithEditedVoters(t *testing.T) { forEachLogStore(t, testRebuildWithEditedVoters) }

func testRebuildWithEditedVoters(t *testing.T, logStore string) {
	c := newTCluster(t, 3)
	c.logStore = logStore
	c.startAll()
	l := c.checkpointing()
	victim := c.follower(l)
	c.stopAndWipe(victim)
	c.voters = map[string][]string{victim: {victim, l}}
	d := c.start(victim)
	if d.Node.Bootstrapped() {
		t.Fatal("the rebuilt node bootstrapped with its edited voters")
	}
	c.caughtUp(d)
}
