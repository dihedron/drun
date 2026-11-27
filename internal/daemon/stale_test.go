package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
	"github.com/dihedron/drun/internal/supervisor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// STO-18: bounded stale reads on any member
func TestBoundedStaleReads(t *testing.T) {
	c := newTCluster(t, 3)
	c.startAll()
	l := c.leader()
	f := c.follower(l)
	eventually(t, 5*time.Second, "running", func() bool { return c.view(f).GetView().GetState() == supervisor.StateRunning })
	if _, err := c.client(l).StoreExec(ctx(t), &adminpb.StoreExecRequest{Name: "ticker", Sql: "CREATE TABLE t (x);\nINSERT INTO t VALUES (1);"}); err != nil {
		t.Fatal(err)
	}
	q := &adminpb.StoreQueryRequest{Name: "ticker", Sql: "SELECT count(*) FROM t", MaxStalenessMs: 5000}

	// the leader: not behind
	r, err := c.client(l).StoreQuery(ctx(t), q)
	if err != nil || r.GetNode() != l || !r.GetStalenessKnown() || r.GetStalenessMs() != 0 || r.GetIndex() == 0 {
		t.Fatalf("on the leader: %v %+v", err, r)
	}
	// a follower in touch: answered there, with its index and lag
	fc := c.client(f)
	eventually(t, 5*time.Second, "a bounded read on a follower", func() bool {
		r, err := fc.StoreQuery(ctx(t), q)
		return err == nil && r.GetNode() == f && r.GetStale() && r.GetStalenessKnown() &&
			r.GetStalenessMs() <= 5000 && r.GetIndex() > 0 && r.GetRows()[0].GetValues()[0].GetInteger() == 1
	})
	// a follower cut off from the leader: refused once beyond the bound
	for id := range c.nodes {
		if id != f {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.nodes[id].Stop(stopCtx)
			cancel()
			delete(c.nodes, id)
		}
	}
	tight := &adminpb.StoreQueryRequest{Name: "ticker", Sql: "SELECT count(*) FROM t", MaxStalenessMs: 500}
	eventually(t, 10*time.Second, "refused beyond the bound", func() bool {
		_, err := fc.StoreQuery(ctx(t), tight)
		return status.Code(err) == codes.FailedPrecondition
	})
	// without a bound, still answered, flagged
	if r, err := fc.StoreQuery(ctx(t), &adminpb.StoreQueryRequest{Name: "ticker", Sql: "SELECT count(*) FROM t", Local: true}); err != nil || !r.GetStale() {
		t.Fatalf("an unbounded local read: %v %+v", err, r)
	}
}
