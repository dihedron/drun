package raftnode

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/fsm"
	"github.com/dihedron/drun/internal/fsm/fsmpb"
	"github.com/dihedron/drun/internal/store"
)

// the FSM, the SQL store and a 3-node cluster over TLS together: batches
// replicate (INV-S-1), a stale epoch's batch is refused (INV-S-2), and a
// restarted node applies nothing twice (INV-S-3, AC-S-3)
func TestStoreOnCluster(t *testing.T) { forEachLogStore(t, testStoreOnCluster) }

func testStoreOnCluster(t *testing.T, logStore string) {
	c := newTestCluster(t, 3, 3)
	c.logStore = logStore
	stores := map[string]*store.Store{}
	c.newStore = func(id string) fsm.Store {
		s, err := store.Open(filepath.Join(c.dirs[id], "data", "store"), func(err error) { t.Errorf("%s: fatal store error: %v", id, err) }, discard)
		if err != nil {
			t.Fatal(err)
		}
		stores[id] = s
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	for _, id := range c.ids {
		c.start(id)
	}
	l := c.leader(10 * time.Second)
	old := c.incarnate(l, "svc")
	epoch := c.incarnate(l, "svc")

	batch := func(epoch uint64, sql string) *fsm.Result {
		data, err := fsm.Encode(&fsmpb.Command{Command: &fsmpb.Command_StoreBatch{StoreBatch: &fsmpb.StoreBatch{
			Runnable: "svc", Epoch: epoch, At: time.Now().UnixMilli(), Statements: []*fsmpb.Statement{{Sql: sql}},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		f := l.Raft().Apply(data, 5*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("apply: %v", err)
		}
		return f.Response().(*fsm.Result) //nolint:forcetypeassert // the FSM's
	}
	if res := batch(epoch, "CREATE TABLE t (x INTEGER, at INTEGER DEFAULT (drun_now()))"); res.Err != nil || res.Batch.GetError() != nil {
		t.Fatalf("create: %v %v", res.Err, res.Batch.GetError())
	}
	for range 20 {
		if res := batch(epoch, "INSERT INTO t (x) VALUES (1)"); res.Err != nil || res.Batch.GetError() != nil {
			t.Fatalf("insert: %v %v", res.Err, res.Batch.GetError())
		}
	}
	if res := batch(old, "INSERT INTO t (x) VALUES (2)"); !errors.Is(res.Err, fsm.ErrFenced) {
		t.Fatalf("stale batch: %v", res.Err)
	}
	if err := l.Raft().Barrier(5 * time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	digests := func() map[string]string {
		out := map[string]string{}
		_, want, err := stores[l.cfg.Node.ID].Digest("svc")
		if err != nil {
			t.Fatalf("leader's digest: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for _, id := range c.ids {
			for {
				d, index, err := stores[id].Digest("svc")
				if err == nil && index == want || time.Now().After(deadline) {
					if err != nil {
						t.Fatalf("%s: digest: %v", id, err)
					}
					out[id] = d
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		return out
	}
	before := digests()
	for _, id := range c.ids {
		if before[id] != before[l.cfg.Node.ID] {
			t.Fatalf("digests differ: %v", before)
		}
	}

	// a restart replays the log after the last snapshot: nothing twice
	follower := c.others(l)[0]
	if err := c.nodes[follower].Raft().Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	batch(epoch, "INSERT INTO t (x) VALUES (3)")
	c.stop(follower)
	_ = stores[follower].Close()
	c.start(follower)
	time.Sleep(time.Second)
	if err := l.Raft().Barrier(5 * time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	after := digests()
	for _, id := range c.ids {
		if after[id] != after[l.cfg.Node.ID] {
			t.Fatalf("digests differ after the restart: %v", after)
		}
	}
	_, rows, err := stores[follower].Query(t.Context(), "svc", "SELECT count(*), sum(x) FROM t")
	if err != nil || rows[0][0] != int64(21) || rows[0][1] != int64(23) {
		t.Fatalf("rows on the restarted node: %v %v", rows, err)
	}
}
