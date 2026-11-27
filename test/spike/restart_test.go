package spike

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// EXT-S-4: on restart hashicorp/raft restores the latest snapshot and then
// applies again the entries after it, although the FSM applied them before
// the restart: the store must skip what it has already applied (STO-3).
// Runs on the production stores: raft-boltdb/v2 and file snapshots.
func TestEXTS4RestartReappliesEntriesAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	start := func(fsm *recordingFSM) (*raft.Raft, *raftboltdb.BoltStore) {
		store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
		if err != nil {
			t.Fatalf("bolt store: %v", err)
		}
		snaps, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
		if err != nil {
			t.Fatalf("snapshot store: %v", err)
		}
		_, trans := raft.NewInmemTransport("solo")
		r, err := raft.NewRaft(testConfig("solo"), fsm, store, store, snaps, trans)
		if err != nil {
			t.Fatalf("raft: %v", err)
		}
		return r, store
	}
	waitLeader := func(r *raft.Raft) {
		deadline := time.Now().Add(5 * time.Second)
		for r.State() != raft.Leader {
			if time.Now().After(deadline) {
				t.Fatal("no leader")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	apply := func(r *raft.Raft, from, to int) {
		for i := from; i < to; i++ {
			if err := r.Apply(fmt.Appendf(nil, "entry-%d", i), time.Second).Error(); err != nil {
				t.Fatalf("apply %d: %v", i, err)
			}
		}
	}

	first := &recordingFSM{}
	r, store := start(first)
	cfg := raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: "solo", Address: "solo"}}}
	if err := r.BootstrapCluster(cfg).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	waitLeader(r)
	apply(r, 0, 10)
	snap := r.Snapshot()
	if err := snap.Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	meta, rc, err := snap.Open()
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	_ = rc.Close()
	apply(r, 10, 15)
	appliedBefore := first.applied()
	if err := r.Shutdown().Error(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	second := &recordingFSM{}
	r, store = start(second)
	defer store.Close() //nolint:errcheck // test teardown
	defer r.Shutdown()
	waitLeader(r)
	if err := r.Barrier(time.Second).Error(); err != nil {
		t.Fatalf("barrier: %v", err)
	}

	if second.restores != 1 {
		t.Fatalf("restores = %d, want 1", second.restores)
	}
	var reapplied int
	for _, before := range appliedBefore {
		if before.Index > meta.Index && second.has(before.Data) {
			reapplied++
		}
	}
	if reapplied != 5 {
		t.Fatalf("%d of the 5 entries after snapshot index %d applied again, want 5", reapplied, meta.Index)
	}
	for _, l := range second.applied() {
		if l.Index <= meta.Index {
			t.Fatalf("entry %d at or before the snapshot index %d applied again", l.Index, meta.Index)
		}
	}
}
