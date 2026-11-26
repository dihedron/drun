package spike

import (
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// sqlFSM applies every entry as one SQL batch in a transaction
type sqlFSM struct{ db *sql.DB }

func (f *sqlFSM) Apply(l *raft.Log) any {
	tx, err := f.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(string(l.Data)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (f *sqlFSM) Snapshot() (raft.FSMSnapshot, error) { return &bytesSnapshot{}, nil }
func (f *sqlFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

// EXT-S-5 (local part): batches of 10 small rows through a 3-node in-memory
// cluster, each node applying them to its own SQLite file. The thresholds
// (500/s on the LAN, 25/s sequential and 300/s concurrent on the WAN
// profile) are checked in the lab (AC-16); this measures the node itself.
func BenchmarkEXTS5Batches(b *testing.B) {
	c := newNodes(b, 3)
	for _, n := range c.nodes {
		db, err := sql.Open("sqlite3", filepath.Join(b.TempDir(), "store.db")+"?_journal_mode=WAL&_synchronous=NORMAL")
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		b.Cleanup(func() { _ = db.Close() })
		db.SetMaxOpenConns(1)
		if _, err := db.Exec("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)"); err != nil {
			b.Fatalf("schema: %v", err)
		}
		r, err := raft.NewRaft(testConfig(n.id), &sqlFSM{db: db}, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), n.trans)
		if err != nil {
			b.Fatalf("raft: %v", err)
		}
		n.raft = r
	}
	if err := c.nodes[0].raft.BootstrapCluster(c.configuration()).Error(); err != nil {
		b.Fatalf("bootstrap: %v", err)
	}
	l := c.leader(5 * time.Second)

	batch := func(i int) []byte {
		var sb strings.Builder
		for j := range 10 {
			fmt.Fprintf(&sb, "INSERT OR REPLACE INTO kv VALUES (%d, 'value-%d');", i*10+j, i)
		}
		return []byte(sb.String())
	}
	apply := func(b *testing.B, i int) {
		f := l.raft.Apply(batch(i), 5*time.Second)
		if err := f.Error(); err != nil {
			b.Fatalf("apply: %v", err)
		}
		if err, ok := f.Response().(error); ok && err != nil {
			b.Fatalf("batch: %v", err)
		}
	}

	b.Run("sequential", func(b *testing.B) {
		i := 0
		for b.Loop() {
			apply(b, i)
			i++
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "batches/s")
	})
	b.Run("concurrent", func(b *testing.B) {
		b.SetParallelism(16)
		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				apply(b, i)
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "batches/s")
	})
}
