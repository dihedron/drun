package migrate

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/dihedron/drun/internal/fsm/fsmpb"
	"github.com/dihedron/drun/internal/store"
	"github.com/dihedron/drun/pkg/runnable"
)

// local is a runnable.Store on a node's store, without Raft: each batch is
// the next entry
type local struct {
	s     *store.Store
	index uint64
}

func newLocal(t *testing.T) *local {
	s, err := store.Open(filepath.Join(t.TempDir(), "store"), func(err error) { t.Errorf("fatal: %v", err) }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return &local{s: s}
}

func (l *local) Exec(_ context.Context, batch []runnable.Statement) ([]runnable.Result, error) {
	b := &fsmpb.StoreBatch{Runnable: "svc"}
	for _, st := range batch {
		ps := &fsmpb.Statement{Sql: st.SQL}
		for _, a := range st.Args {
			switch x := a.(type) {
			case int64:
				ps.Args = append(ps.Args, &fsmpb.Value{Value: &fsmpb.Value_Integer{Integer: x}})
			case string:
				ps.Args = append(ps.Args, &fsmpb.Value{Value: &fsmpb.Value_Text{Text: x}})
			}
		}
		if e := st.Expect; e != nil {
			ps.Expect = &fsmpb.Expectation{RowsAffected: e.RowsAffected, MinRowsAffected: e.MinRowsAffected}
		}
		b.Statements = append(b.Statements, ps)
	}
	l.index++
	res, err := l.s.Exec(l.index, b, nil)
	if err != nil {
		return nil, err
	}
	if e := res.GetError(); e != nil {
		return nil, &runnable.BatchError{Statement: int(e.GetStatement()), Code: int(e.GetCode()), Message: e.GetMessage()}
	}
	out := make([]runnable.Result, len(res.GetResults()))
	for i, r := range res.GetResults() {
		out[i] = runnable.Result{RowsAffected: r.GetRowsAffected(), LastInsertID: r.GetLastInsertId()}
	}
	return out, nil
}

func (l *local) Query(ctx context.Context, sql string, args ...any) (runnable.Rows, error) {
	cols, rows, err := l.s.Query(ctx, "svc", sql, args...)
	if err != nil {
		return nil, err
	}
	return &memRows{cols: cols, rows: rows, i: -1}, nil
}

type memRows struct {
	cols []string
	rows [][]any
	i    int
}

func (r *memRows) Columns() []string { return r.cols }
func (r *memRows) Next() bool        { r.i++; return r.i < len(r.rows) }
func (r *memRows) Err() error        { return nil }
func (r *memRows) Close() error      { return nil }
func (r *memRows) Scan(dest ...any) error {
	for i, d := range dest {
		p, ok := d.(*int64)
		v, isInt := r.rows[r.i][i].(int64)
		if !ok || !isInt {
			return errors.New("memRows scans int64 only")
		}
		*p = v
	}
	return nil
}

func table(name string) Migration {
	return Migration{Name: "create " + name, Statements: []runnable.Statement{{SQL: "CREATE TABLE " + name + " (x INTEGER)"}}}
}

// AC-S-1: from nothing to version 3, then a newer binary to version 5
func TestMigrateUpwards(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	all := []Migration{table("a"), table("b"), table("c"), table("d"), table("e")}
	if found, err := Run(ctx, s, all[:3]); err != nil || found != 0 {
		t.Fatalf("first run: found %d, %v", found, err)
	}
	if v, _ := Version(ctx, s); v != 3 {
		t.Fatalf("version %d, want 3", v)
	}
	// the same binary again: nothing to do
	before := s.index
	if found, err := Run(ctx, s, all[:3]); err != nil || found != 3 || s.index != before+1 {
		t.Fatalf("rerun: found %d, %v, %d batches", found, err, s.index-before)
	}
	if found, err := Run(ctx, s, all); err != nil || found != 3 {
		t.Fatalf("newer binary: found %d, %v", found, err)
	}
	if v, _ := Version(ctx, s); v != 5 {
		t.Fatalf("version %d, want 5", v)
	}
	// an older binary refuses the newer schema (AC-S-8)
	if _, err := Run(ctx, s, all[:4]); !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("older binary: %v, want ErrNewerSchema", err)
	}
}

// a failing migration leaves the schema at the version before it, and a
// fixed binary continues from there
func TestMigrateStopsAtABoundary(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	broken := []Migration{table("a"), table("b"), {Name: "broken", Statements: []runnable.Statement{
		{SQL: "CREATE TABLE c (x)"}, {SQL: "INSERT INTO nowhere VALUES (1)"},
	}}}
	_, err := Run(ctx, s, broken)
	var be *runnable.BatchError
	if !errors.As(err, &be) {
		t.Fatalf("broken migration: %v", err)
	}
	if v, _ := Version(ctx, s); v != 2 {
		t.Fatalf("version %d after a failed migration, want 2", v)
	}
	fixed := []Migration{table("a"), table("b"), table("c")}
	if found, err := Run(ctx, s, fixed); err != nil || found != 2 {
		t.Fatalf("fixed binary: found %d, %v", found, err)
	}
}

// a migration applied twice (a stale incarnation racing, or a retry) is
// refused by its expectation, and changes nothing
func TestMigrationBatchIsConditional(t *testing.T) {
	ctx := context.Background()
	s := newLocal(t)
	if _, err := Run(ctx, s, []Migration{table("a")}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Exec(ctx, []runnable.Statement{
		{SQL: "CREATE TABLE again (x)"},
		{SQL: "UPDATE _schema SET version = 1 WHERE version = 0", Expect: runnable.Exactly(1)},
	})
	var be *runnable.BatchError
	if !errors.As(err, &be) {
		t.Fatalf("replayed migration: %v", err)
	}
	if _, err := s.Query(ctx, "SELECT count(*) FROM again"); err == nil {
		t.Fatal("the refused migration created its table")
	}
}
