package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dihedron/drun/internal/fsm/fsmpb"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type harness struct {
	t      testing.TB
	s      *Store
	dir    string
	index  uint64
	fatals []error
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	h := &harness{t: t, dir: filepath.Join(t.TempDir(), "store")}
	s, err := Open(h.dir, func(err error) { h.fatals = append(h.fatals, err) }, discard)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	h.s = s
	return h
}

func stmt(sql string, args ...any) *fsmpb.Statement {
	s := &fsmpb.Statement{Sql: sql}
	for _, a := range args {
		s.Args = append(s.Args, ToValue(a))
	}
	return s
}

func expect(s *fsmpb.Statement, rows int64) *fsmpb.Statement {
	s.Expect = &fsmpb.Expectation{RowsAffected: &rows}
	return s
}

// exec applies a batch at the next index
func (h *harness) exec(runnable string, limits *fsmpb.StoreLimits, stmts ...*fsmpb.Statement) *fsmpb.BatchResult {
	h.t.Helper()
	h.index++
	return h.execAt(h.index, runnable, limits, stmts...)
}

func (h *harness) execAt(index uint64, runnable string, limits *fsmpb.StoreLimits, stmts ...*fsmpb.Statement) *fsmpb.BatchResult {
	h.t.Helper()
	res, err := h.s.Exec(index, &fsmpb.StoreBatch{Runnable: runnable, Epoch: 7, At: 1_700_000_000_000, Statements: stmts}, limits)
	if err != nil {
		h.t.Fatalf("exec: %v", err)
	}
	return res
}

func (h *harness) ok(runnable string, stmts ...*fsmpb.Statement) *fsmpb.BatchResult {
	h.t.Helper()
	res := h.exec(runnable, nil, stmts...)
	if res.GetError() != nil {
		h.t.Fatalf("batch failed: %v", res.GetError())
	}
	return res
}

func (h *harness) failed(runnable string, want string, stmts ...*fsmpb.Statement) *fsmpb.BatchError {
	h.t.Helper()
	res := h.exec(runnable, nil, stmts...)
	if res.GetError() == nil || !strings.Contains(res.GetError().GetMessage(), want) {
		h.t.Fatalf("batch error %v, want one mentioning %q", res.GetError(), want)
	}
	return res.GetError()
}

func (h *harness) count(runnable, table string) int64 {
	h.t.Helper()
	_, rows, err := h.s.Query(h.t.Context(), runnable, "SELECT count(*) FROM "+table)
	if err != nil {
		h.t.Fatalf("count: %v", err)
	}
	return rows[0][0].(int64) //nolint:forcetypeassert // count(*)
}

func TestBatchAtomicWithResults(t *testing.T) {
	h := newHarness(t)
	h.ok("svc", stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, at INTEGER, e INTEGER)"))
	res := h.ok("svc",
		stmt("INSERT INTO t (v, at, e) VALUES (?, drun_now(), drun_epoch())", "a"),
		stmt("INSERT INTO t (v) VALUES (?), (?) RETURNING id, v", "b", "c"),
		expect(stmt("UPDATE t SET v = upper(v) WHERE v IN ('a', 'b')"), 2),
	)
	r := res.GetResults()
	if len(r) != 3 || r[0].GetRowsAffected() != 1 || r[0].GetLastInsertId() != 1 ||
		len(r[1].GetRows()) != 2 || r[1].GetRows()[1].GetValues()[1].GetText() != "c" || r[2].GetRowsAffected() != 2 {
		t.Fatalf("results %v", r)
	}
	_, rows, err := h.s.Query(t.Context(), "svc", "SELECT at, e FROM t WHERE id = 1")
	if err != nil || rows[0][0] != int64(1_700_000_000_000) || rows[0][1] != int64(7) {
		t.Fatalf("drun_now/drun_epoch: %v %v", rows, err)
	}

	// a failing statement or expectation rolls back the whole batch
	e := h.failed("svc", "expected 5 rows", stmt("INSERT INTO t (v) VALUES ('d')"), expect(stmt("DELETE FROM t WHERE v = 'zz'"), 5))
	if e.GetStatement() != 1 {
		t.Fatalf("failing statement %d", e.GetStatement())
	}
	h.failed("svc", "UNIQUE", stmt("INSERT INTO t (v) VALUES ('e')"), stmt("INSERT INTO t (id) VALUES (1)"))
	if n := h.count("svc", "t"); n != 3 {
		t.Fatalf("%d rows after failed batches, want 3", n)
	}
	h.failed("svc", "one statement", stmt("INSERT INTO t (v) VALUES ('f'); DELETE FROM t"))
	min := int64(1)
	h.failed("svc", "at least", &fsmpb.Statement{Sql: "DELETE FROM t WHERE v = 'none'", Expect: &fsmpb.Expectation{MinRowsAffected: &min}})
}

// STO-4, AC-S-4: refused, the same on every node
func TestDeterminismRules(t *testing.T) {
	a, b := newHarness(t), newHarness(t)
	for _, h := range []*harness{a, b} {
		h.ok("svc", stmt("CREATE TABLE t (x)"))
	}
	for _, q := range []string{
		"INSERT INTO t VALUES (random())",
		"INSERT INTO t VALUES (randomblob(4))",
		"INSERT INTO t VALUES (datetime('now'))",
		"INSERT INTO t VALUES (date('2026-01-01', 'localtime'))",
		"INSERT INTO t VALUES (CURRENT_TIMESTAMP)",
		"ATTACH 'other.db' AS other",
		"VACUUM",
		"BEGIN",
		"SAVEPOINT s",
		"CREATE TEMP TABLE tt (y)",
		"PRAGMA journal_mode = DELETE",
		"PRAGMA cache_size",
		"SELECT load_extension('x')",
		"SELECT * FROM _drun_meta",
		"UPDATE _drun_checkpoint SET seq = 9",
		"CREATE TABLE _drun_mine (x)",
		"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) INSERT INTO t SELECT x FROM c",
	} {
		ea := a.exec("svc", nil, stmt(q)).GetError()
		eb := b.exec("svc", nil, stmt(q)).GetError()
		if ea == nil || eb == nil {
			t.Fatalf("%s: accepted (%v, %v)", q, ea, eb)
		}
		if ea.GetCode() != eb.GetCode() || ea.GetMessage() != eb.GetMessage() {
			t.Fatalf("%s: refused differently: %v / %v", q, ea, eb)
		}
		t.Logf("%s: %s", q, ea.GetMessage())
	}
	// what stays allowed
	a.ok("svc",
		stmt("INSERT INTO t VALUES (date('2026-01-01', '+1 day'))"),
		stmt("PRAGMA table_info(t)"),
		stmt("PRAGMA user_version = 3"),
		stmt("INSERT INTO t SELECT x FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100) SELECT x FROM c)"),
	)
	if len(a.fatals)+len(b.fatals) != 0 {
		t.Fatalf("fatal errors: %v %v", a.fatals, b.fatals)
	}
}

func TestLimits(t *testing.T) {
	h := newHarness(t)
	small := &fsmpb.StoreLimits{MaxResult: 100, MaxSize: 64 << 10, MaxSteps: 10_000_000, MaxBatch: 1 << 20}
	h.ok("svc", stmt("CREATE TABLE t (x)"))
	res := h.exec("svc", small, stmt("INSERT INTO t SELECT zeroblob(10) FROM (SELECT 1 UNION ALL SELECT 2) RETURNING x"))
	if res.GetError() != nil {
		t.Fatalf("small result: %v", res.GetError())
	}
	res = h.exec("svc", small, stmt("INSERT INTO t SELECT zeroblob(60) FROM (SELECT 1 UNION ALL SELECT 2) RETURNING x"))
	if res.GetError() == nil || !strings.Contains(res.GetError().GetMessage(), "result") {
		t.Fatalf("oversized result: %v", res.GetError())
	}
	// STO-8: the database cannot grow beyond max_size
	res = h.exec("svc", small, stmt("INSERT INTO t VALUES (zeroblob(1000000))"))
	if res.GetError() == nil || !strings.Contains(res.GetError().GetMessage(), "full") {
		t.Fatalf("oversized database: %v", res.GetError())
	}
	if len(h.fatals) != 0 {
		t.Fatalf("a full database is not fatal: %v", h.fatals)
	}
}

// STO-3, INV-S-3: an entry applied again changes nothing
func TestExactlyOnce(t *testing.T) {
	h := newHarness(t)
	h.ok("svc", stmt("CREATE TABLE t (x)"))
	h.ok("svc", stmt("INSERT INTO t VALUES (1)"))
	if res := h.execAt(h.index, "svc", nil, stmt("INSERT INTO t VALUES (1)")); res.GetError() != nil || len(res.GetResults()) != 0 {
		t.Fatalf("replayed entry: %v", res)
	}
	if err := h.s.PutCheckpoint(h.index, "svc", &fsmpb.CheckpointMeta{Seq: 1}, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if n := h.count("svc", "t"); n != 1 {
		t.Fatalf("%d rows after a replay", n)
	}
	if _, data, _, _ := h.s.Checkpoint("svc"); data != nil {
		t.Fatalf("replayed checkpoint stored: %q", data)
	}
}

func TestCheckpoints(t *testing.T) {
	h := newHarness(t)
	h.index++
	if err := h.s.PutCheckpoint(h.index, "svc", &fsmpb.CheckpointMeta{Seq: 3, Epoch: 2, CommittedAt: 5}, []byte("state")); err != nil {
		t.Fatal(err)
	}
	meta, data, ok, err := h.s.Checkpoint("svc")
	if err != nil || !ok || meta.GetSeq() != 3 || meta.GetEpoch() != 2 || !bytes.Equal(data, []byte("state")) {
		t.Fatalf("checkpoint %v %q %v %v", meta, data, ok, err)
	}
	h.index++
	if err := h.s.ClearCheckpoint(h.index, "svc"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := h.s.Checkpoint("svc"); ok {
		t.Fatal("checkpoint still there")
	}
}

// INV-S-4: a runnable sees only its own database
func TestIsolation(t *testing.T) {
	h := newHarness(t)
	h.ok("a", stmt("CREATE TABLE secret (x)"), stmt("INSERT INTO secret VALUES (42)"))
	h.ok("b", stmt("CREATE TABLE t (x)"))
	h.failed("b", "no such table", stmt("INSERT INTO t SELECT x FROM secret"))
	if _, _, err := h.s.Query(t.Context(), "b", "SELECT * FROM secret"); err == nil {
		t.Fatal("b read a's table")
	}
	if _, _, err := h.s.Query(t.Context(), "a", "SELECT * FROM _drun_meta"); err == nil {
		t.Fatal("a read a reserved table")
	}
	if _, _, err := h.s.Query(t.Context(), "a", "DELETE FROM secret"); err == nil {
		t.Fatal("a query wrote")
	}
	if _, err := h.s.Exec(1, &fsmpb.StoreBatch{Runnable: "../escape"}, nil); err == nil {
		t.Fatal("a runnable name with a path was accepted")
	}
}

func TestForget(t *testing.T) {
	h := newHarness(t)
	h.ok("svc", stmt("CREATE TABLE t (x)"))
	forgetAt := h.index + 1
	h.index += 2
	h.ok("svc", stmt("CREATE TABLE u (x)")) // a later life of the same name
	if err := h.s.Forget(forgetAt, "svc"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.s.Query(t.Context(), "svc", "SELECT * FROM u"); err != nil {
		t.Fatalf("a replayed Forget removed a newer database: %v", err)
	}
	h.index++
	if err := h.s.Forget(h.index, "svc"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "svc.db")); !os.IsNotExist(err) {
		t.Fatalf("database still there: %v", err)
	}
}

func takeSnapshot(t *testing.T, s *Store) []byte {
	t.Helper()
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSnapshotRestore(t *testing.T) {
	src := newHarness(t)
	src.ok("a", stmt("CREATE TABLE t (x)"), stmt("INSERT INTO t VALUES (1), (2)"))
	src.ok("b", stmt("CREATE TABLE u (y)"))
	data := takeSnapshot(t, src.s)

	dst := newHarness(t)
	dst.ok("stale", stmt("CREATE TABLE z (x)"))
	if err := dst.s.Restore(bytes.NewReader(data)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if n := dst.count("a", "t"); n != 2 {
		t.Fatalf("%d rows restored", n)
	}
	if _, _, err := dst.s.Query(t.Context(), "stale", "SELECT * FROM z"); err == nil {
		t.Fatal("a database not in the snapshot survived")
	}
	for _, name := range []string{"a", "b"} {
		da, ia, err1 := src.s.Digest(name)
		db, ib, err2 := dst.s.Digest(name)
		if err1 != nil || err2 != nil || da != db || ia != ib {
			t.Fatalf("%s: digests %x@%d / %x@%d (%v %v)", name, da, ia, db, ib, err1, err2)
		}
	}

	// a local database newer than the snapshot's is kept
	src.ok("a", stmt("INSERT INTO t VALUES (3)"))
	if err := src.s.Restore(bytes.NewReader(data)); err != nil {
		t.Fatalf("restore over newer: %v", err)
	}
	if n := src.count("a", "t"); n != 3 {
		t.Fatalf("a newer local database was replaced: %d rows", n)
	}

	for name, bad := range map[string][]byte{"truncated": data[:len(data)/2], "path": frame("../x", []byte("x"))} {
		if err := newHarness(t).s.Restore(bytes.NewReader(bad)); err == nil {
			t.Fatalf("%s snapshot restored", name)
		}
	}
}

// frame builds one snapshot frame by hand, for the refusal tests
func frame(name string, data []byte) []byte {
	var buf bytes.Buffer
	writeFrame(&buf, name, bytes.NewReader(data), int64(len(data))) //nolint:errcheck // in memory
	return buf.Bytes()
}

// INV-S-1: two stores fed the same entries hold the same content
func TestDigestsAgreeOnRandomBatches(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // reproducible test sequences
	a, b := newHarness(t), newHarness(t)
	for _, h := range []*harness{a, b} {
		h.ok("svc", stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT, n REAL)"))
	}
	for range 300 {
		var stmts []*fsmpb.Statement
		for range 1 + rng.IntN(4) {
			k := rng.IntN(50)
			switch rng.IntN(4) {
			case 0:
				stmts = append(stmts, stmt("INSERT OR REPLACE INTO kv VALUES (?, ?, ?)", int64(k), fmt.Sprint(rng.IntN(1000)), rng.Float64()))
			case 1:
				stmts = append(stmts, stmt("DELETE FROM kv WHERE k = ?", int64(k)))
			case 2:
				stmts = append(stmts, stmt("UPDATE kv SET n = n * 2 WHERE k < ?", int64(k)))
			default:
				stmts = append(stmts, stmt("INSERT INTO kv VALUES (?, 'dup', 0)", int64(k))) // may fail
			}
		}
		a.exec("svc", nil, stmts...)
		b.exec("svc", nil, stmts...)
	}
	da, _, err1 := a.s.Digest("svc")
	db, _, err2 := b.s.Digest("svc")
	if err1 != nil || err2 != nil || da != db {
		t.Fatalf("digests differ: %x %x (%v %v)", da, db, err1, err2)
	}
	a.ok("svc", stmt("UPDATE kv SET v = 'changed' WHERE k = (SELECT min(k) FROM kv)"))
	if dc, _, _ := a.s.Digest("svc"); dc == da {
		t.Fatal("the digest did not change with the content")
	}
}

func TestIOFailureIsFatal(t *testing.T) {
	h := newHarness(t)
	h.ok("svc", stmt("CREATE TABLE t (x)"))
	h.s.mu.Lock()
	d := h.s.dbs["svc"]
	h.s.mu.Unlock()
	_ = d.conn.Close() // the connection fails as a broken disk would
	h.index++
	_, err := h.s.Exec(h.index, &fsmpb.StoreBatch{Runnable: "svc", Statements: []*fsmpb.Statement{stmt("INSERT INTO t VALUES (1)")}}, nil)
	if err == nil || len(h.fatals) != 1 || !errors.Is(err, h.fatals[0]) {
		t.Fatalf("error %v, fatals %v", err, h.fatals)
	}
}
