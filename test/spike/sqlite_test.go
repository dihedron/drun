package spike

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/mattn/go-sqlite3"
)

// openDB opens a database file and one connection to it, on which every
// statement of the test runs
func openDB(t *testing.T, name string, pragmas ...string) (*sql.Conn, *sqlite3.SQLiteConn) {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("conn %s: %v", name, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var raw *sqlite3.SQLiteConn
	if err := conn.Raw(func(dc any) error {
		var ok bool
		raw, ok = dc.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("driver connection is %T", dc)
		}
		return nil
	}); err != nil {
		t.Fatalf("raw %s: %v", name, err)
	}
	for _, p := range pragmas {
		mustExec(t, conn, "PRAGMA "+p)
	}
	return conn, raw
}

func mustExec(t *testing.T, conn *sql.Conn, query string, args ...any) {
	t.Helper()
	if _, err := conn.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

type authCall struct {
	op         int
	arg1, arg2 string
}

// EXT-S-1: the authorizer sees every function call and pragma (and the
// other operations STO-4 forbids), and a denial fails the statement.
func TestEXTS1AuthorizerSeesFunctionsAndPragmas(t *testing.T) {
	conn, raw := openDB(t, "auth.db")
	mustExec(t, conn, "CREATE TABLE t (x INTEGER)")

	var mu sync.Mutex
	var calls []authCall
	deny := false
	raw.RegisterAuthorizer(func(op int, arg1, arg2, _ string) int {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, authCall{op, arg1, arg2})
		if deny && op != sqlite3.SQLITE_SELECT && op != sqlite3.SQLITE_READ {
			return sqlite3.SQLITE_DENY
		}
		return sqlite3.SQLITE_OK
	})
	seen := func(op int, arg string) bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.ContainsFunc(calls, func(c authCall) bool {
			return c.op == op && (c.arg1 == arg || c.arg2 == arg)
		})
	}

	record := func(query string) []authCall {
		t.Helper()
		mu.Lock()
		calls = nil
		mu.Unlock()
		if _, err := conn.ExecContext(t.Context(), query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}

	for _, tc := range []struct {
		query string
		op    int
		arg   string
	}{
		{"SELECT random()", sqlite3.SQLITE_FUNCTION, "random"},
		{"SELECT randomblob(4)", sqlite3.SQLITE_FUNCTION, "randomblob"},
		{"SELECT date('now')", sqlite3.SQLITE_FUNCTION, "date"},
		{"SELECT unixepoch()", sqlite3.SQLITE_FUNCTION, "unixepoch"},
		{"SELECT CURRENT_TIMESTAMP", sqlite3.SQLITE_FUNCTION, "current_timestamp"},
		{"SELECT x FROM t WHERE x = abs(-1)", sqlite3.SQLITE_FUNCTION, "abs"},
		{"PRAGMA journal_mode", sqlite3.SQLITE_PRAGMA, "journal_mode"},
		{"PRAGMA user_version = 3", sqlite3.SQLITE_PRAGMA, "user_version"},
		{"ATTACH ':memory:' AS other", sqlite3.SQLITE_ATTACH, ":memory:"},
		{"CREATE TEMP TABLE tt (y)", sqlite3.SQLITE_CREATE_TEMP_TABLE, "tt"},
		// VACUUM has no code of its own: it attaches a temporary database
		// with an empty name, so denying SQLITE_ATTACH denies it too
		{"VACUUM", sqlite3.SQLITE_ATTACH, ""},
	} {
		if got := record(tc.query); !seen(tc.op, tc.arg) {
			t.Errorf("%s: authorizer did not see op %d %q; saw %v", tc.query, tc.op, tc.arg, got)
		}
	}

	// the arguments of a function never reach the authorizer: date('now')
	// cannot be told from date('2026-01-01') there, so STO-4 needs another
	// mechanism for 'now' (see the result of EXT-S-1)
	if now, fixed := record("SELECT date('now')"), record("SELECT date('2026-01-01')"); !slices.Equal(now, fixed) {
		t.Errorf("the authorizer tells date('now') %v from date('2026-01-01') %v: STO-4 can use it", now, fixed)
	}

	mu.Lock()
	deny = true
	mu.Unlock()
	for _, q := range []string{"SELECT random()", "PRAGMA journal_mode = DELETE", "ATTACH ':memory:' AS again", "CREATE TEMP TABLE t2 (y)", "VACUUM"} {
		// a denial fails the prepare ("not authorized"), or, for VACUUM's
		// own ATTACH, the step (SQLITE_AUTH)
		_, err := conn.ExecContext(t.Context(), q)
		var serr sqlite3.Error
		if err == nil || !strings.Contains(err.Error(), "not authorized") && (!errors.As(err, &serr) || serr.Code != sqlite3.ErrAuth) {
			t.Errorf("%s with the authorizer denying: %v, want a denial", q, err)
		}
	}
}

// EXT-S-2: a progress handler can be installed on the driver's connections,
// and the number of callbacks for a statement on the same data is the same
// on every node, whatever the node's cache and journal settings.
func TestEXTS2ProgressHandlerIsDeterministic(t *testing.T) {
	const load = `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000)
		INSERT INTO t SELECT i, printf('row-%d', i % 97) FROM n`
	const query = `SELECT sum(x), count(DISTINCT y) FROM t WHERE x % 7 = 3`
	const runaway = `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c`

	type result struct{ load, query int64 }
	var results []result
	for i, pragmas := range [][]string{
		{"journal_mode = WAL", "cache_size = -2000"},
		{"journal_mode = DELETE", "cache_size = 10"},
		{"journal_mode = WAL", "cache_size = -64000", "mmap_size = 268435456"},
	} {
		conn, raw := openDB(t, fmt.Sprintf("node-%d.db", i), pragmas...)
		mustExec(t, conn, "CREATE TABLE t (x INTEGER PRIMARY KEY, y TEXT)")
		mustExec(t, conn, "CREATE INDEX t_y ON t (y)")
		measure := func(q string) int64 {
			p := &progress{}
			remove, err := setProgress(raw, 100, p)
			if err != nil {
				t.Fatalf("progress handler: %v", err)
			}
			defer remove()
			rows, err := conn.QueryContext(context.Background(), q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			for rows.Next() { //nolint:revive // drain
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			if p.calls == 0 {
				t.Fatalf("%s: no progress callback", q)
			}
			return p.calls
		}
		results = append(results, result{load: measure(load), query: measure(query)})

		// a runaway statement is aborted where the budget ends
		p := &progress{limit: 50}
		remove, err := setProgress(raw, 1000, p)
		if err != nil {
			t.Fatalf("progress handler: %v", err)
		}
		_, err = conn.ExecContext(t.Context(), runaway)
		remove()
		var serr sqlite3.Error
		if !errors.As(err, &serr) || serr.Code != sqlite3.ErrInterrupt {
			t.Fatalf("runaway query: %v, want SQLITE_INTERRUPT", err)
		}
		if p.calls != p.limit+1 {
			t.Fatalf("runaway query aborted after %d callbacks, want %d", p.calls, p.limit+1)
		}
	}
	for _, r := range results[1:] {
		if r != results[0] {
			t.Fatalf("callback counts differ between nodes: %v", results)
		}
	}
	t.Logf("callbacks every 100 steps: load %d, query %d", results[0].load, results[0].query)
}

// EXT-S-3: a WAL read transaction gives a consistent copy (backup API or
// VACUUM INTO) while another connection commits.
func TestEXTS3ConsistentCopyWhileWriting(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "live.db")+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close() //nolint:errcheck // test teardown
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE a (v INTEGER); CREATE TABLE total (s INTEGER); INSERT INTO total VALUES (0)`); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// the writer keeps sum(a.v) = total.s in every transaction
	ctx, stop := context.WithCancel(t.Context())
	var writes sync.WaitGroup
	writes.Go(func() {
		for i := 1; ctx.Err() == nil; i++ {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return
			}
			_, err1 := tx.Exec("INSERT INTO a VALUES (?)", i)
			_, err2 := tx.Exec("UPDATE total SET s = s + ?", i)
			if err1 != nil || err2 != nil {
				_ = tx.Rollback()
				return
			}
			_ = tx.Commit()
		}
	})
	defer func() { stop(); writes.Wait() }()

	check := func(path string) {
		t.Helper()
		cp, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatalf("open copy: %v", err)
		}
		defer cp.Close() //nolint:errcheck // test teardown
		var sum, total, rows int64
		if err := cp.QueryRow("SELECT coalesce(sum(v), 0), count(*), (SELECT s FROM total) FROM a").Scan(&sum, &rows, &total); err != nil {
			t.Fatalf("read copy: %v", err)
		}
		if sum != total {
			t.Fatalf("%s: sum %d, total %d: inconsistent", filepath.Base(path), sum, total)
		}
		t.Logf("%s: %d rows, consistent", filepath.Base(path), rows)
	}

	for i := range *repeat {
		vacuumed := filepath.Join(dir, fmt.Sprintf("vacuum-%d.db", i))
		if _, err := db.Exec("VACUUM INTO ?", vacuumed); err != nil {
			t.Fatalf("VACUUM INTO: %v", err)
		}
		check(vacuumed)

		backedUp := filepath.Join(dir, fmt.Sprintf("backup-%d.db", i))
		backup(t, db, backedUp)
		check(backedUp)
	}
}

// backup copies src into a new file with the online backup API, in one step
func backup(t *testing.T, src *sql.DB, path string) {
	t.Helper()
	dst, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	defer dst.Close() //nolint:errcheck // test teardown
	srcConn, err := src.Conn(t.Context())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer srcConn.Close() //nolint:errcheck // test teardown
	dstConn, err := dst.Conn(t.Context())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer dstConn.Close() //nolint:errcheck // test teardown
	err = dstConn.Raw(func(d any) error {
		return srcConn.Raw(func(s any) error {
			b, err := d.(*sqlite3.SQLiteConn).Backup("main", s.(*sqlite3.SQLiteConn), "main") //nolint:forcetypeassert // the driver
			if err != nil {
				return err
			}
			if done, err := b.Step(-1); err != nil || !done {
				_ = b.Finish()
				return fmt.Errorf("backup step: done %v: %w", done, err)
			}
			return b.Finish()
		})
	})
	if err != nil && !strings.Contains(err.Error(), "not an error") {
		t.Fatalf("backup: %v", err)
	}
}
