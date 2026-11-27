package spike

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
)

// timeFunctions are SQLite's date and time functions, with the positions of
// their time-value arguments
var timeFunctions = map[string][]int{
	"date": {0}, "time": {0}, "datetime": {0}, "julianday": {0}, "unixepoch": {0},
	"strftime": {1}, "timediff": {0, 1},
}

var errNotDeterministic = errors.New("not deterministic: use drun_now()")

// overrideTimeFunctions replaces the date and time functions on conn with
// ones that refuse what depends on the node ('now', a missing time value,
// the localtime and utc modifiers) and otherwise delegate to the built-ins
// on builtin, a connection without the overrides
func overrideTimeFunctions(conn *sqlite3.SQLiteConn, builtin *sql.DB) error {
	for name, timeArgs := range timeFunctions {
		impl := func(args ...any) (any, error) {
			for _, i := range timeArgs {
				if i >= len(args) {
					return nil, fmt.Errorf("%s without a time value: %w", name, errNotDeterministic)
				}
				if s, ok := args[i].(string); ok && strings.EqualFold(strings.TrimSpace(s), "now") {
					return nil, fmt.Errorf("%s('now'): %w", name, errNotDeterministic)
				}
			}
			for i, a := range args {
				if s, ok := a.(string); ok && !isTimeArg(i, timeArgs) {
					switch strings.ToLower(strings.TrimSpace(s)) {
					case "localtime", "utc":
						return nil, fmt.Errorf("%s modifier %q: %w", name, s, errNotDeterministic)
					}
				}
			}
			var out any
			placeholders := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
			if err := builtin.QueryRow("SELECT "+name+"("+placeholders+")", args...).Scan(&out); err != nil { //nolint:gosec // name is a constant
				return nil, fmt.Errorf("built-in %s: %w", name, err)
			}
			return out, nil
		}
		if err := conn.RegisterFunc(name, impl, true); err != nil {
			return fmt.Errorf("overriding %s: %w", name, err)
		}
	}
	return nil
}

func isTimeArg(i int, timeArgs []int) bool {
	for _, t := range timeArgs {
		if i == t {
			return true
		}
	}
	return false
}

// EXT-S-6: RegisterFunc overrides SQLite's built-in date and time functions
// on the connection it is called on, everywhere SQL can call them (queries,
// defaults, triggers), so the FSM can refuse 'now' (STO-4, OI-S-4) and keep
// the deterministic forms.
func TestEXTS6OverrideTimeFunctions(t *testing.T) {
	builtin, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer builtin.Close() //nolint:errcheck // test teardown
	builtin.SetMaxOpenConns(1)

	conn, raw := openDB(t, "time.db")
	if err := overrideTimeFunctions(raw, builtin); err != nil {
		t.Fatalf("override: %v", err)
	}
	refused := func(query string) {
		t.Helper()
		_, err := conn.ExecContext(t.Context(), query)
		if err == nil || !strings.Contains(err.Error(), errNotDeterministic.Error()) {
			t.Errorf("%s: %v, want refused", query, err)
		}
	}

	for _, q := range []string{
		"SELECT date('now')", "SELECT date()", "SELECT datetime('now', '+1 day')",
		"SELECT time(' Now ')", "SELECT julianday('NOW')", "SELECT unixepoch()",
		"SELECT strftime('%s', 'now')", "SELECT strftime('%s')",
		"SELECT timediff('now', '2026-01-01')", "SELECT timediff('2026-01-01', 'now')",
		"SELECT date('2026-01-01', 'localtime')", "SELECT datetime('2026-01-01', ' UTC')",
	} {
		refused(q)
	}

	for q, want := range map[string]string{
		"SELECT date('2026-01-01', '+1 day')":             "2026-01-02",
		"SELECT strftime('%Y', '2026-03-04')":             "2026",
		"SELECT unixepoch('1970-01-02')":                  "86400",
		"SELECT CAST(julianday('2000-01-01') AS TEXT)":    "2451544.5",
		"SELECT datetime(1700000000, 'unixepoch')":        "2023-11-14 22:13:20",
		"SELECT timediff('2026-01-02', '2026-01-01')":     "+0000-00-01 00:00:00.000",
		"SELECT date('2026-01-31', 'start of month')":     "2026-01-01",
		"SELECT strftime('%H:%M', '2026-01-01 10:20:30')": "10:20",
	} {
		var got string
		if err := conn.QueryRowContext(t.Context(), q).Scan(&got); err != nil {
			t.Errorf("%s: %v", q, err)
		} else if got != want {
			t.Errorf("%s = %q, want %q", q, got, want)
		}
	}

	// the overrides also run in defaults and triggers
	mustExec(t, conn, "CREATE TABLE d (x INTEGER, at TEXT DEFAULT (date('now')))")
	refused("INSERT INTO d (x) VALUES (1)")
	mustExec(t, conn, "CREATE TABLE e (x INTEGER, at TEXT)")
	mustExec(t, conn, "CREATE TRIGGER e_at AFTER INSERT ON e BEGIN UPDATE e SET at = datetime('now') WHERE rowid = new.rowid; END")
	refused("INSERT INTO e (x) VALUES (1)")

	// they are per connection: the FSM registers them on every connection
	other, _ := openDB(t, "other.db")
	if _, err := other.ExecContext(t.Context(), "SELECT date('now')"); err != nil {
		t.Fatalf("date('now') on a connection without the overrides: %v", err)
	}
}
