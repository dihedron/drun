package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
)

// schema creates the reserved tables (STO-3, STO-12)
const schema = `
CREATE TABLE IF NOT EXISTS _resurgo_meta (id INTEGER PRIMARY KEY CHECK (id = 1), last_index INTEGER NOT NULL);
INSERT OR IGNORE INTO _resurgo_meta VALUES (1, 0);
CREATE TABLE IF NOT EXISTS _resurgo_checkpoint (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	seq INTEGER NOT NULL, epoch INTEGER NOT NULL, committed_at INTEGER NOT NULL, data BLOB
);`

const reservedPrefix = "_resurgo_"

// refused functions: they give different results on different nodes, or
// reach outside the database (STO-4)
var refusedFunctions = map[string]bool{
	"random": true, "randomblob": true, "load_extension": true,
	"current_time": true, "current_date": true, "current_timestamp": true,
}

// pragmas a runnable may use: reading the schema, and user_version
var allowedPragmas = map[string]bool{
	"table_info": true, "table_xinfo": true, "index_list": true, "index_info": true,
	"index_xinfo": true, "foreign_key_list": true, "user_version": true,
}

var refusedOps = map[int]string{
	sqlite3.SQLITE_ATTACH:              "ATTACH (and VACUUM)",
	sqlite3.SQLITE_DETACH:              "DETACH",
	sqlite3.SQLITE_TRANSACTION:         "transactions (a batch is one)",
	sqlite3.SQLITE_SAVEPOINT:           "savepoints",
	sqlite3.SQLITE_CREATE_TEMP_INDEX:   "temporary objects",
	sqlite3.SQLITE_CREATE_TEMP_TABLE:   "temporary objects",
	sqlite3.SQLITE_CREATE_TEMP_TRIGGER: "temporary objects",
	sqlite3.SQLITE_CREATE_TEMP_VIEW:    "temporary objects",
	sqlite3.SQLITE_DROP_TEMP_INDEX:     "temporary objects",
	sqlite3.SQLITE_DROP_TEMP_TABLE:     "temporary objects",
	sqlite3.SQLITE_DROP_TEMP_TRIGGER:   "temporary objects",
	sqlite3.SQLITE_DROP_TEMP_VIEW:      "temporary objects",
}

// authorize applies STO-4 and INV-S-4 to runnable SQL
func authorize(op int, arg1, arg2 string) int {
	switch op {
	case sqlite3.SQLITE_FUNCTION:
		if refusedFunctions[strings.ToLower(arg2)] {
			return sqlite3.SQLITE_DENY
		}
		return sqlite3.SQLITE_OK
	case sqlite3.SQLITE_PRAGMA:
		if allowedPragmas[strings.ToLower(arg1)] {
			return sqlite3.SQLITE_OK
		}
		return sqlite3.SQLITE_DENY
	}
	if _, refused := refusedOps[op]; refused {
		return sqlite3.SQLITE_DENY
	}
	// tables, indexes, triggers and views: arg1 or arg2 names them
	if strings.HasPrefix(strings.ToLower(arg1), reservedPrefix) || strings.HasPrefix(strings.ToLower(arg2), reservedPrefix) {
		return sqlite3.SQLITE_DENY
	}
	return sqlite3.SQLITE_OK
}

// register sets up a connection: the authorizer, the date and time
// overrides, resurgo_now() and resurgo_epoch()
func (s *Store) register(conn *sqlite3.SQLiteConn, enforce *atomic.Bool, now, epoch *atomic.Int64) error {
	conn.RegisterAuthorizer(func(op int, arg1, arg2, _ string) int {
		if !enforce.Load() {
			return sqlite3.SQLITE_OK
		}
		return authorize(op, arg1, arg2)
	})
	if err := overrideTimeFunctions(conn, s.builtin); err != nil {
		return err
	}
	if err := conn.RegisterFunc("resurgo_now", func() int64 { return now.Load() }, false); err != nil {
		return fmt.Errorf("resurgo_now: %w", err)
	}
	if err := conn.RegisterFunc("resurgo_epoch", func() int64 { return epoch.Load() }, false); err != nil {
		return fmt.Errorf("resurgo_epoch: %w", err)
	}
	return nil
}

// timeFunctions are SQLite's date and time functions, with the positions of
// their time-value arguments
var timeFunctions = map[string][]int{
	"date": {0}, "time": {0}, "datetime": {0}, "julianday": {0}, "unixepoch": {0},
	"strftime": {1}, "timediff": {0, 1},
}

var errNotDeterministic = errors.New("not deterministic: use resurgo_now()")

// overrideTimeFunctions replaces the date and time functions with ones that
// refuse what depends on the node ('now', a missing time value, the
// localtime and utc modifiers) and otherwise return what the built-ins do,
// evaluated on builtin, a connection without the overrides (EXT-S-6)
func overrideTimeFunctions(conn *sqlite3.SQLiteConn, builtin *sql.DB) error {
	for name, timeArgs := range timeFunctions {
		impl := func(args ...any) (any, error) {
			if err := deterministicTime(name, timeArgs, args); err != nil {
				return nil, err
			}
			var out any
			placeholders := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
			if err := builtin.QueryRow("SELECT "+name+"("+placeholders+")", args...).Scan(&out); err != nil { //nolint:gosec // name is a constant
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			return out, nil
		}
		if err := conn.RegisterFunc(name, impl, true); err != nil {
			return fmt.Errorf("overriding %s: %w", name, err)
		}
	}
	return nil
}

func deterministicTime(name string, timeArgs []int, args []any) error {
	isTime := map[int]bool{}
	for _, i := range timeArgs {
		isTime[i] = true
		if i >= len(args) {
			return fmt.Errorf("%s without a time value: %w", name, errNotDeterministic)
		}
		if s, ok := args[i].(string); ok && strings.EqualFold(strings.TrimSpace(s), "now") {
			return fmt.Errorf("%s('now'): %w", name, errNotDeterministic)
		}
	}
	for i, a := range args {
		if s, ok := a.(string); ok && !isTime[i] {
			switch strings.ToLower(strings.TrimSpace(s)) {
			case "localtime", "utc":
				return fmt.Errorf("%s modifier %q: %w", name, s, errNotDeterministic)
			}
		}
	}
	return nil
}
