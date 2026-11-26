// Package migrate applies a runnable's schema migrations to its store at
// the start of Run (resurgo-store.md STO-13).
//
// Each migration is one batch that also moves the version on, from n-1 to
// n, with an expectation on exactly one row: a failover in the middle
// leaves the schema at a version boundary, a stale incarnation's migration
// is refused (fencing), and a binary that finds a newer schema than it
// knows refuses to start.
package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/dihedron/resurgo/pkg/runnable"
)

// Migration is one step of the schema: version n is Migrations[n-1].
type Migration struct {
	// Name is for logs and errors.
	Name string
	// Statements change the schema (and data); they run in one batch.
	Statements []runnable.Statement
}

// ErrNewerSchema: the store's schema is newer than this binary knows.
var ErrNewerSchema = errors.New("the schema is newer than this binary knows")

// Run brings the store's schema to len(migrations) and returns the version
// it found.
func Run(ctx context.Context, store runnable.Store, migrations []Migration) (int, error) {
	if _, err := store.Exec(ctx, []runnable.Statement{
		{SQL: "CREATE TABLE IF NOT EXISTS _schema (version INTEGER NOT NULL)"},
		{SQL: "INSERT INTO _schema (version) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM _schema)"},
	}); err != nil {
		return 0, fmt.Errorf("migrate: schema table: %w", err)
	}
	found, err := Version(ctx, store)
	if err != nil {
		return 0, err
	}
	if found > len(migrations) {
		return found, fmt.Errorf("migrate: %w: version %d, this binary knows %d", ErrNewerSchema, found, len(migrations))
	}
	for v := found + 1; v <= len(migrations); v++ {
		m := migrations[v-1]
		batch := append([]runnable.Statement{}, m.Statements...)
		batch = append(batch, runnable.Statement{
			SQL:    "UPDATE _schema SET version = ? WHERE version = ?",
			Args:   []any{int64(v), int64(v - 1)},
			Expect: runnable.Exactly(1),
		})
		if _, err := store.Exec(ctx, batch); err != nil {
			return found, fmt.Errorf("migrate: to version %d (%s): %w", v, m.Name, err)
		}
	}
	return found, nil
}

// Version returns the store's schema version (0 before any migration).
func Version(ctx context.Context, store runnable.Store) (int, error) {
	rows, err := store.Query(ctx, "SELECT version FROM _schema")
	if err != nil {
		return 0, fmt.Errorf("migrate: version: %w", err)
	}
	defer rows.Close() //nolint:errcheck // the error comes from rows.Err
	var v int64
	if !rows.Next() {
		return 0, errors.Join(rows.Err(), errors.New("migrate: no schema version"))
	}
	if err := rows.Scan(&v); err != nil {
		return 0, fmt.Errorf("migrate: version: %w", err)
	}
	return int(v), rows.Err()
}
