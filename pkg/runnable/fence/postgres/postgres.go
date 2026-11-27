// Package postgres fences a runnable's writes to an external PostgreSQL
// database by its epoch (drun_specification.md §8.5, FW-10): once a newer
// incarnation has claimed the fence, every transaction of an older one is
// refused with runnable.ErrFenced, whatever the timing of the two.
//
//	f, err := postgres.Claim(ctx, db, inc) // at the start of Run
//	...
//	err = f.Tx(ctx, db, func(tx pgx.Tx) error { /* the writes */ })
//	if errors.Is(err, runnable.ErrFenced) { return err } // Run must end
//
// Only the writes made through Tx (or after Check, in the caller's own
// transaction) are fenced.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/dihedron/drun/pkg/runnable"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Schema is the fence table, created by Claim if it does not exist; a role
// without CREATE can use one an administrator created from it.
const Schema = `CREATE TABLE IF NOT EXISTS drun_fence (
	cluster text NOT NULL,
	runnable text NOT NULL,
	epoch bigint NOT NULL,
	PRIMARY KEY (cluster, runnable))`

// DB is a *pgx.Conn, a *pgxpool.Pool or a pgx.Tx.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Fence is an incarnation's claim on the fence.
type Fence struct {
	cluster, runnable string
	epoch             int64
}

// Claim creates the fence table if needed and claims the fence for inc's
// epoch; it returns runnable.ErrFenced if a newer epoch holds it. The
// claim waits for the open transactions that passed a Check of an older
// epoch, and those that follow it are refused.
func Claim(ctx context.Context, db DB, inc runnable.Incarnation) (*Fence, error) {
	if inc.Epoch > math.MaxInt64 {
		return nil, fmt.Errorf("fence: epoch %d does not fit a bigint", inc.Epoch)
	}
	f := &Fence{cluster: inc.Cluster, runnable: inc.Runnable, epoch: int64(inc.Epoch)}
	if _, err := db.Exec(ctx, Schema); err != nil {
		// two first-ever claims race on CREATE TABLE IF NOT EXISTS: the loser
		// sees a duplicate, and the table is there on the retry
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || (pe.Code != "23505" && pe.Code != "42P07") {
			return nil, fmt.Errorf("fence: creating the table: %w", err)
		}
		if _, err := db.Exec(ctx, Schema); err != nil {
			return nil, fmt.Errorf("fence: creating the table: %w", err)
		}
	}
	tag, err := db.Exec(ctx, `INSERT INTO drun_fence (cluster, runnable, epoch) VALUES ($1, $2, $3)
		ON CONFLICT (cluster, runnable) DO UPDATE SET epoch = EXCLUDED.epoch
		WHERE drun_fence.epoch <= EXCLUDED.epoch`, f.cluster, f.runnable, f.epoch)
	if err != nil {
		return nil, fmt.Errorf("fence: claiming: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, runnable.ErrFenced
	}
	return f, nil
}

// Check returns runnable.ErrFenced unless the fence still holds this
// epoch; it takes the fence's row FOR SHARE until tx ends, so call it in
// tx before the writes.
func (f *Fence) Check(ctx context.Context, tx pgx.Tx) error {
	var epoch int64
	err := tx.QueryRow(ctx, `SELECT epoch FROM drun_fence WHERE cluster = $1 AND runnable = $2 FOR SHARE`,
		f.cluster, f.runnable).Scan(&epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("fence: no fence for %s/%s: was it claimed?", f.cluster, f.runnable)
	case err != nil:
		return fmt.Errorf("fence: checking: %w", err)
	case epoch != f.epoch:
		return runnable.ErrFenced
	}
	return nil
}

// Tx runs fn in a transaction that passed Check and commits it; it rolls
// back on any error. runnable.ErrFenced, from Check or from fn, is
// returned as it is.
func (f *Fence) Tx(ctx context.Context, db DB, fn func(pgx.Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("fence: begin: %w", err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // a no-op after the commit
	if err := f.Check(ctx, tx); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if errors.Is(err, runnable.ErrFenced) {
			return runnable.ErrFenced
		}
		return fmt.Errorf("fence: transaction: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("fence: commit: %w", err)
	}
	return nil
}
