// Package runnable is the SDK of drun's runnables. This file holds the
// types of the runnable's store (drun-store.md §3); the rest of the SDK
// (the Runnable interface, the plugin server and loader) comes with it.
package runnable

import (
	"context"
	"errors"
	"fmt"
)

// Statement is one SQL statement of a batch, with its parameters.
type Statement struct {
	SQL string
	// Args are int64, float64, string, []byte, bool or nil.
	Args []any
	// Expect, if set, makes the batch fail unless it holds (STO-6).
	Expect *Expectation
}

// Expectation is a condition on the rows a statement affects.
type Expectation struct {
	// RowsAffected: exactly this many rows.
	RowsAffected *int64
	// MinRowsAffected: at least this many rows.
	MinRowsAffected *int64
}

// Exactly expects n rows affected.
func Exactly(n int64) *Expectation { return &Expectation{RowsAffected: &n} }

// AtLeast expects at least n rows affected.
func AtLeast(n int64) *Expectation { return &Expectation{MinRowsAffected: &n} }

// Result is what a statement of a committed batch returned.
type Result struct {
	RowsAffected int64
	LastInsertID int64
	// Rows are the RETURNING rows, bounded by store.max_result (STO-9).
	Rows [][]any
}

// Rows is the result of a query.
type Rows interface {
	Columns() []string
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Store is the runnable's replicated SQL store.
type Store interface {
	// Exec replicates a batch and applies it atomically; it returns once a
	// quorum has committed it and the leader has applied it. ErrFenced: the
	// incarnation is stale; *BatchError: the batch was rolled back, on
	// every node.
	Exec(ctx context.Context, batch []Statement) ([]Result, error)
	// Query reads the leader's local copy, read-only.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// ErrFenced: the incarnation is no longer the current one; Run must return.
var ErrFenced = errors.New("fenced: the incarnation is no longer current")

// BatchError is why a batch was rolled back, the same on every node.
type BatchError struct {
	// Statement is the index of the statement that failed.
	Statement int
	// Code is the SQLite (extended) result code.
	Code    int
	Message string
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("statement %d: %s (code %d)", e.Statement, e.Message, e.Code)
}
