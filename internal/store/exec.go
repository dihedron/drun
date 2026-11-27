package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/dihedron/drun/internal/fsm/fsmpb"
	"github.com/mattn/go-sqlite3"
)

// Exec applies a batch the FSM has fenced (STO-1, STO-2): every statement
// or none. A failure that is the same on every node is in the result; an
// error is this node's, after which the node must stop.
func (s *Store) Exec(index uint64, batch *fsmpb.StoreBatch, limits *fsmpb.StoreLimits) (*fsmpb.BatchResult, error) {
	d, err := s.get(batch.GetRunnable(), true)
	if err != nil {
		if checkName(batch.GetRunnable()) != nil {
			return nil, err
		}
		return nil, s.failed(err)
	}
	var results []*fsmpb.StatementResult
	skipped, berr, err := s.change(d, index, func(tx *sql.Tx) (*fsmpb.BatchError, error) {
		var berr *fsmpb.BatchError
		var err error
		results, berr, err = s.run(d, tx, batch, limits)
		return berr, err
	})
	switch {
	case err != nil:
		return nil, err
	case skipped:
		return &fsmpb.BatchResult{}, nil
	case berr != nil:
		return &fsmpb.BatchResult{Error: berr}, nil
	}
	return &fsmpb.BatchResult{Results: results}, nil
}

// change runs fn in a transaction that applies the entry at index once
// (STO-3): an entry not newer than the database's last one is skipped, and
// the entry's index is written in the same transaction as its changes
func (s *Store) change(d *database, index uint64, fn func(*sql.Tx) (*fsmpb.BatchError, error)) (skipped bool, _ *fsmpb.BatchError, _ error) {
	ctx := context.Background()
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, nil, s.failed(fmt.Errorf("store %s: begin: %w", d.name, err))
	}
	var last uint64
	if err := tx.QueryRowContext(ctx, "SELECT last_index FROM _drun_meta").Scan(&last); err != nil {
		_ = tx.Rollback()
		return false, nil, s.failed(fmt.Errorf("store %s: last index: %w", d.name, err))
	}
	if index <= last {
		_ = tx.Rollback()
		return true, nil, nil
	}
	berr, err := fn(tx)
	if err != nil || berr != nil {
		_ = tx.Rollback()
		return false, berr, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE _drun_meta SET last_index = ?", index); err != nil {
		_ = tx.Rollback()
		return false, nil, s.failed(fmt.Errorf("store %s: last index: %w", d.name, err))
	}
	if err := tx.Commit(); err != nil {
		return false, nil, s.failed(fmt.Errorf("store %s: commit: %w", d.name, err))
	}
	return false, nil, nil
}

func orDefault(v, def uint64) uint64 {
	if v == 0 {
		return def
	}
	return v
}

// run executes the batch's statements in tx
func (s *Store) run(d *database, tx *sql.Tx, batch *fsmpb.StoreBatch, limits *fsmpb.StoreLimits) ([]*fsmpb.StatementResult, *fsmpb.BatchError, error) {
	ctx := context.Background()
	maxSize := orDefault(limits.GetMaxSize(), defaultMaxSize)
	maxResult := orDefault(limits.GetMaxResult(), defaultMaxResult)
	maxSteps := orDefault(limits.GetMaxSteps(), defaultMaxSteps)

	// STO-8: the database cannot grow beyond max_size
	var pageSize uint64
	if err := tx.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return nil, nil, s.failed(fmt.Errorf("store %s: page size: %w", d.name, err))
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA max_page_count = %d", maxSize/pageSize)); err != nil {
		return nil, nil, s.failed(fmt.Errorf("store %s: max page count: %w", d.name, err))
	}

	// STO-7: a budget of virtual machine steps, the same on every node
	p := &progress{limit: int64(maxSteps / stepsPerCallback)} //nolint:gosec // bounded by the configuration
	remove, err := setProgress(d.raw, stepsPerCallback, p)
	if err != nil {
		return nil, nil, s.failed(fmt.Errorf("store %s: %w", d.name, err))
	}
	defer remove()

	d.now.Store(batch.GetAt())
	d.epoch.Store(int64(batch.GetEpoch())) //nolint:gosec // epochs fit
	var results []*fsmpb.StatementResult
	resultSize := 0
	for i, st := range batch.GetStatements() {
		r, berr, err := s.statement(d, tx, i, st, maxResult-uint64(resultSize)) //nolint:gosec // non-negative
		if err != nil || berr != nil {
			if berr != nil && sqlite3.ErrNo(berr.GetCode()&0xff) == sqlite3.ErrInterrupt {
				berr.Message = fmt.Sprintf("the batch exceeded store.max_steps (%d)", maxSteps)
			}
			if berr != nil && sqlite3.ErrNo(berr.GetCode()&0xff) == sqlite3.ErrFull {
				if !s.fullByLimit(d, maxSize) {
					return nil, nil, s.failed(fmt.Errorf("store %s: disk full: %s", d.name, berr.GetMessage()))
				}
				berr.Message = fmt.Sprintf("database full: store.max_size is %d bytes", maxSize)
			}
			return nil, berr, err
		}
		for _, row := range r.GetRows() {
			for _, v := range row.GetValues() {
				resultSize += size(v)
			}
		}
		results = append(results, r)
	}
	return results, nil, nil
}

// statement runs one statement of a batch: a deterministic failure is a
// *BatchError; an error is this node's
func (s *Store) statement(d *database, tx *sql.Tx, i int, st *fsmpb.Statement, budget uint64) (*fsmpb.StatementResult, *fsmpb.BatchError, error) {
	ctx := context.Background()
	fail := func(format string, args ...any) (*fsmpb.StatementResult, *fsmpb.BatchError, error) {
		return nil, &fsmpb.BatchError{Statement: int32(i), Code: int32(sqlite3.ErrError), Message: fmt.Sprintf(format, args...)}, nil //nolint:gosec // statement counts are small
	}

	var before int64
	if err := tx.QueryRowContext(ctx, "SELECT total_changes()").Scan(&before); err != nil {
		return nil, nil, s.failed(fmt.Errorf("store %s: %w", d.name, err))
	}
	d.enforce.Store(true)
	n, err := statements(d.raw, st.GetSql())
	if err != nil {
		d.enforce.Store(false)
		return nil, nil, s.failed(fmt.Errorf("store %s: %w", d.name, err))
	}
	if n == 0 {
		d.enforce.Store(false)
		return fail("an empty statement")
	}
	if n > 1 {
		d.enforce.Store(false)
		return fail("a statement must hold one statement, not several")
	}
	args := make([]any, len(st.GetArgs()))
	for j, a := range st.GetArgs() {
		args[j] = fromValue(a)
	}
	result := &fsmpb.StatementResult{}
	used := uint64(0)
	err = func() error {
		defer d.enforce.Store(false)
		rows, err := tx.QueryContext(ctx, st.GetSql(), args...)
		if err != nil {
			return err
		}
		defer rows.Close() //nolint:errcheck // the error comes from rows.Err
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for k := range vals {
				ptrs[k] = &vals[k]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			row := &fsmpb.Row{}
			for _, v := range vals {
				pv := ToValue(v)
				used += uint64(size(pv)) //nolint:gosec // sizes are non-negative
				row.Values = append(row.Values, pv)
			}
			if used > budget {
				return errResultTooLarge
			}
			result.Rows = append(result.Rows, row)
		}
		return rows.Err()
	}()
	if errors.Is(err, errResultTooLarge) {
		return fail("the result is larger than store.max_result")
	}
	if err != nil {
		return s.classify(d, i, err)
	}

	var after, changes, lastID int64
	if err := tx.QueryRowContext(ctx, "SELECT total_changes(), changes(), last_insert_rowid()").Scan(&after, &changes, &lastID); err != nil {
		return nil, nil, s.failed(fmt.Errorf("store %s: %w", d.name, err))
	}
	if after != before {
		result.RowsAffected = changes
	}
	result.LastInsertId = lastID

	// STO-6: expectations on the rows affected
	if e := st.GetExpect(); e != nil {
		if e.RowsAffected != nil && result.RowsAffected != e.GetRowsAffected() {
			return fail("expected %d rows affected, got %d", e.GetRowsAffected(), result.RowsAffected)
		}
		if e.MinRowsAffected != nil && result.RowsAffected < e.GetMinRowsAffected() {
			return fail("expected at least %d rows affected, got %d", e.GetMinRowsAffected(), result.RowsAffected)
		}
	}
	return result, nil, nil
}

var errResultTooLarge = errors.New("result too large")

// classify tells a failure that is the same on every node (SQL, limits,
// refusals) from one of this node (I/O, corruption)
func (s *Store) classify(d *database, i int, err error) (*fsmpb.StatementResult, *fsmpb.BatchError, error) {
	var se sqlite3.Error
	if errors.As(err, &se) {
		switch se.Code {
		case sqlite3.ErrError, sqlite3.ErrConstraint, sqlite3.ErrMismatch, sqlite3.ErrRange, sqlite3.ErrTooBig,
			sqlite3.ErrInterrupt, sqlite3.ErrAuth, sqlite3.ErrAbort, sqlite3.ErrFull:
			return nil, &fsmpb.BatchError{Statement: int32(i), Code: int32(se.ExtendedCode), Message: se.Error()}, nil //nolint:gosec // small values
		}
	}
	return nil, nil, s.failed(fmt.Errorf("store %s: statement %d: %w", d.name, i, err))
}

// fullByLimit reports whether SQLITE_FULL came from max_page_count (the
// same on every node) rather than from the disk.
// shortcut: judged from the free space of this node's filesystem; a disk
// that fills at the same moment as the limit is reached counts as the limit
func (s *Store) fullByLimit(d *database, maxSize uint64) bool {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &fs); err != nil {
		return false
	}
	fi, err := os.Stat(d.path)
	if err != nil {
		return false
	}
	free := fs.Bavail * uint64(fs.Bsize)                  //nolint:gosec // block sizes are positive
	return free > maxSize-min(maxSize, uint64(fi.Size())) //nolint:gosec // sizes are positive
}
