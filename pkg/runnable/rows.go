package runnable

import (
	"errors"
	"fmt"
)

// MemRows is a query result held in memory.
type MemRows struct {
	cols []string
	rows [][]any
	i    int
}

// NewRows returns the rows of a query result.
func NewRows(cols []string, rows [][]any) *MemRows {
	return &MemRows{cols: cols, rows: rows, i: -1}
}

func (r *MemRows) Columns() []string { return r.cols }
func (r *MemRows) Next() bool        { r.i++; return r.i < len(r.rows) }
func (r *MemRows) Err() error        { return nil }
func (r *MemRows) Close() error      { r.i = len(r.rows); return nil }

// Scan copies the current row into dest: *int64, *int, *float64, *string,
// *[]byte, *bool or *any.
func (r *MemRows) Scan(dest ...any) error {
	if r.i < 0 || r.i >= len(r.rows) {
		return errors.New("runnable: Scan without a row")
	}
	row := r.rows[r.i]
	if len(dest) != len(row) {
		return fmt.Errorf("runnable: Scan of %d columns into %d values", len(row), len(dest))
	}
	for i, d := range dest {
		if err := assign(d, row[i]); err != nil {
			return fmt.Errorf("runnable: column %d: %w", i, err)
		}
	}
	return nil
}

func assign(dest, v any) error {
	switch d := dest.(type) {
	case *any:
		*d = v
		return nil
	case *int64:
		if x, ok := v.(int64); ok {
			*d = x
			return nil
		}
	case *int:
		if x, ok := v.(int64); ok {
			*d = int(x)
			return nil
		}
	case *float64:
		switch x := v.(type) {
		case float64:
			*d = x
			return nil
		case int64:
			*d = float64(x)
			return nil
		}
	case *string:
		switch x := v.(type) {
		case string:
			*d = x
			return nil
		case []byte:
			*d = string(x)
			return nil
		}
	case *[]byte:
		switch x := v.(type) {
		case []byte:
			*d = x
			return nil
		case string:
			*d = []byte(x)
			return nil
		}
	case *bool:
		switch x := v.(type) {
		case bool:
			*d = x
			return nil
		case int64:
			*d = x != 0
			return nil
		}
	}
	return fmt.Errorf("cannot scan %T into %T", v, dest)
}
