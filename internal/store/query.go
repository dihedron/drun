package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
)

// reader is a database's read-only connection: queries (STO-10), the
// checkpoint and digests, never inside an apply's transaction.
// shortcut: one connection per database, so reads of one runnable run one
// at a time; a pool if reads become the bottleneck
type reader struct {
	mu         sync.Mutex
	db         *sql.DB
	conn       *sql.Conn
	enforce    atomic.Bool
	now, epoch atomic.Int64
}

func (r *reader) close() error {
	return errors.Join(r.conn.Close(), r.db.Close())
}

// reader returns the runnable's reader, nil if it has no database
func (s *Store) reader(runnable string) (*reader, error) {
	d, err := s.get(runnable, false)
	if err != nil || d == nil {
		return nil, err
	}
	d.roOnce.Do(func() {
		r := &reader{}
		db, err := sql.Open("sqlite3", "file:"+d.path+"?mode=ro&_query_only=1&_busy_timeout=5000")
		if err != nil {
			d.roErr = err
			return
		}
		db.SetMaxOpenConns(1)
		r.db = db
		if r.conn, err = db.Conn(context.Background()); err != nil {
			_ = db.Close()
			d.roErr = err
			return
		}
		raw, err := rawConn(r.conn)
		if err == nil {
			err = s.register(raw, &r.enforce, &r.now, &r.epoch)
		}
		if err != nil {
			_ = r.close()
			d.roErr = err
			return
		}
		d.ro = r
	})
	if d.roErr != nil {
		return nil, fmt.Errorf("store %s: reader: %w", runnable, d.roErr)
	}
	return d.ro, nil
}

// Query runs a read-only statement on the local database (STO-10), under
// the same rules as a batch; ctx bounds its time (STO-11), and its result
// is bounded by the default store.max_result.
func (s *Store) Query(ctx context.Context, runnable, query string, args ...any) ([]string, [][]any, error) {
	r, err := s.reader(runnable)
	if err != nil {
		return nil, nil, err
	}
	if r == nil {
		return nil, nil, fmt.Errorf("store %s: no database", runnable)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enforce.Store(true)
	defer r.enforce.Store(false)
	rows, err := r.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("store %s: query: %w", runnable, err)
	}
	defer rows.Close() //nolint:errcheck // the error comes from rows.Err
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]any
	used := 0
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		for _, v := range vals {
			used += size(toValue(v))
		}
		if used > defaultMaxResult {
			return nil, nil, fmt.Errorf("store %s: the result is larger than %d bytes", runnable, defaultMaxResult)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store %s: query: %w", runnable, err)
	}
	return cols, out, nil
}

// Digest returns a SHA-256 of the database's logical content (schema and
// rows, in a fixed order) and the index it is at (STO-15, INV-S-1).
func (s *Store) Digest(runnable string) (string, uint64, error) {
	r, err := s.reader(runnable)
	if err != nil || r == nil {
		return "", 0, errors.Join(err, errors.New("no database"))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ctx := context.Background()
	tx, err := r.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", 0, err
	}
	defer tx.Rollback() //nolint:errcheck // read only

	var index uint64
	if err := tx.QueryRowContext(ctx, "SELECT last_index FROM _resurgo_meta").Scan(&index); err != nil {
		return "", 0, fmt.Errorf("store %s: digest: %w", runnable, err)
	}
	h := sha256.New()
	rows, err := tx.QueryContext(ctx, "SELECT type, name, coalesce(sql, '') FROM sqlite_schema ORDER BY type, name")
	if err != nil {
		return "", 0, err
	}
	var tables []string
	for rows.Next() {
		var typ, name, text string
		if err := rows.Scan(&typ, &name, &text); err != nil {
			_ = rows.Close()
			return "", 0, err
		}
		writeField(h, typ, name, text)
		if typ == "table" && !strings.HasPrefix(name, "sqlite_") {
			tables = append(tables, name)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return "", 0, err
	}
	for _, t := range tables {
		if err := digestTable(ctx, tx, h, t); err != nil {
			return "", 0, fmt.Errorf("store %s: digest of %s: %w", runnable, t, err)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), index, nil
}

func digestTable(ctx context.Context, tx *sql.Tx, h interface{ Write([]byte) (int, error) }, table string) error {
	quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
	var ncols int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info(?)", table).Scan(&ncols); err != nil {
		return err
	}
	order := make([]string, ncols)
	for i := range order {
		order[i] = fmt.Sprint(i + 1)
	}
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+quoted+" ORDER BY "+strings.Join(order, ", ")) //nolint:gosec // a quoted table name
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck // the error comes from rows.Err
	writeField(h, "rows", table)
	for rows.Next() {
		vals := make([]any, ncols)
		ptrs := make([]any, ncols)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		for _, v := range vals {
			writeValue(h, v)
		}
	}
	return rows.Err()
}

// writeField writes length-prefixed strings, so that fields cannot run
// into each other
func writeField(h interface{ Write([]byte) (int, error) }, fields ...string) {
	for _, f := range fields {
		_, _ = h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(f))))
		_, _ = h.Write([]byte(f))
	}
}

func writeValue(h interface{ Write([]byte) (int, error) }, v any) {
	switch x := v.(type) {
	case nil:
		_, _ = h.Write([]byte{0})
	case int64:
		_, _ = h.Write(binary.BigEndian.AppendUint64([]byte{1}, uint64(x))) //nolint:gosec // bit pattern
	case float64:
		_, _ = h.Write(binary.BigEndian.AppendUint64([]byte{2}, math.Float64bits(x)))
	case []byte:
		_, _ = h.Write([]byte{3})
		writeField(h, string(x))
	default:
		_, _ = h.Write([]byte{4})
		writeField(h, fmt.Sprint(x))
	}
}
