package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

// Split splits sql into its statements, each ending where SQLite's
// tokenizer says one is complete (a ';' outside literals, comments and
// trigger bodies); text after the last one that is only whitespace or
// comments is dropped, any other is the last statement.
func Split(sql string) []string {
	var out []string
	start := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] == ';' && complete(sql[start:i+1]) {
			out = append(out, strings.TrimSpace(sql[start:i+1]))
			start = i + 1
		}
	}
	if rest := strings.TrimSpace(sql[start:]); rest != "" && !onlyComments(rest) {
		out = append(out, rest)
	}
	return out
}

// onlyComments: text that is comments and whitespace (complete() of it
// with a ';' appended is a statement only if it holds one)
func onlyComments(s string) bool {
	for s = strings.TrimSpace(s); s != ""; s = strings.TrimSpace(s) {
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return true
			}
			s = s[i+1:]
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return true
			}
			s = s[i+2:]
		default:
			return false
		}
	}
	return true
}

// Info describes a runnable's database on this node (STO-16).
type Info struct {
	Exists    bool
	Size      int64 // bytes, with its write-ahead log
	Tables    []string
	LastIndex uint64
}

// Info returns the runnable's database's size, tables and last index.
func (s *Store) Info(runnable string) (Info, error) {
	r, err := s.reader(runnable)
	if err != nil || r == nil {
		return Info{}, err
	}
	var info Info
	info.Exists = true
	for _, suffix := range []string{"", "-wal"} {
		if fi, err := os.Stat(s.path(runnable) + suffix); err == nil {
			info.Size += fi.Size()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ctx := context.Background()
	if err := r.conn.QueryRowContext(ctx, "SELECT last_index FROM _resurgo_meta").Scan(&info.LastIndex); err != nil {
		return Info{}, fmt.Errorf("store %s: last index: %w", runnable, err)
	}
	rows, err := r.conn.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_resurgo\\_%' ESCAPE '\\' ORDER BY name")
	if err != nil {
		return Info{}, fmt.Errorf("store %s: tables: %w", runnable, err)
	}
	defer rows.Close() //nolint:errcheck // read only
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return Info{}, fmt.Errorf("store %s: tables: %w", runnable, err)
		}
		info.Tables = append(info.Tables, name)
	}
	return info, rows.Err()
}

// Backup writes a consistent copy of the runnable's database to w.
func (s *Store) Backup(runnable string, w io.Writer) (int64, error) {
	if err := checkName(runnable); err != nil {
		return 0, err
	}
	tmp, err := s.vacuumInto(runnable)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp)   //nolint:errcheck // a temporary file
	f, err := os.Open(tmp) //nolint:gosec // our temporary file
	if err != nil {
		return 0, fmt.Errorf("store %s: backup: %w", runnable, err)
	}
	defer f.Close() //nolint:errcheck // read only
	return io.Copy(w, f)
}

// DryRun runs a batch under the store's rules and limits in a transaction
// that is rolled back, on a copy of the database, and returns its results
// (STO-17 --dry-run). Nothing it does can stop the node.
func (s *Store) DryRun(batch *fsmpb.StoreBatch, limits *fsmpb.StoreLimits) (*fsmpb.BatchResult, error) {
	name := batch.GetRunnable()
	if err := checkName(name); err != nil {
		return nil, err
	}
	tmp, err := s.vacuumInto(name)
	if errors.Is(err, os.ErrNotExist) {
		tmp, err = s.tempPath(name), nil // an empty database, as the first batch would see
	}
	if err != nil {
		return nil, err
	}
	defer removeDatabase(tmp) //nolint:errcheck // temporary files
	var failure error
	dry := &Store{dir: s.dir, logger: s.logger, builtin: s.builtin, fatal: func(err error) { failure = err }}
	d, err := dry.openPath(name, tmp)
	if err != nil {
		return nil, err
	}
	defer d.close() //nolint:errcheck // a copy
	tx, err := d.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("store %s: dry run: %w", name, err)
	}
	defer tx.Rollback() //nolint:errcheck // always rolled back
	results, berr, err := dry.run(d, tx, batch, limits)
	if err = errors.Join(err, failure); err != nil {
		return nil, fmt.Errorf("store %s: dry run: %w", name, err)
	}
	if berr != nil {
		return &fsmpb.BatchResult{Error: berr}, nil
	}
	return &fsmpb.BatchResult{Results: results}, nil
}
