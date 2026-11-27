// Package store is the runnables' replicated SQL stores (resurgo-store.md):
// one SQLite database per runnable, written only by the FSM, one batch per
// Raft entry, each applied exactly once and deterministically on every
// node. It implements fsm.Store.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
)

// the defaults of the limits (resurgo-store.md STO-7 to STO-9), for a
// runnable whose Incarnate carried none
const (
	defaultMaxSteps  = 50_000_000
	defaultMaxSize   = 256 << 20
	defaultMaxResult = 1 << 20
	// stepsPerCallback is how often the progress handler runs (STO-7)
	stepsPerCallback = 1000
)

// a runnable's name is its database's file name
var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Store is the stores of every runnable of a node.
type Store struct {
	dir    string
	fatal  func(error)
	logger *slog.Logger
	// builtin evaluates SQLite's own date and time functions, for the
	// overrides (STO-4)
	builtin *sql.DB

	mu  sync.Mutex
	dbs map[string]*database
}

// database is one runnable's database and its writer connection
type database struct {
	name, path string
	db         *sql.DB
	conn       *sql.Conn
	raw        *sqlite3.SQLiteConn

	// enforce is set while runnable SQL runs: the authorizer applies the
	// rules of STO-4 only then, not to the store's own statements
	enforce atomic.Bool
	// now and epoch are resurgo_now() and resurgo_epoch() (STO-5)
	now, epoch atomic.Int64

	roOnce sync.Once
	ro     *reader
	roErr  error
}

// Open opens the stores in dir (data_dir/store), created 0700 if missing.
// fatal is called when a database fails in a way that is not the same on
// every node (I/O): the node must then stop rather than carry on behind
// the others.
func Open(dir string, fatal func(error), logger *slog.Logger) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store directory: %w", err)
	}
	builtin, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	builtin.SetMaxOpenConns(1)
	return &Store{dir: dir, fatal: fatal, logger: logger, builtin: builtin, dbs: map[string]*database{}}, nil
}

// Close closes every database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for name, d := range s.dbs {
		errs = append(errs, d.close())
		delete(s.dbs, name)
	}
	errs = append(errs, s.builtin.Close())
	return errors.Join(errs...)
}

func (s *Store) path(name string) string { return filepath.Join(s.dir, name+".db") }

func checkName(name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("store: invalid runnable name %q", name)
	}
	return nil
}

// get returns the runnable's database, opening it; create says whether a
// missing one is created
func (s *Store) get(name string, create bool) (*database, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.dbs[name]; ok {
		return d, nil
	}
	if _, err := os.Stat(s.path(name)); errors.Is(err, os.ErrNotExist) && !create {
		return nil, nil
	}
	d, err := s.open(name)
	if err != nil {
		return nil, err
	}
	s.dbs[name] = d
	return d, nil
}

func (s *Store) open(name string) (*database, error) { return s.openPath(name, s.path(name)) }

// openPath opens a database at path as the runnable's: the store's rules
// and schema
func (s *Store) openPath(name, path string) (*database, error) {
	d := &database{name: name, path: path}
	db, err := sql.Open("sqlite3", "file:"+d.path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_txlock=immediate&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("store %s: %w", name, err)
	}
	db.SetMaxOpenConns(1)
	d.db = db
	if d.conn, err = db.Conn(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store %s: %w", name, err)
	}
	if d.raw, err = rawConn(d.conn); err != nil {
		_ = d.close()
		return nil, fmt.Errorf("store %s: %w", name, err)
	}
	if err := s.register(d.raw, &d.enforce, &d.now, &d.epoch); err != nil {
		_ = d.close()
		return nil, fmt.Errorf("store %s: %w", name, err)
	}
	if _, err := d.conn.ExecContext(context.Background(), schema); err != nil {
		_ = d.close()
		return nil, fmt.Errorf("store %s: %w", name, err)
	}
	if err := os.Chmod(d.path, 0o600); err != nil {
		_ = d.close()
		return nil, fmt.Errorf("store %s: %w", name, err)
	}
	return d, nil
}

func (d *database) close() error {
	var errs []error
	if d.ro != nil {
		errs = append(errs, d.ro.close())
	}
	if d.conn != nil {
		errs = append(errs, d.conn.Close())
	}
	return errors.Join(append(errs, d.db.Close())...)
}

// rawConn returns the driver connection under a *sql.Conn
func rawConn(conn *sql.Conn) (*sqlite3.SQLiteConn, error) {
	var raw *sqlite3.SQLiteConn
	err := conn.Raw(func(dc any) error {
		var ok bool
		if raw, ok = dc.(*sqlite3.SQLiteConn); !ok {
			return fmt.Errorf("driver connection is %T", dc)
		}
		return nil
	})
	return raw, err
}

// names returns the runnables with a database, open or on disk, sorted
func (s *Store) names() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("store directory: %w", err)
	}
	var names []string
	for _, e := range entries {
		if name, ok := strings.CutSuffix(e.Name(), ".db"); ok && validName.MatchString(name) && e.Type().IsRegular() {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// failed reports a failure that is not deterministic: the node must stop
func (s *Store) failed(err error) error {
	s.logger.Error("store failure: the node must stop", "error", err)
	s.fatal(err)
	return err
}
