package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dihedron/resurgo/internal/fsm"
)

// the store's part of a snapshot is a series of frames: the length of the
// runnable's name (uint32), the name, the size of the database (uint64),
// the database; a frame with an empty name ends it. Integers are big-endian.
const maxDatabaseSize = 64 << 30

// Snapshot lists the databases; their copies are made when the snapshot is
// written, while applies continue (STO-14). A copy may hold entries the
// FSM's state in the same snapshot does not: each copy carries its own last
// index, so they are skipped when replayed (STO-3).
func (s *Store) Snapshot() (fsm.StoreSnapshot, error) {
	names, err := s.names()
	if err != nil {
		return nil, err
	}
	return &snapshot{s: s, names: names}, nil
}

type snapshot struct {
	s     *Store
	names []string
}

func (sn *snapshot) Release() {}

func (sn *snapshot) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for _, name := range sn.names {
		n, err := sn.s.copyTo(w, name)
		total += n
		if err != nil {
			return total, err
		}
	}
	n, err := writeFrame(w, "", nil, 0)
	return total + n, err
}

// copyTo writes a consistent copy of the database as a frame
func (s *Store) copyTo(w io.Writer, name string) (int64, error) {
	tmp, err := s.vacuumInto(name)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil // forgotten since the snapshot began
	}
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp)   //nolint:errcheck // a temporary file
	f, err := os.Open(tmp) //nolint:gosec // our temporary file
	if err != nil {
		return 0, fmt.Errorf("store %s: copy: %w", name, err)
	}
	defer f.Close() //nolint:errcheck // read only
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("store %s: copy: %w", name, err)
	}
	return writeFrame(w, name, f, fi.Size())
}

// vacuumInto writes a consistent copy of the database to a temporary file
// in the store's directory, which the caller removes; os.ErrNotExist if
// the runnable has no database
func (s *Store) vacuumInto(name string) (string, error) {
	path := s.path(name)
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("store %s: %w", name, err)
	}
	tmp := s.tempPath(name)
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return "", fmt.Errorf("store %s: copy: %w", name, err)
	}
	_, err = db.ExecContext(context.Background(), "VACUUM INTO ?", tmp)
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("store %s: copy: %w", name, err)
	}
	return tmp, nil
}

func (s *Store) tempPath(name string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return filepath.Join(s.dir, "."+name+"."+hex.EncodeToString(b[:])+".tmp")
}

func writeFrame(w io.Writer, name string, r io.Reader, size int64) (int64, error) {
	header := binary.BigEndian.AppendUint32(nil, uint32(len(name))) //nolint:gosec // names are short
	header = append(header, name...)
	header = binary.BigEndian.AppendUint64(header, uint64(size)) //nolint:gosec // sizes are positive
	n, err := w.Write(header)
	if err != nil || r == nil {
		return int64(n), err
	}
	m, err := io.CopyN(w, r, size)
	return int64(n) + m, err
}

// Restore replaces the databases with a snapshot's. It reads the whole
// snapshot into temporary files first, so that a bad one changes nothing;
// a local database at least as recent as the snapshot's copy is kept (no
// full copy at every restart); a database not in the snapshot is removed.
func (s *Store) Restore(r io.Reader) error {
	staged := map[string]string{}
	defer func() {
		for _, tmp := range staged {
			_ = os.Remove(tmp)
		}
	}()
	for {
		name, tmp, err := s.readFrame(r)
		if err != nil {
			return fmt.Errorf("store: restore: %w", err)
		}
		if name == "" {
			break
		}
		if _, dup := staged[name]; dup {
			_ = os.Remove(tmp)
			return fmt.Errorf("store: restore: %q twice", name)
		}
		staged[name] = tmp
	}
	indexes := map[string]uint64{}
	for name, tmp := range staged {
		index, err := lastIndexOf(tmp)
		if err != nil {
			return fmt.Errorf("store: restore %s: %w", name, err)
		}
		indexes[name] = index
	}

	// the snapshot is good: apply it
	for name, tmp := range staged {
		if local, err := s.localIndex(name); err == nil && local >= indexes[name] {
			continue
		}
		if err := s.replace(name, tmp); err != nil {
			return err
		}
		delete(staged, name)
	}
	names, err := s.names()
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, ok := indexes[name]; !ok {
			if err := s.drop(name); err != nil {
				return err
			}
		}
	}
	return syncDir(s.dir)
}

// readFrame reads one frame into a temporary file, synced
func (s *Store) readFrame(r io.Reader) (string, string, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", "", err
	}
	if n > 64 {
		return "", "", fmt.Errorf("name of %d bytes", n)
	}
	nameBytes := make([]byte, n)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return "", "", err
	}
	var size uint64
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return "", "", err
	}
	name := string(nameBytes)
	if name == "" {
		return "", "", nil
	}
	if err := checkName(name); err != nil {
		return "", "", err
	}
	if size > maxDatabaseSize {
		return "", "", fmt.Errorf("%s: database of %d bytes", name, size)
	}
	tmp := s.tempPath(name)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // our temporary file
	if err != nil {
		return "", "", err
	}
	_, err = io.CopyN(f, r, int64(size))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", "", fmt.Errorf("%s: %w", name, err)
	}
	return name, tmp, nil
}

func lastIndexOf(path string) (uint64, error) {
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		return 0, err
	}
	defer db.Close() //nolint:errcheck // read only
	var index uint64
	if err := db.QueryRowContext(context.Background(), "SELECT last_index FROM _resurgo_meta").Scan(&index); err != nil {
		return 0, fmt.Errorf("not a store database: %w", err)
	}
	return index, nil
}

func (s *Store) localIndex(name string) (uint64, error) {
	if _, err := os.Stat(s.path(name)); err != nil {
		return 0, err
	}
	return lastIndexOf(s.path(name))
}

// replace puts tmp in the place of the runnable's database
func (s *Store) replace(name, tmp string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.dbs[name]; ok {
		delete(s.dbs, name)
		if err := d.close(); err != nil {
			return fmt.Errorf("store %s: close: %w", name, err)
		}
	}
	path := s.path(name)
	if err := removeDatabase(path); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("store %s: restore: %w", name, err)
	}
	return nil
}

// drop closes and removes the runnable's database
func (s *Store) drop(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.dbs[name]; ok {
		delete(s.dbs, name)
		if err := d.close(); err != nil {
			return fmt.Errorf("store %s: close: %w", name, err)
		}
	}
	return removeDatabase(s.path(name))
}

func syncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // the store directory
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read only
	return f.Sync()
}
