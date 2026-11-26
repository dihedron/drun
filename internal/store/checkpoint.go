package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

// PutCheckpoint replaces the runnable's checkpoint (STO-12).
func (s *Store) PutCheckpoint(index uint64, runnable string, meta *fsmpb.CheckpointMeta, data []byte) error {
	d, err := s.get(runnable, true)
	if err != nil {
		return s.failed(err)
	}
	_, _, err = s.change(d, index, func(tx *sql.Tx) (*fsmpb.BatchError, error) {
		_, err := tx.ExecContext(context.Background(), "INSERT OR REPLACE INTO _resurgo_checkpoint VALUES (1, ?, ?, ?, ?)",
			meta.GetSeq(), meta.GetEpoch(), meta.GetCommittedAt(), data)
		if err != nil {
			return nil, s.failed(fmt.Errorf("store %s: checkpoint: %w", runnable, err))
		}
		return nil, nil
	})
	return err
}

// ClearCheckpoint removes the runnable's checkpoint.
func (s *Store) ClearCheckpoint(index uint64, runnable string) error {
	d, err := s.get(runnable, false)
	if err != nil {
		return s.failed(err)
	}
	if d == nil {
		return nil
	}
	_, _, err = s.change(d, index, func(tx *sql.Tx) (*fsmpb.BatchError, error) {
		if _, err := tx.ExecContext(context.Background(), "DELETE FROM _resurgo_checkpoint"); err != nil {
			return nil, s.failed(fmt.Errorf("store %s: checkpoint: %w", runnable, err))
		}
		return nil, nil
	})
	return err
}

// Checkpoint returns the runnable's checkpoint, given to each new
// incarnation (FR-10).
func (s *Store) Checkpoint(runnable string) (*fsmpb.CheckpointMeta, []byte, bool, error) {
	r, err := s.reader(runnable)
	if err != nil || r == nil {
		return nil, nil, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta := &fsmpb.CheckpointMeta{}
	var data []byte
	err = r.conn.QueryRowContext(context.Background(), "SELECT seq, epoch, committed_at, data FROM _resurgo_checkpoint").
		Scan(&meta.Seq, &meta.Epoch, &meta.CommittedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("store %s: checkpoint: %w", runnable, err)
	}
	meta.Size = uint64(len(data))
	return meta, data, true, nil
}

// Forget removes the runnable's database, unless it was created again
// after the entry at index (a Forget applied again on restart).
func (s *Store) Forget(index uint64, runnable string) error {
	d, err := s.get(runnable, false)
	if err != nil {
		return s.failed(err)
	}
	if d == nil {
		return nil
	}
	var last uint64
	if err := d.conn.QueryRowContext(context.Background(), "SELECT last_index FROM _resurgo_meta").Scan(&last); err != nil {
		return s.failed(fmt.Errorf("store %s: last index: %w", runnable, err))
	}
	if last >= index {
		return nil
	}
	s.mu.Lock()
	delete(s.dbs, runnable)
	s.mu.Unlock()
	if err := d.close(); err != nil {
		return s.failed(fmt.Errorf("store %s: close: %w", runnable, err))
	}
	if err := removeDatabase(d.path); err != nil {
		return s.failed(err)
	}
	return nil
}

// removeDatabase removes a database file and its WAL and shared memory
func removeDatabase(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("store: %w", err)
		}
	}
	return nil
}
