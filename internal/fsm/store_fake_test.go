package fsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

// fakeStore keeps checkpoints and a count of applied batches per runnable,
// and fails on demand
type fakeStore struct {
	checkpoints map[string][]byte
	batches     map[string]int
	forgotten   []string
	fail        error
}

func newFakeStore() *fakeStore {
	return &fakeStore{checkpoints: map[string][]byte{}, batches: map[string]int{}}
}

func (s *fakeStore) Exec(_ uint64, batch *fsmpb.StoreBatch, _ *fsmpb.StoreLimits) (*fsmpb.BatchResult, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	s.batches[batch.GetRunnable()]++
	r := &fsmpb.BatchResult{}
	for range batch.GetStatements() {
		r.Results = append(r.Results, &fsmpb.StatementResult{RowsAffected: 1})
	}
	return r, nil
}

func (s *fakeStore) PutCheckpoint(_ uint64, runnable string, _ *fsmpb.CheckpointMeta, data []byte) error {
	if s.fail != nil {
		return s.fail
	}
	s.checkpoints[runnable] = bytes.Clone(data)
	return nil
}

func (s *fakeStore) ClearCheckpoint(_ uint64, runnable string) error {
	if s.fail != nil {
		return s.fail
	}
	delete(s.checkpoints, runnable)
	return nil
}

func (s *fakeStore) Forget(_ uint64, runnable string) error {
	if s.fail != nil {
		return s.fail
	}
	delete(s.checkpoints, runnable)
	delete(s.batches, runnable)
	s.forgotten = append(s.forgotten, runnable)
	return nil
}

// Snapshot encodes the checkpoints, sorted by runnable: name length, name,
// data length, data
func (s *fakeStore) Snapshot() (StoreSnapshot, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	var buf bytes.Buffer
	for _, name := range slices.Sorted(maps.Keys(s.checkpoints)) {
		for _, field := range [][]byte{[]byte(name), s.checkpoints[name]} {
			_ = binary.Write(&buf, binary.BigEndian, uint32(len(field))) //nolint:gosec // test sizes
			buf.Write(field)
		}
	}
	return fakeSnapshot(buf.Bytes()), nil
}

func (s *fakeStore) Restore(r io.Reader) error {
	if s.fail != nil {
		return s.fail
	}
	restored := map[string][]byte{}
	for {
		var fields [2][]byte
		for i := range fields {
			var n uint32
			if err := binary.Read(r, binary.BigEndian, &n); err != nil {
				if i == 0 && errors.Is(err, io.EOF) {
					s.checkpoints = restored
					return nil
				}
				return fmt.Errorf("fake store: %w", err)
			}
			if n > 1<<24 {
				return errors.New("fake store: field too large")
			}
			fields[i] = make([]byte, n)
			if _, err := io.ReadFull(r, fields[i]); err != nil {
				return fmt.Errorf("fake store: %w", err)
			}
		}
		restored[string(fields[0])] = fields[1]
	}
}

type fakeSnapshot []byte

func (s fakeSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(s)
	return int64(n), err
}

func (fakeSnapshot) Release() {}
