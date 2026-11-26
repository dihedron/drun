package fsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// A snapshot is: snapshotMagic, the format version (uint32), the length of
// the encoded State (uint64), the State, then the Store's part (STO-14).
// Integers are big-endian.
const snapshotMagic = "RSGO"

// maxStateSize bounds the State read back, so that a corrupt length cannot
// exhaust memory; the State holds metadata only.
const maxStateSize = 64 << 20

// encodeState encodes the state deterministically: runnables sorted by
// name, failures already sorted by node (INV-7)
func (f *FSM) encodeState() []byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.encodeStateLocked()
}

func (f *FSM) encodeStateLocked() []byte {
	state := &fsmpb.State{FormatVersion: FormatVersion, EpochCounter: f.epochCounter}
	for _, name := range slices.Sorted(maps.Keys(f.runnables)) {
		state.Runnables = append(state.Runnables, f.runnables[name])
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(state)
	if err != nil {
		// the state is built only from decoded messages: it always encodes
		panic(fmt.Sprintf("fsm: encoding the state: %v", err))
	}
	return data
}

// Snapshot copies the state and asks the Store for a copy of the
// databases; raft calls it with applies stopped.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	stored, err := f.store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("%w: snapshot: %w", ErrStore, err)
	}
	return &snapshot{state: f.encodeStateLocked(), store: stored}, nil
}

type snapshot struct {
	state []byte
	store StoreSnapshot
}

// Persist writes the snapshot; it runs while applies continue.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.write(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("persisting the snapshot: %w", err)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("closing the snapshot: %w", err)
	}
	return nil
}

func (s *snapshot) write(w io.Writer) error {
	header := binary.BigEndian.AppendUint32([]byte(snapshotMagic), FormatVersion)
	header = binary.BigEndian.AppendUint64(header, uint64(len(s.state)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(s.state); err != nil {
		return err
	}
	if _, err := s.store.WriteTo(w); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	return nil
}

func (s *snapshot) Release() { s.store.Release() }

// Restore replaces the state and the databases with a snapshot's; a
// snapshot it refuses leaves the state as it was.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close() //nolint:errcheck // read only
	state, err := readState(rc)
	if err != nil {
		return fmt.Errorf("restoring the snapshot: %w", err)
	}
	runnables := make(map[string]*fsmpb.Runnable, len(state.GetRunnables()))
	for _, r := range state.GetRunnables() {
		if _, dup := runnables[r.GetName()]; dup {
			return fmt.Errorf("restoring the snapshot: runnable %q twice", r.GetName())
		}
		if r.GetCurrent().GetEpoch() > state.GetEpochCounter() {
			return fmt.Errorf("restoring the snapshot: %q at epoch %d, beyond the counter %d", r.GetName(), r.GetCurrent().GetEpoch(), state.GetEpochCounter())
		}
		runnables[r.GetName()] = r
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.store.Restore(rc); err != nil {
		return fmt.Errorf("%w: restoring the snapshot: %w", ErrStore, err)
	}
	f.epochCounter = state.GetEpochCounter()
	f.runnables = runnables
	return nil
}

func readState(r io.Reader) (*fsmpb.State, error) {
	var header [len(snapshotMagic) + 4 + 8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}
	if string(header[:len(snapshotMagic)]) != snapshotMagic {
		return nil, errors.New("not a resurgo snapshot")
	}
	if v := binary.BigEndian.Uint32(header[len(snapshotMagic):]); v == 0 || v > FormatVersion {
		return nil, fmt.Errorf("%w: snapshot format version %d, this node reads up to %d", ErrUnsupported, v, FormatVersion)
	}
	size := binary.BigEndian.Uint64(header[len(snapshotMagic)+4:])
	if size > maxStateSize {
		return nil, fmt.Errorf("state of %d bytes, at most %d", size, maxStateSize)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	var state fsmpb.State
	if err := proto.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	if v := state.GetFormatVersion(); v == 0 || v > FormatVersion {
		return nil, fmt.Errorf("%w: state format version %d", ErrUnsupported, v)
	}
	return &state, nil
}
