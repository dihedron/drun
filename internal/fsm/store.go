package fsm

import (
	"io"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

// Store is the runnables' SQL stores (resurgo-store.md), as the FSM sees
// them. Every method that changes a database gets the index of the entry,
// so that the store applies each entry exactly once (STO-3). The FSM calls
// it from Apply, Snapshot and Restore only, one call at a time.
type Store interface {
	// Exec applies a batch the FSM has already fenced. A deterministic
	// failure (SQL error, expectation, limit) is in the result, the same on
	// every node; an error is a failure of this node (I/O), after which the
	// FSM leaves its own state unchanged.
	Exec(index uint64, batch *fsmpb.StoreBatch) (*fsmpb.BatchResult, error)
	// PutCheckpoint replaces the runnable's checkpoint (STO-12).
	PutCheckpoint(index uint64, runnable string, meta *fsmpb.CheckpointMeta, data []byte) error
	// ClearCheckpoint removes the runnable's checkpoint.
	ClearCheckpoint(index uint64, runnable string) error
	// Forget removes the runnable's database.
	Forget(runnable string) error
	// Snapshot takes a consistent copy of every database (STO-14); it is
	// called with applies stopped, and the copy is written later, while
	// they run again.
	Snapshot() (StoreSnapshot, error)
	// Restore replaces every database with the ones a StoreSnapshot wrote.
	Restore(r io.Reader) error
}

// StoreSnapshot is a Store's copy of its databases.
type StoreSnapshot interface {
	// WriteTo writes the copy; Restore reads it back.
	WriteTo(w io.Writer) (int64, error)
	// Release frees the copy.
	Release()
}
