package fsm

import "errors"

// The errors a command can fail with; Result.Err wraps one of them.
var (
	// ErrFenced: the command's epoch is not the runnable's current one, or
	// the current incarnation was released (INV-3).
	ErrFenced = errors.New("fenced: stale incarnation")
	// ErrDisabled: the runnable is disabled.
	ErrDisabled = errors.New("runnable disabled")
	// ErrCompleted: the runnable is a completed task (INV-8).
	ErrCompleted = errors.New("runnable completed")
	// ErrRunning: the runnable has an incarnation that was not released.
	ErrRunning = errors.New("runnable running")
	// ErrTooLarge: a checkpoint is larger than MaxCheckpointSize.
	ErrTooLarge = errors.New("checkpoint too large")
	// ErrUnknownRunnable: no state for the runnable.
	ErrUnknownRunnable = errors.New("unknown runnable")
	// ErrUnsupported: an entry this node cannot decode, an unknown command,
	// or a newer format version (OI-10).
	ErrUnsupported = errors.New("unsupported command")
	// ErrInvalid: a command with an invalid field.
	ErrInvalid = errors.New("invalid command")
	// ErrUnknownPeer: no identity recorded for the peer.
	ErrUnknownPeer = errors.New("unknown peer")
	// ErrStore: the store failed on this node, not deterministically.
	ErrStore = errors.New("store failure")
)
