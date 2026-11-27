package runnable

import (
	"context"
	"errors"
	"fmt"
)

// Instance is a loaded runnable, as the node's supervisor drives it: one
// plugin process per incarnation (ADR-4), or the runnable in this process
// (tests). Load and InProcess return one.
type Instance interface {
	Describe(ctx context.Context) (Description, error)
	Validate(ctx context.Context, config []byte) error
	// Start runs an incarnation, leased until validUntil (CLOCK_BOOTTIME
	// ns); host answers its requests. One Start per Instance.
	Start(ctx context.Context, inc Incarnation, validUntil int64, host Host) (Session, error)
	// Kill ends the instance at once.
	Kill() error
}

// Session is a running incarnation.
type Session interface {
	// Renew extends the lease (a heartbeat, §8.3).
	Renew(validUntil int64) error
	// Stop asks the incarnation to stop: its context is cancelled.
	Stop(reason string)
	// Done is closed when the incarnation has ended.
	Done() <-chan struct{}
	// Exit is how it ended, once Done is closed.
	Exit() Exit
}

// Outcome is how an incarnation ended.
type Outcome int

const (
	// Completed: Run returned nil.
	Completed Outcome = iota + 1
	// Fenced: Run returned ErrFenced.
	Fenced
	// Failed: Run returned another error, or panicked.
	Failed
	// Crashed: the incarnation ended without returning (the process died
	// or was killed).
	Crashed
)

func (o Outcome) String() string {
	switch o {
	case Completed:
		return "completed"
	case Fenced:
		return "fenced"
	case Failed:
		return "failed"
	case Crashed:
		return "crashed"
	}
	return "running"
}

// Exit is the outcome of an incarnation, with the error for Failed and
// Crashed.
type Exit struct {
	Outcome Outcome
	Err     error
}

func (e Exit) String() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Outcome, e.Err)
	}
	return e.Outcome.String()
}

// exitOf maps Run's return to an Exit
func exitOf(err error) Exit {
	switch {
	case err == nil:
		return Exit{Outcome: Completed}
	case errors.Is(err, ErrFenced):
		return Exit{Outcome: Fenced}
	default:
		return Exit{Outcome: Failed, Err: err}
	}
}

// runSafely runs Run, turning a panic into an error
func runSafely(ctx context.Context, impl Runnable, inc Incarnation, host Host) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("runnable: panic: %v", r)
		}
	}()
	return impl.Run(ctx, inc, host)
}
