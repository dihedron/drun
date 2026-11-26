package runnable

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Boottime is CLOCK_BOOTTIME in nanoseconds: the clock of the lease, the
// same for the node and its plugin processes, running while a process is
// stopped (§8.3, EXT-6).
func Boottime() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		panic("runnable: CLOCK_BOOTTIME: " + err.Error())
	}
	return ts.Nano()
}

// The causes of an incarnation's context being cancelled.
var (
	// ErrLeaseExpired: no heartbeat renewed the lease in time.
	ErrLeaseExpired = errors.New("runnable: lease expired")
	// ErrStopped: the node asked the incarnation to stop.
	ErrStopped = errors.New("runnable: stopped by the node")
	// ErrHostGone: the connection to the node broke.
	ErrHostGone = errors.New("runnable: the node is gone")
)

// leaseCheck bounds how long the lease goes unchecked, so that a clock
// that ran while the process could not (a suspended machine) is noticed
const leaseCheck = 50 * time.Millisecond

// lease cancels a context once CLOCK_BOOTTIME passes its valid_until
type lease struct {
	validUntil atomic.Int64
	cancel     context.CancelCauseFunc
}

// withLease returns a context cancelled when the lease expires, and the
// lease to renew
func withLease(parent context.Context, validUntil int64) (context.Context, *lease) {
	ctx, cancel := context.WithCancelCause(parent)
	l := &lease{cancel: cancel}
	l.validUntil.Store(validUntil)
	go l.watch(ctx)
	return ctx, l
}

// renew extends the lease; a heartbeat never shortens it
func (l *lease) renew(validUntil int64) {
	for {
		cur := l.validUntil.Load()
		if validUntil <= cur || l.validUntil.CompareAndSwap(cur, validUntil) {
			return
		}
	}
}

func (l *lease) watch(ctx context.Context) {
	for {
		remaining := time.Duration(l.validUntil.Load() - Boottime())
		if remaining <= 0 {
			l.cancel(ErrLeaseExpired)
			return
		}
		t := time.NewTimer(min(remaining, leaseCheck))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}
