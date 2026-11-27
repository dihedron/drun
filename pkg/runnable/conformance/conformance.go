// Package conformance checks that a runnable behaves as the node needs:
// it describes itself, validates its configuration, and returns within its
// stop grace when stopped, when its lease expires and when it is fenced.
// Run it against a runnable in process and as a plugin binary:
//
//	conformance.Check(t, func(t *testing.T) runnable.Instance {
//		return runnable.InProcess(myRunnable{})
//	}, conformance.Options{Config: cfg})
package conformance

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dihedron/drun/pkg/runnable"
)

// Options of the checks.
type Options struct {
	// Config is a valid configuration of the runnable, as JSON.
	Config []byte
	// Grace is the stop grace the runnable must honour (default 2 s).
	Grace time.Duration
	// Incarnation is the base of the incarnations started (Runnable and
	// Config are filled in).
	Incarnation runnable.Incarnation
}

// Check runs the conformance checks; newInstance returns a fresh instance
// for each check (one process per incarnation).
func Check(t *testing.T, newInstance func(t *testing.T) runnable.Instance, opts Options) {
	if opts.Grace == 0 {
		opts.Grace = 2 * time.Second
	}
	start := func(t *testing.T, host runnable.Host, validUntil int64) (runnable.Instance, runnable.Session, runnable.Description) {
		t.Helper()
		inst := newInstance(t)
		t.Cleanup(func() { _ = inst.Kill() })
		d, err := inst.Describe(t.Context())
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		inc := opts.Incarnation
		inc.Runnable, inc.Config = d.Name, opts.Config
		if inc.Epoch == 0 {
			inc.Epoch = 1
		}
		s, err := inst.Start(t.Context(), inc, validUntil, host)
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		return inst, s, d
	}
	ended := func(t *testing.T, s runnable.Session, within time.Duration, what string) runnable.Exit {
		t.Helper()
		select {
		case <-s.Done():
		case <-time.After(within):
			t.Fatalf("still running %s after %s", what, within)
		}
		e := s.Exit()
		if e.Outcome == runnable.Crashed {
			t.Fatalf("crashed %s: %v", what, e.Err)
		}
		return e
	}
	farLease := func() int64 { return runnable.Boottime() + int64(time.Hour) }

	t.Run("describe", func(t *testing.T) {
		inst := newInstance(t)
		defer inst.Kill() //nolint:errcheck // test teardown
		a, err := inst.Describe(t.Context())
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		b, _ := inst.Describe(t.Context())
		if a.Name == "" || a.Name != b.Name || a.Kind != b.Kind || a.Version != b.Version {
			t.Fatalf("descriptions %+v and %+v", a, b)
		}
	})

	t.Run("validate", func(t *testing.T) {
		inst := newInstance(t)
		defer inst.Kill() //nolint:errcheck // test teardown
		if err := inst.Validate(t.Context(), opts.Config); err != nil {
			t.Fatalf("the configuration given is refused: %v", err)
		}
	})

	t.Run("stops when stopped", func(t *testing.T) {
		_, s, d := start(t, NewHost(), farLease())
		time.Sleep(200 * time.Millisecond)
		select {
		case <-s.Done():
			if d.Kind == runnable.Service {
				t.Fatalf("a service ended by itself: %v", s.Exit())
			}
			return
		default:
		}
		s.Stop("conformance")
		ended(t, s, opts.Grace, "after Stop")
	})

	t.Run("stops when the lease expires", func(t *testing.T) {
		lease := 300 * time.Millisecond
		_, s, _ := start(t, NewHost(), runnable.Boottime()+int64(lease))
		ended(t, s, lease+opts.Grace, "after its lease expired")
	})

	t.Run("stops when fenced", func(t *testing.T) {
		host := NewHost()
		host.SetFenced(true)
		_, s, _ := start(t, host, farLease())
		select {
		case <-host.Called():
		case <-s.Done():
			return
		case <-time.After(2 * time.Second):
			t.Skip("the runnable did not call the node within 2 s")
		}
		e := ended(t, s, opts.Grace, "after being fenced")
		if e.Outcome == runnable.Completed {
			t.Fatalf("a fenced incarnation completed")
		}
	})
}

// Host is an in-memory runnable.Host for tests.
type Host struct {
	mu          sync.Mutex
	fenced      bool
	Checkpoints [][]byte
	Statuses    []string
	Batches     [][]runnable.Statement
	// QueryFunc answers queries (default: no rows).
	QueryFunc func(sql string, args []any) (runnable.Rows, error)
	called    chan struct{}
	callOnce  sync.Once
}

// NewHost returns an empty Host.
func NewHost() *Host { return &Host{called: make(chan struct{})} }

// SetFenced makes every call fail with ErrFenced, as for a stale
// incarnation.
func (h *Host) SetFenced(f bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fenced = f
}

// Called is closed at the first call.
func (h *Host) Called() <-chan struct{} { return h.called }

func (h *Host) enter() error {
	h.callOnce.Do(func() { close(h.called) })
	if h.fenced {
		return runnable.ErrFenced
	}
	return nil
}

func (h *Host) Checkpoint(_ context.Context, data []byte) (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.enter(); err != nil {
		return 0, err
	}
	h.Checkpoints = append(h.Checkpoints, data)
	return uint64(len(h.Checkpoints)), nil
}

func (h *Host) Status(_ context.Context, healthy bool, detail string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.enter(); err != nil {
		return err
	}
	state := "unhealthy"
	if healthy {
		state = "healthy"
	}
	h.Statuses = append(h.Statuses, state+": "+detail)
	return nil
}

func (h *Host) Store() runnable.Store { return hostStore{h} }

type hostStore struct{ h *Host }

func (s hostStore) Exec(_ context.Context, batch []runnable.Statement) ([]runnable.Result, error) {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	if err := s.h.enter(); err != nil {
		return nil, err
	}
	s.h.Batches = append(s.h.Batches, batch)
	return make([]runnable.Result, len(batch)), nil
}

func (s hostStore) Query(_ context.Context, sql string, args ...any) (runnable.Rows, error) {
	s.h.mu.Lock()
	if err := s.h.enter(); err != nil {
		s.h.mu.Unlock()
		return nil, err
	}
	f := s.h.QueryFunc
	s.h.mu.Unlock()
	if f == nil {
		return runnable.NewRows(nil, nil), nil
	}
	return f(sql, args)
}

// Snapshot returns copies of what the host received.
func (h *Host) Snapshot() (checkpoints [][]byte, statuses []string, batches int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][]byte{}, h.Checkpoints...), append([]string{}, h.Statuses...), len(h.Batches)
}
