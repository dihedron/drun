package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// blockedStopStep stands for a step of the stop that ignores its context
func blockedStopStep(release <-chan struct{}) { <-release }

// AC-STOP-1: a stop that blocks forever returns at the grace, with the step
// it was in and every goroutine's stack, the blocked one included
func TestStopWithinTimesOut(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	t0 := time.Now()
	err := stopWithin(ctx, logger, func(context.Context) error { blockedStopStep(release); return nil }, func() string { return "raft" })
	if !errors.Is(err, errStopTimedOut) {
		t.Fatalf("error %v, want %v", err, errStopTimedOut)
	}
	if d := time.Since(t0); d > 2*time.Second {
		t.Fatalf("returned after %s", d)
	}
	out := buf.String()
	for _, want := range []string{"level=ERROR", "stop_step=raft", "blockedStopStep", "goroutine "} {
		if !strings.Contains(out, want) {
			t.Errorf("the log lacks %q:\n%s", want, out)
		}
	}
}

// AC-STOP-2: a stop that finishes returns its own error, and logs nothing
func TestStopWithinReturns(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	want := errors.New("store: closing")
	err := stopWithin(context.Background(), logger, func(context.Context) error { return want }, func() string { return "store" })
	if !errors.Is(err, want) || buf.Len() > 0 {
		t.Fatalf("error %v, log %q", err, buf.String())
	}
}
