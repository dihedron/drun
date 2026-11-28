package main

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
)

// exitStopTimedOut: the stop outlived its grace (STOP-2, drun-stop.md D-2)
const exitStopTimedOut = 75

var errStopTimedOut = errors.New("stop timed out")

// stopWithin runs stop and waits for it until ctx ends (STOP-1); then it
// logs the step the stop was in and every goroutine's stack (STOP-2), and
// returns errStopTimedOut, leaving the stop's goroutine behind
func stopWithin(ctx context.Context, logger *slog.Logger, stop func(context.Context) error, step func() string) error {
	done := make(chan error, 1)
	go func() { done <- stop(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	logger.Error("stop timed out: exiting", "stop_step", step(), "goroutines", string(buf))
	return errStopTimedOut
}
