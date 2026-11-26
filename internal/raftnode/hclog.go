package raftnode

import (
	"context"
	"io"
	"log"
	"log/slog"

	"github.com/hashicorp/go-hclog"
)

// hclogAdapter sends hashicorp/raft's logs to slog
type hclogAdapter struct {
	l    *slog.Logger
	name string
	args []any
}

func newHclog(l *slog.Logger, name string) hclog.Logger {
	return &hclogAdapter{l: l.With("component", name), name: name}
}

func slogLevel(level hclog.Level) slog.Level {
	switch level {
	case hclog.Trace:
		return slog.LevelDebug - 4
	case hclog.Debug:
		return slog.LevelDebug
	case hclog.Warn:
		return slog.LevelWarn
	case hclog.Error:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (a *hclogAdapter) Log(level hclog.Level, msg string, args ...any) {
	if level == hclog.Off {
		return
	}
	a.l.Log(context.Background(), slogLevel(level), msg, args...)
}

func (a *hclogAdapter) Trace(msg string, args ...any) { a.Log(hclog.Trace, msg, args...) }
func (a *hclogAdapter) Debug(msg string, args ...any) { a.Log(hclog.Debug, msg, args...) }
func (a *hclogAdapter) Info(msg string, args ...any)  { a.Log(hclog.Info, msg, args...) }
func (a *hclogAdapter) Warn(msg string, args ...any)  { a.Log(hclog.Warn, msg, args...) }
func (a *hclogAdapter) Error(msg string, args ...any) { a.Log(hclog.Error, msg, args...) }

func (a *hclogAdapter) enabled(level hclog.Level) bool {
	return a.l.Enabled(context.Background(), slogLevel(level))
}

func (a *hclogAdapter) IsTrace() bool { return a.enabled(hclog.Trace) }
func (a *hclogAdapter) IsDebug() bool { return a.enabled(hclog.Debug) }
func (a *hclogAdapter) IsInfo() bool  { return a.enabled(hclog.Info) }
func (a *hclogAdapter) IsWarn() bool  { return a.enabled(hclog.Warn) }
func (a *hclogAdapter) IsError() bool { return a.enabled(hclog.Error) }

func (a *hclogAdapter) ImpliedArgs() []any { return a.args }

func (a *hclogAdapter) With(args ...any) hclog.Logger {
	return &hclogAdapter{l: a.l.With(args...), name: a.name, args: append(append([]any{}, a.args...), args...)}
}

func (a *hclogAdapter) Name() string { return a.name }

func (a *hclogAdapter) Named(name string) hclog.Logger {
	if a.name != "" {
		name = a.name + "." + name
	}
	return &hclogAdapter{l: a.l.With("component", name), name: name, args: a.args}
}

func (a *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{l: a.l.With("component", name), name: name, args: a.args}
}

// SetLevel: the level is slog's, set once for the process
func (a *hclogAdapter) SetLevel(hclog.Level) {}

func (a *hclogAdapter) GetLevel() hclog.Level {
	for _, level := range []hclog.Level{hclog.Trace, hclog.Debug, hclog.Info, hclog.Warn} {
		if a.enabled(level) {
			return level
		}
	}
	return hclog.Error
}

func (a *hclogAdapter) StandardLogger(*hclog.StandardLoggerOptions) *log.Logger {
	return slog.NewLogLogger(a.l.Handler(), slog.LevelInfo)
}

func (a *hclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return a.StandardLogger(opts).Writer()
}
