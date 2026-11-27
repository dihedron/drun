package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/daemon"
	"github.com/dihedron/resurgo/internal/supervisor"
	"github.com/dihedron/resurgo/pkg/runnable"
)

// ServeCommand runs a node until SIGTERM or SIGINT, then hands leadership
// over (FR-12).
type ServeCommand struct {
	Config string `long:"config" short:"c" default:"/etc/resurgo/resurgo.yaml" description:"the node's configuration"`
}

func (c *ServeCommand) Execute([]string) error {
	cfg, err := config.Load(c.Config)
	if err != nil {
		return err
	}
	logger := slog.Default()
	if _, set := os.LookupEnv(envName("LOG_LEVEL")); !set {
		// a node logs at info unless told otherwise (journald adds the time)
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	d, err := daemon.Start(cfg, daemon.Options{
		Load: pluginLoader(logger),
		Fatal: func(err error) {
			// the store failed where the other nodes did not: carrying on
			// would diverge from them
			logger.Error("fatal store failure: stopping the node", "node_id", cfg.Node.ID, "error", err)
			os.Exit(70)
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	logger.Info("node running", "node_id", cfg.Node.ID, "cluster", cfg.Cluster.Name, "runnables", len(cfg.Runnables))
	<-ctx.Done()
	stop()

	logger.Info("stopping: handing leadership over", "node_id", cfg.Node.ID)
	grace := 10 * time.Second
	for _, r := range cfg.Runnables {
		grace = max(grace, r.ShutdownGrace+10*time.Second)
	}
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := d.Stop(sctx); err != nil {
		return fmt.Errorf("stopping: %w", err)
	}
	logger.Info("stopped", "node_id", cfg.Node.ID)
	return nil
}

// pluginLoader loads a runnable's pinned binary and checks it as FR-16
// requires: it describes itself with the configured name and kind and
// accepts its configuration
func pluginLoader(logger *slog.Logger) func(context.Context, supervisor.Spec) (runnable.Instance, error) {
	return func(ctx context.Context, spec supervisor.Spec) (runnable.Instance, error) {
		inst, err := runnable.Load(ctx, runnable.Plugin{
			Path: spec.Binary, SHA256: spec.SHA256, CheckOwnership: true,
			Logger: logger.With("runnable", spec.Name),
		})
		if err != nil {
			return nil, err
		}
		if err := check(ctx, inst, spec); err != nil {
			_ = inst.Kill()
			return nil, fmt.Errorf("runnable %s: %w", spec.Name, err)
		}
		return inst, nil
	}
}

func check(ctx context.Context, inst runnable.Instance, spec supervisor.Spec) error {
	d, err := inst.Describe(ctx)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	if d.Name != spec.Name {
		return fmt.Errorf("the binary is the runnable %q", d.Name)
	}
	if d.Kind.String() != spec.Kind {
		return fmt.Errorf("the binary is a %s, configured as a %s", d.Kind, spec.Kind)
	}
	if err := inst.Validate(ctx, spec.ConfigJSON); err != nil {
		return fmt.Errorf("configuration refused: %w", err)
	}
	return nil
}
