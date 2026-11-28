package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dihedron/drun/internal/config"
	"github.com/dihedron/drun/internal/daemon"
	"github.com/dihedron/drun/internal/supervisor"
	"github.com/dihedron/drun/pkg/metadata"
	"github.com/dihedron/drun/pkg/runnable"
	"github.com/dihedron/drun/pkg/runnable/manifest"
)

// ServeCommand runs a node until SIGTERM or SIGINT, then hands leadership
// over (FR-12).
type ServeCommand struct {
	Config string `long:"config" short:"c" default:"/etc/drun/drun.yaml" description:"the node's configuration"`
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
	unsigned := resolveManifests(cfg, manifest.KeyDir, metadata.Version, true, logger)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	d, err := daemon.Start(cfg, daemon.Options{
		Load: pluginLoader(logger, unsigned),
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
	t0 := time.Now()
	if err := stopWithin(sctx, logger, d.Stop, d.StopStep); err != nil {
		if errors.Is(err, errStopTimedOut) {
			// the store and the raft log are crash-safe: as a SIGKILL would
			os.Exit(exitStopTimedOut)
		}
		return fmt.Errorf("stopping: %w", err)
	}
	logger.Info("stopped", "node_id", cfg.Node.ID, "took", time.Since(t0).Round(time.Millisecond))
	return nil
}

// resolveManifests pins every runnable configured with a key to the
// SHA-256 of its manifest, verified against the key (§8.8); it returns why
// each runnable whose manifest failed cannot run, which leaves it unpinned
func resolveManifests(cfg *config.Config, keyDir, version string, requireRootOwned bool, logger *slog.Logger) map[string]error {
	failed := map[string]error{}
	for i := range cfg.Runnables {
		r := &cfg.Runnables[i]
		if r.Key == "" {
			continue
		}
		keys, err := manifest.LoadKeys(keyDir, r.Key, requireRootOwned)
		var m manifest.Manifest
		if err == nil {
			m, err = manifest.Verify(r.Binary, r.Name, version, keys, requireRootOwned)
		}
		if err != nil {
			failed[r.Name] = err
			continue
		}
		r.SHA256 = m.SHA256
		logger.Info("runnable verified by its manifest", "runnable", r.Name, "version", m.Version, "key", r.Key)
	}
	return failed
}

// pluginLoader loads a runnable's pinned binary and checks it as FR-16
// requires: it describes itself with the configured name and kind and
// accepts its configuration
func pluginLoader(logger *slog.Logger, unsigned map[string]error) func(context.Context, supervisor.Spec) (runnable.Instance, error) {
	return func(ctx context.Context, spec supervisor.Spec) (runnable.Instance, error) {
		if err := unsigned[spec.Name]; err != nil {
			return nil, err // FR-16: invalid on this node
		}
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
