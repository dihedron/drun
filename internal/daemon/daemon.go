// Package daemon assembles a node (drun serve): its runnables' store,
// the FSM, the Raft node, the supervisor and the admin API, started and
// stopped in order.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/dihedron/drun/internal/admin"
	"github.com/dihedron/drun/internal/config"
	"github.com/dihedron/drun/internal/fsm"
	"github.com/dihedron/drun/internal/metrics"
	"github.com/dihedron/drun/internal/raftnode"
	"github.com/dihedron/drun/internal/store"
	"github.com/dihedron/drun/internal/supervisor"
	"github.com/dihedron/drun/pkg/runnable"
	"github.com/hashicorp/raft"
)

// validateTimeout bounds the check of one runnable at start (FR-16)
const validateTimeout = 30 * time.Second

// Options are what a node needs besides its configuration.
type Options struct {
	// Load starts an instance of a runnable, checked as FR-16 requires; it
	// is used once per runnable at start and for every incarnation.
	Load func(ctx context.Context, spec supervisor.Spec) (runnable.Instance, error)
	// Fatal is called when the store fails in a way that is not the same
	// on every node: the node must stop.
	Fatal  func(error)
	Logger *slog.Logger
}

// Daemon is a running node.
type Daemon struct {
	cfg    *config.Config
	logger *slog.Logger
	specs  []supervisor.Spec

	Store      *store.Store
	FSM        *fsm.FSM
	Node       *raftnode.Node
	Supervisor *supervisor.Supervisor
	Admin      *admin.Server
	Metrics    *metrics.Metrics
	metrics    *http.Server

	mu      sync.Mutex
	invalid map[string]error // FR-16, by runnable

	cancel context.CancelFunc
	done   sync.WaitGroup
}

// Start starts a node; Stop stops it.
func Start(cfg *config.Config, o Options) (_ *Daemon, err error) {
	logger := o.Logger.With("node_id", cfg.Node.ID)
	specs, err := supervisor.Specs(cfg)
	if err != nil {
		return nil, err
	}
	d := &Daemon{cfg: cfg, logger: logger, specs: specs, invalid: map[string]error{}}
	var cleanup []func() error
	defer func() {
		if err != nil {
			for i := len(cleanup) - 1; i >= 0; i-- {
				_ = cleanup[i]()
			}
		}
	}()

	if d.Store, err = store.Open(filepath.Join(cfg.Node.DataDir, "store"), o.Fatal, logger); err != nil {
		return nil, err
	}
	cleanup = append(cleanup, d.Store.Close)
	d.FSM = fsm.New(d.Store, logger)
	d.Metrics = metrics.New(source{d}, supervisor.States)
	if cfg.Metrics.Listen != "" {
		// before the Raft stores open: raft-wal binds the go-metrics instance then
		if d.Metrics.Raft, err = metrics.InstallRaft(); err != nil {
			return nil, err
		}
	}
	if d.Node, err = raftnode.New(cfg, d.FSM, d.FSM.Peers, o.Logger); err != nil { // it adds node_id
		return nil, err
	}
	cleanup = append(cleanup, d.Node.Shutdown)

	d.validate(o.Load)
	load := func(ctx context.Context, spec supervisor.Spec) (runnable.Instance, error) {
		inst, err := o.Load(ctx, spec)
		d.setInvalid(spec.Name, err)
		return inst, err
	}
	d.Supervisor = supervisor.New(supervisor.Options{
		Raft: d.Node.Raft(), FSM: d.FSM, Store: d.Store, Node: cfg.Node.ID, Cluster: cfg.Cluster.Name,
		Lease: cfg.Lease.Duration, RenewInterval: cfg.Lease.RenewInterval, Runnables: specs,
		Load: load, Events: d.Metrics, Logger: logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done.Add(2)
	go func() { defer d.done.Done(); d.Supervisor.Run(ctx) }()
	go func() { defer d.done.Done(); d.watchDrift(ctx) }()
	cleanup = append(cleanup, func() error { cancel(); d.done.Wait(); return nil })

	d.Admin = admin.New(admin.Options{
		Node: d.Node, NodeID: cfg.Node.ID, Cluster: cfg.Cluster.Name, FSM: d.FSM, Store: d.Store,
		Supervisor: d.Supervisor, Specs: specs, Validation: d.Invalid, Logger: logger,
	})
	cleanup = append(cleanup, func() error { d.Admin.Stop(context.Background()); return nil })
	if err := d.serveAdmin(); err != nil {
		return nil, err
	}
	if err := d.serveMetrics(); err != nil {
		return nil, err
	}
	return d, nil
}

// serveMetrics serves /metrics on metrics.listen, if set
func (d *Daemon) serveMetrics() error {
	if d.cfg.Metrics.Listen == "" {
		return nil
	}
	ln, err := net.Listen("tcp", d.cfg.Metrics.Listen)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", d.Metrics)
	d.metrics = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := d.metrics.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			d.logger.Error("metrics endpoint stopped", "error", err)
		}
	}()
	return nil
}

// source reads the scrape-time values of the metrics
type source struct{ d *Daemon }

func (s source) Leader() (bool, uint64) {
	r := s.d.Node.Raft()
	term, _ := strconv.ParseUint(r.Stats()["term"], 10, 64)
	return r.State() == raft.Leader, term
}

func (s source) Runnables() []metrics.Runnable {
	views := map[string]string{}
	for _, v := range s.d.Supervisor.Views() {
		views[v.Runnable] = v.State
	}
	var out []metrics.Runnable
	for _, spec := range s.d.specs {
		r, _ := s.d.FSM.Runnable(spec.Name)
		out = append(out, metrics.Runnable{
			Name: spec.Name, Epoch: r.GetCurrent().GetEpoch(), State: views[spec.Name],
			CheckpointSeq: r.GetCheckpoint().GetSeq(), CheckpointBytes: r.GetCheckpoint().GetSize(),
			Drift: supervisor.Drift(r, spec),
		})
	}
	return out
}

func (d *Daemon) serveAdmin() error {
	sock, err := admin.UnixEndpoint(d.cfg.Admin.Socket, d.cfg.Admin.AllowedGroups)
	if err != nil {
		return err
	}
	if err := d.Admin.Serve(sock); err != nil {
		return err
	}
	if err := d.Admin.ServePeers(); err != nil {
		return err
	}
	if r := d.cfg.Admin.Remote; r != nil {
		remote, err := admin.TLSEndpoint(r, d.logger)
		if err != nil {
			return err
		}
		if err := d.Admin.Serve(remote); err != nil {
			return err
		}
		d.logger.Info("remote admin endpoint open", "address", r.Listen, "identities", len(r.AllowedIdentities))
	}
	return nil
}

// validate checks every runnable once (FR-16); a node with an invalid
// runnable still joins, reports it, and gives leadership away when it
// fails to run it (OI-11)
func (d *Daemon) validate(load func(context.Context, supervisor.Spec) (runnable.Instance, error)) {
	for _, spec := range d.specs {
		ctx, cancel := context.WithTimeout(context.Background(), validateTimeout)
		inst, err := load(ctx, spec)
		cancel()
		if err == nil {
			err = inst.Kill()
		}
		d.setInvalid(spec.Name, err)
	}
}

func (d *Daemon) setInvalid(name string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil && d.invalid[name] == nil {
		d.logger.Error("runnable cannot run on this node", "runnable", name, "error", err)
	}
	if err == nil && d.invalid[name] != nil {
		d.logger.Info("runnable valid again on this node", "runnable", name)
	}
	if err == nil {
		delete(d.invalid, name)
	} else {
		d.invalid[name] = err
	}
}

// Invalid returns why this node cannot run a runnable, or nil (FR-16).
func (d *Daemon) Invalid(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.invalid[name]
}

// watchDrift warns, once per incarnation, when this node's digests of a
// runnable differ from those its current incarnation recorded (FR-15)
func (d *Daemon) watchDrift(ctx context.Context) {
	warned := map[string]uint64{}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		for _, spec := range d.specs {
			r, _ := d.FSM.Runnable(spec.Name)
			if epoch := r.GetCurrent().GetEpoch(); supervisor.Drift(r, spec) && warned[spec.Name] != epoch {
				warned[spec.Name] = epoch
				d.logger.Warn("configuration drift: this node's runnable differs from the current incarnation's",
					"runnable", spec.Name, "epoch", epoch, "incarnation_node", r.GetCurrent().GetNode())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// Stop hands leadership over if this node is leader, stopping its
// runnables first (FR-12), then stops the node.
func (d *Daemon) Stop(ctx context.Context) error {
	var errs []error
	if err := d.Supervisor.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("handing over: %w", err))
	}
	d.Admin.Stop(ctx)
	if d.metrics != nil {
		_ = d.metrics.Shutdown(ctx)
	}
	d.cancel()
	d.done.Wait()
	errs = append(errs, d.Node.Shutdown(), d.Store.Close())
	return errors.Join(errs...)
}
