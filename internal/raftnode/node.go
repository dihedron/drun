// Package raftnode runs hashicorp/raft for a node: the bbolt log and
// stable store and the file snapshots under data_dir, the cluster port
// (Transport), static bootstrap (FR-1), membership (FR-2) and leadership
// transfer (FR-3).
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/fsm"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	wal "github.com/hashicorp/raft-wal"
)

const (
	retainSnapshots = 2
	changeTimeout   = 10 * time.Second
	// probeTimeout bounds the questions a node with no state asks before
	// bootstrapping
	probeTimeout = 3 * time.Second
)

// ErrNotLeader: a call only the leader executes reached another node.
type ErrNotLeader struct {
	// Leader is the current leader, empty if none is known.
	ID      raft.ServerID
	Address raft.ServerAddress
}

func (e *ErrNotLeader) Error() string {
	if e.ID == "" {
		return "not the leader, and no leader is known"
	}
	return fmt.Sprintf("not the leader: the leader is %s (%s)", e.ID, e.Address)
}

// Node is a running Raft server.
type Node struct {
	cfg    *config.Config
	logger *slog.Logger
	raft   *raft.Raft
	trans  *Transport
	net    *raft.NetworkTransport
	store  logStore
	peers  *Peers

	mu    sync.RWMutex
	addrs map[raft.ServerAddress]raft.ServerID

	observations chan raft.Observation
	observer     *raft.Observer
	done         chan struct{}

	existing     bool
	running      atomic.Pointer[raft.Raft] // set once raft runs, for probes
	bootstrapped bool
}

// New starts the node's Raft server with f, and bootstraps the cluster
// from the configured voters if the node has no Raft state yet (FR-1).
// recorded returns the peer identities recorded through the log (the FSM's
// Peers); nil: the configured ones only.
func New(cfg *config.Config, f raft.FSM, recorded func() map[string]string, logger *slog.Logger) (_ *Node, err error) {
	logger = logger.With("node_id", cfg.Node.ID)
	raftDir := filepath.Join(cfg.Node.DataDir, "raft")
	for _, dir := range []string{cfg.Node.DataDir, raftDir} {
		if err := checkPrivateDir(dir); err != nil {
			return nil, err
		}
	}

	n := &Node{cfg: cfg, logger: logger, addrs: map[raft.ServerAddress]raft.ServerID{}, done: make(chan struct{})}
	for _, v := range cfg.Cluster.Voters {
		n.addrs[raft.ServerAddress(v.Address)] = raft.ServerID(v.ID)
	}
	var cleanup []func() error
	defer func() {
		if err != nil {
			for i := len(cleanup) - 1; i >= 0; i-- {
				_ = cleanup[i]()
			}
		}
	}()

	identity, err := NewFileIdentity(cfg.Cluster.TLS.Cert, cfg.Cluster.TLS.Key, cfg.Cluster.TLS.CA, logger)
	if err != nil {
		return nil, err
	}
	n.peers = NewPeers(cfg.Cluster.TLS.Peers, recorded, logger)
	n.trans, err = Listen(cfg.Cluster.Bind, cfg.Cluster.Advertise, identity, n.peers, logger)
	if err != nil {
		return nil, err
	}
	n.trans.SetResolver(n.resolve)
	n.net = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream: n.trans.RaftLayer(), MaxPool: 3, Timeout: 10 * time.Second, Logger: newHclog(logger, "raft-net"),
	})
	cleanup = append(cleanup, n.net.Close)

	if n.store, err = openLogStore(cfg.Raft.LogStore, raftDir, logger); err != nil {
		return nil, err
	}
	cleanup = append(cleanup, n.store.Close)
	snaps, err := raft.NewFileSnapshotStoreWithLogger(raftDir, retainSnapshots, newHclog(logger, "raft-snapshots"))
	if err != nil {
		return nil, fmt.Errorf("raft snapshots: %w", err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.Node.ID)
	rc.HeartbeatTimeout = cfg.Raft.HeartbeatTimeout
	rc.ElectionTimeout = cfg.Raft.ElectionTimeout
	rc.LeaderLeaseTimeout = cfg.Raft.LeaderLeaseTimeout
	rc.SnapshotThreshold = cfg.Raft.SnapshotThreshold
	rc.TrailingLogs = cfg.Raft.TrailingLogs
	rc.Logger = newHclog(logger, "raft")

	existing, err := raft.HasExistingState(n.store, n.store, snaps)
	if err != nil {
		return nil, fmt.Errorf("raft state: %w", err)
	}
	n.existing = existing
	n.trans.SetStateful(n.stateful)
	n.raft, err = raft.NewRaft(rc, f, n.store, n.store, snaps, n.net)
	if err != nil {
		return nil, fmt.Errorf("raft: %w", err)
	}
	n.running.Store(n.raft)
	cleanup = append(cleanup, func() error { return n.raft.Shutdown().Error() })

	switch {
	case existing:
		logger.Info("existing raft state: the configured voters are ignored")
	case !n.isInitialVoter():
		logger.Info("no raft state, not an initial voter: waiting to be added (resurgo member add)")
	case n.clusterExists():
		// a rebuilt node: bootstrapping would write a first entry that may
		// differ from the cluster's at the same index and term
		logger.Info("no raft state, but a configured voter has some: waiting for the cluster's leader (if this node was removed: resurgo member add)")
	default:
		err := n.raft.BootstrapCluster(n.initialConfiguration()).Error()
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
		n.bootstrapped = err == nil
		logger.Info("bootstrapped the cluster from the configured voters", "voters", len(cfg.Cluster.Voters))
	}

	// addresses of members added later come from the raft configuration
	if f := n.raft.GetConfiguration(); f.Error() == nil {
		n.learn(f.Configuration().Servers...)
	}
	n.observations = make(chan raft.Observation, 16)
	n.observer = raft.NewObserver(n.observations, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.PeerObservation)
		return ok
	})
	n.raft.RegisterObserver(n.observer)
	go n.watchPeers()
	return n, nil
}

// logStore is the Raft log and stable store (ADR-1, FW-2)
type logStore interface {
	raft.LogStore
	raft.StableStore
	io.Closer
}

// openLogStore opens the configured log store in dir, refusing to start on
// the other store's state: changing raft.log_store means rebuilding the node
func openLogStore(kind, dir string, logger *slog.Logger) (logStore, error) {
	boltPath, walDir := filepath.Join(dir, "raft.db"), filepath.Join(dir, "wal")
	_, boltErr := os.Stat(boltPath)
	walEntries, _ := os.ReadDir(walDir)
	switch kind {
	case config.LogStoreWAL:
		if boltErr == nil {
			return nil, fmt.Errorf("raft.log_store is %q, but %s holds a bolt log: rebuild the node to change raft.log_store (stop it, empty its data_dir, start it)", kind, boltPath)
		}
		if err := os.MkdirAll(walDir, 0o700); err != nil {
			return nil, fmt.Errorf("raft store: %w", err)
		}
		// raft-wal is experimental (its README): opt-in only
		w, err := wal.Open(walDir, wal.WithLogger(newHclog(logger, "raft-wal")))
		if err != nil {
			return nil, fmt.Errorf("raft store (wal): %w", err)
		}
		return w, nil
	default:
		if len(walEntries) > 0 {
			return nil, fmt.Errorf("raft.log_store is %q, but %s holds a wal log: rebuild the node to change raft.log_store (stop it, empty its data_dir, start it)", kind, walDir)
		}
		b, err := raftboltdb.NewBoltStore(boltPath)
		if err != nil {
			return nil, fmt.Errorf("raft store: %w", err)
		}
		return b, nil
	}
}

// checkPrivateDir creates dir 0700 if missing and refuses one that others
// can enter or that belongs to another user (SEC-7)
func checkPrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("data directory %s is not a directory", dir)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("data directory %s has mode %o: it must be 0700", dir, fi.Mode().Perm())
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
		return fmt.Errorf("data directory %s belongs to uid %d, not to %d", dir, st.Uid, os.Getuid())
	}
	return nil
}

func (n *Node) isInitialVoter() bool {
	for _, v := range n.cfg.Cluster.Voters {
		if v.ID == n.cfg.Node.ID {
			return true
		}
	}
	return false
}

func (n *Node) initialConfiguration() raft.Configuration {
	var c raft.Configuration
	for _, v := range n.cfg.Cluster.Voters {
		c.Servers = append(c.Servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(v.ID), Address: raft.ServerAddress(v.Address)})
	}
	return c
}

func (n *Node) resolve(addr raft.ServerAddress) (raft.ServerID, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	id, ok := n.addrs[addr]
	return id, ok
}

func (n *Node) learn(servers ...raft.Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, s := range servers {
		n.addrs[s.Address] = s.ID
	}
}

func (n *Node) watchPeers() {
	for {
		select {
		case o := <-n.observations:
			if p, ok := o.Data.(raft.PeerObservation); ok && !p.Removed {
				n.learn(p.Peer)
			}
		case <-n.done:
			return
		}
	}
}

// stateful answers probes: whether this node has Raft state
func (n *Node) stateful() bool {
	if r := n.running.Load(); r != nil {
		return r.LastIndex() > 0
	}
	return n.existing
}

// clusterExists asks the other configured voters, in parallel, whether
// any has Raft state; unreachable ones count as no
func (n *Node) clusterExists() bool {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	answers := make(chan bool, len(n.cfg.Cluster.Voters))
	asked := 0
	for _, v := range n.cfg.Cluster.Voters {
		if v.ID == n.cfg.Node.ID {
			continue
		}
		asked++
		go func() {
			has, err := n.trans.Probe(ctx, raft.ServerAddress(v.Address))
			if err != nil {
				n.logger.Debug("probe before bootstrapping", "peer", v.ID, "error", err)
			}
			answers <- has
		}()
	}
	for range asked {
		if <-answers {
			return true
		}
	}
	return false
}

// Bootstrapped reports whether this node created the cluster when it
// started (FR-1).
func (n *Node) Bootstrapped() bool { return n.bootstrapped }

// Raft is the raft server, for the supervisor and the admin API.
func (n *Node) Raft() *raft.Raft { return n.raft }

// Transport is the cluster port, for forwarded admin calls (C5).
func (n *Node) Transport() *Transport { return n.trans }

// Identities returns every known peer identity, by node ID (SEC-2).
func (n *Node) Identities() map[string]string { return n.peers.All() }

func (n *Node) leaderOnly() error {
	if n.raft.State() == raft.Leader {
		return nil
	}
	addr, id := n.raft.LeaderWithID()
	return &ErrNotLeader{ID: id, Address: addr}
}

// AddVoter adds a voter (FR-2). identity, if not empty, is the identity
// its certificate carries: it is recorded through the log first, so that
// every node accepts the new member; if empty, the identity must be known.
func (n *Node) AddVoter(id, address, identity string) error {
	return n.add(id, address, identity, true)
}

// AddNonvoter adds a non-voter (FR-2), as AddVoter.
func (n *Node) AddNonvoter(id, address, identity string) error {
	return n.add(id, address, identity, false)
}

func (n *Node) add(id, address, identity string, voter bool) error {
	if err := n.leaderOnly(); err != nil {
		return err
	}
	if err := n.recordIdentity(id, identity); err != nil {
		return fmt.Errorf("adding %s: %w", id, err)
	}
	n.learn(raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(address)})
	var f raft.IndexFuture
	if voter {
		f = n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, changeTimeout)
	} else {
		f = n.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(address), 0, changeTimeout)
	}
	if err := f.Error(); err != nil {
		return fmt.Errorf("adding %s: %w", id, err)
	}
	n.logger.Info("member added", "member", id, "address", address, "voter", voter)
	return nil
}

// recordIdentity makes sure every node knows id's identity
func (n *Node) recordIdentity(id, identity string) error {
	known, ok := n.peers.Identity(id)
	if identity == "" {
		if !ok {
			return errors.New("its identity is neither configured in cluster.tls.peers nor given")
		}
		return nil
	}
	if ok && known == identity {
		return nil
	}
	if configured, ok := n.peers.Configured(id); ok {
		return fmt.Errorf("cluster.tls.peers configures its identity as %q, not %q", configured, identity)
	}
	if n.peers.recorded == nil {
		return errors.New("this node cannot record identities")
	}
	if err := n.apply(&fsmpb.Command{Command: &fsmpb.Command_SetPeer{SetPeer: &fsmpb.SetPeer{Id: id, Identity: identity}}}); err != nil {
		return fmt.Errorf("recording its identity: %w", err)
	}
	n.logger.Info("peer identity recorded", "peer", id, "identity", identity)
	return nil
}

// apply commits a command and returns its error, if any
func (n *Node) apply(cmd *fsmpb.Command) error {
	data, err := fsm.Encode(cmd)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, changeTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if res, ok := f.Response().(*fsm.Result); ok && res.Err != nil {
		return res.Err
	}
	return nil
}

// Remove removes a member (FR-2). Removing the leader itself first
// transfers leadership, then reports the new leader in an ErrNotLeader,
// where the removal is retried.
func (n *Node) Remove(id string) error {
	if err := n.leaderOnly(); err != nil {
		return err
	}
	if id == n.cfg.Node.ID {
		if err := n.TransferLeadership(""); err != nil {
			return err
		}
		return n.leaderOnly()
	}
	if err := n.raft.RemoveServer(raft.ServerID(id), 0, changeTimeout).Error(); err != nil {
		return fmt.Errorf("removing %s: %w", id, err)
	}
	n.logger.Info("member removed", "member", id)
	// a recorded identity goes with the member; a configured one stays
	if n.peers.recorded != nil {
		if _, ok := n.peers.recorded()[id]; ok {
			if err := n.apply(&fsmpb.Command{Command: &fsmpb.Command_RemovePeer{RemovePeer: &fsmpb.RemovePeer{Id: id}}}); err != nil {
				return fmt.Errorf("removed %s, but not its identity: %w", id, err)
			}
		}
	}
	return nil
}

// TransferLeadership hands leadership to the named voter, or to any
// other voter if to is empty (FR-3).
func (n *Node) TransferLeadership(to string) error {
	if err := n.leaderOnly(); err != nil {
		return err
	}
	var f raft.Future
	if to == "" {
		f = n.raft.LeadershipTransfer()
	} else {
		cf := n.raft.GetConfiguration()
		if err := cf.Error(); err != nil {
			return fmt.Errorf("transferring leadership: %w", err)
		}
		var addr raft.ServerAddress
		for _, s := range cf.Configuration().Servers {
			if s.ID == raft.ServerID(to) && s.Suffrage == raft.Voter {
				addr = s.Address
			}
		}
		if addr == "" {
			return fmt.Errorf("transferring leadership: %s is not a voter", to)
		}
		f = n.raft.LeadershipTransferToServer(raft.ServerID(to), addr)
	}
	if err := f.Error(); err != nil {
		return fmt.Errorf("transferring leadership: %w", err)
	}
	n.logger.Info("leadership transferred", "to", to)
	return nil
}

// Shutdown stops the node.
func (n *Node) Shutdown() error {
	close(n.done)
	n.raft.DeregisterObserver(n.observer)
	err := n.raft.Shutdown().Error()
	return errors.Join(err, n.net.Close(), n.store.Close())
}
