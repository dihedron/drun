// Package spike holds the regression tests of the external assumptions the
// design relies on (EXT-1 to EXT-9 in resurgo_specification.md §12.1 and
// EXT-S-1 to EXT-S-5 in resurgo-store.md §7): re-run them on every bump of
// hashicorp/raft, hashicorp/go-plugin or mattn/go-sqlite3 ("make spike").
package spike

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// repeat is how many times the randomised tests repeat; "make spike" raises it
var repeat = flag.Int("spike.repeat", 5, "repetitions of the randomised spike tests")

// the LAN timings of §8.6 divided by five, so that the tests run quickly and
// keep the same ratios
const (
	heartbeatTimeout   = 200 * time.Millisecond
	electionTimeout    = 200 * time.Millisecond
	leaderLeaseTimeout = 100 * time.Millisecond
)

// recordingFSM records every log it applies and how often it was restored
type recordingFSM struct {
	mu       sync.Mutex
	logs     []raft.Log
	restores int
}

func (f *recordingFSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, raft.Log{Index: l.Index, Term: l.Term, Type: l.Type, Data: slices.Clone(l.Data)})
	return l.Index
}

// Snapshot persists only the data of the applied logs, one per line
func (f *recordingFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf bytes.Buffer
	for _, l := range f.logs {
		buf.Write(l.Data)
		buf.WriteByte('\n')
	}
	return &bytesSnapshot{data: buf.Bytes()}, nil
}

func (f *recordingFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close() //nolint:errcheck // read-only
	if _, err := io.ReadAll(rc); err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores++
	return nil
}

// has reports whether an entry with this data was applied
func (f *recordingFSM) has(data []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.ContainsFunc(f.logs, func(l raft.Log) bool { return bytes.Equal(l.Data, data) })
}

func (f *recordingFSM) applied() []raft.Log {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.logs)
}

type bytesSnapshot struct{ data []byte }

func (s *bytesSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("writing snapshot: %w", err)
	}
	return sink.Close()
}

func (s *bytesSnapshot) Release() {}

type node struct {
	id    raft.ServerID
	addr  raft.ServerAddress
	trans *raft.InmemTransport
	fsm   *recordingFSM
	raft  *raft.Raft
}

type cluster struct {
	t     testing.TB
	nodes []*node
}

func testConfig(id raft.ServerID) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = id
	c.HeartbeatTimeout = heartbeatTimeout
	c.ElectionTimeout = electionTimeout
	c.LeaderLeaseTimeout = leaderLeaseTimeout
	c.CommitTimeout = 5 * time.Millisecond
	c.Logger = hclog.New(&hclog.LoggerOptions{Name: string(id), Level: hclog.Error})
	return c
}

// newNodes creates n connected but not started nodes
func newNodes(t testing.TB, n int) *cluster {
	c := &cluster{t: t}
	for i := range n {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node-%d", i)))
		c.nodes = append(c.nodes, &node{id: raft.ServerID(addr), addr: addr, trans: trans, fsm: &recordingFSM{}})
	}
	c.heal()
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) configuration() raft.Configuration {
	var cfg raft.Configuration
	for _, n := range c.nodes {
		cfg.Servers = append(cfg.Servers, raft.Server{Suffrage: raft.Voter, ID: n.id, Address: n.addr})
	}
	return cfg
}

// start starts the node with in-memory stores, changed by tweak if not nil
func (c *cluster) start(n *node, tweak func(*raft.Config)) {
	cfg := testConfig(n.id)
	if tweak != nil {
		tweak(cfg)
	}
	r, err := raft.NewRaft(cfg, n.fsm, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), n.trans)
	if err != nil {
		c.t.Fatalf("starting %s: %v", n.id, err)
	}
	n.raft = r
}

// newCluster starts and bootstraps an n-node cluster and waits for its leader
func newCluster(t testing.TB, n int, tweak func(*raft.Config)) *cluster {
	c := newNodes(t, n)
	for _, nd := range c.nodes {
		c.start(nd, tweak)
	}
	if err := c.nodes[0].raft.BootstrapCluster(c.configuration()).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	c.leader(5 * time.Second)
	return c
}

func (c *cluster) shutdown() {
	for _, n := range c.nodes {
		if n.raft != nil {
			_ = n.raft.Shutdown().Error()
		}
	}
}

// leader waits for exactly one leader among the connected nodes
func (c *cluster) leader(timeout time.Duration, among ...*node) *node {
	c.t.Helper()
	if len(among) == 0 {
		among = c.nodes
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*node
		for _, n := range among {
			if n.raft != nil && n.raft.State() == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %s", timeout)
	return nil
}

func (c *cluster) others(n *node) []*node {
	return slices.DeleteFunc(slices.Clone(c.nodes), func(o *node) bool { return o == n })
}

// isolate cuts n off from every other node, in both directions
func (c *cluster) isolate(n *node) {
	n.trans.DisconnectAll()
	for _, o := range c.others(n) {
		o.trans.Disconnect(n.addr)
	}
}

// heal connects every node to every other
func (c *cluster) heal() {
	for _, a := range c.nodes {
		for _, b := range c.nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}
}
