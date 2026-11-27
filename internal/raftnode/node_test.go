package raftnode

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/fsm"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/dihedron/resurgo/internal/tlstest"
	"github.com/hashicorp/raft"
)

// nopStore is a fsm.Store without databases
type nopStore struct{}

func (nopStore) Exec(uint64, *fsmpb.StoreBatch, *fsmpb.StoreLimits) (*fsmpb.BatchResult, error) {
	return &fsmpb.BatchResult{}, nil
}
func (nopStore) PutCheckpoint(uint64, string, *fsmpb.CheckpointMeta, []byte) error { return nil }
func (nopStore) ClearCheckpoint(uint64, string) error                              { return nil }
func (nopStore) Forget(uint64, string) error                                       { return nil }
func (nopStore) Snapshot() (fsm.StoreSnapshot, error)                              { return nopSnapshot{}, nil }
func (nopStore) Restore(io.Reader) error                                           { return nil }

type nopSnapshot struct{}

func (nopSnapshot) WriteTo(io.Writer) (int64, error) { return 0, nil }
func (nopSnapshot) Release()                         {}

func freePort(t testing.TB) string {
	t.Helper()
	// the port is free when returned, not reserved: one never comes twice
	for {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		if _, dup := handedOut.LoadOrStore(addr, true); !dup {
			return addr
		}
	}
}

var handedOut sync.Map

type testCluster struct {
	t      testing.TB
	ca     *tlstest.CA
	ids    []string
	addrs  map[string]string
	voters []string // the initial voters
	dirs   map[string]string
	nodes  map[string]*Node
	fsms   map[string]*fsm.FSM
	// unlisted nodes are in no other node's cluster.tls.peers
	unlisted map[string]bool
	// newStore, if set, gives each node's store (default: none)
	newStore func(id string) fsm.Store
	// logStore is raft.log_store (default: bolt)
	logStore string
}

// forEachLogStore runs a test with each Raft log store (ADR-1, FW-2)
func forEachLogStore(t *testing.T, test func(t *testing.T, logStore string)) {
	for _, s := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		t.Run(s, func(t *testing.T) { test(t, s) })
	}
}

// newTestCluster prepares nodes 0..n-1, the first voters of them initial
// voters, with real certificates and ports
func newTestCluster(t testing.TB, n, voters int) *testCluster {
	c := &testCluster{t: t, ca: tlstest.NewCA(t), addrs: map[string]string{}, dirs: map[string]string{}, nodes: map[string]*Node{}, fsms: map[string]*fsm.FSM{}, unlisted: map[string]bool{}}
	for i := range n {
		id := fmt.Sprintf("node-%d", i)
		c.ids = append(c.ids, id)
		c.addrs[id] = freePort(t)
		c.dirs[id] = t.TempDir()
		if i < voters {
			c.voters = append(c.voters, id)
		}
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			_ = nd.Shutdown()
		}
	})
	return c
}

func (c *testCluster) identity(id string) string { return "spiffe://example.org/resurgo/" + id }

func (c *testCluster) configFor(id string) *config.Config {
	c.t.Helper()
	dir := c.dirs[id]
	files := tlstest.Files{Cert: filepath.Join(dir, "node.crt"), Key: filepath.Join(dir, "node.key"), CA: filepath.Join(dir, "ca.crt")}
	if _, err := os.Stat(files.Cert); err != nil {
		files = c.ca.WriteFiles(c.t, dir, c.identity(id))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "node: { id: %s, data_dir: %s }\n", id, filepath.Join(dir, "data"))
	fmt.Fprintf(&b, "cluster:\n  name: test\n  bind: %s\n  advertise: %s\n  voters:\n", c.addrs[id], c.addrs[id])
	for _, v := range c.voters {
		fmt.Fprintf(&b, "    - { id: %s, address: %s }\n", v, c.addrs[v])
	}
	fmt.Fprintf(&b, "  tls:\n    cert: %s\n    key: %s\n    ca: %s\n    peers:\n", files.Cert, files.Key, files.CA)
	for _, other := range c.ids {
		if other == id || !c.unlisted[other] {
			fmt.Fprintf(&b, "      %s: %q\n", other, c.identity(other))
		}
	}
	if c.logStore != "" {
		b.WriteString("raft: { log_store: " + c.logStore + " }\n")
	}
	path := filepath.Join(dir, "resurgo.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		c.t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		c.t.Fatalf("config of %s: %v", id, err)
	}
	return cfg
}

func (c *testCluster) start(id string) *Node {
	c.t.Helper()
	var st fsm.Store = nopStore{}
	if c.newStore != nil {
		st = c.newStore(id)
	}
	f := fsm.New(st, discard)
	nd, err := New(c.configFor(id), f, f.Peers, discard)
	if err != nil {
		c.t.Fatalf("starting %s: %v", id, err)
	}
	c.nodes[id], c.fsms[id] = nd, f
	return nd
}

func (c *testCluster) stop(id string) {
	c.t.Helper()
	if err := c.nodes[id].Shutdown(); err != nil {
		c.t.Fatalf("stopping %s: %v", id, err)
	}
	delete(c.nodes, id)
}

func (c *testCluster) leader(timeout time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, nd := range c.nodes {
			if nd.Raft().State() == raft.Leader {
				leaders = append(leaders, nd)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %s", timeout)
	return nil
}

func (c *testCluster) incarnate(l *Node, runnable string) uint64 {
	c.t.Helper()
	data, err := fsm.Encode(&fsmpb.Command{Command: &fsmpb.Command_Incarnate{Incarnate: &fsmpb.Incarnate{Runnable: runnable, Node: string(l.cfg.Node.ID)}}})
	if err != nil {
		c.t.Fatal(err)
	}
	f := l.Raft().Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.t.Fatalf("apply: %v", err)
	}
	res := f.Response().(*fsm.Result) //nolint:forcetypeassert // the FSM's
	if res.Err != nil {
		c.t.Fatalf("incarnate: %v", res.Err)
	}
	return res.Epoch
}

func TestBootstrapReplicateRestart(t *testing.T) { forEachLogStore(t, testBootstrapReplicateRestart) }

func testBootstrapReplicateRestart(t *testing.T, logStore string) {
	c := newTestCluster(t, 3, 3)
	c.logStore = logStore
	for _, id := range c.ids {
		c.start(id)
	}
	l := c.leader(10 * time.Second)
	if e := c.incarnate(l, "svc"); e != 1 {
		t.Fatalf("epoch %d", e)
	}
	if err := l.Raft().Barrier(5 * time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // the followers apply
	for id, f := range c.fsms {
		if f.EpochCounter() != 1 {
			t.Fatalf("%s: epoch counter %d", id, f.EpochCounter())
		}
	}

	// a restarted node keeps its state; its voter list is not used again
	follower := c.others(l)[0]
	c.stop(follower)
	c.voters = []string{follower} // a different list: ignored
	c.start(follower)
	deadline := time.Now().Add(10 * time.Second)
	for c.fsms[follower].EpochCounter() != 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if c.fsms[follower].EpochCounter() != 1 {
		t.Fatal("the restarted node did not get its state back")
	}
	cf := c.nodes[follower].Raft().GetConfiguration()
	if err := cf.Error(); err != nil || len(cf.Configuration().Servers) != 3 {
		t.Fatalf("restarted node's configuration %v %v", cf.Configuration(), err)
	}
}

func (c *testCluster) others(l *Node) []string {
	var out []string
	for id := range c.nodes {
		if id != l.cfg.Node.ID {
			out = append(out, id)
		}
	}
	return out
}

func TestMembershipAndTransfer(t *testing.T) { forEachLogStore(t, testMembershipAndTransfer) }

func testMembershipAndTransfer(t *testing.T, logStore string) {
	c := newTestCluster(t, 4, 3)
	c.logStore = logStore
	extra := c.ids[3]
	c.unlisted[extra] = true
	for _, id := range c.voters {
		c.start(id)
	}
	l := c.leader(10 * time.Second)

	// a non-voter joins that no other node's configuration knows: its
	// identity is recorded through the log, without restarts; it is not an
	// initial voter, so it does not bootstrap
	nd := c.start(extra)
	if err := l.AddNonvoter(extra, c.addrs[extra], ""); err == nil {
		t.Fatal("added a member whose identity nobody knows")
	}
	if err := l.AddNonvoter(extra, c.addrs[extra], c.identity(extra)); err != nil {
		t.Fatalf("add non-voter: %v", err)
	}
	c.incarnate(l, "svc")
	deadline := time.Now().Add(10 * time.Second)
	for c.fsms[extra].EpochCounter() != 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if c.fsms[extra].EpochCounter() != 1 {
		t.Fatal("the non-voter did not replicate")
	}
	if err := nd.AddVoter("x", "127.0.0.1:1", ""); !isNotLeader(err) {
		t.Fatalf("add on a follower: %v, want ErrNotLeader", err)
	}
	if err := l.AddVoter("unknown", "127.0.0.1:1", ""); err == nil || !strings.Contains(err.Error(), "cluster.tls.peers") {
		t.Fatalf("add without identity: %v", err)
	}
	if err := l.AddVoter(c.voters[1], c.addrs[c.voters[1]], "spiffe://example.org/other"); err == nil || !strings.Contains(err.Error(), "configures") {
		t.Fatalf("add with an identity other than the configured one: %v", err)
	}
	if err := l.Remove(extra); err != nil {
		t.Fatalf("remove non-voter: %v", err)
	}
	if _, ok := c.fsms[l.cfg.Node.ID].Peers()[extra]; ok {
		t.Fatal("the removed member's identity is still recorded")
	}
	// the removed member can no longer connect
	conn, err := nd.Transport().RaftLayer().Dial(raft.ServerAddress(c.addrs[l.cfg.Node.ID]), 2*time.Second)
	if err == nil {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}
	if err == nil || os.IsTimeout(err) {
		t.Fatalf("the removed member connected: %v", err)
	}

	// transfer to a named voter
	// others is a map's order: one call, then any voter but the removed one
	var target string
	for _, id := range c.others(l) {
		if id != extra {
			target = id
			break
		}
	}
	if err := l.TransferLeadership(target); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if nl := c.leader(10 * time.Second); nl.cfg.Node.ID != target {
		t.Fatalf("leader %s after a transfer to %s", nl.cfg.Node.ID, target)
	}

	// removing the leader transfers leadership first
	l = c.leader(10 * time.Second)
	old := l.cfg.Node.ID
	err = l.Remove(old)
	if !isNotLeader(err) {
		t.Fatalf("removing the leader: %v, want ErrNotLeader after the transfer", err)
	}
	nl := c.leader(10 * time.Second)
	if nl.cfg.Node.ID == old {
		t.Fatal("the leader did not transfer")
	}
	if err := nl.Remove(old); err != nil {
		t.Fatalf("removing the old leader at the new one: %v", err)
	}
	cf := nl.Raft().GetConfiguration()
	if err := cf.Error(); err != nil {
		t.Fatal(err)
	}
	for _, s := range cf.Configuration().Servers {
		if string(s.ID) == old || string(s.ID) == extra {
			t.Fatalf("%s still a member: %v", s.ID, cf.Configuration().Servers)
		}
	}
}

func isNotLeader(err error) bool {
	var nl *ErrNotLeader
	return errors.As(err, &nl)
}

func TestDataDirMustBePrivate(t *testing.T) {
	c := newTestCluster(t, 1, 1)
	cfg := c.configFor(c.ids[0])
	if err := os.MkdirAll(cfg.Node.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cfg.Node.DataDir, 0o755); err != nil { //nolint:gosec // the test's point
		t.Fatal(err)
	}
	if _, err := New(cfg, fsm.New(nopStore{}, discard), nil, discard); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Fatalf("New with a 0755 data directory: %v", err)
	}
}

// changing raft.log_store on a node with state is refused: rebuild it
func TestLogStoreSwitchRefused(t *testing.T) {
	for _, from := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		t.Run(from, func(t *testing.T) {
			c := newTestCluster(t, 1, 1)
			c.logStore = from
			c.start("node-0")
			c.leader(10 * time.Second)
			c.stop("node-0")
			c.logStore = map[string]string{config.LogStoreBolt: config.LogStoreWAL, config.LogStoreWAL: config.LogStoreBolt}[from]
			f := fsm.New(nopStore{}, discard)
			if _, err := New(c.configFor("node-0"), f, f.Peers, discard); err == nil || !strings.Contains(err.Error(), "log_store") {
				t.Fatalf("switching from %s: %v", from, err)
			}
		})
	}
}

// unset, the store is the one the data is in, wal for a new node
func TestLogStoreUnset(t *testing.T) {
	for _, kind := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		dir := t.TempDir()
		st, err := openLogStore(kind, dir, discard)
		if err != nil {
			t.Fatal(err)
		}
		_ = st.StoreLog(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("x")})
		_ = st.Close()
		st, err = openLogStore("", dir, discard)
		if err != nil {
			t.Fatalf("reopening %s unset: %v", kind, err)
		}
		if got := fmt.Sprintf("%T", st); (kind == config.LogStoreWAL) != strings.Contains(got, "wal") {
			t.Fatalf("data in %s reopened as %s", kind, got)
		}
		_ = st.Close()
	}
	st, err := openLogStore("", t.TempDir(), discard)
	if err != nil || !strings.Contains(fmt.Sprintf("%T", st), "wal") {
		t.Fatalf("a new node's store: %T %v", st, err)
	}
	_ = st.Close()
}
