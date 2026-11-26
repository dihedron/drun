package raftnode

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
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

func (nopStore) Exec(uint64, *fsmpb.StoreBatch) (*fsmpb.BatchResult, error) {
	return &fsmpb.BatchResult{}, nil
}
func (nopStore) PutCheckpoint(uint64, string, *fsmpb.CheckpointMeta, []byte) error { return nil }
func (nopStore) ClearCheckpoint(uint64, string) error                              { return nil }
func (nopStore) Forget(string) error                                               { return nil }
func (nopStore) Snapshot() (fsm.StoreSnapshot, error)                              { return nopSnapshot{}, nil }
func (nopStore) Restore(io.Reader) error                                           { return nil }

type nopSnapshot struct{}

func (nopSnapshot) WriteTo(io.Writer) (int64, error) { return 0, nil }
func (nopSnapshot) Release()                         {}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // only the port is needed
	return ln.Addr().String()
}

type testCluster struct {
	t      *testing.T
	ca     *tlstest.CA
	ids    []string
	addrs  map[string]string
	voters []string // the initial voters
	dirs   map[string]string
	nodes  map[string]*Node
	fsms   map[string]*fsm.FSM
}

// newTestCluster prepares nodes 0..n-1, the first voters of them initial
// voters, with real certificates and ports
func newTestCluster(t *testing.T, n, voters int) *testCluster {
	c := &testCluster{t: t, ca: tlstest.NewCA(t), addrs: map[string]string{}, dirs: map[string]string{}, nodes: map[string]*Node{}, fsms: map[string]*fsm.FSM{}}
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
		fmt.Fprintf(&b, "      %s: %q\n", other, c.identity(other))
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
	f := fsm.New(nopStore{}, discard)
	nd, err := New(c.configFor(id), f, discard)
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

func TestBootstrapReplicateRestart(t *testing.T) {
	c := newTestCluster(t, 3, 3)
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

func TestMembershipAndTransfer(t *testing.T) {
	c := newTestCluster(t, 4, 3)
	for _, id := range c.voters {
		c.start(id)
	}
	l := c.leader(10 * time.Second)

	// a non-voter joins: it has no voter list of its own entry, and does
	// not bootstrap
	extra := c.ids[3]
	nd := c.start(extra)
	if err := l.AddNonvoter(extra, c.addrs[extra]); err != nil {
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
	if err := nd.AddVoter("x", "127.0.0.1:1"); !isNotLeader(err) {
		t.Fatalf("add on a follower: %v, want ErrNotLeader", err)
	}
	if err := l.AddVoter("unknown", "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "peers") {
		t.Fatalf("add without identity: %v", err)
	}
	if err := l.Remove(extra); err != nil {
		t.Fatalf("remove non-voter: %v", err)
	}

	// transfer to a named voter
	target := c.others(l)[0]
	if target == extra {
		target = c.others(l)[1]
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
	err := l.Remove(old)
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
	if _, err := New(cfg, fsm.New(nopStore{}, discard), discard); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Fatalf("New with a 0755 data directory: %v", err)
	}
}
