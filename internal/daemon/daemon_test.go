package daemon

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/admin"
	"github.com/dihedron/drun/internal/admin/adminpb"
	"github.com/dihedron/drun/internal/config"
	"github.com/dihedron/drun/internal/supervisor"
	"github.com/dihedron/drun/internal/tlstest"
	"github.com/dihedron/drun/pkg/runnable"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var discard = slog.New(slog.DiscardHandler)

// ticker checkpoints a counter until stopped
type ticker struct{}

func (ticker) Describe() runnable.Description {
	return runnable.Description{Name: "ticker", Kind: runnable.Service}
}
func (ticker) Validate(context.Context, []byte) error { return nil }
func (ticker) Run(ctx context.Context, _ runnable.Incarnation, host runnable.Host) error {
	_ = host.Status(ctx, true, "ticking")
	for n := uint64(1); ; n++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		if _, err := host.Checkpoint(ctx, fmt.Appendf(nil, "%d", n)); err != nil {
			return err
		}
	}
}

type tcluster struct {
	t     *testing.T
	ca    *tlstest.CA
	ids   []string
	addrs map[string]string
	dirs  map[string]string
	socks map[string]string
	nodes map[string]*Daemon
	// extra is appended to a node's admin configuration, top at the top level
	extra, top map[string]string
	// runnableConfig is each node's configuration of the runnable
	runnableConfig map[string]string
	// invalid makes a node's loader fail
	invalid map[string]error
	// voters overrides a node's cluster.voters (default: every node)
	voters map[string][]string
	// logStore is raft.log_store (default: bolt)
	logStore string
	group    string
}

func freePort(t *testing.T) string {
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

func newTCluster(t *testing.T, n int) *tcluster {
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	g, err := user.LookupGroupId(u.Gid)
	if err != nil {
		t.Fatal(err)
	}
	// unix socket paths are short: not under t.TempDir
	sockDir, err := os.MkdirTemp("", "rsg")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	c := &tcluster{t: t, ca: tlstest.NewCA(t), addrs: map[string]string{}, dirs: map[string]string{}, socks: map[string]string{},
		nodes: map[string]*Daemon{}, extra: map[string]string{}, top: map[string]string{}, runnableConfig: map[string]string{}, invalid: map[string]error{}, group: g.Name}
	for i := range n {
		id := fmt.Sprintf("node-%d", i)
		c.ids = append(c.ids, id)
		c.addrs[id], c.dirs[id], c.socks[id] = freePort(t), t.TempDir(), filepath.Join(sockDir, id+".sock")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, d := range c.nodes {
			_ = d.Stop(ctx)
		}
	})
	return c
}

func identity(id string) string { return "spiffe://example.org/drun/" + id }

func (c *tcluster) config(id string) *config.Config {
	c.t.Helper()
	dir := c.dirs[id]
	files := c.ca.WriteFiles(c.t, dir, identity(id))
	var b strings.Builder
	fmt.Fprintf(&b, "node: { id: %s, data_dir: %s }\n", id, filepath.Join(dir, "data"))
	fmt.Fprintf(&b, "cluster:\n  name: test\n  bind: %s\n  advertise: %s\n  voters:\n", c.addrs[id], c.addrs[id])
	voters := c.ids
	if v, ok := c.voters[id]; ok {
		voters = v
	}
	for _, v := range voters {
		fmt.Fprintf(&b, "    - { id: %s, address: %s }\n", v, c.addrs[v])
	}
	fmt.Fprintf(&b, "  tls:\n    cert: %s\n    key: %s\n    ca: %s\n    peers:\n", files.Cert, files.Key, files.CA)
	for _, other := range c.ids {
		fmt.Fprintf(&b, "      %s: %q\n", other, identity(other))
	}
	logStore := c.logStore
	if logStore == "" {
		logStore = config.LogStoreBolt
	}
	fmt.Fprintf(&b, "raft: { heartbeat_timeout: 300ms, election_timeout: 300ms, leader_lease_timeout: 150ms, log_store: %s }\n", logStore)
	b.WriteString("lease: { duration: 200ms, renew_interval: 60ms }\n")
	fmt.Fprintf(&b, "admin:\n  socket: %s\n  allowed_groups: [%q]\n%s", c.socks[id], c.group, c.extra[id])
	b.WriteString(c.top[id])
	rc := c.runnableConfig[id]
	if rc == "" {
		rc = "{ n: 1 }"
	}
	fmt.Fprintf(&b, "runnables:\n  - name: ticker\n    binary: /usr/libexec/ticker\n    sha256: %s\n    config: %s\n", strings.Repeat("ab", 32), rc)
	b.WriteString("    takeover_delay: 300ms\n    stop_grace: 200ms\n    shutdown_grace: 2s\n    checkpoint: { min_interval: 10ms }\n")
	b.WriteString("    restart: { initial: 50ms, max: 200ms, handover_after: 2 }\n")
	path := filepath.Join(dir, "drun.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		c.t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		c.t.Fatalf("configuration of %s: %v", id, err)
	}
	return cfg
}

func (c *tcluster) start(id string) *Daemon {
	c.t.Helper()
	d, err := Start(c.config(id), Options{
		Load: func(context.Context, supervisor.Spec) (runnable.Instance, error) {
			if err := c.invalid[id]; err != nil {
				return nil, err
			}
			return runnable.InProcess(ticker{}), nil
		},
		Fatal:  func(err error) { c.t.Errorf("%s: fatal: %v", id, err) },
		Logger: discard,
	})
	if err != nil {
		c.t.Fatalf("starting %s: %v", id, err)
	}
	c.nodes[id] = d
	return d
}

func (c *tcluster) startAll() {
	for _, id := range c.ids {
		c.start(id)
	}
}

func (c *tcluster) leader() string {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []string
		for id, d := range c.nodes {
			if d.Node.Raft().State() == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no single leader")
	return ""
}

func (c *tcluster) follower(leader string) string {
	for _, id := range c.ids {
		if id != leader && c.nodes[id] != nil {
			return id
		}
	}
	c.t.Fatal("no follower")
	return ""
}

// SEC-6: the socket and its directory belong to the first allowed group
func TestSocketGroup(t *testing.T) {
	c := newTCluster(t, 1)
	c.start("node-0")
	g, err := user.LookupGroup(c.group)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{c.socks["node-0"], filepath.Dir(c.socks["node-0"])} {
		var st syscall.Stat_t
		if err := syscall.Stat(p, &st); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(st.Gid) != g.Gid {
			t.Fatalf("%s has gid %d, want %s", p, st.Gid, g.Gid)
		}
	}
}

func (c *tcluster) client(id string) *admin.Client {
	c.t.Helper()
	cl, err := admin.Dial("unix://"+c.socks[id], admin.ClientTLS{})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// forEachLogStore runs a test with each Raft log store (ADR-1, FW-2)
func forEachLogStore(t *testing.T, test func(t *testing.T, logStore string)) {
	for _, s := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		t.Run(s, func(t *testing.T) { test(t, s) })
	}
}

func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("not %s within %s", what, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func (c *tcluster) view(id string) *adminpb.RunnableInfo {
	c.t.Helper()
	r, err := c.client(id).RunnableShow(ctx(c.t), &adminpb.RunnableShowRequest{Name: "ticker"})
	if err != nil {
		c.t.Fatalf("runnable show on %s: %v", id, err)
	}
	return r.GetRunnables()[0]
}

// a follower forwards mutations to the leader; show includes the
// leader's views
func TestForwardedToTheLeader(t *testing.T) { forEachLogStore(t, testForwardedToTheLeader) }

func testForwardedToTheLeader(t *testing.T, logStore string) {
	c := newTCluster(t, 3)
	c.logStore = logStore
	c.startAll()
	l := c.leader()
	f := c.follower(l)
	eventually(t, 5*time.Second, "running, as the follower knows", func() bool {
		v := c.view(f)
		return v.GetView().GetState() == supervisor.StateRunning && v.GetState().GetCurrent() != nil
	})
	if v := c.view(f); v.GetView().GetNode() != l || v.GetState().GetCurrent().GetNode() != l || !v.GetConfigured() || v.GetDrift() {
		t.Fatalf("show on the follower: %+v", v)
	}

	if _, err := c.client(f).RunnableSet(ctx(t), &adminpb.RunnableSetRequest{Name: "ticker", Disabled: true}); err != nil {
		t.Fatalf("disable through a follower: %v", err)
	}
	if r, _ := c.nodes[l].FSM.Runnable("ticker"); !r.GetDisabled() {
		t.Fatal("the leader did not disable the runnable")
	}
	eventually(t, 5*time.Second, "shown disabled", func() bool { return c.view(f).GetView().GetState() == supervisor.StateDisabled })
	if _, err := c.client(f).RunnableSet(ctx(t), &adminpb.RunnableSetRequest{Name: "ticker"}); err != nil {
		t.Fatalf("enable: %v", err)
	}
	eventually(t, 5*time.Second, "running again", func() bool { return c.view(f).GetView().GetState() == supervisor.StateRunning })

	if _, err := c.client(f).RunnableRestart(ctx(t), &adminpb.RunnableRequest{Name: "ticker"}); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if _, err := c.client(f).RunnableReset(ctx(t), &adminpb.RunnableResetRequest{Name: "ticker", Failures: true}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := c.client(f).RunnableReset(ctx(t), &adminpb.RunnableResetRequest{Name: "ticker"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reset of nothing: %v", err)
	}
	if _, err := c.client(f).RunnableSet(ctx(t), &adminpb.RunnableSetRequest{Name: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown runnable: %v", err)
	}

	cp, err := c.client(f).CheckpointShow(ctx(t), &adminpb.CheckpointShowRequest{Name: "ticker", Data: true})
	if err != nil || !cp.GetFound() || cp.GetMeta().GetSeq() == 0 || len(cp.GetData()) == 0 {
		t.Fatalf("checkpoint show: %v %+v", err, cp)
	}
	cs, err := c.client(f).ClusterShow(ctx(t), &adminpb.Empty{})
	if err != nil || cs.GetLeaderId() != l || len(cs.GetMembers()) != 3 || cs.GetMembers()[0].GetIdentity() == "" {
		t.Fatalf("cluster show: %v %+v", err, cs)
	}
}

// FR-2, FR-3 through a follower
func TestMembershipAndTransfer(t *testing.T) {
	c := newTCluster(t, 3)
	c.startAll()
	l := c.leader()
	f := c.follower(l)
	cl := c.client(f)

	if _, err := cl.MemberAdd(ctx(t), &adminpb.MemberAddRequest{Id: "node-9", Address: freePort(t), Identity: identity("node-9"), NonVoter: true}); err != nil {
		t.Fatalf("member add: %v", err)
	}
	// the follower answers from its own state
	eventually(t, 5*time.Second, "shown added", func() bool {
		cs, _ := cl.ClusterShow(ctx(t), &adminpb.Empty{})
		return len(cs.GetMembers()) == 4 && cs.GetMembers()[3].GetIdentity() == identity("node-9")
	})
	if _, err := cl.MemberRemove(ctx(t), &adminpb.MemberRemoveRequest{Id: "node-9"}); err != nil {
		t.Fatalf("member remove: %v", err)
	}

	if _, err := cl.LeaderTransfer(ctx(t), &adminpb.LeaderTransferRequest{To: f}); err != nil {
		t.Fatalf("leader transfer: %v", err)
	}
	if nl := c.leader(); nl != f {
		t.Fatalf("leader %s, want %s", nl, f)
	}

	// removing the leader: handed over, then removed by the new leader
	if _, err := c.client(l).MemberRemove(ctx(t), &adminpb.MemberRemoveRequest{Id: f}); err != nil {
		t.Fatalf("removing the leader: %v", err)
	}
	eventually(t, 5*time.Second, "removed", func() bool {
		cs, err := c.client(l).ClusterShow(ctx(t), &adminpb.Empty{})
		return err == nil && len(cs.GetMembers()) == 2 && cs.GetLeaderId() != f
	})
}

// a forwarded call that reaches a follower is not forwarded again
func TestNoSecondHop(t *testing.T) {
	c := newTCluster(t, 3)
	c.startAll()
	l := c.leader()
	f := c.follower(l)
	from := c.nodes[l]
	conn, err := from.Node.Transport().DialAdmin(ctx(t), raft.ServerAddress(c.addrs[f]))
	if err != nil {
		t.Fatal(err)
	}
	cl := adminClientOver(t, conn)
	_, err = cl.RunnableSet(ctx(t), &adminpb.RunnableSetRequest{Name: "ticker", Disabled: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a call without its caller: %v", err)
	}
	_, err = cl.RunnableSet(withCaller(ctx(t), "uid=0"), &adminpb.RunnableSetRequest{Name: "ticker", Disabled: true})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("a forwarded call on a follower: %v", err)
	}
}

func adminClientOver(t *testing.T, conn net.Conn) adminpb.AdminClient {
	t.Helper()
	used := false
	cc, err := grpc.NewClient("passthrough:///peer", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			if used {
				return nil, errors.New("one connection only")
			}
			used = true
			return conn, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return adminpb.NewAdminClient(cc)
}

func withCaller(ctx context.Context, who string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "drun-caller", who)
}

// SEC-6: a caller in no allowed group is refused
func TestSocketRefusesOtherGroups(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root is always allowed")
	}
	c := newTCluster(t, 1)
	d := c.start("node-0")
	c.leader()
	sock := filepath.Join(filepath.Dir(c.socks["node-0"]), "closed.sock")
	e, err := admin.UnixEndpoint(sock, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Admin.Serve(e); err != nil {
		t.Fatal(err)
	}
	cl, err := admin.Dial("unix://"+sock, admin.ClientTLS{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close() //nolint:errcheck // test
	if _, err := cl.ClusterShow(ctx(t), &adminpb.Empty{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a caller in no allowed group: %v", err)
	}
	if fi, err := os.Stat(sock); err != nil || fi.Mode().Perm() != 0o660 {
		t.Fatalf("socket mode: %v %v", fi.Mode(), err)
	}
	// a live socket is not taken over
	if _, err := admin.UnixEndpoint(sock, []string{"no-such-group-here"}); err == nil {
		t.Fatal("an unknown group was accepted")
	}
	if _, err := e.Listen(); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("listening on a live socket: %v", err)
	}
}

// SEC-11: the remote endpoint admits only allowed identities, over TLS 1.3
func TestRemoteEndpoint(t *testing.T) {
	c := newTCluster(t, 1)
	addr := freePort(t)
	dir := t.TempDir()
	srv := c.ca.WriteFiles(t, dir, identity("node-0"))
	c.extra["node-0"] = fmt.Sprintf("  remote:\n    listen: %s\n    tls: { cert: %s, key: %s, ca: %s }\n    allowed_identities: [\"spiffe://example.org/ops\"]\n",
		addr, srv.Cert, srv.Key, srv.CA)
	c.start("node-0")
	c.leader()

	issue := func(ca *tlstest.CA, id string) admin.ClientTLS {
		d := t.TempDir()
		f := ca.WriteFiles(t, d, id)
		return admin.ClientTLS{Cert: f.Cert, Key: f.Key, CA: srv.CA}
	}
	call := func(tc admin.ClientTLS) error {
		cl, err := admin.Dial("tls://"+addr, tc)
		if err != nil {
			return err
		}
		defer cl.Close() //nolint:errcheck // test
		_, err = cl.ClusterShow(ctx(t), &adminpb.Empty{})
		return err
	}
	ops := issue(c.ca, "spiffe://example.org/ops")
	if err := call(ops); err != nil {
		t.Fatalf("an allowed identity: %v", err)
	}
	ops.ServerIdentity = identity("node-0")
	if err := call(ops); err != nil {
		t.Fatalf("with the server identity checked: %v", err)
	}
	ops.ServerIdentity = identity("node-7")
	if err := call(ops); err == nil {
		t.Fatal("a server with another identity was accepted")
	}
	if err := call(issue(c.ca, "spiffe://example.org/intruder")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("another identity: %v", err)
	}
	if err := call(issue(tlstest.NewCA(t), "spiffe://example.org/ops")); err == nil {
		t.Fatal("a certificate of another CA was accepted")
	}
	if err := call(admin.ClientTLS{}); err == nil {
		t.Fatal("a client without a certificate was accepted")
	}
	// TLS 1.2 and no client certificate, at the TLS level
	for name, cfg := range map[string]*tls.Config{
		"TLS 1.2":        {MaxVersion: tls.VersionTLS12, InsecureSkipVerify: true}, //nolint:gosec // the test
		"no certificate": {MinVersion: tls.VersionTLS13, InsecureSkipVerify: true}, //nolint:gosec // the test
	} {
		conn, err := tls.Dial("tcp", addr, cfg)
		if err == nil {
			// TLS 1.3 reports a refused client certificate on the first read
			_, err = conn.Read(make([]byte, 1))
			_ = conn.Close()
		}
		if err == nil || errors.Is(err, io.EOF) && name == "TLS 1.2" {
			t.Fatalf("%s: accepted (%v)", name, err)
		}
	}
}

// FR-15, FR-16: drift and validation are reported by the node concerned
func TestDriftAndValidation(t *testing.T) {
	c := newTCluster(t, 3)
	c.runnableConfig["node-2"] = "{ n: 2 }"
	c.invalid["node-2"] = errors.New("binary does not match")
	c.startAll()
	// a node that cannot run the runnable gives leadership away (OI-11)
	eventually(t, 10*time.Second, "running elsewhere", func() bool {
		v := c.view("node-2").GetView()
		return v.GetState() == supervisor.StateRunning && v.GetNode() != "node-2"
	})
	eventually(t, 5*time.Second, "applied on node-2", func() bool { return c.view("node-2").GetState().GetCurrent() != nil })
	v := c.view("node-2")
	if !v.GetDrift() || !strings.Contains(v.GetValidationError(), "binary does not match") {
		t.Fatalf("on the drifted node: %+v", v)
	}
	if v := c.view("node-0"); v.GetDrift() || v.GetValidationError() != "" {
		t.Fatalf("on a matching node: %+v", v)
	}
}

// STO-15 to STO-17 through a follower
func TestStoreCommands(t *testing.T) { forEachLogStore(t, testStoreCommands) }

func testStoreCommands(t *testing.T, logStore string) {
	c := newTCluster(t, 3)
	c.logStore = logStore
	c.startAll()
	l := c.leader()
	f := c.follower(l)
	cl := c.client(f)
	eventually(t, 5*time.Second, "running", func() bool { return c.view(f).GetView().GetState() == supervisor.StateRunning })
	first := c.view(f).GetView().GetEpoch()

	ex, err := cl.StoreExec(ctx(t), &adminpb.StoreExecRequest{Name: "ticker", Sql: "CREATE TABLE t (x);\nINSERT INTO t VALUES (1), (2);"})
	if err != nil || ex.GetResult().GetError() != nil || ex.GetEpoch() <= first {
		t.Fatalf("store exec: %v %+v", err, ex)
	}
	// the runnable starts again after the operator's incarnation
	eventually(t, 5*time.Second, "running after the batch", func() bool {
		v := c.view(f).GetView()
		return v.GetState() == supervisor.StateRunning && v.GetEpoch() > ex.GetEpoch()
	})

	q, err := cl.StoreQuery(ctx(t), &adminpb.StoreQueryRequest{Name: "ticker", Sql: "SELECT count(*) FROM t"})
	if err != nil || q.GetNode() != l || q.GetStale() || q.GetRows()[0].GetValues()[0].GetInteger() != 2 {
		t.Fatalf("store query: %v %+v", err, q)
	}
	eventually(t, 5*time.Second, "a local query", func() bool {
		q, err := cl.StoreQuery(ctx(t), &adminpb.StoreQueryRequest{Name: "ticker", Sql: "SELECT count(*) FROM t", Local: true})
		return err == nil && q.GetNode() == f && q.GetStale() && q.GetRows()[0].GetValues()[0].GetInteger() == 2
	})
	if _, err := cl.StoreQuery(ctx(t), &adminpb.StoreQueryRequest{Name: "ticker", Sql: "DELETE FROM t"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a write through query: %v", err)
	}

	dry, err := cl.StoreExec(ctx(t), &adminpb.StoreExecRequest{Name: "ticker", Sql: "INSERT INTO t VALUES (3); SELECT count(*) FROM t", DryRun: true})
	if err != nil || dry.GetEpoch() != 0 || dry.GetResult().GetResults()[1].GetRows()[0].GetValues()[0].GetInteger() != 3 {
		t.Fatalf("dry run: %v %+v", err, dry)
	}

	show, err := cl.StoreShow(ctx(t), &adminpb.StoreShowRequest{Name: "ticker"})
	if err != nil {
		t.Fatalf("store show: %v", err)
	}
	si := show.GetStores()[0]
	if !si.GetExists() || si.GetSize() == 0 || len(si.GetTables()) != 1 || si.GetTables()[0] != "t" || len(si.GetDigests()) != 3 || si.GetMismatch() {
		t.Fatalf("store show: %+v", si)
	}
	for _, d := range si.GetDigests() {
		if d.GetError() != "" || d.GetDigest() == "" {
			t.Fatalf("digest %+v", d)
		}
	}

	stream, err := cl.StoreBackup(ctx(t), &adminpb.RunnableRequest{Name: "ticker"})
	if err != nil {
		t.Fatal(err)
	}
	var backup []byte
	for {
		ch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("backup: %v", err)
		}
		backup = append(backup, ch.GetData()...)
	}
	if !bytes.HasPrefix(backup, []byte("SQLite format 3\x00")) {
		t.Fatalf("backup of %d bytes is not a database", len(backup))
	}

	if _, err := cl.StoreReset(ctx(t), &adminpb.RunnableRequest{Name: "ticker"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reset while running: %v", err)
	}
	ex, err = cl.StoreExec(ctx(t), &adminpb.StoreExecRequest{Name: "ticker", Sql: "DELETE FROM t", Disable: true})
	if err != nil || ex.GetResult().GetResults()[0].GetRowsAffected() != 2 {
		t.Fatalf("store exec --disable: %v %+v", err, ex)
	}
	eventually(t, 5*time.Second, "disabled", func() bool { return c.view(f).GetView().GetState() == supervisor.StateDisabled })
	if _, err := cl.StoreReset(ctx(t), &adminpb.RunnableRequest{Name: "ticker"}); err != nil {
		t.Fatalf("store reset: %v", err)
	}
	if info, err := c.nodes[l].Store.Info("ticker"); err != nil || info.Exists {
		t.Fatalf("after the reset: %+v %v", info, err)
	}
}

// §10.3 on metrics.listen
func TestMetricsEndpoint(t *testing.T) { forEachLogStore(t, testMetricsEndpoint) }

func testMetricsEndpoint(t *testing.T, logStore string) {
	c := newTCluster(t, 1)
	c.logStore = logStore
	storeMetric := map[string]string{config.LogStoreBolt: "drun_raft_boltdb_store_logs_count ", config.LogStoreWAL: "drun_raft_wal_log_appends_total "}[logStore]
	addr := freePort(t)
	c.top["node-0"] = "metrics: { listen: " + addr + " }\n"
	c.start("node-0")
	c.leader()
	eventually(t, 5*time.Second, "a checkpoint counted", func() bool {
		resp, err := http.Get("http://" + addr + "/metrics") //nolint:noctx // test
		if err != nil {
			return false
		}
		defer resp.Body.Close() //nolint:errcheck // test
		body, _ := io.ReadAll(resp.Body)
		out := string(body)
		return strings.Contains(out, "drun_is_leader 1") &&
			strings.Contains(out, `drun_incarnation_state{runnable="ticker",state="running"} 1`) &&
			strings.Contains(out, `drun_failover_seconds{kind="planned",runnable="ticker"}`) &&
			strings.Contains(out, `drun_checkpoint_commit_seconds_count{runnable="ticker"}`) &&
			// Raft's own (FW-3): hashicorp/raft's and the log store's
			strings.Contains(out, "drun_raft_state_leader_total 1") &&
			strings.Contains(out, "drun_raft_commit_time_count ") &&
			strings.Contains(out, storeMetric)
	})
}
