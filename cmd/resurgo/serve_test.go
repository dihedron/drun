package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dihedron/resurgo/internal/tlstest"
)

// a one-node cluster: serve, the CLI against its socket, then SIGTERM
func TestServeAndCLI(t *testing.T) {
	dir := t.TempDir()
	ca := tlstest.NewCA(t)
	files := ca.WriteFiles(t, dir, "spiffe://example.org/resurgo/solo")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	sockDir, err := os.MkdirTemp("", "rsg")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sockDir) //nolint:errcheck // test
	sock := filepath.Join(sockDir, "admin.sock")
	u, _ := user.Current()
	g, _ := user.LookupGroupId(u.Gid)
	cfg := fmt.Sprintf(`node: { id: solo, data_dir: %s }
cluster:
  name: test
  bind: %s
  advertise: %s
  voters: [{ id: solo, address: %s }]
  tls: { cert: %s, key: %s, ca: %s, peers: { solo: "spiffe://example.org/resurgo/solo" } }
admin: { socket: %s, allowed_groups: [%q] }
`, filepath.Join(dir, "data"), addr, addr, addr, files.Cert, files.Key, files.CA, sock, g.Name)
	path := filepath.Join(dir, "resurgo.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envName("LOG_LEVEL"), "off")
	t.Setenv("RESURGO_ADMIN", "unix://"+sock)

	// serve runs on its own: run() sets the global options the CLI calls use
	served := make(chan error, 1)
	go func() { served <- (&ServeCommand{Config: path}).Execute(nil) }()
	cli := func(args ...string) (string, int) {
		var out bytes.Buffer
		stdout = &out
		defer func() { stdout = os.Stdout }()
		code := run(args)
		return out.String(), code
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if out, code := cli("leader", "show"); code == 0 && strings.HasPrefix(out, "solo ") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the node did not become leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if out, code := cli("cluster", "show"); code != 0 || !strings.Contains(out, "IDENTITY") || !strings.Contains(out, "spiffe://example.org/resurgo/solo") {
		t.Fatalf("cluster show: %d\n%s", code, out)
	}
	out, code := cli("--json", "cluster", "show")
	var shown struct {
		LeaderID string `json:"leaderId"`
	}
	if err := json.Unmarshal([]byte(out), &shown); code != 0 || err != nil || shown.LeaderID != "solo" {
		t.Fatalf("cluster show --json: %d %v\n%s", code, err, out)
	}
	if out, code := cli("runnable", "show"); code != 0 || !strings.HasPrefix(out, "NAME") {
		t.Fatalf("runnable show: %d\n%s", code, out)
	}
	if _, code := cli("runnable", "disable", "nope"); code != 1 {
		t.Fatalf("disabling an unknown runnable: exit %d", code)
	}
	if _, code := cli("store", "show"); code != 0 {
		t.Fatalf("store show: exit %d", code)
	}
	if _, code := cli("store", "query", "nope", "SELECT 1"); code != 1 {
		t.Fatalf("querying an unknown store: exit %d", code)
	}
	if _, code := cli("store", "exec", "nope"); code != 1 {
		t.Fatalf("store exec without statements: exit %d", code)
	}
	if _, code := cli("member", "add", "only-an-id"); code != 1 {
		t.Fatalf("member add without an address: exit %d", code)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not stop on SIGTERM")
	}
}
