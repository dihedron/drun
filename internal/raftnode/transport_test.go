package raftnode

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/tlstest"
	"github.com/hashicorp/raft"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// testPeers: a SPIFFE identity, a DNS identity, and one more
var testPeers = map[string]string{
	"a": "spiffe://example.org/drun/a",
	"b": "b.drun.example.org",
	"c": "spiffe://example.org/drun/c",
}

type endpoint struct {
	id    string
	files tlstest.Files
	trans *Transport
	net   *raft.NetworkTransport
}

// newEndpoints starts a transport per id, all resolving each other
func newEndpoints(t *testing.T, ca *tlstest.CA, ids ...string) map[string]*endpoint {
	t.Helper()
	eps := map[string]*endpoint{}
	addrs := map[raft.ServerAddress]raft.ServerID{}
	var mu sync.Mutex
	resolve := func(a raft.ServerAddress) (raft.ServerID, bool) {
		mu.Lock()
		defer mu.Unlock()
		id, ok := addrs[a]
		return id, ok
	}
	for _, id := range ids {
		files := ca.WriteFiles(t, t.TempDir(), testPeers[id])
		identity, err := NewFileIdentity(files.Cert, files.Key, files.CA, discard)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := Listen("127.0.0.1:0", "", identity, NewPeers(testPeers, nil, discard), discard)
		if err != nil {
			t.Fatal(err)
		}
		tr.SetResolver(resolve)
		nt := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{Stream: tr.RaftLayer(), MaxPool: 2, Timeout: 2 * time.Second, Logger: newHclog(discard, "raft-net")})
		t.Cleanup(func() { _ = nt.Close() })
		mu.Lock()
		addrs[nt.LocalAddr()] = raft.ServerID(id)
		mu.Unlock()
		eps[id] = &endpoint{id: id, files: files, trans: tr, net: nt}
	}
	return eps
}

func header(ep *endpoint) raft.RPCHeader {
	return raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte(ep.id), Addr: []byte(ep.net.LocalAddr())}
}

// answer responds to the next RPC the endpoint receives, or reports none
func answer(ep *endpoint, within time.Duration) (raft.RPC, bool) {
	select {
	case rpc := <-ep.net.Consumer():
		var resp any
		switch rpc.Command.(type) {
		case *raft.RequestVoteRequest:
			resp = &raft.RequestVoteResponse{Granted: true}
		case *raft.AppendEntriesRequest:
			resp = &raft.AppendEntriesResponse{Success: true}
		case *raft.InstallSnapshotRequest:
			data, _ := io.ReadAll(rpc.Reader)
			resp = &raft.InstallSnapshotResponse{Success: bytes.Equal(data, []byte("snapshot-bytes"))}
		}
		rpc.Respond(resp, nil)
		return rpc, true
	case <-time.After(within):
		return raft.RPC{}, false
	}
}

func TestRaftOverMutualTLS(t *testing.T) {
	eps := newEndpoints(t, tlstest.NewCA(t), "a", "b")
	a, b := eps["a"], eps["b"]

	done := make(chan raft.RPC, 3)
	go func() {
		for range 3 {
			rpc, _ := answer(a, 5*time.Second)
			done <- rpc
		}
	}()
	var vote raft.RequestVoteResponse
	if err := b.net.RequestVote("a", a.net.LocalAddr(), &raft.RequestVoteRequest{RPCHeader: header(b), Term: 2}, &vote); err != nil || !vote.Granted {
		t.Fatalf("RequestVote: %v granted %v", err, vote.Granted)
	}
	var app raft.AppendEntriesResponse
	entries := []*raft.Log{{Index: 1, Term: 2, Type: raft.LogCommand, Data: bytes.Repeat([]byte("x"), 100_000)}}
	if err := b.net.AppendEntries("a", a.net.LocalAddr(), &raft.AppendEntriesRequest{RPCHeader: header(b), Term: 2, Entries: entries}, &app); err != nil || !app.Success {
		t.Fatalf("AppendEntries: %v success %v", err, app.Success)
	}
	var snap raft.InstallSnapshotResponse
	data := []byte("snapshot-bytes")
	if err := b.net.InstallSnapshot("a", a.net.LocalAddr(), &raft.InstallSnapshotRequest{RPCHeader: header(b), Term: 2, Size: int64(len(data))}, &snap, bytes.NewReader(data)); err != nil || !snap.Success {
		t.Fatalf("InstallSnapshot: %v success %v", err, snap.Success)
	}
	for range 3 {
		if rpc := <-done; string(rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID) != "b" { //nolint:forcetypeassert // raft requests
			t.Fatalf("request from %v", rpc.Command)
		}
	}
}

// SEC-2: a valid certificate cannot speak in another server's name
func TestImpersonationRefused(t *testing.T) {
	eps := newEndpoints(t, tlstest.NewCA(t), "a", "b", "c")
	a, b, c := eps["a"], eps["b"], eps["c"]

	for name, hdr := range map[string]raft.RPCHeader{
		"another id":      {ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("c"), Addr: []byte(c.net.LocalAddr())},
		"another address": {ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("b"), Addr: []byte(c.net.LocalAddr())},
		"no id":           {ProtocolVersion: raft.ProtocolVersionMax},
	} {
		t.Run(name, func(t *testing.T) {
			var vote raft.RequestVoteResponse
			err := b.net.RequestVote("a", a.net.LocalAddr(), &raft.RequestVoteRequest{RPCHeader: hdr, Term: 9}, &vote)
			if err == nil {
				t.Fatal("the impersonating request was answered")
			}
			if rpc, got := answer(a, 300*time.Millisecond); got {
				t.Fatalf("raft received the impersonating request: %v", rpc.Command)
			}
		})
	}
	// the same peer, speaking for itself, still gets through
	var vote raft.RequestVoteResponse
	go answer(a, 5*time.Second)
	if err := b.net.RequestVote("a", a.net.LocalAddr(), &raft.RequestVoteRequest{RPCHeader: header(b), Term: 9}, &vote); err != nil {
		t.Fatalf("honest request after the refusals: %v", err)
	}
}

func TestDialChecksServerIdentity(t *testing.T) {
	eps := newEndpoints(t, tlstest.NewCA(t), "a", "b")
	a, b := eps["a"], eps["b"]
	// b believes c lives at a's address: a's certificate does not say c
	b.trans.SetResolver(func(raft.ServerAddress) (raft.ServerID, bool) { return "c", true })
	_, err := b.trans.RaftLayer().Dial(a.net.LocalAddr(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("dial: %v, want an identity error", err)
	}
	b.trans.SetResolver(func(raft.ServerAddress) (raft.ServerID, bool) { return "", false })
	if _, err := b.trans.RaftLayer().Dial(a.net.LocalAddr(), time.Second); err == nil {
		t.Fatal("dialled an unknown address")
	}
}

func TestHandshakeRefused(t *testing.T) {
	ca := tlstest.NewCA(t)
	a := newEndpoints(t, ca, "a")["a"]
	addr := string(a.net.LocalAddr())

	pair := func(ca *tlstest.CA, identity string) tls.Certificate {
		certPEM, keyPEM := ca.Issue(t, identity)
		c, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	for name, cfg := range map[string]*tls.Config{
		"no client certificate": {MinVersion: tls.VersionTLS13},
		"another CA":            {MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair(tlstest.NewCA(t), testPeers["b"])}},
		"unknown identity":      {MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair(ca, "spiffe://example.org/drun/z")}},
		"TLS 1.2":               {MaxVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair(ca, testPeers["b"])}},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.InsecureSkipVerify = true //nolint:gosec // the server is not what is tested
			conn, err := tls.Dial("tcp", addr, cfg)
			if err == nil {
				// TLS 1.3 client certificates are checked after the client's
				// handshake ends: the refusal shows on the first read
				_, _ = conn.Write([]byte{streamRaft})
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, err = conn.Read(make([]byte, 1))
				_ = conn.Close()
			}
			if err == nil {
				t.Fatal("connection accepted")
			}
		})
	}
}

func TestAdminStreamAndUnknownStream(t *testing.T) {
	eps := newEndpoints(t, tlstest.NewCA(t), "a", "b")
	a, b := eps["a"], eps["b"]

	accepted := make(chan *PeerConn, 1)
	go func() {
		pc, err := a.trans.AcceptAdmin()
		if err == nil {
			accepted <- pc
		}
	}()
	conn, err := b.trans.DialAdmin(context.Background(), a.net.LocalAddr())
	if err != nil {
		t.Fatalf("admin dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck // test teardown
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case pc := <-accepted:
		if pc.Peer != "b" {
			t.Fatalf("admin stream from %q", pc.Peer)
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(pc, buf); err != nil || string(buf) != "hello" {
			t.Fatalf("admin stream read %q %v", buf, err)
		}
		_ = pc.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("no admin stream")
	}

	other, err := b.trans.dial(context.Background(), a.net.LocalAddr(), 'X')
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer other.Close() //nolint:errcheck // test teardown
	_ = other.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := other.Read(make([]byte, 1)); err == nil {
		t.Fatal("unknown stream kept open")
	}
}

func TestCertificateRotation(t *testing.T) {
	ca := tlstest.NewCA(t)
	eps := newEndpoints(t, ca, "a", "b")
	a, b := eps["a"], eps["b"]
	if _, err := b.trans.RaftLayer().Dial(a.net.LocalAddr(), time.Second); err != nil {
		t.Fatalf("dial before rotation: %v", err)
	}

	// b's files now hold a certificate a does not accept: the next dial
	// reads them
	time.Sleep(10 * time.Millisecond) // a later modification time
	certPEM, keyPEM := ca.Issue(t, "spiffe://example.org/drun/z")
	for path, data := range map[string][]byte{b.files.Cert: certPEM, b.files.Key: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := b.trans.RaftLayer().Dial(a.net.LocalAddr(), time.Second)
	if err == nil {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}
	if err == nil || os.IsTimeout(err) {
		t.Fatalf("dial with the rotated certificate: %v, want a refusal", err)
	}

	// files that do not load leave the last good identity in use
	identity, err := NewFileIdentity(a.files.Cert, a.files.Key, a.files.CA, discard)
	if err != nil {
		t.Fatal(err)
	}
	good, _ := identity.Certificate()
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(a.files.Cert, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := identity.Certificate(); err != nil || got != good {
		t.Fatalf("after a broken certificate file: %v, %v; want the previous one", got, err)
	}
	if _, err := NewFileIdentity(a.files.Cert, a.files.Key, a.files.CA, discard); err == nil {
		t.Fatal("a node started with a broken certificate file")
	}
}

func TestPeersDirectory(t *testing.T) {
	recorded := map[string]string{"d": "spiffe://example.org/drun/d", "a": "spiffe://example.org/drun/not-a"}
	p := NewPeers(testPeers, func() map[string]string { return recorded }, discard)
	if id, ok := p.Identity("d"); !ok || id != recorded["d"] {
		t.Fatalf("recorded identity: %q %v", id, ok)
	}
	if id, _ := p.Identity("a"); id != testPeers["a"] {
		t.Fatalf("configured identity overridden: %q", id)
	}
	ca := tlstest.NewCA(t)
	for identity, want := range map[string]string{recorded["d"]: "d", testPeers["a"]: "a", recorded["a"]: ""} {
		certPEM, _ := ca.Issue(t, identity)
		cert := parseCert(t, certPEM)
		got, err := p.ID(cert)
		if got != want || (want == "") != (err != nil) {
			t.Fatalf("%s: id %q, error %v; want %q", identity, got, err, want)
		}
	}
}

func parseCert(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// the probe stream answers whether the node has Raft state, to members only
func TestProbe(t *testing.T) {
	eps := newEndpoints(t, tlstest.NewCA(t), "a", "b")
	a, b := eps["a"], eps["b"]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if has, err := b.trans.Probe(ctx, a.net.LocalAddr()); err != nil || has {
		t.Fatalf("probe before SetStateful: %v %v", has, err)
	}
	a.trans.SetStateful(func() bool { return true })
	if has, err := b.trans.Probe(ctx, a.net.LocalAddr()); err != nil || !has {
		t.Fatalf("probe of a stateful node: %v %v", has, err)
	}
	// a node of another CA is refused at the handshake
	stranger := newEndpoints(t, tlstest.NewCA(t), "c")["c"]
	stranger.trans.SetResolver(func(raft.ServerAddress) (raft.ServerID, bool) { return "a", true })
	if _, err := stranger.trans.Probe(ctx, a.net.LocalAddr()); err == nil {
		t.Fatal("a stranger's probe was answered")
	}
}
