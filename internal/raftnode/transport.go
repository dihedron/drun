package raftnode

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// The first byte of every connection on the cluster port says what it
// carries (ADR-3).
const (
	streamRaft  byte = 'R'
	streamAdmin byte = 'A'
)

const handshakeTimeout = 10 * time.Second

// ErrClosed is returned by Accept once the transport is closed.
var ErrClosed = errors.New("transport closed")

// Resolver maps a server address to its ID, from the cluster's
// configuration.
type Resolver func(raft.ServerAddress) (raft.ServerID, bool)

// Transport is the cluster port: TLS 1.3 with mutual authentication
// (SEC-1), each peer bound to the server ID its certificate identifies
// (SEC-2), carrying Raft and forwarded admin connections (ADR-3).
type Transport struct {
	ln        net.Listener
	advertise net.Addr
	identity  IdentitySource
	peers     *Peers
	logger    *slog.Logger

	mu      sync.RWMutex
	resolve Resolver

	raftConns  chan net.Conn
	adminConns chan net.Conn
	closed     chan struct{}
	closeOnce  sync.Once
}

// PeerConn is an admin connection from an authenticated peer.
type PeerConn struct {
	net.Conn
	Peer raft.ServerID
}

// Listen opens the cluster port on bind; advertise is the address the
// other nodes reach it on (empty: the listening address).
func Listen(bind, advertise string, identity IdentitySource, peers *Peers, logger *slog.Logger) (*Transport, error) {
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("cluster port: %w", err)
	}
	addr := ln.Addr()
	if advertise != "" {
		if addr, err = net.ResolveTCPAddr("tcp", advertise); err != nil {
			_ = ln.Close()
			return nil, fmt.Errorf("advertise address %q: %w", advertise, err)
		}
	}
	t := &Transport{
		ln: ln, advertise: addr, identity: identity, peers: peers, logger: logger,
		resolve:    func(raft.ServerAddress) (raft.ServerID, bool) { return "", false },
		raftConns:  make(chan net.Conn),
		adminConns: make(chan net.Conn),
		closed:     make(chan struct{}),
	}
	go t.serve()
	return t, nil
}

// SetResolver sets how addresses map to server IDs.
func (t *Transport) SetResolver(r Resolver) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resolve = r
}

func (t *Transport) lookup(addr raft.ServerAddress) (raft.ServerID, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.resolve(addr)
}

// Addr is the address of the listening socket.
func (t *Transport) Addr() net.Addr { return t.ln.Addr() }

// Close closes the port and every Accept.
func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closed)
		err = t.ln.Close()
	})
	return err
}

func (t *Transport) serve() {
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.closed:
				return
			default:
			}
			t.logger.Warn("cluster port: accept", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go t.handle(conn)
	}
}

func (t *Transport) serverConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// verified in VerifyConnection, against the current roots
		ClientAuth: tls.RequireAnyClientCert,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return t.identity.Certificate()
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			roots, err := t.identity.Roots()
			if err != nil {
				return err
			}
			leaf, err := VerifyChain(cs, roots, x509.ExtKeyUsageClientAuth)
			if err != nil {
				return err
			}
			_, err = t.peers.ID(leaf)
			return err
		},
	}
}

// handle authenticates a connection and routes it by its first byte
func (t *Transport) handle(raw net.Conn) {
	conn := tls.Server(raw, t.serverConfig())
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	if err := conn.Handshake(); err != nil {
		t.logger.Warn("cluster port: handshake refused", "remote", raw.RemoteAddr(), "error", err)
		_ = conn.Close()
		return
	}
	peer, err := t.peers.ID(conn.ConnectionState().PeerCertificates[0])
	if err != nil {
		_ = conn.Close()
		return
	}
	r := bufio.NewReader(conn)
	kind, err := r.ReadByte()
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})
	var out net.Conn
	var ch chan net.Conn
	switch kind {
	case streamRaft:
		out, ch = newGate(conn, r, raft.ServerID(peer), t.lookup), t.raftConns
	case streamAdmin:
		out, ch = &PeerConn{Conn: &bufferedConn{Conn: conn, r: r}, Peer: raft.ServerID(peer)}, t.adminConns
	default:
		t.logger.Warn("cluster port: unknown stream", "peer", peer, "kind", kind)
		_ = conn.Close()
		return
	}
	select {
	case ch <- out:
	case <-t.closed:
		_ = conn.Close()
	}
}

// bufferedConn reads what the router's buffer already holds first
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// dial opens a stream of kind to the server at address, checking that it
// presents the identity configured for that server
func (t *Transport) dial(ctx context.Context, address raft.ServerAddress, kind byte) (net.Conn, error) {
	id, ok := t.lookup(address)
	if !ok {
		return nil, fmt.Errorf("no known server at %s", address)
	}
	want, ok := t.peers.Identity(string(id))
	if !ok {
		return nil, fmt.Errorf("no identity configured for %s", id)
	}
	cert, err := t.identity.Certificate()
	if err != nil {
		return nil, err
	}
	roots, err := t.identity.Roots()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{*cert},
		// identities are not host names: the chain and the identity are
		// checked in VerifyConnection
		InsecureSkipVerify: true, //nolint:gosec // see above
		VerifyConnection: func(cs tls.ConnectionState) error {
			leaf, err := VerifyChain(cs, roots, x509.ExtKeyUsageServerAuth)
			if err != nil {
				return err
			}
			if !HasIdentity(leaf, want) {
				return fmt.Errorf("%s does not present the identity of %s", address, id)
			}
			return nil
		},
	}
	conn, err := (&tls.Dialer{Config: cfg}).DialContext(ctx, "tcp", string(address))
	if err != nil {
		return nil, fmt.Errorf("dialling %s (%s): %w", id, address, err)
	}
	if _, err := conn.Write([]byte{kind}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dialling %s (%s): %w", id, address, err)
	}
	return conn, nil
}

// RaftLayer is the raft.StreamLayer of the transport.
func (t *Transport) RaftLayer() raft.StreamLayer { return raftLayer{t} }

type raftLayer struct{ t *Transport }

func (l raftLayer) Accept() (net.Conn, error) {
	select {
	case c := <-l.t.raftConns:
		return c, nil
	case <-l.t.closed:
		return nil, ErrClosed
	}
}

func (l raftLayer) Close() error   { return l.t.Close() }
func (l raftLayer) Addr() net.Addr { return l.t.advertise }

func (l raftLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return l.t.dial(ctx, address, streamRaft)
}

// AcceptAdmin returns the next admin stream from a peer (forwarded admin
// calls, C5).
func (t *Transport) AcceptAdmin() (*PeerConn, error) {
	select {
	case c := <-t.adminConns:
		return c.(*PeerConn), nil //nolint:forcetypeassert // only PeerConns are sent
	case <-t.closed:
		return nil, ErrClosed
	}
}

// DialAdmin opens an admin stream to a peer.
func (t *Transport) DialAdmin(ctx context.Context, address raft.ServerAddress) (net.Conn, error) {
	return t.dial(ctx, address, streamAdmin)
}
