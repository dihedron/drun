package admin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/raftnode"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/credentials"
)

// Endpoint is a way to reach the admin API: where it listens, and the
// credentials whose server handshake authenticates each connection. The
// handshake yields an AuthInfo that is a callerInfo; the API sees only
// the Caller it gives.
type Endpoint interface {
	Listen() (net.Listener, error)
	Credentials() credentials.TransportCredentials
}

// Caller is who makes an admin call, for authorization and the audit log.
type Caller struct {
	// Name is the caller as the endpoint knows it: uid=N on the socket, the
	// certificate identity on the remote endpoint.
	Name string
	// Node is where the call was made: the node that forwarded it, or this
	// node.
	Node string
}

// callerInfo is the AuthInfo of every endpoint: the caller, or why it
// may not call; a refused caller is told so by each call (PermissionDenied)
// rather than by a dropped connection
type callerInfo interface {
	credentials.AuthInfo
	caller() (Caller, error)
}

// serverOnly is the part of TransportCredentials a server does not use
type serverOnly struct{ kind string }

func (s serverOnly) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("admin: server credentials only")
}
func (s serverOnly) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: s.kind}
}
func (s serverOnly) OverrideServerName(string) error { return nil }

// unixEndpoint is the local socket (SEC-6): 0660, callers checked with
// SO_PEERCRED: root, or a member of one of the allowed groups.
type unixEndpoint struct {
	path string
	gids map[string]bool
}

// UnixEndpoint returns the local socket endpoint; the groups must exist.
func UnixEndpoint(path string, allowedGroups []string) (Endpoint, error) {
	e := &unixEndpoint{path: path, gids: map[string]bool{}}
	for _, name := range allowedGroups {
		g, err := user.LookupGroup(name)
		if err != nil {
			return nil, fmt.Errorf("admin.allowed_groups: %w", err)
		}
		e.gids[g.Gid] = true
	}
	return e, nil
}

func (e *unixEndpoint) Listen() (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(e.path), 0o750); err != nil {
		return nil, fmt.Errorf("admin socket: %w", err)
	}
	if _, err := os.Lstat(e.path); err == nil {
		// a socket left by a node that died is removed; a live one is not
		if c, err := net.DialTimeout("unix", e.path, time.Second); err == nil {
			_ = c.Close()
			return nil, fmt.Errorf("admin socket %s: in use by another process", e.path)
		}
		if err := os.Remove(e.path); err != nil {
			return nil, fmt.Errorf("admin socket: %w", err)
		}
	}
	ln, err := net.Listen("unix", e.path)
	if err != nil {
		return nil, fmt.Errorf("admin socket: %w", err)
	}
	if err := os.Chmod(e.path, 0o660); err != nil { //nolint:gosec // SEC-6: the admin group connects
		_ = ln.Close()
		return nil, fmt.Errorf("admin socket: %w", err)
	}
	return ln, nil
}

func (e *unixEndpoint) Credentials() credentials.TransportCredentials { return unixCreds{e} }

type unixCreds struct{ e *unixEndpoint }

func (c unixCreds) ClientHandshake(ctx context.Context, a string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return serverOnly{}.ClientHandshake(ctx, a, conn)
}
func (c unixCreds) Info() credentials.ProtocolInfo          { return serverOnly{"unix"}.Info() }
func (c unixCreds) Clone() credentials.TransportCredentials { return c }
func (c unixCreds) OverrideServerName(string) error         { return nil }
func (c unixCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return conn, c.e.authorize(conn), nil
}

type unixInfo struct {
	who Caller
	err error
}

func (unixInfo) AuthType() string          { return "unix" }
func (i unixInfo) caller() (Caller, error) { return i.who, i.err }

func (e *unixEndpoint) authorize(conn net.Conn) unixInfo {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return unixInfo{err: errors.New("not a unix socket")}
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return unixInfo{err: err}
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil || credErr != nil {
		return unixInfo{err: fmt.Errorf("SO_PEERCRED: %w", errors.Join(err, credErr))}
	}
	uid := strconv.FormatUint(uint64(cred.Uid), 10)
	who := Caller{Name: "uid=" + uid}
	if cred.Uid == 0 || e.gids[strconv.FormatUint(uint64(cred.Gid), 10)] {
		return unixInfo{who: who}
	}
	if u, err := user.LookupId(uid); err == nil {
		if gids, err := u.GroupIds(); err == nil {
			for _, g := range gids {
				if e.gids[g] {
					return unixInfo{who: who}
				}
			}
		}
	}
	return unixInfo{who: who, err: fmt.Errorf("%s is neither root nor in admin.allowed_groups", who.Name)}
}

// tlsEndpoint is the opt-in remote endpoint (SEC-11): TCP, TLS 1.3 with
// mutual authentication; the client's certificate must chain to the CA
// and carry one of the allowed identities.
type tlsEndpoint struct {
	listen   string
	identity *raftnode.FileIdentity
	allowed  []string
}

// TLSEndpoint returns the remote endpoint of admin.remote.
func TLSEndpoint(r *config.Remote, logger *slog.Logger) (Endpoint, error) {
	id, err := raftnode.NewFileIdentity(r.TLS.Cert, r.TLS.Key, r.TLS.CA, logger)
	if err != nil {
		return nil, fmt.Errorf("admin.remote: %w", err)
	}
	return &tlsEndpoint{listen: r.Listen, identity: id, allowed: r.AllowedIdentities}, nil
}

func (e *tlsEndpoint) Listen() (net.Listener, error) {
	ln, err := net.Listen("tcp", e.listen)
	if err != nil {
		return nil, fmt.Errorf("admin.remote: %w", err)
	}
	return ln, nil
}

func (e *tlsEndpoint) Credentials() credentials.TransportCredentials {
	return tlsCreds{e: e, TransportCredentials: credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		// the chain is checked in VerifyConnection, against the current CA
		ClientAuth: tls.RequireAnyClientCert,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return e.identity.Certificate()
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			roots, err := e.identity.Roots()
			if err != nil {
				return err
			}
			_, err = raftnode.VerifyChain(cs, roots, x509.ExtKeyUsageClientAuth)
			return err
		},
	})}
}

type tlsCreds struct {
	credentials.TransportCredentials
	e *tlsEndpoint
}

func (c tlsCreds) Clone() credentials.TransportCredentials {
	return tlsCreds{TransportCredentials: c.TransportCredentials.Clone(), e: c.e}
}

func (c tlsCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}
	ti, _ := info.(credentials.TLSInfo)
	out := tlsInfo{TLSInfo: ti, err: errors.New("the client certificate carries no allowed identity")}
	if leaf := ti.State.PeerCertificates; len(leaf) > 0 {
		for _, id := range c.e.allowed {
			if raftnode.HasIdentity(leaf[0], id) {
				out.who, out.err = Caller{Name: id}, nil
				break
			}
		}
	}
	return conn, out, nil
}

type tlsInfo struct {
	credentials.TLSInfo
	who Caller
	err error
}

func (i tlsInfo) caller() (Caller, error) { return i.who, i.err }

// peerEndpoint serves the calls other nodes forward over the cluster
// port's admin stream; the caller travels in the call's metadata, the
// forwarding node is the peer the transport authenticated.
type peerEndpoint struct{ t *raftnode.Transport }

func (e peerEndpoint) Listen() (net.Listener, error) {
	l := &peerListener{t: e.t, conns: make(chan net.Conn), done: make(chan struct{})}
	go l.pump()
	return l, nil
}
func (e peerEndpoint) Credentials() credentials.TransportCredentials { return peerCreds{} }

// peerListener hands on the transport's admin streams until closed
type peerListener struct {
	t     *raftnode.Transport
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

// pump runs until the transport closes; streams arriving after the
// listener closed are dropped
func (l *peerListener) pump() {
	for {
		c, err := l.t.AcceptAdmin()
		if err != nil {
			return
		}
		select {
		case l.conns <- c:
		case <-l.done:
			_ = c.Close()
		}
	}
}

func (l *peerListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *peerListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}
func (l *peerListener) Addr() net.Addr { return l.t.Addr() }

type peerCreds struct{}

func (peerCreds) ClientHandshake(ctx context.Context, a string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return serverOnly{}.ClientHandshake(ctx, a, conn)
}
func (peerCreds) Info() credentials.ProtocolInfo            { return serverOnly{"peer"}.Info() }
func (c peerCreds) Clone() credentials.TransportCredentials { return c }
func (peerCreds) OverrideServerName(string) error           { return nil }
func (peerCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	pc, ok := conn.(*raftnode.PeerConn)
	if !ok {
		return nil, nil, errors.New("admin: not a peer connection")
	}
	return conn, peerInfo{node: string(pc.Peer)}, nil
}

// peerInfo: the caller is in the metadata of each call
type peerInfo struct{ node string }

func (peerInfo) AuthType() string { return "peer" }
