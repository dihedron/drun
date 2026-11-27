package admin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/dihedron/drun/internal/admin/adminpb"
	"github.com/dihedron/drun/internal/raftnode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientTLS is the client side of the remote endpoint.
type ClientTLS struct {
	Cert, Key, CA string
	// ServerIdentity, if set, is the identity the node's certificate must
	// carry; otherwise any certificate of the CA is accepted.
	ServerIdentity string
}

// Client is a connection to a node's admin API.
type Client struct {
	adminpb.AdminClient
	conn *grpc.ClientConn
}

func (c *Client) Close() error { return c.conn.Close() }

// Dial connects to a node's admin API: unix:///path for the local socket,
// tls://host:port for the remote endpoint, with tlsc.
func Dial(target string, tlsc ClientTLS) (*Client, error) {
	var opts []grpc.DialOption
	switch {
	case strings.HasPrefix(target, "unix://"):
		path := strings.TrimPrefix(target, "unix://")
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()), // local: SO_PEERCRED
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", path)
			}))
		target = "passthrough:///admin"
	case strings.HasPrefix(target, "tls://"):
		creds, err := tlsc.credentials()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
		target = "passthrough:///" + strings.TrimPrefix(target, "tls://")
	default:
		return nil, fmt.Errorf("admin address %q: neither unix:///path nor tls://host:port", target)
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("admin: %w", err)
	}
	return &Client{AdminClient: adminpb.NewAdminClient(conn), conn: conn}, nil
}

func (c ClientTLS) credentials() (credentials.TransportCredentials, error) {
	if c.Cert == "" || c.Key == "" || c.CA == "" {
		return nil, errors.New("a remote admin endpoint needs a client certificate, its key and the CA (--cert, --key, --ca)")
	}
	id, err := raftnode.NewFileIdentity(c.Cert, c.Key, c.CA, slog.New(slog.DiscardHandler))
	if err != nil {
		return nil, err
	}
	cert, _ := id.Certificate()
	roots, _ := id.Roots()
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{*cert},
		// node certificates carry identities, not host names: the chain
		// and the identity are checked in VerifyConnection
		InsecureSkipVerify: true, //nolint:gosec // see above
		VerifyConnection: func(cs tls.ConnectionState) error {
			leaf, err := raftnode.VerifyChain(cs, roots, x509.ExtKeyUsageServerAuth)
			if err != nil {
				return err
			}
			if c.ServerIdentity != "" && !raftnode.HasIdentity(leaf, c.ServerIdentity) {
				return fmt.Errorf("the node does not present the identity %s", c.ServerIdentity)
			}
			return nil
		},
	}), nil
}
