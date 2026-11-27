package raftnode

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// the request types of raft's NetworkTransport (net_transport.go)
const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow
	rpcRequestPreVote
)

// requestHeader is the part of every raft request the gate checks: the
// codec flattens the embedded RPCHeader, and skips the other fields
type requestHeader struct {
	raft.RPCHeader
	// Size is InstallSnapshot's: the raw bytes that follow the request
	Size int64
}

// errImpersonation: a request names a server other than the connection's
var errImpersonation = errors.New("request in the name of another server")

// gate binds a Raft connection to the peer its certificate identifies
// (SEC-2): it decodes each request's header before raft reads any of it,
// and closes the connection on a request in another server's name
type gate struct {
	net.Conn
	peer    raft.ServerID
	resolve func(raft.ServerAddress) (raft.ServerID, bool)

	rec     recorder
	dec     *codec.Decoder
	pending []byte // checked bytes raft has not read yet
	raw     int64  // snapshot bytes to pass through unchecked
	err     error
}

func newGate(conn net.Conn, r *bufio.Reader, peer raft.ServerID, resolve func(raft.ServerAddress) (raft.ServerID, bool)) *gate {
	g := &gate{Conn: conn, peer: peer, resolve: resolve, rec: recorder{r: r}}
	// with an io.ByteScanner and no buffer, the decoder reads exactly one
	// request: never into the next one
	g.dec = codec.NewDecoder(&g.rec, &codec.MsgpackHandle{})
	return g
}

func (g *gate) Read(p []byte) (int, error) {
	if len(g.pending) == 0 && g.raw == 0 {
		if g.err != nil {
			return 0, g.err
		}
		if err := g.next(); err != nil {
			g.err = err
			if !errors.Is(err, io.EOF) {
				_ = g.Close()
			}
			if len(g.pending) == 0 {
				return 0, err
			}
		}
	}
	if len(g.pending) > 0 {
		n := copy(p, g.pending)
		g.pending = g.pending[n:]
		return n, nil
	}
	if int64(len(p)) > g.raw {
		p = p[:g.raw]
	}
	n, err := g.rec.r.Read(p)
	g.raw -= int64(n)
	return n, err
}

// next reads, checks and holds back one request
func (g *gate) next() error {
	g.rec.buf.Reset()
	typ, err := g.rec.ReadByte()
	if err != nil {
		return err
	}
	switch typ {
	case rpcAppendEntries, rpcRequestVote, rpcInstallSnapshot, rpcTimeoutNow, rpcRequestPreVote:
	default:
		return fmt.Errorf("unknown request type %d", typ)
	}
	var hdr requestHeader
	if err := g.dec.Decode(&hdr); err != nil {
		return fmt.Errorf("decoding a request: %w", err)
	}
	if id := raft.ServerID(hdr.ID); id != g.peer {
		return fmt.Errorf("%w: %q from %q", errImpersonation, id, g.peer)
	}
	if len(hdr.Addr) > 0 {
		if id, ok := g.resolve(raft.ServerAddress(hdr.Addr)); !ok || id != g.peer {
			return fmt.Errorf("%w: address %q from %q", errImpersonation, hdr.Addr, g.peer)
		}
	}
	if typ == rpcInstallSnapshot {
		if hdr.Size < 0 {
			return fmt.Errorf("snapshot of %d bytes", hdr.Size)
		}
		g.raw = hdr.Size
	}
	g.pending = bytes.Clone(g.rec.buf.Bytes())
	return nil
}

// recorder reads from r and keeps what it read
type recorder struct {
	r   *bufio.Reader
	buf bytes.Buffer
}

func (r *recorder) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.buf.Write(p[:n])
	return n, err
}

func (r *recorder) ReadByte() (byte, error) {
	b, err := r.r.ReadByte()
	if err == nil {
		r.buf.WriteByte(b)
	}
	return b, err
}

func (r *recorder) UnreadByte() error {
	if err := r.r.UnreadByte(); err != nil {
		return err
	}
	r.buf.Truncate(r.buf.Len() - 1)
	return nil
}
