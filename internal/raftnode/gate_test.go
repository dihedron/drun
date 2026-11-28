package raftnode

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// pipeConn is a net.Conn reading from a buffer
type pipeConn struct {
	net.Conn
	closed bool
}

func (c *pipeConn) Close() error { c.closed = true; return nil }

func encodeRequest(t testing.TB, typ uint8, req any) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteByte(typ)
	if err := codec.NewEncoder(&buf, &codec.MsgpackHandle{}).Encode(req); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func readAll(data []byte) ([]byte, *pipeConn, error) {
	conn := &pipeConn{}
	resolve := func(a raft.ServerAddress) (raft.ServerID, bool) {
		return map[raft.ServerAddress]raft.ServerID{"addr-b": "b"}[a], a == "addr-b"
	}
	g := newGate(conn, bufio.NewReader(bytes.NewReader(data)), "b", resolve)
	out, err := io.ReadAll(g)
	return out, conn, err
}

var hdrB = raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("b"), Addr: []byte("addr-b")}

// the gate passes honest requests on byte for byte, several in a row
func TestGatePassesRequestsUnchanged(t *testing.T) {
	var stream []byte
	stream = append(stream, encodeRequest(t, rpcRequestVote, &raft.RequestVoteRequest{RPCHeader: hdrB, Term: 3})...)
	stream = append(stream, encodeRequest(t, rpcAppendEntries, &raft.AppendEntriesRequest{RPCHeader: hdrB, Entries: []*raft.Log{{Index: 1, Data: []byte("data")}}})...)
	stream = append(stream, encodeRequest(t, rpcInstallSnapshot, &raft.InstallSnapshotRequest{RPCHeader: hdrB, Size: 5})...)
	stream = append(stream, "12345"...)
	stream = append(stream, encodeRequest(t, rpcTimeoutNow, &raft.TimeoutNowRequest{RPCHeader: hdrB})...)
	out, conn, err := readAll(stream)
	if err != nil || !bytes.Equal(out, stream) || conn.closed {
		t.Fatalf("got %d of %d bytes, error %v, closed %v", len(out), len(stream), err, conn.closed)
	}
}

// the gate stops before the impersonating request: raft sees the honest
// requests before it and nothing of it
func TestGateStopsAtImpersonation(t *testing.T) {
	honest := encodeRequest(t, rpcRequestVote, &raft.RequestVoteRequest{RPCHeader: hdrB, Term: 3})
	forged := encodeRequest(t, rpcRequestVote, &raft.RequestVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("a")}, Term: 4})
	out, conn, err := readAll(append(bytes.Clone(honest), forged...))
	if !errors.Is(err, errImpersonation) || !bytes.Equal(out, honest) || !conn.closed {
		t.Fatalf("got %d bytes (honest %d), error %v, closed %v", len(out), len(honest), err, conn.closed)
	}
}

// an address no known server holds is not an impersonation: a wiped node
// knows only the configured voters, and must still accept a leader added
// later, or it can never learn the configuration that names it
func TestGatePassesUnknownAddress(t *testing.T) {
	stream := encodeRequest(t, rpcAppendEntries, &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte("b"), Addr: []byte("addr-new")}})
	out, conn, err := readAll(stream)
	if err != nil || !bytes.Equal(out, stream) || conn.closed {
		t.Fatalf("got %d of %d bytes, error %v, closed %v", len(out), len(stream), err, conn.closed)
	}
}

// an address another server holds is still refused
func TestGateStopsAtAnotherServersAddress(t *testing.T) {
	stream := encodeRequest(t, rpcAppendEntries, &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte("b"), Addr: []byte("addr-a")}})
	conn := &pipeConn{}
	resolve := func(a raft.ServerAddress) (raft.ServerID, bool) {
		id, ok := map[raft.ServerAddress]raft.ServerID{"addr-a": "a", "addr-b": "b"}[a]
		return id, ok
	}
	_, err := io.ReadAll(newGate(conn, bufio.NewReader(bytes.NewReader(stream)), "b", resolve))
	if !errors.Is(err, errImpersonation) || !conn.closed {
		t.Fatalf("error %v, closed %v", err, conn.closed)
	}
}

func FuzzGate(f *testing.F) {
	f.Add(encodeRequest(f, rpcRequestVote, &raft.RequestVoteRequest{RPCHeader: hdrB, Term: 3}))
	f.Add(encodeRequest(f, rpcAppendEntries, &raft.AppendEntriesRequest{RPCHeader: hdrB, Entries: []*raft.Log{{Index: 1, Data: []byte("d")}}}))
	f.Add(encodeRequest(f, rpcInstallSnapshot, &raft.InstallSnapshotRequest{RPCHeader: hdrB, Size: 2}))
	f.Add([]byte{9, 0x80})
	f.Fuzz(func(t *testing.T, data []byte) {
		out, _, _ := readAll(data)
		if !bytes.HasPrefix(data, out) {
			t.Fatal("the gate changed the bytes it passed on")
		}
	})
}
