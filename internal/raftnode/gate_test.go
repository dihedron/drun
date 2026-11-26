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
