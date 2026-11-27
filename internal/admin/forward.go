package admin

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/dihedron/resurgo/internal/admin/adminpb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// the metadata that carries a forwarded call's caller
const callerKey = "resurgo-caller"

// electionWait is how long a call waits for a leader to be known
const electionWait = 5 * time.Second

// leaderOnly are the methods the leader executes; mutating ones are
// audited (SEC-6)
var leaderOnly = map[string]bool{
	adminpb.Admin_MemberAdd_FullMethodName:       true,
	adminpb.Admin_MemberRemove_FullMethodName:    true,
	adminpb.Admin_LeaderTransfer_FullMethodName:  true,
	adminpb.Admin_RunnableSet_FullMethodName:     true,
	adminpb.Admin_RunnableRestart_FullMethodName: true,
	adminpb.Admin_RunnableReset_FullMethodName:   true,
	adminpb.Admin_Views_FullMethodName:           false,
}

type callerCtx struct{}

// caller returns the call's caller
func callerOf(ctx context.Context) Caller {
	c, _ := ctx.Value(callerCtx{}).(Caller)
	return c
}

// intercept authorizes every call, forwards the leader's calls when this
// node is not the leader, and audits mutations
func (s *Server) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	who, forwarded, err := s.authenticate(ctx)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	ctx = context.WithValue(ctx, callerCtx{}, who)
	mutation, ok := leaderOnly[info.FullMethod]
	var reply any
	switch {
	case ok && s.raft.State() != raft.Leader && forwarded:
		// no second hop: leadership moved while the call travelled
		err = status.Error(codes.Unavailable, "the call reached a node that is no longer the leader: retry")
	case ok && s.raft.State() != raft.Leader:
		reply, err = s.forwardCall(ctx, info.FullMethod, req)
		mutation = false // the leader audits it
	default:
		reply, err = handler(ctx, req)
	}
	if mutation {
		args := []any{"method", info.FullMethod[strings.LastIndexByte(info.FullMethod, '/')+1:], "caller", who.Name, "from", who.Node}
		if n, ok := req.(interface{ GetName() string }); ok {
			args = append(args, "runnable", n.GetName())
		}
		if err != nil {
			s.o.Logger.Info("admin call refused", append(args, "error", err)...)
		} else {
			s.o.Logger.Info("admin call", args...)
		}
	}
	return reply, err
}

// authenticate returns the caller of a call, and whether another node
// forwarded it
func (s *Server) authenticate(ctx context.Context) (Caller, bool, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return Caller{}, false, status.Error(codes.PermissionDenied, "no peer")
	}
	switch info := p.AuthInfo.(type) {
	case callerInfo:
		who, err := info.caller()
		who.Node = s.o.NodeID
		return who, false, err
	case peerInfo:
		md, _ := metadata.FromIncomingContext(ctx)
		names := md.Get(callerKey)
		if len(names) != 1 {
			return Caller{}, true, status.Error(codes.PermissionDenied, "a forwarded call without its caller")
		}
		return Caller{Name: names[0], Node: info.node}, true, nil
	}
	return Caller{}, false, status.Error(codes.PermissionDenied, "unknown endpoint")
}

// forwardCall re-invokes a leader's call on the leader
func (s *Server) forwardCall(ctx context.Context, method string, req any) (any, error) {
	var reply any
	switch method {
	case adminpb.Admin_Views_FullMethodName:
		reply = &adminpb.ViewsReply{}
	default:
		reply = &adminpb.Empty{}
	}
	return reply, s.forward(ctx, method, req, reply)
}

// forward invokes method on the leader over the cluster port's admin
// stream, on behalf of the call's caller
func (s *Server) forward(ctx context.Context, method string, req, reply any) error {
	addr, id, err := s.otherLeader(ctx)
	if err != nil {
		return err
	}
	// shortcut: one connection per forwarded call; admin calls are rare,
	// keep a connection per leader if that changes
	conn, err := grpc.NewClient("passthrough:///"+string(id),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // the stream is mutual TLS already
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return s.o.Node.Transport().DialAdmin(ctx, addr)
		}))
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	defer conn.Close() //nolint:errcheck // nothing to do about it
	ctx = metadata.AppendToOutgoingContext(ctx, callerKey, callerOf(ctx).Name)
	return conn.Invoke(ctx, method, req, reply)
}

// otherLeader waits, during an election, for another node to be leader
func (s *Server) otherLeader(ctx context.Context) (raft.ServerAddress, raft.ServerID, error) {
	ctx, cancel := context.WithTimeout(ctx, electionWait)
	defer cancel()
	for {
		if addr, id := s.raft.LeaderWithID(); id != "" && string(id) != s.o.NodeID {
			return addr, id, nil
		}
		select {
		case <-ctx.Done():
			return "", "", status.Error(codes.Unavailable, "no other node is known as the leader: retry")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
