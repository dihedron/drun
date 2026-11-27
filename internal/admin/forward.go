package admin

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// the metadata that carries a forwarded call's caller
const callerKey = "drun-caller"

// electionWait is how long a call waits for a leader to be known
const electionWait = 5 * time.Second

// route is how a method runs: on the leader (reply is the type of its
// reply, for forwarding), and whether it is audited (SEC-6, STO-16)
type route struct {
	leader  bool
	audited bool
	reply   func() any
}

func empty() any { return &adminpb.Empty{} }

var routes = map[string]route{
	adminpb.Admin_MemberAdd_FullMethodName:       {leader: true, audited: true, reply: empty},
	adminpb.Admin_MemberRemove_FullMethodName:    {leader: true, audited: true, reply: empty},
	adminpb.Admin_MemberShow_FullMethodName:      {leader: true, reply: func() any { return &adminpb.MemberShowReply{} }},
	adminpb.Admin_MemberPromote_FullMethodName:   {leader: true, audited: true, reply: empty},
	adminpb.Admin_MemberDemote_FullMethodName:    {leader: true, audited: true, reply: empty},
	adminpb.Admin_LeaderTransfer_FullMethodName:  {leader: true, audited: true, reply: empty},
	adminpb.Admin_RunnableSet_FullMethodName:     {leader: true, audited: true, reply: empty},
	adminpb.Admin_RunnableRestart_FullMethodName: {leader: true, audited: true, reply: empty},
	adminpb.Admin_RunnableReset_FullMethodName:   {leader: true, audited: true, reply: empty},
	adminpb.Admin_Views_FullMethodName:           {leader: true, reply: func() any { return &adminpb.ViewsReply{} }},
	adminpb.Admin_StoreQuery_FullMethodName:      {leader: true, audited: true, reply: func() any { return &adminpb.StoreQueryReply{} }},
	adminpb.Admin_StoreExec_FullMethodName:       {leader: true, audited: true, reply: func() any { return &adminpb.StoreExecReply{} }},
	adminpb.Admin_StoreReset_FullMethodName:      {leader: true, audited: true, reply: empty},
	adminpb.Admin_StoreBackup_FullMethodName:     {audited: true},
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
	r := routes[info.FullMethod]
	if q, ok := req.(*adminpb.StoreQueryRequest); ok && (q.GetLocal() || q.GetMaxStalenessMs() > 0) {
		r.leader = false
	}
	var reply any
	switch {
	case r.leader && s.raft.State() != raft.Leader && forwarded:
		// no second hop: leadership moved while the call travelled
		err = status.Error(codes.Unavailable, "the call reached a node that is no longer the leader: retry")
	case r.leader && s.raft.State() != raft.Leader:
		reply = r.reply()
		err = s.forward(ctx, info.FullMethod, req, reply)
		r.audited = false // the leader audits it
	default:
		reply, err = handler(ctx, req)
	}
	if r.audited {
		s.audit(info.FullMethod, who, req, err)
	}
	return reply, err
}

// interceptStream authorizes and audits a streaming call; none is
// forwarded
func (s *Server) interceptStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	who, _, err := s.authenticate(ss.Context())
	if err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	err = handler(srv, &callerStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), callerCtx{}, who)})
	if routes[info.FullMethod].audited {
		s.audit(info.FullMethod, who, nil, err)
	}
	return err
}

type callerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (c *callerStream) Context() context.Context { return c.ctx }

// audit logs a call with its caller, never its results; a query or a
// batch is logged with its SQL
func (s *Server) audit(method string, who Caller, req any, err error) {
	args := []any{"method", method[strings.LastIndexByte(method, '/')+1:], "caller", who.Name, "from", who.Node}
	if n, ok := req.(interface{ GetName() string }); ok {
		args = append(args, "runnable", n.GetName())
	}
	if q, ok := req.(interface{ GetSql() string }); ok {
		args = append(args, "sql", q.GetSql())
	}
	if err != nil {
		s.o.Logger.Info("admin call refused", append(args, "error", err)...)
	} else {
		s.o.Logger.Info("admin call", args...)
	}
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

// forward invokes method on the leader over the cluster port's admin
// stream, on behalf of the call's caller
func (s *Server) forward(ctx context.Context, method string, req, reply any) error {
	addr, id, err := s.otherLeader(ctx)
	if err != nil {
		return err
	}
	return s.invokeOn(ctx, id, addr, method, req, reply)
}

// invokeOn invokes method on another member over the cluster port's admin
// stream, on behalf of the call's caller
func (s *Server) invokeOn(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, method string, req, reply any) error {
	// shortcut: one connection per call; admin calls are rare, keep a
	// connection per member if that changes
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
