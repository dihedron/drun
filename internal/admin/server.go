// Package admin is a node's admin API (drun_specification.md §10.2): a
// gRPC service on pluggable endpoints (the local socket, the opt-in remote
// TLS endpoint), whose mutations the leader executes; a follower forwards
// them over the cluster port's admin stream (ADR-3).
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/dihedron/drun/internal/admin/adminpb"
	"github.com/dihedron/drun/internal/fsm"
	"github.com/dihedron/drun/internal/fsm/fsmpb"
	"github.com/dihedron/drun/internal/raftnode"
	"github.com/dihedron/drun/internal/store"
	"github.com/dihedron/drun/internal/supervisor"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// applyTimeout bounds an admin command's wait for its commit
const applyTimeout = 10 * time.Second

// Options are what the admin API serves.
type Options struct {
	Node       *raftnode.Node
	NodeID     string
	Cluster    string
	FSM        *fsm.FSM
	Store      *store.Store
	Supervisor *supervisor.Supervisor
	// Specs are the runnables configured on this node.
	Specs []supervisor.Spec
	// Validation returns why this node cannot run a runnable (FR-16), or nil.
	Validation func(name string) error
	// HeartbeatTimeout is raft.heartbeat_timeout: a member's longest
	// healthy time without contact (FR-17).
	HeartbeatTimeout time.Duration
	Logger           *slog.Logger
}

// Server is the admin API of a node.
type Server struct {
	adminpb.UnimplementedAdminServer
	o    Options
	raft *raft.Raft

	mu      sync.Mutex
	servers []*grpc.Server
}

// New returns the admin API; Serve exposes it on endpoints.
func New(o Options) *Server {
	if o.Validation == nil {
		o.Validation = func(string) error { return nil }
	}
	return &Server{o: o, raft: o.Node.Raft()}
}

// Serve serves the API on an endpoint until Stop.
func (s *Server) Serve(e Endpoint) error {
	ln, err := e.Listen()
	if err != nil {
		return err
	}
	g := grpc.NewServer(grpc.Creds(e.Credentials()), grpc.UnaryInterceptor(s.intercept), grpc.StreamInterceptor(s.interceptStream))
	adminpb.RegisterAdminServer(g, s)
	s.mu.Lock()
	s.servers = append(s.servers, g)
	s.mu.Unlock()
	go func() {
		if err := g.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			s.o.Logger.Error("admin endpoint stopped", "address", ln.Addr(), "error", err)
		}
	}()
	return nil
}

// ServePeers serves the calls other nodes forward to this one.
func (s *Server) ServePeers() error { return s.Serve(peerEndpoint{s.o.Node.Transport()}) }

// Stop closes every endpoint, waiting for calls in progress at most ctx.
func (s *Server) Stop(ctx context.Context) {
	s.mu.Lock()
	servers := s.servers
	s.servers = nil
	s.mu.Unlock()
	for _, g := range servers {
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			g.Stop()
		}
	}
}

func (s *Server) ClusterShow(context.Context, *adminpb.Empty) (*adminpb.ClusterShowReply, error) {
	stats := s.raft.Stats()
	addr, id := s.raft.LeaderWithID()
	r := &adminpb.ClusterShowReply{
		Node: s.o.NodeID, Cluster: s.o.Cluster, State: s.raft.State().String(),
		LeaderId: string(id), LeaderAddress: string(addr),
		Term: parseUint(stats["term"]), CommitIndex: s.raft.CommitIndex(), AppliedIndex: s.raft.AppliedIndex(),
		LastContact: stats["last_contact"],
	}
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, toStatus(err)
	}
	identities := s.o.Node.Identities()
	for _, srv := range f.Configuration().Servers {
		r.Members = append(r.Members, &adminpb.Member{
			Id: string(srv.ID), Address: string(srv.Address), Suffrage: srv.Suffrage.String(),
			Identity: identities[string(srv.ID)], Leader: srv.ID == id,
		})
	}
	return r, nil
}

func parseUint(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// MemberRemove removes a member; removing the leader is a planned
// handover first (FR-2, FR-12), then the new leader removes it.
func (s *Server) MemberRemove(ctx context.Context, r *adminpb.MemberRemoveRequest) (*adminpb.Empty, error) {
	if r.GetId() != s.o.NodeID {
		return &adminpb.Empty{}, toStatus(s.o.Node.Remove(r.GetId()))
	}
	if err := s.o.Supervisor.Handover(ctx, ""); err != nil {
		return nil, toStatus(err)
	}
	reply := &adminpb.Empty{}
	return reply, s.forward(ctx, adminpb.Admin_MemberRemove_FullMethodName, r, reply)
}

// LeaderTransfer is a planned handover (FR-3, FR-12).
func (s *Server) LeaderTransfer(ctx context.Context, r *adminpb.LeaderTransferRequest) (*adminpb.Empty, error) {
	if r.GetTo() == s.o.NodeID {
		return &adminpb.Empty{}, nil
	}
	return &adminpb.Empty{}, toStatus(s.o.Supervisor.Handover(ctx, r.GetTo()))
}

func (s *Server) RunnableSet(_ context.Context, r *adminpb.RunnableSetRequest) (*adminpb.Empty, error) {
	if err := s.known(r.GetName()); err != nil {
		return nil, err
	}
	return &adminpb.Empty{}, s.apply(&fsmpb.Command{Command: &fsmpb.Command_SetDisabled{SetDisabled: &fsmpb.SetDisabled{Runnable: r.GetName(), Disabled: r.GetDisabled()}}})
}

func (s *Server) RunnableRestart(ctx context.Context, r *adminpb.RunnableRequest) (*adminpb.Empty, error) {
	if err := s.configured(r.GetName()); err != nil {
		return nil, err
	}
	return &adminpb.Empty{}, toStatus(s.o.Supervisor.Restart(ctx, r.GetName()))
}

func (s *Server) RunnableReset(_ context.Context, r *adminpb.RunnableResetRequest) (*adminpb.Empty, error) {
	if !r.GetCheckpoint() && !r.GetCompletion() && !r.GetFailures() {
		return nil, status.Error(codes.InvalidArgument, "nothing to reset: choose the checkpoint, the completion or the failures")
	}
	if err := s.known(r.GetName()); err != nil {
		return nil, err
	}
	return &adminpb.Empty{}, s.apply(&fsmpb.Command{Command: &fsmpb.Command_ResetRunnable{ResetRunnable: &fsmpb.Reset{
		Runnable: r.GetName(), Checkpoint: r.GetCheckpoint(), Completion: r.GetCompletion(), Failures: r.GetFailures(),
	}}})
}

func (s *Server) Views(context.Context, *adminpb.Empty) (*adminpb.ViewsReply, error) {
	out := &adminpb.ViewsReply{}
	for _, v := range s.o.Supervisor.Views() {
		out.Views = append(out.Views, &adminpb.View{
			Runnable: v.Runnable, State: v.State, Epoch: v.Epoch, Healthy: v.Healthy,
			Detail: v.Detail, Since: v.Since.UnixMilli(), Node: s.o.NodeID,
		})
	}
	return out, nil
}

// RunnableShow answers from this node's replicated state and
// configuration, with the leader's views (FR-14, FR-15, FR-16).
func (s *Server) RunnableShow(ctx context.Context, r *adminpb.RunnableShowRequest) (*adminpb.RunnableShowReply, error) {
	specs := map[string]supervisor.Spec{}
	names := map[string]bool{}
	for _, sp := range s.o.Specs {
		specs[sp.Name], names[sp.Name] = sp, true
	}
	for _, n := range s.o.FSM.Runnables() {
		names[n] = true
	}
	if r.GetName() != "" {
		if !names[r.GetName()] {
			return nil, status.Errorf(codes.NotFound, "runnable %q is neither configured on %s nor known to the cluster", r.GetName(), s.o.NodeID)
		}
		names = map[string]bool{r.GetName(): true}
	}

	out := &adminpb.RunnableShowReply{Node: s.o.NodeID}
	views := map[string]*adminpb.View{}
	vr, err := s.leaderViews(ctx)
	if err != nil {
		out.ViewsError = err.Error()
	}
	for _, v := range vr.GetViews() {
		views[v.GetRunnable()] = v
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	for _, name := range sorted {
		info := &adminpb.RunnableInfo{Name: name, View: views[name]}
		info.State, _ = s.o.FSM.Runnable(name)
		if sp, ok := specs[name]; ok {
			info.Configured, info.BinarySha256, info.ConfigSha256 = true, sp.BinarySHA256, sp.ConfigSHA256
			if err := s.o.Validation(name); err != nil {
				info.ValidationError = err.Error()
			}
			info.Drift = supervisor.Drift(info.State, sp)
		}
		out.Runnables = append(out.Runnables, info)
	}
	return out, nil
}

func (s *Server) leaderViews(ctx context.Context) (*adminpb.ViewsReply, error) {
	if s.raft.State() == raft.Leader {
		return s.Views(ctx, &adminpb.Empty{})
	}
	vr := &adminpb.ViewsReply{}
	if err := s.forward(ctx, adminpb.Admin_Views_FullMethodName, &adminpb.Empty{}, vr); err != nil {
		return nil, fmt.Errorf("the leader's views: %w", err)
	}
	return vr, nil
}

func (s *Server) CheckpointShow(_ context.Context, r *adminpb.CheckpointShowRequest) (*adminpb.CheckpointShowReply, error) {
	if err := s.known(r.GetName()); err != nil {
		return nil, err
	}
	meta, data, found, err := s.o.Store.Checkpoint(r.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &adminpb.CheckpointShowReply{Found: found, Meta: meta}
	if r.GetData() {
		out.Data = data
	}
	return out, nil
}

// known: the runnable has a state in the cluster
func (s *Server) known(name string) error {
	if _, ok := s.o.FSM.Runnable(name); !ok {
		return status.Errorf(codes.NotFound, "runnable %q is not known to the cluster", name)
	}
	return nil
}

// configured: the runnable is configured on this node
func (s *Server) configured(name string) error {
	for _, sp := range s.o.Specs {
		if sp.Name == name {
			return nil
		}
	}
	return status.Errorf(codes.NotFound, "runnable %q is not configured on %s", name, s.o.NodeID)
}

// apply commits a command on the leader
func (s *Server) apply(cmd *fsmpb.Command) error {
	_, err := s.applyResult(cmd)
	return err
}

// applyResult commits a command on the leader and returns its result
func (s *Server) applyResult(cmd *fsmpb.Command) (*fsm.Result, error) {
	data, err := fsm.Encode(cmd)
	if err != nil {
		return nil, toStatus(err)
	}
	f := s.raft.Apply(data, applyTimeout)
	if err := f.Error(); err != nil {
		return nil, toStatus(err)
	}
	res, ok := f.Response().(*fsm.Result)
	if !ok {
		return nil, status.Error(codes.Internal, "no result from the FSM")
	}
	if res.Err != nil {
		return nil, toStatus(res.Err)
	}
	return res, nil
}

// toStatus maps an error to the gRPC code a client can act on
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	var nl *raftnode.ErrNotLeader
	switch {
	case errors.As(err, &nl), errors.Is(err, raft.ErrNotLeader), errors.Is(err, supervisor.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, fsm.ErrUnknownRunnable):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, fsm.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, fsm.ErrRunning), errors.Is(err, fsm.ErrDisabled), errors.Is(err, fsm.ErrCompleted),
		errors.Is(err, fsm.ErrFenced), errors.Is(err, fsm.ErrUnknownPeer):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Error(codes.Unknown, err.Error())
}
