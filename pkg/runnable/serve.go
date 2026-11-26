package runnable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dihedron/resurgo/pkg/runnable/runnablepb"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

// Handshake is the go-plugin handshake of the protocol (§8.2): a plugin
// of another protocol version is refused before the first call.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "RESURGO_RUNNABLE",
	MagicCookieValue: "runnable-v1",
}

const pluginName = "runnable"

// Serve runs impl as a plugin process; it is a runnable's main.
func Serve(impl Runnable) {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         plugin.PluginSet{pluginName: &grpcPlugin{impl: impl}},
		GRPCServer:      plugin.DefaultGRPCServer,
	})
}

// grpcPlugin is the go-plugin binding of the protocol, on both sides
type grpcPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	impl Runnable
}

func (p *grpcPlugin) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	runnablepb.RegisterRunnableServer(s, &server{impl: p.impl})
	return nil
}

func (p *grpcPlugin) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return runnablepb.NewRunnableClient(c), nil
}

// server is the plugin's side of the protocol
type server struct {
	runnablepb.UnimplementedRunnableServer
	impl Runnable
}

func (s *server) Describe(context.Context, *runnablepb.DescribeRequest) (*runnablepb.Description, error) {
	return toPBDescription(s.impl.Describe()), nil
}

func (s *server) Validate(ctx context.Context, req *runnablepb.ValidateRequest) (*runnablepb.ValidateResponse, error) {
	if err := s.impl.Validate(ctx, req.GetConfig()); err != nil {
		return &runnablepb.ValidateResponse{Error: err.Error()}, nil
	}
	return &runnablepb.ValidateResponse{}, nil
}

// Run runs one incarnation: Start first, then the incarnation's requests
// and the node's heartbeats, results and stop, until Run returns
func (s *server) Run(stream runnablepb.Runnable_RunServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return errors.New("runnable: the stream did not begin with Start")
	}
	ctx, l := withLease(stream.Context(), start.GetValidUntilBoottimeNs())
	h := &streamHost{send: stream.Send, pending: map[uint64]chan *runnablepb.HostMessage{}}
	go h.receive(stream, l)
	err = runSafely(ctx, s.impl, fromPBIncarnation(start.GetIncarnation()), h)
	l.cancel(nil)
	exit := &runnablepb.Exit{}
	switch e := exitOf(err); e.Outcome {
	case Completed:
		exit.Outcome = &runnablepb.Exit_Completed{Completed: true}
	case Fenced:
		exit.Outcome = &runnablepb.Exit_Fenced{Fenced: true}
	default:
		exit.Outcome = &runnablepb.Exit_Error{Error: e.Err.Error()}
	}
	return h.sendMsg(&runnablepb.RunnableMessage{Msg: &runnablepb.RunnableMessage_Exit{Exit: exit}})
}

// streamHost is the Host a plugin's incarnation talks to: each call is a
// request on the stream, answered by a result with the same id
type streamHost struct {
	send   func(*runnablepb.RunnableMessage) error
	sendMu sync.Mutex
	nextID atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan *runnablepb.HostMessage
	broken  error
}

func (h *streamHost) sendMsg(m *runnablepb.RunnableMessage) error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()
	return h.send(m)
}

// receive dispatches the node's messages until the stream ends
func (h *streamHost) receive(stream runnablepb.Runnable_RunServer, l *lease) {
	for {
		m, err := stream.Recv()
		if err != nil {
			l.cancel(ErrHostGone)
			h.mu.Lock()
			h.broken = ErrHostGone
			for id, ch := range h.pending {
				close(ch)
				delete(h.pending, id)
			}
			h.mu.Unlock()
			return
		}
		var id uint64
		switch x := m.GetMsg().(type) {
		case *runnablepb.HostMessage_Heartbeat:
			l.renew(x.Heartbeat.GetValidUntilBoottimeNs())
			continue
		case *runnablepb.HostMessage_Stop:
			l.cancel(ErrStopped)
			continue
		case *runnablepb.HostMessage_CheckpointResult:
			id = x.CheckpointResult.GetId()
		case *runnablepb.HostMessage_StatusResult:
			id = x.StatusResult.GetId()
		case *runnablepb.HostMessage_ExecResult:
			id = x.ExecResult.GetId()
		case *runnablepb.HostMessage_QueryResult:
			id = x.QueryResult.GetId()
		default:
			continue
		}
		h.mu.Lock()
		ch, ok := h.pending[id]
		delete(h.pending, id)
		h.mu.Unlock()
		if ok {
			ch <- m
		}
	}
}

// call sends a request and waits for its result
func (h *streamHost) call(ctx context.Context, build func(id uint64) *runnablepb.RunnableMessage) (*runnablepb.HostMessage, error) {
	id := h.nextID.Add(1)
	ch := make(chan *runnablepb.HostMessage, 1)
	h.mu.Lock()
	if h.broken != nil {
		h.mu.Unlock()
		return nil, h.broken
	}
	h.pending[id] = ch
	h.mu.Unlock()
	if err := h.sendMsg(build(id)); err != nil {
		h.mu.Lock()
		delete(h.pending, id)
		h.mu.Unlock()
		return nil, fmt.Errorf("runnable: %w", err)
	}
	select {
	case m, ok := <-ch:
		if !ok {
			return nil, ErrHostGone
		}
		return m, nil
	case <-ctx.Done():
		h.mu.Lock()
		delete(h.pending, id)
		h.mu.Unlock()
		return nil, context.Cause(ctx)
	}
}

func (h *streamHost) Checkpoint(ctx context.Context, data []byte) (uint64, error) {
	m, err := h.call(ctx, func(id uint64) *runnablepb.RunnableMessage {
		return &runnablepb.RunnableMessage{Msg: &runnablepb.RunnableMessage_Checkpoint{Checkpoint: &runnablepb.CheckpointRequest{Id: id, Data: data}}}
	})
	if err != nil {
		return 0, err
	}
	r := m.GetCheckpointResult()
	return r.GetSeq(), fromPBError(r.GetError())
}

func (h *streamHost) Status(ctx context.Context, healthy bool, detail string) error {
	m, err := h.call(ctx, func(id uint64) *runnablepb.RunnableMessage {
		return &runnablepb.RunnableMessage{Msg: &runnablepb.RunnableMessage_Status{Status: &runnablepb.StatusReport{Id: id, Healthy: healthy, Detail: detail}}}
	})
	if err != nil {
		return err
	}
	return fromPBError(m.GetStatusResult().GetError())
}

func (h *streamHost) Store() Store { return streamStore{h} }

type streamStore struct{ h *streamHost }

func (s streamStore) Exec(ctx context.Context, batch []Statement) ([]Result, error) {
	stmts, err := toPBStatements(batch)
	if err != nil {
		return nil, err
	}
	m, err := s.h.call(ctx, func(id uint64) *runnablepb.RunnableMessage {
		return &runnablepb.RunnableMessage{Msg: &runnablepb.RunnableMessage_Exec{Exec: &runnablepb.StoreExec{Id: id, Statements: stmts}}}
	})
	if err != nil {
		return nil, err
	}
	r := m.GetExecResult()
	if err := fromPBError(r.GetError()); err != nil {
		return nil, err
	}
	return fromPBResults(r.GetResults()), nil
}

func (s streamStore) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	pargs, err := toPBValues(args)
	if err != nil {
		return nil, err
	}
	m, err := s.h.call(ctx, func(id uint64) *runnablepb.RunnableMessage {
		return &runnablepb.RunnableMessage{Msg: &runnablepb.RunnableMessage_Query{Query: &runnablepb.StoreQuery{Id: id, Sql: sql, Args: pargs}}}
	})
	if err != nil {
		return nil, err
	}
	r := m.GetQueryResult()
	if err := fromPBError(r.GetError()); err != nil {
		return nil, err
	}
	rows := make([][]any, len(r.GetRows()))
	for i, row := range r.GetRows() {
		rows[i] = fromPBValues(row.GetValues())
	}
	return NewRows(r.GetColumns(), rows), nil
}
