package admin

import (
	"context"
	"fmt"
	"time"

	"github.com/dihedron/resurgo/internal/admin/adminpb"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/dihedron/resurgo/internal/store"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// the default store.query_timeout, for a runnable not configured here
	defaultQueryTimeout = 30 * time.Second
	// chunkSize is the size of a backup's messages
	chunkSize = 256 << 10
)

// StoreShow describes the runnables' databases on this node, with every
// member's digest (STO-15, STO-16).
func (s *Server) StoreShow(ctx context.Context, r *adminpb.StoreShowRequest) (*adminpb.StoreShowReply, error) {
	names := s.o.FSM.Runnables()
	if r.GetName() != "" {
		if err := s.known(r.GetName()); err != nil {
			return nil, err
		}
		names = []string{r.GetName()}
	}
	members, err := s.members()
	if err != nil {
		return nil, err
	}
	out := &adminpb.StoreShowReply{}
	for _, name := range names {
		info, err := s.o.Store.Info(name)
		if err != nil {
			return nil, toStatus(err)
		}
		si := &adminpb.StoreInfo{Name: name, Exists: info.Exists, Size: info.Size, Tables: info.Tables, LastIndex: info.LastIndex}
		byIndex := map[uint64]string{}
		for _, m := range members {
			d := &adminpb.Digest{}
			if m.ID == raft.ServerID(s.o.NodeID) {
				d, _ = s.StoreDigest(ctx, &adminpb.RunnableRequest{Name: name})
			} else if err := s.invokeOn(ctx, m.ID, m.Address, adminpb.Admin_StoreDigest_FullMethodName, &adminpb.RunnableRequest{Name: name}, d); err != nil {
				d = &adminpb.Digest{Node: string(m.ID), Error: status.Convert(err).Message()}
			}
			if d.GetError() == "" && d.GetDigest() != "" {
				if seen, ok := byIndex[d.GetIndex()]; ok && seen != d.GetDigest() {
					si.Mismatch = true
				}
				byIndex[d.GetIndex()] = d.GetDigest()
			}
			si.Digests = append(si.Digests, d)
		}
		out.Stores = append(out.Stores, si)
	}
	return out, nil
}

func (s *Server) members() ([]raft.Server, error) {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, toStatus(err)
	}
	return f.Configuration().Servers, nil
}

// StoreDigest is this node's digest of a database.
func (s *Server) StoreDigest(_ context.Context, r *adminpb.RunnableRequest) (*adminpb.Digest, error) {
	d := &adminpb.Digest{Node: s.o.NodeID}
	digest, index, err := s.o.Store.Digest(r.GetName())
	if err != nil {
		d.Error = err.Error()
	}
	d.Digest, d.Index = digest, index
	return d, nil
}

// StoreQuery runs a read-only statement: on the leader once it confirmed
// its leadership, or with local on this node (STO-16).
func (s *Server) StoreQuery(ctx context.Context, r *adminpb.StoreQueryRequest) (*adminpb.StoreQueryReply, error) {
	if err := s.known(r.GetName()); err != nil {
		return nil, err
	}
	out := &adminpb.StoreQueryReply{Node: s.o.NodeID}
	if r.GetLocal() {
		out.Stale = s.raft.State() != raft.Leader
	} else if err := s.raft.VerifyLeader().Error(); err != nil {
		return nil, toStatus(err)
	}
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout(r.GetName()))
	defer cancel()
	cols, rows, err := s.o.Store.Query(ctx, r.GetName(), r.GetSql())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	out.Columns = cols
	for _, row := range rows {
		pr := &fsmpb.Row{}
		for _, v := range row {
			pr.Values = append(pr.Values, store.ToValue(v))
		}
		out.Rows = append(out.Rows, pr)
	}
	return out, nil
}

func (s *Server) queryTimeout(name string) time.Duration {
	for _, sp := range s.o.Specs {
		if sp.Name == name && sp.Store.QueryTimeout > 0 {
			return sp.Store.QueryTimeout
		}
	}
	return defaultQueryTimeout
}

// StoreExec is an operator's batch (STO-17): the runnable is stopped and
// released, the batch is committed as an incarnation of the operator's
// own, released, and the runnable starts again with a newer epoch (or
// stays disabled, with disable).
func (s *Server) StoreExec(ctx context.Context, r *adminpb.StoreExecRequest) (*adminpb.StoreExecReply, error) {
	state, ok := s.o.FSM.Runnable(r.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "runnable %q is not known to the cluster", r.GetName())
	}
	stmts := store.Split(r.GetSql())
	if len(stmts) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no statement to run")
	}
	batch := &fsmpb.StoreBatch{Runnable: r.GetName(), At: time.Now().UnixMilli()}
	for _, sql := range stmts {
		batch.Statements = append(batch.Statements, &fsmpb.Statement{Sql: sql})
	}
	if r.GetDryRun() {
		res, err := s.o.Store.DryRun(batch, state.GetStoreLimits())
		if err != nil {
			return nil, toStatus(err)
		}
		return &adminpb.StoreExecReply{Result: res}, nil
	}

	// held, so that the runnable cannot incarnate between the operator's
	// incarnation and its batch
	if err := s.o.Supervisor.Hold(ctx, r.GetName()); err != nil {
		return nil, toStatus(err)
	}
	defer s.o.Supervisor.Resume(r.GetName())
	if r.GetDisable() {
		if err := s.apply(&fsmpb.Command{Command: &fsmpb.Command_SetDisabled{SetDisabled: &fsmpb.SetDisabled{Runnable: r.GetName(), Disabled: true}}}); err != nil {
			return nil, err
		}
	}
	who := callerOf(ctx)
	inc, err := s.applyResult(&fsmpb.Command{Command: &fsmpb.Command_Incarnate{Incarnate: &fsmpb.Incarnate{
		Runnable: r.GetName(), Node: s.o.NodeID, Operator: who.Name + "@" + who.Node, At: time.Now().UnixMilli(),
	}}})
	if err != nil {
		return nil, err
	}
	batch.Epoch = inc.Epoch
	res, err := s.applyResult(&fsmpb.Command{Command: &fsmpb.Command_StoreBatch{StoreBatch: batch}})
	// released whatever the batch's outcome: the runnable need not wait
	rerr := s.apply(&fsmpb.Command{Command: &fsmpb.Command_Release{Release: &fsmpb.Release{Runnable: r.GetName(), Epoch: inc.Epoch, At: time.Now().UnixMilli()}}})
	if err != nil {
		return nil, err
	}
	if rerr != nil {
		return nil, status.Errorf(status.Code(rerr), "the batch was committed, but not the release of the operator's incarnation: %s", status.Convert(rerr).Message())
	}
	return &adminpb.StoreExecReply{Result: res.Batch, Epoch: inc.Epoch}, nil
}

// StoreReset drops a runnable's database, committed (STO-16): refused
// while it runs.
func (s *Server) StoreReset(_ context.Context, r *adminpb.RunnableRequest) (*adminpb.Empty, error) {
	if err := s.known(r.GetName()); err != nil {
		return nil, err
	}
	return &adminpb.Empty{}, s.apply(&fsmpb.Command{Command: &fsmpb.Command_ResetRunnable{ResetRunnable: &fsmpb.Reset{Runnable: r.GetName(), Store: true}}})
}

// StoreBackup streams a consistent copy of the database on this node.
func (s *Server) StoreBackup(r *adminpb.RunnableRequest, stream adminpb.Admin_StoreBackupServer) error {
	if err := s.known(r.GetName()); err != nil {
		return err
	}
	w := &chunker{send: func(b []byte) error { return stream.Send(&adminpb.Chunk{Data: b}) }}
	if _, err := s.o.Store.Backup(r.GetName(), w); err != nil {
		return toStatus(err)
	}
	return w.flush()
}

// chunker cuts a stream into chunkSize messages
type chunker struct {
	buf  []byte
	send func([]byte) error
}

func (c *chunker) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		k := min(chunkSize-len(c.buf), len(p))
		c.buf, p = append(c.buf, p[:k]...), p[k:]
		if len(c.buf) == chunkSize {
			if err := c.flush(); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

func (c *chunker) flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	err := c.send(c.buf)
	c.buf = nil
	if err != nil {
		return fmt.Errorf("sending the backup: %w", err)
	}
	return nil
}
