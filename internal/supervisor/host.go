package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dihedron/resurgo/internal/fsm"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/dihedron/resurgo/pkg/runnable"
	"google.golang.org/protobuf/proto"
)

// host is the node as one incarnation sees it
type host struct {
	c     *controller
	epoch uint64

	mu             sync.Mutex
	lastCheckpoint time.Time
	unhealthySince time.Time
}

func newHost(c *controller, epoch uint64) *host { return &host{c: c, epoch: epoch} }

// unhealthyFor is how long the incarnation has reported itself unhealthy
func (h *host) unhealthyFor() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unhealthySince.IsZero() {
		return 0
	}
	return time.Since(h.unhealthySince)
}

// fenced maps the FSM's refusals to the SDK's
func fenced(err error) error {
	if errors.Is(err, fsm.ErrFenced) {
		return runnable.ErrFenced
	}
	return err
}

// Checkpoint replicates a checkpoint (FR-9): too large is refused, too
// frequent is held back.
func (h *host) Checkpoint(ctx context.Context, data []byte) (uint64, error) {
	spec := h.c.spec
	if max := uint64(spec.Checkpoint.MaxSize); uint64(len(data)) > max {
		return 0, fmt.Errorf("checkpoint of %d bytes: checkpoint.max_size is %d", len(data), max)
	}
	h.mu.Lock()
	wait := time.Until(h.lastCheckpoint.Add(spec.Checkpoint.MinInterval))
	h.mu.Unlock()
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		}
	}
	res, err := h.c.s.apply(&fsmpb.Command{Command: &fsmpb.Command_Checkpoint{Checkpoint: &fsmpb.Checkpoint{
		Runnable: spec.Name, Epoch: h.epoch, Data: data, At: time.Now().UnixMilli(),
	}}})
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		return 0, fenced(res.Err)
	}
	h.mu.Lock()
	h.lastCheckpoint = time.Now()
	h.mu.Unlock()
	return res.Seq, nil
}

// Status records the incarnation's health and progress.
func (h *host) Status(_ context.Context, healthy bool, detail string) error {
	h.mu.Lock()
	switch {
	case healthy:
		h.unhealthySince = time.Time{}
	case h.unhealthySince.IsZero():
		h.unhealthySince = time.Now()
	}
	h.mu.Unlock()
	h.c.s.setView(h.c.spec.Name, func(v *View) { v.Healthy, v.Detail = healthy, detail })
	return nil
}

func (h *host) Store() runnable.Store { return hostStore{h} }

type hostStore struct{ h *host }

// Exec replicates a batch (resurgo-store.md STO-1, STO-2).
func (s hostStore) Exec(_ context.Context, batch []runnable.Statement) ([]runnable.Result, error) {
	spec := s.h.c.spec
	stmts, err := toStatements(batch)
	if err != nil {
		return nil, err
	}
	sb := &fsmpb.StoreBatch{Runnable: spec.Name, Epoch: s.h.epoch, Statements: stmts, At: time.Now().UnixMilli()}
	if size := proto.Size(sb); uint64(size) > uint64(spec.Store.MaxBatch) { //nolint:gosec // sizes are positive
		return nil, fmt.Errorf("batch of %d bytes: store.max_batch is %d", size, spec.Store.MaxBatch)
	}
	res, err := s.h.c.s.apply(&fsmpb.Command{Command: &fsmpb.Command_StoreBatch{StoreBatch: sb}})
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return nil, fenced(res.Err)
	}
	if e := res.Batch.GetError(); e != nil {
		return nil, &runnable.BatchError{Statement: int(e.GetStatement()), Code: int(e.GetCode()), Message: e.GetMessage()}
	}
	return fromResults(res.Batch.GetResults()), nil
}

// Query reads the local database, while the incarnation is current
// (STO-10), within store.query_timeout (STO-11).
func (s hostStore) Query(ctx context.Context, sql string, args ...any) (runnable.Rows, error) {
	spec := s.h.c.spec
	if r, ok := s.h.c.s.o.FSM.Runnable(spec.Name); !ok || r.GetCurrent().GetEpoch() != s.h.epoch {
		return nil, runnable.ErrFenced
	}
	ctx, cancel := context.WithTimeout(ctx, spec.Store.QueryTimeout)
	defer cancel()
	cols, rows, err := s.h.c.s.o.Store.Query(ctx, spec.Name, sql, args...)
	if err != nil {
		return nil, err
	}
	return runnable.NewRows(cols, rows), nil
}

func toStatements(batch []runnable.Statement) ([]*fsmpb.Statement, error) {
	out := make([]*fsmpb.Statement, len(batch))
	for i, st := range batch {
		ps := &fsmpb.Statement{Sql: st.SQL}
		for _, a := range st.Args {
			v, err := toValue(a)
			if err != nil {
				return nil, fmt.Errorf("statement %d: %w", i, err)
			}
			ps.Args = append(ps.Args, v)
		}
		if e := st.Expect; e != nil {
			ps.Expect = &fsmpb.Expectation{RowsAffected: e.RowsAffected, MinRowsAffected: e.MinRowsAffected}
		}
		out[i] = ps
	}
	return out, nil
}

func toValue(v any) (*fsmpb.Value, error) {
	switch x := v.(type) {
	case nil:
		return &fsmpb.Value{Value: &fsmpb.Value_Null{Null: true}}, nil
	case int64:
		return &fsmpb.Value{Value: &fsmpb.Value_Integer{Integer: x}}, nil
	case int:
		return &fsmpb.Value{Value: &fsmpb.Value_Integer{Integer: int64(x)}}, nil
	case float64:
		return &fsmpb.Value{Value: &fsmpb.Value_Real{Real: x}}, nil
	case string:
		return &fsmpb.Value{Value: &fsmpb.Value_Text{Text: x}}, nil
	case []byte:
		return &fsmpb.Value{Value: &fsmpb.Value_Blob{Blob: x}}, nil
	case bool:
		return &fsmpb.Value{Value: &fsmpb.Value_Boolean{Boolean: x}}, nil
	}
	return nil, fmt.Errorf("%T is not a store value", v)
}

func fromValue(v *fsmpb.Value) any {
	switch x := v.GetValue().(type) {
	case *fsmpb.Value_Integer:
		return x.Integer
	case *fsmpb.Value_Real:
		return x.Real
	case *fsmpb.Value_Text:
		return x.Text
	case *fsmpb.Value_Blob:
		return x.Blob
	case *fsmpb.Value_Boolean:
		return x.Boolean
	}
	return nil
}

func fromResults(pb []*fsmpb.StatementResult) []runnable.Result {
	out := make([]runnable.Result, len(pb))
	for i, r := range pb {
		res := runnable.Result{RowsAffected: r.GetRowsAffected(), LastInsertID: r.GetLastInsertId()}
		for _, row := range r.GetRows() {
			vals := make([]any, len(row.GetValues()))
			for j, v := range row.GetValues() {
				vals[j] = fromValue(v)
			}
			res.Rows = append(res.Rows, vals)
		}
		out[i] = res
	}
	return out
}
