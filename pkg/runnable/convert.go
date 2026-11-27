package runnable

import (
	"errors"
	"fmt"
	"time"

	"github.com/dihedron/resurgo/pkg/runnable/runnablepb"
)

func toPBDescription(d Description) *runnablepb.Description {
	return &runnablepb.Description{Name: d.Name, Version: d.Version, Kind: runnablepb.Kind(d.Kind), Stateful: d.Stateful, ConfigSchema: d.ConfigSchema} //nolint:gosec // two kinds
}

func fromPBDescription(d *runnablepb.Description) Description {
	return Description{Name: d.GetName(), Version: d.GetVersion(), Kind: Kind(d.GetKind()), Stateful: d.GetStateful(), ConfigSchema: d.GetConfigSchema()}
}

func toPBIncarnation(inc Incarnation) *runnablepb.Incarnation {
	pb := &runnablepb.Incarnation{Runnable: inc.Runnable, Epoch: inc.Epoch, Term: inc.Term, Node: inc.Node, Cluster: inc.Cluster, Config: inc.Config}
	if c := inc.Checkpoint; c != nil {
		pb.Checkpoint = &runnablepb.Checkpoint{Seq: c.Seq, Epoch: c.Epoch, Data: c.Data, CommittedAtUnixMs: c.CommittedAt.UnixMilli()}
	}
	return pb
}

func fromPBIncarnation(pb *runnablepb.Incarnation) Incarnation {
	inc := Incarnation{Runnable: pb.GetRunnable(), Epoch: pb.GetEpoch(), Term: pb.GetTerm(), Node: pb.GetNode(), Cluster: pb.GetCluster(), Config: pb.GetConfig()}
	if c := pb.GetCheckpoint(); c != nil {
		inc.Checkpoint = &Checkpoint{Seq: c.GetSeq(), Epoch: c.GetEpoch(), Data: c.GetData(), CommittedAt: time.UnixMilli(c.GetCommittedAtUnixMs())}
	}
	return inc
}

func toPBValue(v any) (*runnablepb.Value, error) {
	switch x := v.(type) {
	case nil:
		return &runnablepb.Value{Value: &runnablepb.Value_Null{Null: true}}, nil
	case int64:
		return &runnablepb.Value{Value: &runnablepb.Value_Integer{Integer: x}}, nil
	case int:
		return &runnablepb.Value{Value: &runnablepb.Value_Integer{Integer: int64(x)}}, nil
	case float64:
		return &runnablepb.Value{Value: &runnablepb.Value_Real{Real: x}}, nil
	case string:
		return &runnablepb.Value{Value: &runnablepb.Value_Text{Text: x}}, nil
	case []byte:
		return &runnablepb.Value{Value: &runnablepb.Value_Blob{Blob: x}}, nil
	case bool:
		return &runnablepb.Value{Value: &runnablepb.Value_Boolean{Boolean: x}}, nil
	}
	return nil, fmt.Errorf("runnable: %T is not a store value: use int64, float64, string, []byte, bool or nil", v)
}

func fromPBValue(v *runnablepb.Value) any {
	switch x := v.GetValue().(type) {
	case *runnablepb.Value_Integer:
		return x.Integer
	case *runnablepb.Value_Real:
		return x.Real
	case *runnablepb.Value_Text:
		return x.Text
	case *runnablepb.Value_Blob:
		return x.Blob
	case *runnablepb.Value_Boolean:
		return x.Boolean
	}
	return nil
}

func toPBValues(vs []any) ([]*runnablepb.Value, error) {
	out := make([]*runnablepb.Value, len(vs))
	for i, v := range vs {
		pv, err := toPBValue(v)
		if err != nil {
			return nil, err
		}
		out[i] = pv
	}
	return out, nil
}

func fromPBValues(vs []*runnablepb.Value) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = fromPBValue(v)
	}
	return out
}

func toPBStatements(batch []Statement) ([]*runnablepb.Statement, error) {
	out := make([]*runnablepb.Statement, len(batch))
	for i, st := range batch {
		args, err := toPBValues(st.Args)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		ps := &runnablepb.Statement{Sql: st.SQL, Args: args}
		if e := st.Expect; e != nil {
			ps.RowsAffected, ps.MinRowsAffected = e.RowsAffected, e.MinRowsAffected
		}
		out[i] = ps
	}
	return out, nil
}

// FromPBStatements converts a batch from the protocol (for the node).
func FromPBStatements(pb []*runnablepb.Statement) []Statement {
	out := make([]Statement, len(pb))
	for i, ps := range pb {
		st := Statement{SQL: ps.GetSql(), Args: fromPBValues(ps.GetArgs())}
		if ps.RowsAffected != nil || ps.MinRowsAffected != nil {
			st.Expect = &Expectation{RowsAffected: ps.RowsAffected, MinRowsAffected: ps.MinRowsAffected}
		}
		out[i] = st
	}
	return out
}

func toPBResults(results []Result) ([]*runnablepb.StatementResult, error) {
	out := make([]*runnablepb.StatementResult, len(results))
	for i, r := range results {
		pr := &runnablepb.StatementResult{RowsAffected: r.RowsAffected, LastInsertId: r.LastInsertID}
		for _, row := range r.Rows {
			vals, err := toPBValues(row)
			if err != nil {
				return nil, err
			}
			pr.Rows = append(pr.Rows, &runnablepb.Row{Values: vals})
		}
		out[i] = pr
	}
	return out, nil
}

func fromPBResults(pb []*runnablepb.StatementResult) []Result {
	out := make([]Result, len(pb))
	for i, pr := range pb {
		r := Result{RowsAffected: pr.GetRowsAffected(), LastInsertID: pr.GetLastInsertId()}
		for _, row := range pr.GetRows() {
			r.Rows = append(r.Rows, fromPBValues(row.GetValues()))
		}
		out[i] = r
	}
	return out
}

// toPBError encodes a request's error for the stream
func toPBError(err error) *runnablepb.Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrFenced) {
		return &runnablepb.Error{Fenced: true, Message: err.Error()}
	}
	var be *BatchError
	if errors.As(err, &be) {
		return &runnablepb.Error{Batch: true, Statement: int32(be.Statement), Code: int32(be.Code), Message: be.Message} //nolint:gosec // small values
	}
	return &runnablepb.Error{Message: err.Error()}
}

// fromPBError decodes a request's error from the stream
func fromPBError(e *runnablepb.Error) error {
	switch {
	case e == nil:
		return nil
	case e.GetFenced():
		return ErrFenced
	case e.GetBatch():
		return &BatchError{Statement: int(e.GetStatement()), Code: int(e.GetCode()), Message: e.GetMessage()}
	}
	return errors.New(e.GetMessage())
}
