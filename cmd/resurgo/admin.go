package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dihedron/resurgo/internal/admin"
	"github.com/dihedron/resurgo/internal/admin/adminpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// output writes a reply as JSON with --json, or as text; write errors
// surface at the tabwriter's Flush
func output(m proto.Message, text func(w *tabwriter.Writer)) error {
	if root.JSON {
		b, err := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: true}.Marshal(m)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, string(b))
		return err
	}
	w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	text(w)
	return w.Flush()
}

type ClusterShowCommand struct{}

func (c *ClusterShowCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.ClusterShow(ctx, &adminpb.Empty{})
		if err != nil {
			return err
		}
		return output(r, func(w *tabwriter.Writer) {
			_, _ = fmt.Fprintf(w, "cluster %s, seen from %s (%s)\n", r.GetCluster(), r.GetNode(), r.GetState())
			_, _ = fmt.Fprintf(w, "term %d, commit index %d, applied index %d, last contact %s\n\n", r.GetTerm(), r.GetCommitIndex(), r.GetAppliedIndex(), r.GetLastContact())
			_, _ = fmt.Fprintln(w, "ID\tADDRESS\tSUFFRAGE\tLEADER\tIDENTITY")
			for _, m := range r.GetMembers() {
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", m.GetId(), m.GetAddress(), m.GetSuffrage(), yes(m.GetLeader()), m.GetIdentity())
			}
		})
	})
}

type LeaderShowCommand struct{}

func (c *LeaderShowCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.ClusterShow(ctx, &adminpb.Empty{})
		if err != nil {
			return err
		}
		leader := &adminpb.Member{Id: r.GetLeaderId(), Address: r.GetLeaderAddress(), Leader: true}
		return output(leader, func(w *tabwriter.Writer) {
			if r.GetLeaderId() == "" {
				_, _ = fmt.Fprintf(w, "no leader is known to %s (term %d)\n", r.GetNode(), r.GetTerm())
				return
			}
			_, _ = fmt.Fprintf(w, "%s (%s), term %d\n", r.GetLeaderId(), r.GetLeaderAddress(), r.GetTerm())
		})
	})
}

type MemberAddCommand struct {
	Identity string `long:"identity" description:"the identity its certificate carries, if not in cluster.tls.peers"`
	NonVoter bool   `long:"non-voter" description:"add it as a non-voter"`
	Args     struct {
		ID      string `positional-arg-name:"ID"`
		Address string `positional-arg-name:"ADDRESS"`
	} `positional-args:"yes" required:"yes"`
}

func (c *MemberAddCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.MemberAdd(ctx, &adminpb.MemberAddRequest{Id: c.Args.ID, Address: c.Args.Address, Identity: c.Identity, NonVoter: c.NonVoter})
		return err
	})
}

type MemberRemoveCommand struct {
	Args struct {
		ID string `positional-arg-name:"ID"`
	} `positional-args:"yes" required:"yes"`
}

func (c *MemberRemoveCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.MemberRemove(ctx, &adminpb.MemberRemoveRequest{Id: c.Args.ID})
		return err
	})
}

type LeaderTransferCommand struct {
	To string `long:"to" description:"the voter to hand leadership to (default: any other)"`
}

func (c *LeaderTransferCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.LeaderTransfer(ctx, &adminpb.LeaderTransferRequest{To: c.To})
		return err
	})
}

type nameArg struct {
	Name string `positional-arg-name:"NAME"`
}

type RunnableShowCommand struct {
	Args struct {
		Name string `positional-arg-name:"NAME"`
	} `positional-args:"yes"`
}

func (c *RunnableShowCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.RunnableShow(ctx, &adminpb.RunnableShowRequest{Name: c.Args.Name})
		if err != nil {
			return err
		}
		return output(r, func(w *tabwriter.Writer) {
			if r.GetViewsError() != "" {
				_, _ = fmt.Fprintf(w, "warning: %s\n\n", r.GetViewsError())
			}
			if c.Args.Name != "" {
				showRunnable(w, r.GetNode(), r.GetRunnables()[0])
				return
			}
			_, _ = fmt.Fprintln(w, "NAME\tSTATE\tEPOCH\tNODE\tHEALTHY\tCHECKPOINT\tFAILURES\tNOTES")
			for _, ri := range r.GetRunnables() {
				st, v := ri.GetState(), ri.GetView()
				_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%d\t%s\n", ri.GetName(), state(ri), st.GetCurrent().GetEpoch(), st.GetCurrent().GetNode(),
					yes(v.GetHealthy()), checkpoint(ri), len(st.GetFailures()), notes(r.GetNode(), ri))
			}
		})
	})
}

// state is the leader's view, or the replicated state without one
func state(ri *adminpb.RunnableInfo) string {
	switch {
	case ri.GetView() != nil:
		return ri.GetView().GetState()
	case ri.GetState().GetDisabled():
		return "disabled"
	case ri.GetState().GetCompleted() != nil:
		return "completed"
	}
	return "-"
}

func checkpoint(ri *adminpb.RunnableInfo) string {
	cp := ri.GetState().GetCheckpoint()
	if cp == nil {
		return "-"
	}
	return fmt.Sprintf("#%d, %d B, %s ago", cp.GetSeq(), cp.GetSize(), since(cp.GetCommittedAt()))
}

func notes(node string, ri *adminpb.RunnableInfo) string {
	var n []string
	if !ri.GetConfigured() {
		n = append(n, "not configured on "+node)
	}
	if ri.GetDrift() {
		n = append(n, "drift on "+node)
	}
	if ri.GetValidationError() != "" {
		n = append(n, "invalid on "+node)
	}
	return strings.Join(n, "; ")
}

func showRunnable(w *tabwriter.Writer, node string, ri *adminpb.RunnableInfo) {
	st, v := ri.GetState(), ri.GetView()
	cur := st.GetCurrent()
	row := func(k, format string, args ...any) {
		_, _ = fmt.Fprintf(w, "%s:\t"+format+"\n", append([]any{k}, args...)...)
	}
	row("name", "%s", ri.GetName())
	row("state", "%s", state(ri))
	if v != nil {
		row("healthy", "%s, %s (leader %s, since %s)", yes(v.GetHealthy()), v.GetDetail(), v.GetNode(), time.UnixMilli(v.GetSince()).Format(time.RFC3339))
	}
	row("disabled", "%s", yes(st.GetDisabled()))
	if cur != nil {
		row("incarnation", "epoch %d, term %d, on %s, started %s, released %s", cur.GetEpoch(), cur.GetTerm(), cur.GetNode(),
			time.UnixMilli(cur.GetStartedAt()).Format(time.RFC3339), yes(st.GetReleased()))
		row("digests", "binary %s, configuration %s", short(cur.GetBinarySha256()), short(cur.GetConfigSha256()))
	}
	if ri.GetConfigured() {
		row("on "+node, "binary %s, configuration %s, drift %s", short(ri.GetBinarySha256()), short(ri.GetConfigSha256()), yes(ri.GetDrift()))
	} else {
		row("on "+node, "not configured")
	}
	if ri.GetValidationError() != "" {
		row("invalid on "+node, "%s", ri.GetValidationError())
	}
	row("checkpoint", "%s", checkpoint(ri))
	if c := st.GetCompleted(); c != nil {
		row("completed", "epoch %d, %s", c.GetEpoch(), time.UnixMilli(c.GetAt()).Format(time.RFC3339))
	}
	for _, f := range st.GetFailures() {
		row("failures on "+f.GetNode(), "%d consecutive, last %s ago: %s", f.GetConsecutive(), since(f.GetLastAt()), f.GetLastReason())
	}
}

func short(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	return hex.EncodeToString(b)[:12]
}

func since(ms int64) string { return time.Since(time.UnixMilli(ms)).Round(time.Second).String() }

func yes(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

type RunnableEnableCommand struct {
	Args nameArg `positional-args:"yes" required:"yes"`
}

func (c *RunnableEnableCommand) Execute([]string) error { return setDisabled(c.Args.Name, false) }

type RunnableDisableCommand struct {
	Args nameArg `positional-args:"yes" required:"yes"`
}

func (c *RunnableDisableCommand) Execute([]string) error { return setDisabled(c.Args.Name, true) }

func setDisabled(name string, disabled bool) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.RunnableSet(ctx, &adminpb.RunnableSetRequest{Name: name, Disabled: disabled})
		return err
	})
}

type RunnableRestartCommand struct {
	Args nameArg `positional-args:"yes" required:"yes"`
}

func (c *RunnableRestartCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.RunnableRestart(ctx, &adminpb.RunnableRequest{Name: c.Args.Name})
		return err
	})
}

type RunnableResetCommand struct {
	Checkpoint bool    `long:"checkpoint" description:"clear the checkpoint"`
	Completion bool    `long:"completion" description:"clear a task's completion: it runs again"`
	Failures   bool    `long:"failures" description:"clear the recorded failures"`
	Args       nameArg `positional-args:"yes" required:"yes"`
}

func (c *RunnableResetCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.RunnableReset(ctx, &adminpb.RunnableResetRequest{Name: c.Args.Name, Checkpoint: c.Checkpoint, Completion: c.Completion, Failures: c.Failures})
		return err
	})
}

type CheckpointShowCommand struct {
	Data bool    `long:"data" description:"write the checkpoint's bytes to stdout"`
	Args nameArg `positional-args:"yes" required:"yes"`
}

func (c *CheckpointShowCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.CheckpointShow(ctx, &adminpb.CheckpointShowRequest{Name: c.Args.Name, Data: c.Data})
		if err != nil {
			return err
		}
		if !r.GetFound() {
			return fmt.Errorf("runnable %s has no checkpoint", c.Args.Name)
		}
		if c.Data {
			_, err := stdout.Write(r.GetData())
			return err
		}
		m := r.GetMeta()
		return output(m, func(w *tabwriter.Writer) {
			_, _ = fmt.Fprintf(w, "seq:\t%d\nepoch:\t%d\nsize:\t%d B\ncommitted:\t%s (%s ago)\n", m.GetSeq(), m.GetEpoch(), m.GetSize(),
				time.UnixMilli(m.GetCommittedAt()).Format(time.RFC3339), since(m.GetCommittedAt()))
		})
	})
}
