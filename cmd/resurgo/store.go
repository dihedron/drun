package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/dihedron/resurgo/internal/admin"
	"github.com/dihedron/resurgo/internal/admin/adminpb"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
)

type StoreShowCommand struct {
	Args struct {
		Name string `positional-arg-name:"NAME"`
	} `positional-args:"yes"`
}

func (c *StoreShowCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.StoreShow(ctx, &adminpb.StoreShowRequest{Name: c.Args.Name})
		if err != nil {
			return err
		}
		return output(r, func(w *tabwriter.Writer) {
			for i, si := range r.GetStores() {
				if i > 0 {
					_, _ = fmt.Fprintln(w)
				}
				if !si.GetExists() {
					_, _ = fmt.Fprintf(w, "%s:\tno database\n", si.GetName())
					continue
				}
				_, _ = fmt.Fprintf(w, "%s:\t%d B, last index %d, tables %s\n", si.GetName(), si.GetSize(), si.GetLastIndex(), strings.Join(si.GetTables(), ", "))
				for _, d := range si.GetDigests() {
					if d.GetError() != "" {
						_, _ = fmt.Fprintf(w, "  %s\terror: %s\n", d.GetNode(), d.GetError())
						continue
					}
					_, _ = fmt.Fprintf(w, "  %s\tindex %d\t%s\n", d.GetNode(), d.GetIndex(), d.GetDigest())
				}
				if si.GetMismatch() {
					_, _ = fmt.Fprintln(w, "  MISMATCH: databases at the same index differ")
				}
			}
		})
	})
}

type StoreQueryCommand struct {
	Local bool `long:"local" description:"query the node called, possibly behind the leader, instead of the leader"`
	Args  struct {
		Name string `positional-arg-name:"NAME"`
		SQL  string `positional-arg-name:"SQL"`
	} `positional-args:"yes" required:"yes"`
}

func (c *StoreQueryCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.StoreQuery(ctx, &adminpb.StoreQueryRequest{Name: c.Args.Name, Sql: c.Args.SQL, Local: c.Local})
		if err != nil {
			return err
		}
		if r.GetStale() {
			fmt.Fprintf(os.Stderr, "warning: answered by %s, a follower: possibly behind the leader\n", r.GetNode())
		}
		return output(r, func(w *tabwriter.Writer) {
			_, _ = fmt.Fprintln(w, strings.Join(r.GetColumns(), "\t"))
			rows(w, r.GetRows())
		})
	})
}

func rows(w io.Writer, rs []*fsmpb.Row) {
	for _, row := range rs {
		cells := make([]string, len(row.GetValues()))
		for i, v := range row.GetValues() {
			cells[i] = value(v)
		}
		_, _ = fmt.Fprintln(w, strings.Join(cells, "\t"))
	}
}

func value(v *fsmpb.Value) string {
	switch x := v.GetValue().(type) {
	case *fsmpb.Value_Integer:
		return fmt.Sprint(x.Integer)
	case *fsmpb.Value_Real:
		return fmt.Sprint(x.Real)
	case *fsmpb.Value_Text:
		return x.Text
	case *fsmpb.Value_Blob:
		return fmt.Sprintf("x'%x'", x.Blob)
	case *fsmpb.Value_Boolean:
		return fmt.Sprint(x.Boolean)
	}
	return "NULL"
}

type StoreExecCommand struct {
	File    string `long:"file" short:"f" description:"read the statements from a file (- for stdin)"`
	Disable bool   `long:"disable" description:"disable the runnable first, and leave it disabled (repairs before a restart)"`
	DryRun  bool   `long:"dry-run" description:"run in a rolled-back transaction on a copy of the leader's database and show the results"`
	Args    struct {
		Name string   `positional-arg-name:"NAME"`
		SQL  []string `positional-arg-name:"SQL"`
	} `positional-args:"yes" required:"yes"`
}

func (c *StoreExecCommand) Execute([]string) error {
	sql := strings.Join(c.Args.SQL, ";\n")
	switch {
	case c.File != "" && sql != "":
		return errors.New("give the statements or --file, not both")
	case c.File == "-":
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		sql = string(b)
	case c.File != "":
		b, err := os.ReadFile(c.File)
		if err != nil {
			return err
		}
		sql = string(b)
	case sql == "":
		return errors.New("no statement: give them as arguments or with --file")
	}
	return call(func(ctx context.Context, cl *admin.Client) error {
		r, err := cl.StoreExec(ctx, &adminpb.StoreExecRequest{Name: c.Args.Name, Sql: sql, Disable: c.Disable, DryRun: c.DryRun})
		if err != nil {
			return err
		}
		if e := r.GetResult().GetError(); e != nil {
			return fmt.Errorf("statement %d failed, nothing was applied: %s", e.GetStatement()+1, e.GetMessage())
		}
		return output(r, func(w *tabwriter.Writer) {
			for i, res := range r.GetResult().GetResults() {
				if len(res.GetRows()) > 0 {
					rows(w, res.GetRows())
					continue
				}
				_, _ = fmt.Fprintf(w, "statement %d:\t%d rows affected\n", i+1, res.GetRowsAffected())
			}
			if c.DryRun {
				_, _ = fmt.Fprintln(w, "dry run: nothing was applied")
			} else {
				_, _ = fmt.Fprintf(w, "committed as the operator's incarnation, epoch %d\n", r.GetEpoch())
			}
		})
	})
}

type StoreBackupCommand struct {
	Args struct {
		Name string `positional-arg-name:"NAME"`
		File string `positional-arg-name:"FILE"`
	} `positional-args:"yes" required:"yes"`
}

// Execute writes the copy next to FILE and renames it, so that FILE is
// either the whole database or untouched
func (c *StoreBackupCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		stream, err := cl.StoreBackup(ctx, &adminpb.RunnableRequest{Name: c.Args.Name})
		if err != nil {
			return err
		}
		f, err := os.CreateTemp(filepath.Dir(c.Args.File), "."+filepath.Base(c.Args.File)+".*")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name()) //nolint:errcheck // gone once renamed
		for {
			ch, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = f.Close()
				return err
			}
			if _, err := f.Write(ch.GetData()); err != nil {
				_ = f.Close()
				return err
			}
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
		return os.Rename(f.Name(), c.Args.File)
	})
}

type StoreResetCommand struct {
	Args nameArg `positional-args:"yes" required:"yes"`
}

func (c *StoreResetCommand) Execute([]string) error {
	return call(func(ctx context.Context, cl *admin.Client) error {
		_, err := cl.StoreReset(ctx, &adminpb.RunnableRequest{Name: c.Args.Name})
		return err
	})
}
