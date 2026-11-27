package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
)

// report is what the checker found
type report struct {
	rows, epochs, repeats int
	// violations: rows of an epoch committed after a row of a newer one
	violations []string
}

// check is "check --dsn-file F [--runnable NAME]": in commit order (the
// rows' ids: an incarnation's claim waits for its predecessor's open
// transactions, so its rows come after them) the epochs never decrease.
// Rows repeated after a failover (the seq since the last acknowledged
// checkpoint, written again) are at-least-once, not violations. Exit 0:
// no violation; 1: violations; 2: the check could not run.
func check(args []string, out, errw io.Writer) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(errw)
	dsnFile := fs.String("dsn-file", "", "the file holding the PostgreSQL connection string")
	name := fs.String("runnable", "fenced-counter", "the runnable whose rows to check")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	dsn, err := readDSN(*dsnFile)
	if err != nil {
		_, _ = fmt.Fprintln(errw, "check:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		_, _ = fmt.Fprintln(errw, "check:", err)
		return 2
	}
	defer conn.Close(context.Background()) //nolint:errcheck // read only
	r, err := scan(ctx, conn, *name)
	if err != nil {
		_, _ = fmt.Fprintln(errw, "check:", err)
		return 2
	}
	_, _ = fmt.Fprintf(out, "%s: %d rows, %d epochs, %d rows repeated after a failover, %d violations\n", *name, r.rows, r.epochs, r.repeats, len(r.violations))
	for _, v := range r.violations {
		_, _ = fmt.Fprintln(out, "violation:", v)
	}
	if len(r.violations) > 0 {
		return 1
	}
	return 0
}

func scan(ctx context.Context, conn *pgx.Conn, name string) (report, error) {
	rows, err := conn.Query(ctx, `SELECT id, epoch, seq FROM counter_rows WHERE runnable = $1 ORDER BY id`, name)
	if err != nil {
		return report{}, err
	}
	defer rows.Close()
	var r report
	var last, maxSeq int64
	for rows.Next() {
		var id, epoch, seq int64
		if err := rows.Scan(&id, &epoch, &seq); err != nil {
			return report{}, err
		}
		r.rows++
		switch {
		case epoch < last:
			r.violations = append(r.violations, fmt.Sprintf("row %d of epoch %d committed after epoch %d", id, epoch, last))
		case epoch > last:
			r.epochs++
			last = epoch
		}
		if seq <= maxSeq {
			r.repeats++
		}
		maxSeq = max(maxSeq, seq)
	}
	return r, rows.Err()
}
