// resurgo-runnable-fenced-counter is the stateful example runnable: a
// service that appends (epoch, seq) rows to PostgreSQL, every write
// fenced by its epoch as §8.5 of the specification describes, so that a
// stale incarnation can never commit after a newer one (AC-5, AC-13).
//
// Its configuration, as JSON:
//
//	{"dsn_file": "/etc/resurgo/fenced-counter/dsn", "interval": "1s"}
//
// dsn_file holds the PostgreSQL connection string: the secret stays out of
// the node's configuration and the Raft log (SEC-8).
//
// "resurgo-runnable-fenced-counter check --dsn-file F" checks the rows
// (see check.go).
package main

import (
	"os"

	"github.com/dihedron/resurgo/pkg/runnable"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "check" {
		os.Exit(check(os.Args[2:], os.Stdout, os.Stderr))
	}
	runnable.Serve(counter{})
}
