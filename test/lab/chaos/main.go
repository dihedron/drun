// chaos is the drun lab's chaos campaign (.specs/drun-lab.md §3): random
// faults from a seed, for a duration, with continuous checks of the
// invariants. It drives test/lab/lab.sh and writes its run under
// test/lab/.state/runs/<id>/: events.jsonl, stats.json and report.md.
//
//	chaos [-duration 6h] [-seed N] [-lab test/lab/lab.sh]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// phase of the schedule, as a share of the duration (§3.3)
type phase struct {
	name   string
	share  float64
	faults bool
	wan    bool
	heavy  bool // membership faults three times as likely
}

var schedule = []phase{
	{name: "baseline", share: 15.0 / 360},
	{name: "lan", share: 120.0 / 360, faults: true},
	{name: "to-wan", share: 15.0 / 360, wan: true},
	{name: "wan", share: 120.0 / 360, faults: true, wan: true},
	{name: "membership", share: 75.0 / 360, faults: true, wan: true, heavy: true},
	{name: "final", share: 15.0 / 360, wan: true},
}

func main() {
	duration := flag.Duration("duration", 6*time.Hour, "how long the campaign runs")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "the seed of the faults")
	script := flag.String("lab", "test/lab/lab.sh", "the lab's script")
	flag.Parse()
	if err := run(*duration, *seed, *script); err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(1)
	}
}

func run(duration time.Duration, seed uint64, script string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	id := time.Now().Format("20060102-150405")
	dir := filepath.Join(filepath.Dir(script), ".state", "runs", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	events, err := os.Create(filepath.Join(dir, "events.jsonl")) //nolint:gosec // the run's own directory
	if err != nil {
		return err
	}
	defer events.Close() //nolint:errcheck // flushed by the writes
	logf, err := os.Create(filepath.Join(dir, "chaos.log")) //nolint:gosec // the run's own directory
	if err != nil {
		return err
	}
	defer logf.Close() //nolint:errcheck // flushed by the writes
	logger := slog.New(slog.NewTextHandler(logf, nil))
	ips := map[string]string{
		"drun-a1": "10.70.1.31", "drun-a2": "10.70.1.32", "drun-b1": "10.70.2.31", "drun-b2": "10.70.2.32",
		"drun-c1": "10.70.3.31", "drun-c2": "10.70.3.32", "drun-x": "10.70.3.39",
	}
	c := &campaign{
		lab: &lab{script: script, ips: ips}, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), //nolint:gosec // reproducible faults, not secrets
		log: logger, dir: dir, events: events,
		voters: []string{"drun-a1", "drun-a2", "drun-b1", "drun-b2", "drun-c1"}, spare: "drun-x",
		down: map[string]string{}, lastEpoch: map[string]map[string]uint64{}, twoSince: map[string]time.Time{},
		stats: stats{Faults: map[string]int{}, Recovery: map[string][]time.Duration{}, Checks: map[string]int{},
			Started: time.Now(), Seed: seed, PhaseTimes: map[string]string{}},
	}
	c.event("start", "seed", seed, "duration", duration, "dir", dir)
	if err := c.refreshMembers(ctx); err != nil {
		return fmt.Errorf("the lab is not serving: %w", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	go c.check(cctx)

	all := faults()
	for _, p := range schedule {
		end := time.Now().Add(time.Duration(float64(duration) * p.share))
		c.stats.PhaseTimes[p.name] = time.Now().Format(time.TimeOnly)
		c.event("phase", "name", p.name, "until", end.Format(time.TimeOnly))
		if p.wan && !c.wan {
			c.toWAN(ctx)
		}
		for time.Now().Before(end) && ctx.Err() == nil && c.failed() == nil {
			if !p.faults {
				time.Sleep(10 * time.Second)
				continue
			}
			f := c.choose(all, p.heavy)
			c.mu.Lock()
			c.stats.Faults[f.name]++
			n := c.stats.Faults[f.name]
			c.mu.Unlock()
			c.event("fault", "name", f.name, "n", n)
			if err := f.run(ctx, c); err != nil {
				c.labErr(fmt.Errorf("%s: %w", f.name, err))
			}
			c.healAll(ctx)
			c.quiet(ctx)
		}
		if c.failed() != nil || ctx.Err() != nil {
			break
		}
	}
	c.healAll(ctx)
	if c.failed() == nil {
		c.quiet(ctx)
		c.slowChecks(ctx)
	} else {
		c.evidence(context.Background(), "safety")
	}
	cancel()
	c.stats.Ended = time.Now()
	return c.report()
}

// choose picks a fault by weight
func (c *campaign) choose(all []fault, heavy bool) fault {
	total := 0
	weight := func(f fault) int {
		if heavy && f.membership {
			return 3 * f.weight
		}
		return f.weight
	}
	for _, f := range all {
		total += weight(f)
	}
	n := c.rng.IntN(total)
	for _, f := range all {
		if n -= weight(f); n < 0 {
			return f
		}
	}
	return all[len(all)-1]
}

// toWAN switches every node to raft.profile wan, one node at a time,
// followers first (a rolling restart, itself a test)
func (c *campaign) toWAN(ctx context.Context) {
	c.event("to-wan")
	c.do(ctx, "profile", "wan")
	c.do(ctx, "wan", "set", "wan30")
	c.wan = true
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		c.labErr(err)
		return
	}
	order := append([]string{}, c.voters...)
	sort.SliceStable(order, func(i, j int) bool { return order[j] == l })
	for _, n := range append(order, c.spare) {
		t0 := time.Now()
		c.do(ctx, "node", "restart", n)
		if n != c.spare {
			c.recover(ctx, "rolling-restart", t0)
			c.quiet(ctx)
		}
	}
	// the spare is not a member: it stays stopped
	c.do(ctx, "node", "stop", c.spare)
}

func (c *campaign) report() error {
	b, _ := json.MarshalIndent(c.stats, "", "  ")
	if err := os.WriteFile(filepath.Join(c.dir, "stats.json"), b, 0o600); err != nil {
		return err
	}
	var r strings.Builder
	s := c.stats
	verdict := "CLEAN: no safety violation, no liveness failure"
	switch {
	case len(s.Safety) > 0:
		verdict = "FAILED: safety violation"
	case len(s.Liveness) > 0:
		verdict = fmt.Sprintf("SAFE, with %d liveness failures", len(s.Liveness))
	}
	fmt.Fprintf(&r, "# Chaos run %s\n\n%s\n\nSeed %d, %s to %s (%s).\n\n", filepath.Base(c.dir), verdict, s.Seed,
		s.Started.Format(time.DateTime), s.Ended.Format(time.DateTime), s.Ended.Sub(s.Started).Round(time.Minute))
	total := 0
	names := make([]string, 0, len(s.Faults))
	for n, k := range s.Faults {
		total += k
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintf(&r, "## Faults (%d)\n\n| Fault | Count |\n| :--- | ---: |\n", total)
	for _, n := range names {
		fmt.Fprintf(&r, "| %s | %d |\n", n, s.Faults[n])
	}
	rnames := make([]string, 0, len(s.Recovery))
	for n := range s.Recovery {
		rnames = append(rnames, n)
	}
	sort.Strings(rnames)
	fmt.Fprintf(&r, "\n## Recovery (fault to serving again)\n\n| Fault | n | p50 | p99 | max |\n| :--- | ---: | ---: | ---: | ---: |\n")
	for _, n := range rnames {
		d := append([]time.Duration{}, s.Recovery[n]...)
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		q := func(p float64) time.Duration { return d[int(p*float64(len(d)-1))].Round(10 * time.Millisecond) }
		fmt.Fprintf(&r, "| %s | %d | %s | %s | %s |\n", n, len(d), q(0.5), q(0.99), d[len(d)-1].Round(10*time.Millisecond))
	}
	fmt.Fprintf(&r, "\n## Checks\n\n")
	for k, v := range s.Checks {
		fmt.Fprintf(&r, "- %s: %d\n", k, v)
	}
	for title, list := range map[string][]string{"Safety violations": s.Safety, "Liveness failures": s.Liveness, "Lab errors": s.LabErrors} {
		fmt.Fprintf(&r, "\n## %s (%d)\n\n", title, len(list))
		for _, e := range list {
			fmt.Fprintf(&r, "- %s\n", e)
		}
	}
	if err := os.WriteFile(filepath.Join(c.dir, "report.md"), []byte(r.String()), 0o600); err != nil {
		return err
	}
	c.event("end", "verdict", verdict)
	if len(s.Safety) > 0 {
		return fmt.Errorf("%s: see %s", verdict, c.dir)
	}
	return nil
}
