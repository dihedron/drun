package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

var runnables = []string{"ticker", "fenced-counter"}

// errSafety stops the campaign: an invariant broke
var errSafety = errors.New("safety violation")

// campaign is one run (.specs/drun-lab.md §3)
type campaign struct {
	lab    *lab
	rng    *rand.Rand
	log    *slog.Logger
	dir    string
	events *os.File

	mu        sync.Mutex
	voters    []string // the current voters
	spare     string   // the node that is not a member
	down      map[string]string
	wan       bool // raft.profile wan
	lastEpoch map[string]map[string]uint64
	twoSince  map[string]time.Time // runnable -> since when two nodes run it
	stats     stats
	stop      error
}

type stats struct {
	Faults     map[string]int             `json:"faults"`
	Recovery   map[string][]time.Duration `json:"recovery"`
	Liveness   []string                   `json:"liveness"`
	Safety     []string                   `json:"safety"`
	LabErrors  []string                   `json:"lab_errors"`
	Checks     map[string]int             `json:"checks"`
	Started    time.Time                  `json:"started"`
	Ended      time.Time                  `json:"ended"`
	Seed       uint64                     `json:"seed"`
	PhaseTimes map[string]string          `json:"phases"`
}

func (c *campaign) event(kind string, kv ...any) {
	c.log.Info(kind, kv...)
	m := map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano), "kind": kind}
	for i := 0; i+1 < len(kv); i += 2 {
		m[fmt.Sprint(kv[i])] = fmt.Sprint(kv[i+1])
	}
	b, _ := json.Marshal(m)
	c.mu.Lock()
	_, _ = c.events.Write(append(b, '\n'))
	c.mu.Unlock()
}

// downNodes are the nodes a fault holds: their view does not count
func (c *campaign) downNodes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for n := range c.down {
		out = append(out, n)
	}
	return out
}

func (c *campaign) nodes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append(slices.Clone(c.voters), c.spare)
}

// pick returns a random element
func pick[T any](r *rand.Rand, s []T) T { return s[r.IntN(len(s))] }

// between returns a random duration in [lo, hi]
func (c *campaign) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(c.rng.Int64N(int64(hi-lo)+1))
}

// fault is one kind of injection
type fault struct {
	name   string
	weight int
	// heavy: membership changes get three times the weight in the
	// membership phase
	membership bool
	run        func(ctx context.Context, c *campaign) error
}

// leader is the leader per the voters' metrics, waiting up to d for one
func (c *campaign) leader(ctx context.Context, d time.Duration) (string, error) {
	deadline := time.Now().Add(d)
	for {
		s := c.lab.scrape(ctx, c.nodes())
		if l := s.leaderOf(c.downNodes()...); l != "" {
			return l, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return "", fmt.Errorf("no leader within %s", d)
		}
		time.Sleep(time.Second)
	}
}

// follower is a running voter that is not the leader
func (c *campaign) follower(leader string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var fs []string
	for _, v := range c.voters {
		if v != leader && c.down[v] == "" {
			fs = append(fs, v)
		}
	}
	if len(fs) == 0 {
		return ""
	}
	return pick(c.rng, fs)
}

// budget is the time a recovery may take
func (c *campaign) budget() time.Duration {
	if c.wan {
		return 90 * time.Second
	}
	return 60 * time.Second
}

// recover waits until the cluster serves again, and records how long it
// took since from, under name
func (c *campaign) recover(ctx context.Context, name string, from time.Time, exclude ...string) {
	deadline := time.Now().Add(c.budget())
	for {
		s := c.lab.scrape(ctx, c.nodes())
		if s.serving(runnables, exclude...) {
			d := time.Since(from)
			c.mu.Lock()
			c.stats.Recovery[name] = append(c.stats.Recovery[name], d)
			c.mu.Unlock()
			c.event("recovered", "fault", name, "after", d.Round(10*time.Millisecond))
			return
		}
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(deadline) {
			msg := fmt.Sprintf("%s: not serving %s after the fault (leader %q, running %v)", time.Now().Format(time.TimeOnly), c.budget(), s.leaderOf(), s.running)
			c.mu.Lock()
			c.stats.Liveness = append(c.stats.Liveness, name+": "+msg)
			c.mu.Unlock()
			c.event("liveness", "fault", name, "detail", msg)
			c.evidence(ctx, "liveness-"+name)
			// give it five more minutes before calling it stuck
			if !c.waitServing(ctx, 5*time.Minute) && ctx.Err() == nil {
				c.fail(fmt.Errorf("%w: stuck after %s: %s", errSafety, name, msg))
			}
			return
		}
		time.Sleep(time.Second)
	}
}

func (c *campaign) waitServing(ctx context.Context, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if c.lab.scrape(ctx, c.nodes()).serving(runnables) {
			return true
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func (c *campaign) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stop == nil {
		c.stop = err
		c.stats.Safety = append(c.stats.Safety, err.Error())
	}
}

func (c *campaign) failed() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stop
}

// labErr records a lab or tooling error, which does not stop the run
func (c *campaign) labErr(err error) {
	c.mu.Lock()
	c.stats.LabErrors = append(c.stats.LabErrors, time.Now().Format(time.TimeOnly)+" "+err.Error())
	c.mu.Unlock()
	c.event("lab-error", "error", err)
}

// do runs a lab command, recording a failure as a lab error
func (c *campaign) do(ctx context.Context, args ...string) bool {
	if _, err := c.lab.run(ctx, 3*time.Minute, args...); err != nil {
		c.labErr(err)
		return false
	}
	return true
}

// check is the continuous checker (§3.2): every 10 s
func (c *campaign) check(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	slow := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s := c.lab.scrape(ctx, c.nodes())
		c.checkSample(s)
		if time.Since(slow) > 10*time.Minute {
			slow = time.Now()
			c.slowChecks(ctx)
		}
	}
}

// checkSample: epochs never go back on a node; no runnable runs on two
// nodes for longer than any lease (20 s)
func (c *campaign) checkSample(s sample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.Checks["samples"]++
	for r, byNode := range s.epoch {
		if c.lastEpoch[r] == nil {
			c.lastEpoch[r] = map[string]uint64{}
		}
		for n, e := range byNode {
			if prev := c.lastEpoch[r][n]; e < prev && c.down[n] != "wiped" {
				err := fmt.Errorf("%w: %s's epoch went back on %s: %d after %d", errSafety, r, n, e, prev)
				c.stats.Safety = append(c.stats.Safety, err.Error())
				if c.stop == nil {
					c.stop = err
				}
			}
			c.lastEpoch[r][n] = e
		}
	}
	for _, r := range runnables {
		if len(s.running[r]) > 1 {
			if c.twoSince[r].IsZero() {
				c.twoSince[r] = s.at
			} else if s.at.Sub(c.twoSince[r]) > 20*time.Second {
				err := fmt.Errorf("%w: %s running on %v for %s", errSafety, r, s.running[r], s.at.Sub(c.twoSince[r]).Round(time.Second))
				c.stats.Safety = append(c.stats.Safety, err.Error())
				if c.stop == nil {
					c.stop = err
				}
			}
		} else {
			c.twoSince[r] = time.Time{}
		}
	}
}

type memberShow struct {
	Members []struct {
		ID       string `json:"id"`
		Suffrage string `json:"suffrage"`
		Leader   bool   `json:"leader"`
		Healthy  bool   `json:"healthy"`
		Reason   string `json:"reason"`
	} `json:"members"`
}

type storeShow struct {
	Stores []struct {
		Name     string `json:"name"`
		Mismatch bool   `json:"mismatch"`
	} `json:"stores"`
}

// slowChecks: the fenced-counter's checker and the store digests (every 10
// minutes and at the end)
func (c *campaign) slowChecks(ctx context.Context) {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		c.event("check-skipped", "why", err)
		return
	}
	out, err := c.lab.run(ctx, 2*time.Minute, "ssh", l, "/usr/libexec/drun/runnables/drun-runnable-fenced-counter", "check", "--dsn-file", "/etc/drun/fenced-counter/dsn")
	c.mu.Lock()
	c.stats.Checks["fenced-counter"]++
	c.mu.Unlock()
	switch {
	case err != nil && strings.Contains(err.Error(), "exit status 1"):
		c.fail(fmt.Errorf("%w: fenced-counter check: %s", errSafety, strings.TrimSpace(out)))
	case err != nil:
		c.labErr(fmt.Errorf("fenced-counter check: %w", err))
	default:
		c.event("check", "what", "fenced-counter", "result", strings.TrimSpace(lastLine([]byte(out))))
	}
	var st storeShow
	if err := c.lab.drunJSON(ctx, l, &st, "store", "show"); err != nil {
		c.labErr(err)
		return
	}
	c.mu.Lock()
	c.stats.Checks["store-digests"]++
	c.mu.Unlock()
	for _, s := range st.Stores {
		if s.Mismatch {
			c.fail(fmt.Errorf("%w: store %s: digests differ at the same index", errSafety, s.Name))
		}
	}
}

// evidence saves what the nodes say into the run directory
func (c *campaign) evidence(ctx context.Context, tag string) {
	dir := filepath.Join(c.dir, "evidence", time.Now().Format("150405")+"-"+tag)
	_ = os.MkdirAll(dir, 0o700)
	for _, n := range c.nodes() {
		for name, args := range map[string][]string{
			"journal":  {"ssh", n, "journalctl", "-u", "drun", "--since", "-15min", "--no-pager"},
			"runnable": {"drun", n, "runnable", "show"},
			"members":  {"drun", n, "member", "show"},
			"cluster":  {"drun", n, "cluster", "show"},
		} {
			out, err := c.lab.run(ctx, time.Minute, args...)
			if err != nil {
				out += "\n" + err.Error()
			}
			_ = os.WriteFile(filepath.Join(dir, n+"."+name), []byte(out), 0o600)
		}
	}
	c.event("evidence", "dir", dir)
}

// refreshMembers reads the voters from the leader
func (c *campaign) refreshMembers(ctx context.Context) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	var ms memberShow
	if err := c.lab.drunJSON(ctx, l, &ms, "member", "show"); err != nil {
		return err
	}
	var voters []string
	members := map[string]bool{}
	for _, m := range ms.Members {
		members[m.ID] = true
		if m.Suffrage == "Voter" {
			voters = append(voters, m.ID)
		}
	}
	sort.Strings(voters)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(voters) > 0 {
		c.voters = voters
	}
	return nil
}

// quiet: everything healed, members healthy (§3.2)
func (c *campaign) quiet(ctx context.Context) {
	if !c.waitServing(ctx, 3*time.Minute) && ctx.Err() == nil {
		c.fail(fmt.Errorf("%w: not serving at a quiet point", errSafety))
		return
	}
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		c.labErr(err)
		return
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var ms memberShow
		err := c.lab.drunJSON(ctx, l, &ms, "member", "show")
		unhealthy := []string{}
		for _, m := range ms.Members {
			if !m.Healthy {
				unhealthy = append(unhealthy, m.ID+": "+m.Reason)
			}
		}
		if err == nil && len(unhealthy) == 0 {
			c.mu.Lock()
			c.stats.Checks["quiet-points"]++
			c.mu.Unlock()
			return
		}
		if time.Now().After(deadline) {
			msg := fmt.Sprintf("members unhealthy at a quiet point: %v %v", unhealthy, err)
			c.mu.Lock()
			c.stats.Liveness = append(c.stats.Liveness, msg)
			c.mu.Unlock()
			c.event("liveness", "detail", msg)
			return
		}
		time.Sleep(5 * time.Second)
		if l2, err := c.leader(ctx, c.budget()); err == nil {
			l = l2
		}
	}
}
