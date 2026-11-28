package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// the faults of §3.1; clock steps are left out (the containers share the
// lab VM's clock)
func faults() []fault {
	return []fault{
		{name: "kill-leader", weight: 8, run: killLeader},
		{name: "kill-follower", weight: 6, run: killFollower},
		{name: "pause-leader", weight: 6, run: pauseLeader},
		{name: "pause-follower", weight: 3, run: pauseFollower},
		{name: "restart-node", weight: 4, run: restartNode},
		{name: "transfer-to", weight: 8, run: transferTo},
		{name: "transfer-any", weight: 3, run: transferAny},
		{name: "isolate-leader", weight: 6, run: isolateLeader},
		{name: "partition-region", weight: 5, run: partitionRegion},
		{name: "oneway-cut", weight: 3, run: oneway},
		{name: "wan-profile", weight: 6, run: wanProfile},
		{name: "loss-burst", weight: 3, run: lossBurst},
		{name: "demote-promote", weight: 4, membership: true, run: demotePromote},
		{name: "rebuild", weight: 3, membership: true, run: rebuild},
		{name: "spare-swap", weight: 2, membership: true, run: spareSwap},
		{name: "runnable-restart", weight: 3, run: runnableRestart},
		{name: "disable-enable", weight: 2, run: disableEnable},
		{name: "store-exec", weight: 1, run: storeExec},
		{name: "pg-kill", weight: 2, run: pgKill},
		{name: "double-kill", weight: 5, run: doubleKill},
	}
}

func (c *campaign) markDown(n, why string) {
	c.mu.Lock()
	c.down[n] = why
	c.mu.Unlock()
}

func (c *campaign) markUp(n string) {
	c.mu.Lock()
	delete(c.down, n)
	c.mu.Unlock()
}

func (c *campaign) hold(ctx context.Context, lo, hi time.Duration) {
	d := c.between(lo, hi)
	c.event("hold", "for", d.Round(time.Millisecond))
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// any up voter, to run a command on
func (c *campaign) anyUp(ctx context.Context) string {
	s := c.lab.scrape(ctx, c.nodes())
	if l := s.leaderOf(); l != "" {
		return l
	}
	for _, v := range c.voters {
		if s.up[v] {
			return v
		}
	}
	return c.voters[0]
}

func killLeader(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	c.markDown(l, "killed")
	t0 := time.Now()
	c.do(ctx, "node", "kill", l)
	c.recover(ctx, "kill-leader", t0, l)
	c.hold(ctx, 5*time.Second, 60*time.Second)
	c.do(ctx, "node", "start", l)
	c.markUp(l)
	return nil
}

func killFollower(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	f := c.follower(l)
	if f == "" {
		return nil
	}
	c.markDown(f, "killed")
	c.do(ctx, "node", "kill", f)
	c.hold(ctx, 5*time.Second, 60*time.Second)
	c.do(ctx, "node", "start", f)
	c.markUp(f)
	return nil
}

func pauseLeader(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	c.markDown(l, "paused")
	t0 := time.Now()
	c.do(ctx, "node", "pause", l)
	hold := c.between(time.Second, 20*time.Second)
	c.event("hold", "for", hold)
	time.Sleep(hold)
	c.do(ctx, "node", "resume", l)
	c.markUp(l)
	c.recover(ctx, "pause-leader", t0)
	return nil
}

func pauseFollower(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	f := c.follower(l)
	if f == "" {
		return nil
	}
	c.markDown(f, "paused")
	c.do(ctx, "node", "pause", f)
	c.hold(ctx, time.Second, 20*time.Second)
	c.do(ctx, "node", "resume", f)
	c.markUp(f)
	return nil
}

func restartNode(ctx context.Context, c *campaign) error {
	n := pick(c.rng, c.voters)
	t0 := time.Now()
	c.do(ctx, "node", "restart", n)
	c.recover(ctx, "restart-node", t0)
	return nil
}

func transferTo(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	to := c.follower(l)
	if to == "" {
		return nil
	}
	t0 := time.Now()
	c.do(ctx, "drun", l, "leader", "transfer", "--to", to)
	c.recover(ctx, "transfer-to", t0)
	return nil
}

func transferAny(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	t0 := time.Now()
	c.do(ctx, "drun", l, "leader", "transfer")
	c.recover(ctx, "transfer-any", t0)
	return nil
}

func isolateLeader(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	c.markDown(l, "isolated")
	t0 := time.Now()
	c.do(ctx, "cut", "isolate", l)
	c.recover(ctx, "isolate-leader", t0, l)
	c.hold(ctx, 2*time.Second, 30*time.Second)
	c.do(ctx, "cut", "clear")
	c.markUp(l)
	return nil
}

// inRegion: the members in region r
func (c *campaign) inRegion(r string) []string {
	var out []string
	for _, n := range c.nodes() {
		if regionOf(n) == r {
			out = append(out, n)
		}
	}
	return out
}

// regionOf is a node's region
func regionOf(n string) string {
	if n == "drun-x" {
		return "c"
	}
	return n[5:6]
}

func partitionRegion(ctx context.Context, c *campaign) error {
	// a region with at most two voters, so that a quorum stays
	counts := map[string]int{}
	for _, v := range c.voters {
		counts[regionOf(v)]++
	}
	var ok []string
	for _, r := range []string{"a", "b", "c"} {
		if counts[r] <= len(c.voters)-(len(c.voters)/2+1) {
			ok = append(ok, r)
		}
	}
	if len(ok) == 0 {
		return nil
	}
	r := pick(c.rng, ok)
	t0 := time.Now()
	c.do(ctx, "wan", "partition", r)
	c.recover(ctx, "partition-"+r, t0, c.inRegion(r)...)
	c.hold(ctx, 5*time.Second, 60*time.Second)
	c.do(ctx, "wan", "heal")
	return nil
}

func oneway(ctx context.Context, c *campaign) error {
	src := pick(c.rng, c.voters)
	dst := pick(c.rng, slices.DeleteFunc(slices.Clone(c.voters), func(v string) bool { return v == src }))
	t0 := time.Now()
	c.do(ctx, "cut", "oneway", src, dst)
	c.hold(ctx, 5*time.Second, 30*time.Second)
	c.do(ctx, "cut", "clear")
	c.recover(ctx, "oneway-cut", t0)
	return nil
}

func (c *campaign) profiles() []string {
	if c.wan {
		return []string{"wan30", "wanbad", "random"}
	}
	return []string{"lan", "lan", "wan30", "random"}
}

func wanProfile(ctx context.Context, c *campaign) error {
	p := pick(c.rng, c.profiles())
	if p != "random" {
		jitter := pick(c.rng, []string{"0", "10%", "25%", "50%"})
		c.do(ctx, "wan", "set", p, "--jitter", jitter)
		c.event("wan", "profile", p, "jitter", jitter)
		return nil
	}
	var args []string
	for _, pair := range []string{"a-b", "a-c", "b-c"} {
		d := 1 + c.rng.IntN(60)
		args = append(args, "--link", fmt.Sprintf("%s=%d/%d/%d", pair, d, c.rng.IntN(d/2+1), c.rng.IntN(2)))
	}
	c.do(ctx, append([]string{"wan", "set"}, args...)...)
	c.event("wan", "links", strings.Join(args, " "))
	return nil
}

func lossBurst(ctx context.Context, c *campaign) error {
	pair := pick(c.rng, []string{"a-b", "a-c", "b-c"})
	loss := 5 + c.rng.IntN(16)
	c.do(ctx, "wan", "set", "--link", fmt.Sprintf("%s=%d/2/%d", pair, 10+c.rng.IntN(20), loss))
	c.hold(ctx, 10*time.Second, 60*time.Second)
	return wanProfile(ctx, c)
}

func demotePromote(ctx context.Context, c *campaign) error {
	if len(c.voters) < 4 {
		return nil
	}
	v := pick(c.rng, c.voters)
	at := c.anyUp(ctx)
	t0 := time.Now()
	if !c.do(ctx, "drun", at, "member", "demote", v) {
		return nil
	}
	c.recover(ctx, "demote", t0)
	_ = c.refreshMembers(ctx)
	c.hold(ctx, 30*time.Second, 120*time.Second)
	c.do(ctx, "drun", c.anyUp(ctx), "--timeout", "3m", "member", "promote", v, "--stable", "5s")
	return c.refreshMembers(ctx)
}

// rebuild: remove a follower, wipe it, add it back through the gate
func rebuild(ctx context.Context, c *campaign) error {
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	f := c.follower(l)
	if f == "" || len(c.voters) < 5 {
		return nil
	}
	c.markDown(f, "wiped")
	if !c.do(ctx, "drun", l, "member", "remove", f) {
		c.markUp(f)
		return nil
	}
	c.do(ctx, "node", "wipe", f)
	c.mu.Lock()
	for _, r := range runnables {
		delete(c.lastEpoch[r], f)
	}
	c.mu.Unlock()
	c.do(ctx, "node", "start", f)
	c.do(ctx, "drun", c.anyUp(ctx), "--timeout", "3m", "member", "add", f, c.lab.ips[f]+":7350",
		"--identity", "spiffe://drun-lab/node/"+f, "--stable", "5s")
	c.markUp(f)
	return c.refreshMembers(ctx)
}

// spareSwap: the spare joins as a voter, then a follower leaves and becomes
// the spare
func spareSwap(ctx context.Context, c *campaign) error {
	s := c.spare
	c.do(ctx, "node", "wipe", s)
	c.do(ctx, "node", "start", s)
	if !c.do(ctx, "drun", c.anyUp(ctx), "--timeout", "3m", "member", "add", s, c.lab.ips[s]+":7350",
		"--identity", "spiffe://drun-lab/node/"+s, "--stable", "5s") {
		// a failed add may leave the spare a nonvoter: it stays the spare
		c.do(ctx, "drun", c.anyUp(ctx), "member", "remove", s)
		c.do(ctx, "node", "stop", s)
		return nil
	}
	if err := c.refreshMembers(ctx); err != nil {
		return err
	}
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	y := c.follower(l)
	if y == "" || y == s {
		return nil
	}
	if !c.do(ctx, "drun", l, "member", "remove", y) {
		return nil
	}
	c.do(ctx, "node", "wipe", y)
	c.mu.Lock()
	c.spare = y
	for _, r := range runnables {
		delete(c.lastEpoch[r], y)
	}
	c.mu.Unlock()
	c.event("spare", "is", y, "joined", s)
	return c.refreshMembers(ctx)
}

func runnableRestart(ctx context.Context, c *campaign) error {
	r := pick(c.rng, runnables)
	t0 := time.Now()
	c.do(ctx, "drun", c.anyUp(ctx), "runnable", "restart", r)
	c.recover(ctx, "runnable-restart", t0)
	return nil
}

func disableEnable(ctx context.Context, c *campaign) error {
	r := pick(c.rng, runnables)
	c.do(ctx, "drun", c.anyUp(ctx), "runnable", "disable", r)
	c.hold(ctx, 10*time.Second, 60*time.Second)
	t0 := time.Now()
	c.do(ctx, "drun", c.anyUp(ctx), "runnable", "enable", r)
	c.recover(ctx, "enable", t0)
	return nil
}

func storeExec(ctx context.Context, c *campaign) error {
	t0 := time.Now()
	c.do(ctx, "drun", c.anyUp(ctx), "store", "exec", "ticker", "CREATE TABLE IF NOT EXISTS chaos (at INTEGER); INSERT INTO chaos VALUES (drun_now());")
	c.recover(ctx, "store-exec", t0)
	return nil
}

func pgKill(ctx context.Context, c *campaign) error {
	c.do(ctx, "pg", "kill")
	c.hold(ctx, 10*time.Second, 60*time.Second)
	t0 := time.Now()
	c.do(ctx, "pg", "start")
	c.recover(ctx, "pg-kill", t0)
	return nil
}

// doubleKill: a follower, then the leader: two of five voters down
func doubleKill(ctx context.Context, c *campaign) error {
	if len(c.voters) < 5 {
		return nil
	}
	l, err := c.leader(ctx, c.budget())
	if err != nil {
		return err
	}
	f := c.follower(l)
	if f == "" {
		return nil
	}
	c.markDown(f, "killed")
	c.do(ctx, "node", "kill", f)
	c.markDown(l, "killed")
	t0 := time.Now()
	c.do(ctx, "node", "kill", l)
	c.recover(ctx, "double-kill", t0, l, f)
	c.hold(ctx, 5*time.Second, 30*time.Second)
	c.do(ctx, "node", "start", f)
	c.do(ctx, "node", "start", l)
	c.markUp(f)
	c.markUp(l)
	return nil
}

// healAll undoes whatever a fault left: cuts, partitions, paused or
// stopped members, a stopped PostgreSQL
func (c *campaign) healAll(ctx context.Context) {
	c.do(ctx, "cut", "clear")
	c.do(ctx, "wan", "heal")
	c.do(ctx, "pg", "start")
	c.mu.Lock()
	voters := slices.Clone(c.voters)
	c.down = map[string]string{}
	c.mu.Unlock()
	for _, v := range voters {
		c.do(ctx, "node", "resume", v)
		c.do(ctx, "node", "start", v)
	}
}
