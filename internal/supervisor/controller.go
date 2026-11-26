package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dihedron/resurgo/internal/config"
	"github.com/dihedron/resurgo/internal/fsm"
	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/dihedron/resurgo/pkg/runnable"
	"github.com/hashicorp/raft"
)

// States of a runnable on this node, as View shows them.
const (
	StateIdle      = "idle"      // not leader
	StateDisabled  = "disabled"  // disabled by an operator
	StateCompleted = "completed" // a task that completed
	StateWaiting   = "waiting"   // takeover delay, or back-off
	StateRunning   = "running"
	StateStopping  = "stopping"
	StateHeld      = "held" // stopped for a handover
)

// View is what this node knows of a runnable, for "runnable show".
type View struct {
	Runnable string
	State    string
	Epoch    uint64
	Healthy  bool
	Detail   string
	// Since is when the state last changed.
	Since time.Time
}

// Views returns the runnables' views.
func (s *Supervisor) Views() []View {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]View, 0, len(s.views))
	for _, spec := range s.o.Runnables {
		out = append(out, *s.views[spec.Name])
	}
	return out
}

func (s *Supervisor) setView(name string, f func(*View)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.views[name]
	before := v.State
	f(v)
	if v.State != before {
		v.Since = time.Now()
	}
}

// command is a planned stop (FR-12) or the end of one
type command struct {
	hold   bool // stay stopped until leadership changes; false: restart
	resume bool // a held controller starts again
	done   chan struct{}
}

// controller runs one runnable's incarnations while this node is leader
type controller struct {
	s    *Supervisor
	l    *leadership
	spec Spec
	cmds chan *command
	hold bool
	// own are the epochs this controller started: a restart after one of
	// them needs no takeover delay
	own map[uint64]bool
}

func newController(s *Supervisor, l *leadership, spec Spec) *controller {
	return &controller{s: s, l: l, spec: spec, cmds: make(chan *command), own: map[uint64]bool{}}
}

func (c *controller) log() *slog.Logger {
	return c.s.o.Logger.With("runnable", c.spec.Name, "term", c.l.term)
}

func (c *controller) state(state string, epoch uint64) {
	c.s.setView(c.spec.Name, func(v *View) { v.State, v.Epoch = state, epoch })
}

// handle applies a command received while no incarnation runs
func (c *controller) handle(cmd *command) {
	switch {
	case cmd.resume:
		c.hold = false
	default:
		c.hold = cmd.hold
	}
	close(cmd.done)
}

// wait sleeps for d, handling commands; false once leadership ended
func (c *controller) wait(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-c.l.ctx.Done():
			return false
		case cmd := <-c.cmds:
			c.handle(cmd)
			return true
		case <-t.C:
			return true
		}
	}
}

func (c *controller) run() {
	defer c.state(StateIdle, 0)
	backoff := time.Duration(0)
	for c.l.ctx.Err() == nil {
		r, known := c.s.o.FSM.Runnable(c.spec.Name)
		switch {
		case c.hold:
			c.state(StateHeld, 0)
			c.wait(time.Hour)
			continue
		case known && r.GetDisabled():
			c.state(StateDisabled, 0)
			c.wait(c.s.o.RenewInterval)
			continue
		case known && r.GetCompleted() != nil:
			c.state(StateCompleted, r.GetCompleted().GetEpoch())
			c.wait(c.s.o.RenewInterval)
			continue
		}
		if backoff > 0 {
			c.state(StateWaiting, 0)
			if !c.wait(backoff) {
				return
			}
			backoff = 0
			continue // the state may have changed meanwhile
		}

		epoch, err := c.incarnate()
		if err != nil {
			c.log().Warn("incarnate failed", "error", err)
			c.wait(c.s.o.RenewInterval)
			continue
		}
		// FR-13: a predecessor that may still act is given time to stop
		if c.needsDelay(r) {
			c.state(StateWaiting, epoch)
			if until := time.Until(c.l.since.Add(c.spec.TakeoverDelay)); until > 0 {
				c.log().Info("waiting for the takeover delay", "epoch", epoch, "delay", until)
				if !c.wait(until) {
					return
				}
			}
		}
		c.own[epoch] = true
		exit, how := c.incarnation(epoch)
		backoff = c.after(epoch, exit, how)
	}
}

// needsDelay: the predecessor was neither released nor completed, and
// was not an incarnation of this controller (which has ended)
func (c *controller) needsDelay(before *fsmpb.Runnable) bool {
	cur := before.GetCurrent()
	return cur != nil && !before.GetReleased() && before.GetCompleted() == nil && !c.own[cur.GetEpoch()]
}

func (c *controller) incarnate() (uint64, error) {
	res, err := c.s.apply(&fsmpb.Command{Command: &fsmpb.Command_Incarnate{Incarnate: &fsmpb.Incarnate{
		Runnable: c.spec.Name, Node: c.s.o.Node,
		BinarySha256: c.spec.BinarySHA256, ConfigSha256: c.spec.ConfigSHA256,
		StoreLimits: &fsmpb.StoreLimits{
			MaxBatch: uint64(c.spec.Store.MaxBatch), MaxSteps: c.spec.Store.MaxSteps,
			MaxSize: uint64(c.spec.Store.MaxSize), MaxResult: uint64(c.spec.Store.MaxResult),
		},
		At: time.Now().UnixMilli(),
	}}})
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		return 0, res.Err
	}
	return res.Epoch, nil
}

// how an incarnation was stopped
type stopKind int

const (
	stopNatural   stopKind = iota // it ended by itself
	stopLost                      // leadership lost, or not confirmed
	stopFenced                    // a newer incarnation exists
	stopPlanned                   // planned stop, released
	stopUnhealthy                 // unhealthy too long
	stopFailedRun                 // it could not start
)

// incarnation runs one incarnation to its end
func (c *controller) incarnation(epoch uint64) (runnable.Exit, stopKind) {
	inc := runnable.Incarnation{
		Runnable: c.spec.Name, Epoch: epoch, Term: c.l.term, Node: c.s.o.Node,
		Cluster: c.s.o.Cluster, Config: c.spec.ConfigJSON,
	}
	if meta, data, ok, err := c.s.o.Store.Checkpoint(c.spec.Name); err != nil {
		return runnable.Exit{Outcome: runnable.Failed, Err: fmt.Errorf("reading the checkpoint: %w", err)}, stopFailedRun
	} else if ok {
		inc.Checkpoint = &runnable.Checkpoint{Seq: meta.GetSeq(), Epoch: meta.GetEpoch(), Data: data, CommittedAt: time.UnixMilli(meta.GetCommittedAt())}
	}

	t0 := runnable.Boottime()
	if err := c.s.o.Raft.VerifyLeader().Error(); err != nil {
		return runnable.Exit{}, stopLost
	}
	inst, err := c.s.o.Load(c.l.ctx, c.spec)
	if err != nil {
		return runnable.Exit{Outcome: runnable.Failed, Err: fmt.Errorf("loading: %w", err)}, stopFailedRun
	}
	defer inst.Kill() //nolint:errcheck // the process is gone either way
	h := newHost(c, epoch)
	sess, err := inst.Start(c.l.ctx, inc, t0+int64(c.s.o.Lease), h)
	if err != nil {
		return runnable.Exit{Outcome: runnable.Failed, Err: fmt.Errorf("starting: %w", err)}, stopFailedRun
	}
	c.state(StateRunning, epoch)
	c.s.setView(c.spec.Name, func(v *View) { v.Healthy, v.Detail = true, "" })
	c.log().Info("incarnation started", "epoch", epoch)

	tick := time.NewTicker(c.s.o.RenewInterval)
	defer tick.Stop()
	for {
		select {
		case <-sess.Done():
			return sess.Exit(), stopNatural
		case <-c.l.ctx.Done():
			return c.stop(sess, inst, c.spec.StopGrace, "leadership lost"), stopLost
		case cmd := <-c.cmds:
			if cmd.resume {
				close(cmd.done)
				continue
			}
			c.hold = cmd.hold
			exit := c.plannedStop(sess, inst, epoch)
			close(cmd.done)
			return exit, stopPlanned
		case <-tick.C:
		}

		// §8.3: renew the lease only after confirming leadership, from a
		// reading taken before the confirmation
		t0 := runnable.Boottime()
		if err := c.s.o.Raft.VerifyLeader().Error(); err != nil {
			c.log().Warn("leadership not confirmed: stopping", "epoch", epoch, "error", err)
			return c.stop(sess, inst, c.spec.StopGrace, "leadership not confirmed"), stopLost
		}
		_ = sess.Renew(t0 + int64(c.s.o.Lease))

		r, known := c.s.o.FSM.Runnable(c.spec.Name)
		switch {
		case !known || r.GetCurrent().GetEpoch() != epoch:
			return c.stop(sess, inst, c.spec.StopGrace, "fenced"), stopFenced
		case r.GetDisabled():
			exit := c.plannedStop(sess, inst, epoch)
			return exit, stopPlanned
		}
		if h.unhealthyFor() > c.spec.Health.Timeout {
			c.log().Warn("unhealthy too long: stopping", "epoch", epoch, "timeout", c.spec.Health.Timeout)
			c.stop(sess, inst, c.spec.StopGrace, "unhealthy")
			return runnable.Exit{Outcome: runnable.Failed, Err: errors.New("unhealthy for longer than health.timeout")}, stopUnhealthy
		}
	}
}

// stop asks the incarnation to stop and kills it after grace (FR-11)
func (c *controller) stop(sess runnable.Session, inst runnable.Instance, grace time.Duration, reason string) runnable.Exit {
	c.state(StateStopping, 0)
	sess.Stop(reason)
	select {
	case <-sess.Done():
		return sess.Exit()
	case <-time.After(grace):
		c.log().Warn("did not stop within its grace: killing", "grace", grace)
		_ = inst.Kill()
		<-sess.Done()
		return sess.Exit()
	}
}

// plannedStop stops within shutdown_grace and commits Release if it did
// (FR-12): the successor then needs no takeover delay
func (c *controller) plannedStop(sess runnable.Session, inst runnable.Instance, epoch uint64) runnable.Exit {
	c.state(StateStopping, epoch)
	sess.Stop("planned stop")
	select {
	case <-sess.Done():
	case <-time.After(c.spec.ShutdownGrace):
		c.log().Warn("did not stop within its shutdown grace: killing", "grace", c.spec.ShutdownGrace)
		_ = inst.Kill()
		<-sess.Done()
		return sess.Exit()
	}
	res, err := c.s.apply(&fsmpb.Command{Command: &fsmpb.Command_Release{Release: &fsmpb.Release{Runnable: c.spec.Name, Epoch: epoch, At: time.Now().UnixMilli()}}})
	if err == nil && res.Err == nil {
		c.log().Info("incarnation released", "epoch", epoch)
	} else {
		c.log().Warn("release not committed", "epoch", epoch, "error", errors.Join(err, resErr(res)))
	}
	return sess.Exit()
}

func resErr(res *fsm.Result) error {
	if res == nil {
		return nil
	}
	return res.Err
}

// after records how an incarnation ended and returns the back-off
func (c *controller) after(epoch uint64, exit runnable.Exit, how stopKind) time.Duration {
	c.log().Info("incarnation ended", "epoch", epoch, "exit", exit.String(), "stop", int(how))
	switch how {
	case stopLost, stopFenced, stopPlanned:
		return 0
	}
	if exit.Outcome == runnable.Fenced {
		return 0
	}
	if exit.Outcome == runnable.Completed && c.spec.Kind == config.KindTask {
		res, err := c.s.apply(&fsmpb.Command{Command: &fsmpb.Command_Complete{Complete: &fsmpb.Complete{Runnable: c.spec.Name, Epoch: epoch, At: time.Now().UnixMilli()}}})
		if err != nil || res.Err != nil {
			c.log().Warn("completion not committed", "epoch", epoch, "error", errors.Join(err, resErr(res)))
		}
		return 0
	}
	// FR-7, FR-8: any other end is a failure
	reason := exit.String()
	if exit.Outcome == runnable.Completed {
		reason = "a service returned"
	}
	if _, err := c.s.apply(&fsmpb.Command{Command: &fsmpb.Command_Fail{Fail: &fsmpb.Fail{
		Runnable: c.spec.Name, Epoch: epoch, Node: c.s.o.Node, Reason: reason, At: time.Now().UnixMilli(),
	}}}); err != nil {
		c.log().Warn("failure not committed", "epoch", epoch, "error", err)
	}
	failures := c.failures(c.s.o.Node)
	if c.spec.Restart.HandoverAfter > 0 && failures >= uint64(c.spec.Restart.HandoverAfter) { //nolint:gosec // positive
		c.handOver(failures)
	}
	return c.backoff(failures)
}

// failures returns a node's consecutive failures of the runnable
func (c *controller) failures(node string) uint64 {
	r, _ := c.s.o.FSM.Runnable(c.spec.Name)
	for _, f := range r.GetFailures() {
		if f.GetNode() == node {
			return f.GetConsecutive()
		}
	}
	return 0
}

// backoff is restart.initial · 2^(n-1), at most restart.max, ±20 %
func (c *controller) backoff(failures uint64) time.Duration {
	d := c.spec.Restart.Initial
	for i := uint64(1); i < failures && d < c.spec.Restart.Max; i++ {
		d *= 2
	}
	d = min(d, c.spec.Restart.Max)
	jitter := 0.8 + 0.4*rand.Float64() //nolint:gosec // jitter, not security
	return time.Duration(float64(d) * jitter)
}

// handOver gives leadership to the voter with the fewest recent failures
// of this runnable, if one has fewer than this node (FR-8, OI-5)
func (c *controller) handOver(mine uint64) {
	f := c.s.o.Raft.GetConfiguration()
	if f.Error() != nil {
		return
	}
	best, bestFailures := "", mine
	for _, srv := range f.Configuration().Servers {
		id := string(srv.ID)
		if srv.Suffrage != raft.Voter || id == c.s.o.Node {
			continue
		}
		if n := c.failures(id); n < bestFailures {
			best, bestFailures = id, n
		}
	}
	if best == "" {
		c.log().Warn("every voter failed recently: backing off here", "failures", mine)
		return
	}
	c.log().Warn("repeated failures: handing leadership over", "failures", mine, "to", best)
	// asynchronous: the handover stops this controller too
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.spec.ShutdownGrace+applyTimeout)
		defer cancel()
		if err := c.s.Handover(ctx, best); err != nil {
			c.log().Warn("handover failed", "to", best, "error", err)
		}
	}()
}
