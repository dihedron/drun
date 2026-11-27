// Package metrics serves a node's metrics (drun_specification.md
// §10.3) in the Prometheus text format, written by hand: a dozen
// families do not need a client library; Raft's own (FW-3) come through
// a go-metrics sink (raft.go).
package metrics

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Runnable is what a scrape reads of a runnable.
type Runnable struct {
	Name                           string
	Epoch                          uint64
	State                          string
	CheckpointSeq, CheckpointBytes uint64
	Drift                          bool
}

// Source gives the values read at each scrape.
type Source interface {
	// Leader: whether this node is leader, and the Raft term.
	Leader() (bool, uint64)
	Runnables() []Runnable
}

// buckets of drun_checkpoint_commit_seconds
var buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type histogram struct {
	counts []uint64 // per bucket, not cumulative
	sum    float64
	n      uint64
}

func newHistogram(bk []float64) *histogram { return &histogram{counts: make([]uint64, len(bk))} }

func (h *histogram) observe(bk []float64, v float64) {
	if i, _ := slices.BinarySearch(bk, v); i < len(bk) {
		h.counts[i]++
	}
	h.sum += v
	h.n++
}

// Metrics counts the supervisor's events (it is a supervisor.Events) and
// serves every family on /metrics.
type Metrics struct {
	src    Source
	states []string

	mu            sync.Mutex
	incarnations  map[[2]string]uint64  // runnable, outcome
	failover      map[[2]string]float64 // runnable, kind: the last one
	takeoverWait  map[string]float64
	commits       map[string]*histogram
	renewFailures uint64

	// Raft is Raft's own metrics, if collected (FW-3)
	Raft *RaftSink
}

// New returns the metrics of a node; states are the incarnation states
// drun_incarnation_state reports.
func New(src Source, states []string) *Metrics {
	return &Metrics{src: src, states: states, incarnations: map[[2]string]uint64{}, failover: map[[2]string]float64{},
		takeoverWait: map[string]float64{}, commits: map[string]*histogram{}}
}

func (m *Metrics) Started(runnable string, first, planned bool, failover, wait time.Duration) {
	if !first {
		return // a restart on the same leader is not a failover
	}
	kind := "unplanned"
	if planned {
		kind = "planned"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failover[[2]string{runnable, kind}] = failover.Seconds()
	m.takeoverWait[runnable] = wait.Seconds()
}

func (m *Metrics) Ended(runnable, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incarnations[[2]string{runnable, outcome}]++
}

func (m *Metrics) CheckpointCommitted(runnable string, took time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.commits[runnable]
	if h == nil {
		h = newHistogram(buckets)
		m.commits[runnable] = h
	}
	h.observe(buckets, took.Seconds())
}

func (m *Metrics) LeaseRenewFailed(string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewFailures++
}

// family writes one metric family
type family struct {
	b *strings.Builder
}

func (f family) head(name, typ, help string) {
	fmt.Fprintf(f.b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

// sample writes name{labels} value; labels are name, value pairs, in the
// order given
func (f family) sample(name string, value float64, labels ...string) {
	f.b.WriteString(name)
	if len(labels) > 0 {
		f.b.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				f.b.WriteByte(',')
			}
			fmt.Fprintf(f.b, "%s=\"%s\"", labels[i], escape(labels[i+1]))
		}
		f.b.WriteByte('}')
	}
	f.b.WriteByte(' ')
	f.b.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
	f.b.WriteByte('\n')
}

// histogram writes the series of one histogram; labels as in sample
func (f family) histogram(name string, bk []float64, h *histogram, labels ...string) {
	var cum uint64
	for i, le := range bk {
		cum += h.counts[i]
		f.sample(name+"_bucket", float64(cum), append(labels, "le", strconv.FormatFloat(le, 'g', -1, 64))...)
	}
	f.sample(name+"_bucket", float64(h.n), append(labels, "le", "+Inf")...)
	f.sample(name+"_sum", h.sum, labels...)
	f.sample(name+"_count", float64(h.n), labels...)
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(m.text()))
}

func (m *Metrics) text() string {
	var b strings.Builder
	f := family{&b}
	leader, term := m.src.Leader()
	runnables := m.src.Runnables()

	f.head("drun_is_leader", "gauge", "1 if this node is the leader")
	f.sample("drun_is_leader", b2f(leader))
	f.head("drun_raft_term", "gauge", "the Raft term")
	f.sample("drun_raft_term", float64(term))
	f.head("drun_epoch", "gauge", "the epoch of the runnable's current incarnation")
	for _, r := range runnables {
		f.sample("drun_epoch", float64(r.Epoch), "runnable", r.Name)
	}
	f.head("drun_incarnation_state", "gauge", "1 for the state the leader's supervisor shows the runnable in")
	for _, r := range runnables {
		for _, st := range m.states {
			f.sample("drun_incarnation_state", b2f(st == r.State), "runnable", r.Name, "state", st)
		}
	}
	f.head("drun_checkpoint_bytes", "gauge", "the size of the runnable's latest checkpoint")
	for _, r := range runnables {
		f.sample("drun_checkpoint_bytes", float64(r.CheckpointBytes), "runnable", r.Name)
	}
	f.head("drun_checkpoint_seq", "gauge", "the sequence number of the runnable's latest checkpoint")
	for _, r := range runnables {
		f.sample("drun_checkpoint_seq", float64(r.CheckpointSeq), "runnable", r.Name)
	}
	f.head("drun_config_drift", "gauge", "1 if this node's digests of the runnable differ from its current incarnation's")
	for _, r := range runnables {
		f.sample("drun_config_drift", b2f(r.Drift), "runnable", r.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	f.head("drun_incarnations_total", "counter", "incarnations run by this node, by how they ended")
	for _, k := range sortedKeys(m.incarnations) {
		f.sample("drun_incarnations_total", float64(m.incarnations[k]), "outcome", k[1], "runnable", k[0])
	}
	f.head("drun_failover_seconds", "gauge", "from becoming leader to the runnable's first incarnation, the last time")
	for _, k := range sortedKeys(m.failover) {
		f.sample("drun_failover_seconds", m.failover[k], "kind", k[1], "runnable", k[0])
	}
	f.head("drun_takeover_wait_seconds", "gauge", "the takeover delay waited before the runnable's first incarnation, the last time")
	for _, k := range sortedKeys(m.takeoverWait) {
		f.sample("drun_takeover_wait_seconds", m.takeoverWait[k], "runnable", k)
	}
	f.head("drun_checkpoint_commit_seconds", "histogram", "time to commit a checkpoint")
	for _, name := range sortedKeys(m.commits) {
		f.histogram("drun_checkpoint_commit_seconds", buckets, m.commits[name], "runnable", name)
	}
	f.head("drun_lease_renew_failures_total", "counter", "lease renewals that failed: leadership not confirmed, or the renewal not delivered")
	f.sample("drun_lease_renew_failures_total", float64(m.renewFailures))
	if m.Raft != nil {
		b.WriteString(m.Raft.text())
	}
	return b.String()
}

func sortedKeys[K interface{ ~string | ~[2]string }, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b K) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	return keys
}
