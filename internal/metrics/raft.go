package metrics

import (
	"fmt"
	"strings"
	"sync"
	"unicode"

	gometrics "github.com/hashicorp/go-metrics"
)

// raftBuckets of every sample: go-metrics timers are in milliseconds
var raftBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// raftBlocked are the keys hashicorp/raft also reports with the peer ID in
// the name; each has a twin with a peer_id label (drun_specification.md §10.3)
var raftBlocked = []string{
	"raft.replication.appendEntries.rpc.",
	"raft.replication.appendEntries.logs.",
	"raft.replication.heartbeat.",
	"raft.replication.installSnapshot.",
}

// RaftSink is a go-metrics sink that keeps what hashicorp/raft and its log
// stores report, cumulative from start, for /metrics (FW-3).
type RaftSink struct {
	mu       sync.Mutex
	families map[string]*raftFamily // by name, without type suffix
}

type raftFamily struct {
	typ    string // counter, gauge, histogram
	key    string // the go-metrics key, for HELP
	series map[string]*raftSeries
}

type raftSeries struct {
	labels []string // name, value pairs
	value  float64
	h      *histogram
}

// NewRaftSink returns an empty sink.
func NewRaftSink() *RaftSink {
	return &RaftSink{families: map[string]*raftFamily{}}
}

// InstallRaft makes a new sink the process-wide go-metrics instance;
// call it before the Raft stores are opened (raft-wal binds it at open).
func InstallRaft() (*RaftSink, error) {
	s := NewRaftSink()
	conf := gometrics.DefaultConfig("")
	conf.HostName, conf.EnableHostname, conf.EnableRuntimeMetrics = "", false, false
	conf.BlockedPrefixes = raftBlocked
	if _, err := gometrics.NewGlobal(conf, s); err != nil {
		return nil, fmt.Errorf("raft metrics: %w", err)
	}
	return s, nil
}

func (s *RaftSink) observe(typ string, key []string, labels []gometrics.Label, f func(*raftSeries)) {
	name := "drun_" + snake(key)
	pairs := make([]string, 0, 2*len(labels))
	for _, l := range labels {
		pairs = append(pairs, snake([]string{l.Name}), l.Value)
	}
	id := strings.Join(pairs, "\x00")
	s.mu.Lock()
	defer s.mu.Unlock()
	fam := s.families[name]
	if fam == nil {
		fam = &raftFamily{typ: typ, key: strings.Join(key, "."), series: map[string]*raftSeries{}}
		s.families[name] = fam
	}
	if fam.typ != typ {
		return // a clash: the first type wins
	}
	ser := fam.series[id]
	if ser == nil {
		ser = &raftSeries{labels: pairs}
		fam.series[id] = ser
	}
	f(ser)
}

func (s *RaftSink) SetGauge(key []string, val float32) { s.SetGaugeWithLabels(key, val, nil) }
func (s *RaftSink) SetGaugeWithLabels(key []string, val float32, labels []gometrics.Label) {
	s.SetPrecisionGaugeWithLabels(key, float64(val), labels)
}
func (s *RaftSink) SetPrecisionGauge(key []string, val float64) {
	s.SetPrecisionGaugeWithLabels(key, val, nil)
}
func (s *RaftSink) SetPrecisionGaugeWithLabels(key []string, val float64, labels []gometrics.Label) {
	s.observe("gauge", key, labels, func(r *raftSeries) { r.value = val })
}
func (s *RaftSink) EmitKey([]string, float32)             {} // not a Prometheus type; unused by raft
func (s *RaftSink) IncrCounter(key []string, val float32) { s.IncrCounterWithLabels(key, val, nil) }
func (s *RaftSink) IncrCounterWithLabels(key []string, val float32, labels []gometrics.Label) {
	s.observe("counter", key, labels, func(r *raftSeries) { r.value += float64(val) })
}
func (s *RaftSink) AddSample(key []string, val float32) { s.AddSampleWithLabels(key, val, nil) }
func (s *RaftSink) AddSampleWithLabels(key []string, val float32, labels []gometrics.Label) {
	s.observe("histogram", key, labels, func(r *raftSeries) {
		if r.h == nil {
			r.h = newHistogram(raftBuckets)
		}
		r.h.observe(raftBuckets, float64(val))
	})
}
func (s *RaftSink) Shutdown() {}

// text renders every family, sorted by name, then by labels
func (s *RaftSink) text() string {
	var b strings.Builder
	f := family{&b}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range sortedKeys(s.families) {
		fam := s.families[name]
		full := name
		if fam.typ == "counter" {
			full += "_total"
		}
		f.head(full, fam.typ, "hashicorp/go-metrics "+fam.key)
		for _, id := range sortedKeys(fam.series) {
			r := fam.series[id]
			if r.h != nil {
				f.histogram(name, raftBuckets, r.h, r.labels...)
			} else {
				f.sample(full, r.value, r.labels...)
			}
		}
	}
	return b.String()
}

// snake joins a go-metrics key with _, in snake case, keeping [a-z0-9_]
func snake(key []string) string {
	var b strings.Builder
	for i, seg := range key {
		if i > 0 {
			b.WriteByte('_')
		}
		rs := []rune(seg)
		for j, r := range rs {
			switch {
			case r >= 'A' && r <= 'Z':
				if j > 0 && (unicode.IsLower(rs[j-1]) || unicode.IsDigit(rs[j-1]) ||
					(unicode.IsUpper(rs[j-1]) && j+1 < len(rs) && unicode.IsLower(rs[j+1]))) {
					b.WriteByte('_')
				}
				b.WriteRune(unicode.ToLower(r))
			case r < 128 && (unicode.IsLower(r) || unicode.IsDigit(r) || r == '_'):
				b.WriteRune(r)
			default:
				b.WriteByte('_')
			}
		}
	}
	return b.String()
}

var _ gometrics.PrecisionGaugeMetricSink = (*RaftSink)(nil)
