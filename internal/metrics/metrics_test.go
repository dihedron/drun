package metrics

import (
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

type fakeSource struct{}

func (fakeSource) Leader() (bool, uint64) { return true, 7 }
func (fakeSource) Runnables() []Runnable {
	return []Runnable{{Name: "svc", Epoch: 12, State: "running", CheckpointSeq: 3, CheckpointBytes: 40, Drift: true}}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("content type %q", ct)
	}
	return rec.Body.String()
}

func TestExposition(t *testing.T) {
	m := New(fakeSource{}, []string{"idle", "running"})
	m.Started("svc", true, false, 3*time.Second, 2*time.Second)
	m.Started("svc", false, false, time.Hour, 0) // a restart: not a failover
	m.Ended("svc", "failed")
	m.Ended("svc", "failed")
	m.CheckpointCommitted("svc", 3*time.Millisecond)
	m.CheckpointCommitted("svc", 2*time.Second)
	m.LeaseRenewFailed("svc")
	out := scrape(t, m)

	for _, want := range []string{
		"# TYPE resurgo_is_leader gauge\nresurgo_is_leader 1\n",
		"resurgo_raft_term 7\n",
		`resurgo_epoch{runnable="svc"} 12` + "\n",
		`resurgo_incarnation_state{runnable="svc",state="running"} 1` + "\n",
		`resurgo_incarnation_state{runnable="svc",state="idle"} 0` + "\n",
		"# TYPE resurgo_incarnations_total counter\n",
		`resurgo_incarnations_total{outcome="failed",runnable="svc"} 2` + "\n",
		`resurgo_failover_seconds{kind="unplanned",runnable="svc"} 3` + "\n",
		`resurgo_takeover_wait_seconds{runnable="svc"} 2` + "\n",
		`resurgo_checkpoint_bytes{runnable="svc"} 40` + "\n",
		`resurgo_checkpoint_seq{runnable="svc"} 3` + "\n",
		"# TYPE resurgo_checkpoint_commit_seconds histogram\n",
		`resurgo_checkpoint_commit_seconds_bucket{runnable="svc",le="0.005"} 1` + "\n",
		`resurgo_checkpoint_commit_seconds_bucket{runnable="svc",le="+Inf"} 2` + "\n",
		`resurgo_checkpoint_commit_seconds_count{runnable="svc"} 2` + "\n",
		`resurgo_checkpoint_commit_seconds_sum{runnable="svc"} 2.003` + "\n",
		"resurgo_lease_renew_failures_total 1\n",
		`resurgo_config_drift{runnable="svc"} 1` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	// every sample line is name{labels} value, every family has HELP and TYPE
	sample := regexp.MustCompile(`^[a-z_]+(\{([a-z_]+="[^"]*",?)*\})? [-+0-9.eInf]+$`)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if !strings.HasPrefix(line, "# ") && !sample.MatchString(line) {
			t.Errorf("malformed line %q", line)
		}
	}
	if strings.Count(out, "# HELP") != strings.Count(out, "# TYPE") {
		t.Error("a family without HELP or TYPE")
	}
}

func TestLabelEscaping(t *testing.T) {
	if got := escape("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Fatalf("escape: %s", got)
	}
}
