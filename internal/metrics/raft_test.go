package metrics

import (
	"regexp"
	"strings"
	"testing"
	"time"

	gometrics "github.com/hashicorp/go-metrics"
)

func TestSnake(t *testing.T) {
	for in, want := range map[string]string{
		"raft.rpc.appendEntries":       "raft_rpc_append_entries",
		"raft.net.getRPCType":          "raft_net_get_rpc_type",
		"raft.boltdb.numFreePages":     "raft_boltdb_num_free_pages",
		"raft.wal.log_entries_written": "raft_wal_log_entries_written",
		"raft.x-y.z9":                  "raft_x_y_z9",
	} {
		if got := snake(strings.Split(in, ".")); got != want {
			t.Errorf("snake(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestRaftSink(t *testing.T) {
	s := NewRaftSink()
	s.IncrCounter([]string{"raft", "apply"}, 2)
	s.IncrCounter([]string{"raft", "apply"}, 3)
	s.SetGauge([]string{"raft", "peers"}, 3)
	s.SetGauge([]string{"raft", "peers"}, 5)
	s.SetPrecisionGaugeWithLabels([]string{"raft", "wal", "last_segment_age"}, 1.5, nil)
	s.AddSample([]string{"raft", "commitTime"}, 0.3)
	s.AddSample([]string{"raft", "commitTime"}, 20000)
	s.AddSampleWithLabels([]string{"raft", "replication", "heartbeat"}, 4, []gometrics.Label{{Name: "peer_id", Value: `n"2`}})
	s.IncrCounterWithLabels([]string{"raft", "peers"}, 1, nil) // a gauge already: dropped
	out := s.text()

	for _, want := range []string{
		"# TYPE drun_raft_apply_total counter\ndrun_raft_apply_total 5\n",
		"# TYPE drun_raft_peers gauge\ndrun_raft_peers 5\n",
		"drun_raft_wal_last_segment_age 1.5\n",
		"# TYPE drun_raft_commit_time histogram\n",
		`drun_raft_commit_time_bucket{le="0.25"} 0` + "\n",
		`drun_raft_commit_time_bucket{le="0.5"} 1` + "\n",
		`drun_raft_commit_time_bucket{le="10000"} 1` + "\n",
		`drun_raft_commit_time_bucket{le="+Inf"} 2` + "\n",
		"drun_raft_commit_time_count 2\n",
		`drun_raft_replication_heartbeat_bucket{peer_id="n\"2",le="5"} 1` + "\n",
		`drun_raft_replication_heartbeat_sum{peer_id="n\"2"} 4` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "drun_raft_peers_total") {
		t.Errorf("a counter on a gauge's name was kept:\n%s", out)
	}
}

// TestInstallRaft checks the global go-metrics instance feeds the sink,
// without the per-peer keys, and that /metrics renders it.
func TestInstallRaft(t *testing.T) {
	r, err := InstallRaft()
	if err != nil {
		t.Fatal(err)
	}
	m := New(fakeSource{}, []string{"running"})
	m.Raft = r
	gometrics.IncrCounter([]string{"raft", "state", "leader"}, 1)
	gometrics.MeasureSince([]string{"raft", "replication", "heartbeat", "n2"}, time.Now())
	gometrics.MeasureSinceWithLabels([]string{"raft", "replication", "heartbeat"}, time.Now(), []gometrics.Label{{Name: "peer_id", Value: "n2"}})
	out := scrape(t, m)
	for _, want := range []string{
		"drun_raft_state_leader_total 1\n",
		`drun_raft_replication_heartbeat_count{peer_id="n2"} 1` + "\n",
		"drun_is_leader 1\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	for _, bad := range []string{"heartbeat_n2", "host", "runtime"} {
		if strings.Contains(out, bad) {
			t.Errorf("unexpected %q in\n%s", bad, out)
		}
	}
}

func FuzzSnake(f *testing.F) {
	f.Add("raft.net.getRPCType")
	f.Add("raft.replication.heartbeat.nodé-2")
	valid := regexp.MustCompile(`^[a-z0-9_]*$`)
	f.Fuzz(func(t *testing.T, key string) {
		if got := snake(strings.Split(key, ".")); !valid.MatchString(got) {
			t.Fatalf("snake(%q) = %q", key, got)
		}
	})
}
