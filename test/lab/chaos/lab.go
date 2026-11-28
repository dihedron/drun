package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// lab drives test/lab/lab.sh
type lab struct {
	script string
	ips    map[string]string // node -> address
}

// run runs lab.sh ARGS within d and returns its combined output
func (l *lab) run(ctx context.Context, d time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	cmd := exec.CommandContext(ctx, l.script, args...) //nolint:gosec // the lab's own script
	cmd.Env = append(cmd.Environ(), "LAB_ASSUME_YES=yes")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("lab.sh %s: %w: %s", strings.Join(args, " "), err, lastLine(out))
	}
	return string(out), nil
}

func lastLine(b []byte) string {
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	return lines[len(lines)-1]
}

// drunJSON runs the drun CLI with --json on node and decodes its reply
func (l *lab) drunJSON(ctx context.Context, node string, v any, args ...string) error {
	out, err := l.run(ctx, 2*time.Minute, append([]string{"drun", node, "--json"}, args...)...)
	if err != nil {
		return err
	}
	if i := strings.IndexByte(out, '{'); i >= 0 {
		out = out[i:]
	}
	if err := json.Unmarshal([]byte(out), v); err != nil {
		return fmt.Errorf("drun %s on %s: %w: %.200s", strings.Join(args, " "), node, err, out)
	}
	return nil
}

// sample is one scrape of every node's metrics
type sample struct {
	at      time.Time
	up      map[string]bool              // answered
	leader  map[string]bool              // drun_is_leader 1
	term    map[string]uint64            // drun_raft_term
	running map[string]map[string]bool   // runnable -> node -> running
	epoch   map[string]map[string]uint64 // runnable -> node -> epoch
}

var metricLine = regexp.MustCompile(`^(drun_[a-z_]+)(\{([^}]*)\})? ([-+0-9.eE]+|NaN|\+Inf)$`)

// parseMetrics reads one node's /metrics into s
func parseMetrics(s *sample, node, text string) {
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		m := metricLine.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		labels := map[string]string{}
		for _, kv := range strings.Split(m[3], ",") {
			if k, v, ok := strings.Cut(kv, "="); ok {
				labels[k] = strings.Trim(v, `"`)
			}
		}
		val, _ := strconv.ParseFloat(m[4], 64)
		switch m[1] {
		case "drun_is_leader":
			s.leader[node] = val == 1
		case "drun_raft_term":
			s.term[node] = uint64(val)
		case "drun_incarnation_state":
			if labels["state"] == "running" && val == 1 {
				r := labels["runnable"]
				if s.running[r] == nil {
					s.running[r] = map[string]bool{}
				}
				s.running[r][node] = true
			}
		case "drun_epoch":
			r := labels["runnable"]
			if s.epoch[r] == nil {
				s.epoch[r] = map[string]uint64{}
			}
			s.epoch[r][node] = uint64(val)
		}
	}
}

// scrape reads every node's metrics in one command in the lab VM
func (l *lab) scrape(ctx context.Context, nodes []string) sample {
	s := sample{at: time.Now(), up: map[string]bool{}, leader: map[string]bool{}, term: map[string]uint64{},
		running: map[string]map[string]bool{}, epoch: map[string]map[string]uint64{}}
	var script strings.Builder
	for _, n := range nodes {
		fmt.Fprintf(&script, "echo '### %s'; curl -s -m 2 http://%s:9350/metrics || true; ", n, l.ips[n])
	}
	out, err := l.run(ctx, 30*time.Second, "ssh", "lxd", "bash", "-c", script.String())
	if err != nil {
		return s
	}
	parts := strings.Split(out, "### ")
	for _, p := range parts[1:] {
		node, body, _ := strings.Cut(p, "\n")
		node = strings.TrimSpace(node)
		if strings.Contains(body, "drun_is_leader") {
			s.up[node] = true
			parseMetrics(&s, node, body)
		}
	}
	return s
}

// leaderOf is the one node that calls itself leader, ignoring the nodes
// excluded (a node cut off or faulted may still believe it leads)
func (s sample) leaderOf(exclude ...string) string {
	var leaders []string
	for n, l := range s.leader {
		if l && !slices.Contains(exclude, n) {
			leaders = append(leaders, n)
		}
	}
	if len(leaders) != 1 {
		return ""
	}
	return leaders[0]
}

// serving: a leader other than the excluded nodes, and every runnable
// running on it
func (s sample) serving(runnables []string, exclude ...string) bool {
	l := s.leaderOf(exclude...)
	if l == "" {
		return false
	}
	for _, r := range runnables {
		if !s.running[r][l] {
			return false
		}
	}
	return true
}
