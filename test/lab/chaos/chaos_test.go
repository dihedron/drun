package main

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestParseMetrics(t *testing.T) {
	s := sample{up: map[string]bool{}, leader: map[string]bool{}, term: map[string]uint64{},
		running: map[string]map[string]bool{}, epoch: map[string]map[string]uint64{}}
	parseMetrics(&s, "n1", `# HELP drun_is_leader x
drun_is_leader 1
drun_raft_term 7
drun_epoch{runnable="ticker"} 12
drun_incarnation_state{runnable="ticker",state="running"} 1
drun_incarnation_state{runnable="ticker",state="idle"} 0
drun_incarnation_state{runnable="fenced-counter",state="running"} 1
`)
	parseMetrics(&s, "n2", "drun_is_leader 0\ndrun_incarnation_state{runnable=\"ticker\",state=\"running\"} 0\n")
	if s.leaderOf() != "n1" || s.term["n1"] != 7 || s.epoch["ticker"]["n1"] != 12 || !s.running["ticker"]["n1"] || s.running["ticker"]["n2"] {
		t.Fatalf("%+v", s)
	}
	if !s.serving([]string{"ticker", "fenced-counter"}) {
		t.Fatal("not serving")
	}
}

// two nodes running one runnable for longer than 20 s is a violation; a
// momentary overlap is not
func TestCheckSample(t *testing.T) {
	c := &campaign{lastEpoch: map[string]map[string]uint64{}, twoSince: map[string]time.Time{}, down: map[string]string{},
		stats: stats{Checks: map[string]int{}}}
	two := func(at time.Time) sample {
		return sample{at: at, running: map[string]map[string]bool{"ticker": {"a": true, "b": true}}, epoch: map[string]map[string]uint64{}}
	}
	t0 := time.Now()
	c.checkSample(two(t0))
	c.checkSample(two(t0.Add(10 * time.Second)))
	if c.failed() != nil {
		t.Fatal("a 10 s overlap failed")
	}
	c.checkSample(two(t0.Add(25 * time.Second)))
	if c.failed() == nil {
		t.Fatal("a 25 s overlap passed")
	}
	c = &campaign{lastEpoch: map[string]map[string]uint64{}, twoSince: map[string]time.Time{}, down: map[string]string{},
		stats: stats{Checks: map[string]int{}}}
	c.checkSample(sample{epoch: map[string]map[string]uint64{"ticker": {"a": 5}}, running: map[string]map[string]bool{}})
	c.checkSample(sample{epoch: map[string]map[string]uint64{"ticker": {"a": 4}}, running: map[string]map[string]bool{}})
	if c.failed() == nil {
		t.Fatal("an epoch going back passed")
	}
}

func TestChooseHeavy(t *testing.T) {
	c := &campaign{rng: rand.New(rand.NewPCG(1, 2))}
	all := faults()
	count := func(heavy bool) int {
		n := 0
		for range 20000 {
			if c.choose(all, heavy).membership {
				n++
			}
		}
		return n
	}
	if light, heavy := count(false), count(true); heavy < 2*light {
		t.Fatalf("membership faults: %d light, %d heavy", light, heavy)
	}
}
