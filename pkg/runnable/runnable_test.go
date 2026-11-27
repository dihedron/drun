package runnable_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dihedron/drun/pkg/runnable"
	"github.com/dihedron/drun/pkg/runnable/conformance"
)

// modeEnv selects what the re-executed test binary is
const modeEnv = "DRUN_RUNNABLE_TEST"

func TestMain(m *testing.M) {
	switch os.Getenv(modeEnv) {
	case "plugin":
		runnable.Serve(sample{})
	case "host":
		runHost()
	default:
		os.Exit(m.Run())
	}
}

// sample is a runnable whose behaviour its configuration chooses
type sample struct{}

type sampleConfig struct {
	Mode string `json:"mode"` // service (default), task, crash, panic, stubborn
}

func (sample) Describe() runnable.Description {
	return runnable.Description{Name: "sample", Version: "1.0", Kind: runnable.Service}
}

func (sample) Validate(_ context.Context, config []byte) error {
	var c sampleConfig
	if err := json.Unmarshal(config, &c); err != nil {
		return fmt.Errorf("sample: %w", err)
	}
	return nil
}

func (sample) Run(ctx context.Context, inc runnable.Incarnation, host runnable.Host) error {
	var c sampleConfig
	_ = json.Unmarshal(inc.Config, &c)
	switch c.Mode {
	case "task":
		return nil
	case "crash":
		os.Exit(3)
	case "panic":
		panic("sample panics")
	case "stubborn":
		select {} // ignores its context: only a kill ends it
	}
	data := []byte("first")
	if inc.Checkpoint != nil {
		data = append(inc.Checkpoint.Data, '!')
	}
	if _, err := host.Checkpoint(ctx, data); err != nil {
		return err
	}
	if _, err := host.Store().Exec(ctx, []runnable.Statement{{SQL: "INSERT INTO t VALUES (?)", Args: []any{int64(inc.Epoch)}, Expect: runnable.Exactly(1)}}); err != nil {
		return err
	}
	rows, err := host.Store().Query(ctx, "SELECT count(*) FROM t")
	if err != nil {
		return err
	}
	var n int64
	if rows.Next() && rows.Scan(&n) != nil {
		return errors.New("sample: bad count")
	}
	if err := host.Status(ctx, true, fmt.Sprintf("rows=%d", n)); err != nil {
		return err
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func selfDigest(t testing.TB) string {
	t.Helper()
	data, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func loadSelf(t *testing.T) runnable.Instance {
	t.Helper()
	t.Setenv(modeEnv, "plugin")
	inst, err := runnable.Load(t.Context(), runnable.Plugin{Path: os.Args[0], SHA256: selfDigest(t), Logger: discard})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	t.Cleanup(func() { _ = inst.Kill() })
	return inst
}

func countingHost() *conformance.Host {
	h := conformance.NewHost()
	h.QueryFunc = func(string, []any) (runnable.Rows, error) {
		return runnable.NewRows([]string{"count(*)"}, [][]any{{int64(41)}}), nil
	}
	return h
}

func config(mode string) []byte { return fmt.Appendf(nil, `{"mode":%q}`, mode) }

func far() int64 { return runnable.Boottime() + int64(time.Hour) }

// both loaders: an incarnation gets its checkpoint, talks to the node,
// and stops when asked
func TestIncarnationRoundTrip(t *testing.T) {
	for name, load := range map[string]func(*testing.T) runnable.Instance{
		"plugin":     loadSelf,
		"in-process": func(*testing.T) runnable.Instance { return runnable.InProcess(sample{}) },
	} {
		t.Run(name, func(t *testing.T) {
			inst := load(t)
			if d, err := inst.Describe(t.Context()); err != nil || d.Name != "sample" || d.Version != "1.0" {
				t.Fatalf("describe: %+v %v", d, err)
			}
			if err := inst.Validate(t.Context(), []byte("not json")); err == nil {
				t.Fatal("an invalid configuration was accepted")
			}
			host := countingHost()
			inc := runnable.Incarnation{Runnable: "sample", Epoch: 7, Config: config("service"),
				Checkpoint: &runnable.Checkpoint{Seq: 3, Epoch: 6, Data: []byte("before"), CommittedAt: time.UnixMilli(1000)}}
			s, err := inst.Start(t.Context(), inc, far(), host)
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				cps, statuses, batches := host.Snapshot()
				if len(statuses) == 1 {
					if string(cps[0]) != "before!" || statuses[0] != "healthy: rows=41" || batches != 1 {
						t.Fatalf("checkpoints %q statuses %q batches %d", cps, statuses, batches)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the incarnation did not report: %q %q", cps, statuses)
				}
				time.Sleep(10 * time.Millisecond)
			}
			s.Stop("test")
			select {
			case <-s.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("did not stop")
			}
			if e := s.Exit(); e.Outcome != runnable.Failed || !strings.Contains(e.Err.Error(), "stopped") {
				t.Fatalf("exit %v, want a service's failure after a stop", e)
			}
		})
	}
}

func TestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		want    runnable.Outcome
		plugins bool // only the plugin loader can see this one
	}{
		{"task", runnable.Completed, false},
		{"panic", runnable.Failed, false},
		{"crash", runnable.Crashed, true},
	} {
		loaders := map[string]func(*testing.T) runnable.Instance{"plugin": loadSelf}
		if !tc.plugins {
			loaders["in-process"] = func(*testing.T) runnable.Instance { return runnable.InProcess(sample{}) }
		}
		for name, load := range loaders {
			t.Run(tc.mode+"/"+name, func(t *testing.T) {
				s, err := load(t).Start(t.Context(), runnable.Incarnation{Runnable: "sample", Epoch: 1, Config: config(tc.mode)}, far(), countingHost())
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-s.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("did not end")
				}
				if got := s.Exit(); got.Outcome != tc.want {
					t.Fatalf("exit %v, want %v", got, tc.want)
				}
			})
		}
	}

	t.Run("fenced", func(t *testing.T) {
		host := countingHost()
		host.SetFenced(true)
		s, err := loadSelf(t).Start(t.Context(), runnable.Incarnation{Runnable: "sample", Epoch: 1, Config: config("service")}, far(), host)
		if err != nil {
			t.Fatal(err)
		}
		<-s.Done()
		if got := s.Exit(); got.Outcome != runnable.Fenced {
			t.Fatalf("exit %v, want fenced", got)
		}
	})

	t.Run("killed", func(t *testing.T) {
		inst := loadSelf(t)
		s, err := inst.Start(t.Context(), runnable.Incarnation{Runnable: "sample", Epoch: 1, Config: config("stubborn")}, far(), countingHost())
		if err != nil {
			t.Fatal(err)
		}
		s.Stop("test")
		select {
		case <-s.Done():
			t.Fatal("a stubborn runnable stopped by itself")
		case <-time.After(300 * time.Millisecond):
		}
		if err := inst.Kill(); err != nil {
			t.Fatalf("kill: %v", err)
		}
		<-s.Done()
		if got := s.Exit(); got.Outcome != runnable.Crashed {
			t.Fatalf("exit %v, want crashed", got)
		}
	})
}

// the lease ends an incarnation without heartbeats; heartbeats keep it
func TestLease(t *testing.T) {
	for name, load := range map[string]func(*testing.T) runnable.Instance{
		"plugin":     loadSelf,
		"in-process": func(*testing.T) runnable.Instance { return runnable.InProcess(sample{}) },
	} {
		t.Run(name, func(t *testing.T) {
			const lease = 400 * time.Millisecond
			s, err := load(t).Start(t.Context(), runnable.Incarnation{Runnable: "sample", Epoch: 1, Config: config("service")}, runnable.Boottime()+int64(lease), countingHost())
			if err != nil {
				t.Fatal(err)
			}
			// renewed for a while: still running past the first lease
			for range 6 {
				time.Sleep(lease / 4)
				if err := s.Renew(runnable.Boottime() + int64(lease)); err != nil {
					t.Fatalf("renew: %v", err)
				}
			}
			select {
			case <-s.Done():
				t.Fatalf("ended while renewed: %v", s.Exit())
			default:
			}
			// a renewal into the past does not shorten it
			_ = s.Renew(runnable.Boottime() - int64(time.Second))
			stopped := time.Now()
			select {
			case <-s.Done():
			case <-time.After(lease + 2*time.Second):
				t.Fatal("the lease did not end the incarnation")
			}
			if waited := time.Since(stopped); waited > lease+500*time.Millisecond {
				t.Fatalf("ended %s after the last renewal, lease %s", waited, lease)
			}
			if e := s.Exit(); e.Err == nil || !strings.Contains(e.Err.Error(), "lease expired") {
				t.Fatalf("exit %v, want lease expired", e)
			}
		})
	}
}

func TestLoadRefusesWrongDigest(t *testing.T) {
	t.Setenv(modeEnv, "plugin")
	wrong := strings.Repeat("00", sha256.Size)
	if _, err := runnable.Load(t.Context(), runnable.Plugin{Path: os.Args[0], SHA256: wrong, Logger: discard}); err == nil {
		t.Fatal("a binary with another digest was started")
	}
	if _, err := runnable.Load(t.Context(), runnable.Plugin{Path: os.Args[0], SHA256: "xyz", Logger: discard}); err == nil {
		t.Fatal("a malformed digest was accepted")
	}
	// a test binary is not root's: SEC-4 refuses it
	if _, err := runnable.Load(t.Context(), runnable.Plugin{Path: os.Args[0], SHA256: selfDigest(t), Logger: discard, CheckOwnership: true}); err == nil || !strings.Contains(err.Error(), "root") {
		t.Fatalf("a binary not owned by root: %v", err)
	}
}

// runHost loads the plugin as the node does, prints its pid and waits to
// be killed
func runHost() {
	data, _ := os.ReadFile(os.Args[0])
	sum := sha256.Sum256(data)
	_ = os.Setenv(modeEnv, "plugin")
	inst, err := runnable.Load(context.Background(), runnable.Plugin{Path: os.Args[0], SHA256: hex.EncodeToString(sum[:]), Logger: discard})
	if err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		os.Exit(1)
	}
	s, err := inst.Start(context.Background(), runnable.Incarnation{Runnable: "sample", Epoch: 1, Config: config("stubborn")}, far(), conformance.NewHost())
	if err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		os.Exit(1)
	}
	_ = s
	fmt.Println(pidOf())
	select {}
}

// pidOf finds the plugin: the host's only child
func pidOf() int {
	children, _ := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", os.Getpid(), os.Getpid()))
	for _, f := range strings.Fields(string(children)) {
		if pid, err := strconv.Atoi(f); err == nil {
			return pid
		}
	}
	// the child may have been forked by another thread of the host
	entries, _ := os.ReadDir(fmt.Sprintf("/proc/%d/task", os.Getpid()))
	for _, e := range entries {
		children, _ := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", os.Getpid(), e.Name()))
		for _, f := range strings.Fields(string(children)) {
			if pid, err := strconv.Atoi(f); err == nil {
				return pid
			}
		}
	}
	return 0
}

func alive(pid int) bool {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	return len(fields) > 0 && fields[0] != "Z"
}

// SEC-10: the plugin dies with the node, through the loader
func TestPluginDiesWithTheNode(t *testing.T) {
	host := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$") //nolint:gosec // the test binary
	host.Env = append(os.Environ(), modeEnv+"=host")
	host.Stderr = os.Stderr
	out, err := host.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("plugin pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || pid == 0 || !alive(pid) {
		t.Fatalf("plugin pid %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	_ = host.Process.Kill()
	_ = host.Wait()
	deadline := time.Now().Add(time.Second)
	for alive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if alive(pid) {
		t.Fatal("the plugin outlived the node")
	}
}

func TestConformance(t *testing.T) {
	opts := conformance.Options{Config: config("service"), Grace: 2 * time.Second}
	t.Run("in-process", func(t *testing.T) {
		conformance.Check(t, func(*testing.T) runnable.Instance { return runnable.InProcess(sample{}) }, opts)
	})
	t.Run("plugin", func(t *testing.T) {
		conformance.Check(t, loadSelf, opts)
	})
}
