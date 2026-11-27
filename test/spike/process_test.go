package spike

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// childEnv selects what the re-executed test binary does instead of
// running the tests
const childEnv = "SPIKE_CHILD"

func TestMain(m *testing.M) {
	switch os.Getenv(childEnv) {
	case "host":
		runHost()
	case "plugin":
		runPlugin()
	case "clock":
		runClock()
	default:
		os.Exit(m.Run())
	}
}

// child re-executes the test binary in the given mode
func child(ctx context.Context, mode string, extraEnv ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$") //nolint:gosec // the test binary itself
	cmd.Env = append(os.Environ(), append(extraEnv, childEnv+"="+mode)...)
	cmd.Stderr = os.Stderr
	return cmd
}

var handshake = plugin.HandshakeConfig{ProtocolVersion: 1, MagicCookieKey: "DRUN_SPIKE", MagicCookieValue: "spike"}

// nopPlugin serves no service of its own: go-plugin's health check is enough
type nopPlugin struct{ plugin.NetRPCUnsupportedPlugin }

func (nopPlugin) GRPCServer(*plugin.GRPCBroker, *grpc.Server) error { return nil }
func (nopPlugin) GRPCClient(context.Context, *plugin.GRPCBroker, *grpc.ClientConn) (any, error) {
	return nil, nil
}

var pluginSet = plugin.PluginSet{"nop": nopPlugin{}}

func runPlugin() {
	plugin.Serve(&plugin.ServeConfig{HandshakeConfig: handshake, Plugins: pluginSet, GRPCServer: plugin.DefaultGRPCServer})
}

// runHost starts a plugin as the node will, prints its pid and waits to be
// killed; SPIKE_PDEATHSIG=1 sets the parent-death signal on the plugin
func runHost() {
	// Pdeathsig fires when the thread that forked the child exits, not the
	// process: keep the starting goroutine on one thread that never exits
	runtime.LockOSThread()
	// go-plugin appends the host's environment after cmd.Env, so the mode
	// must change in the host's own environment, or the plugin starts a host
	if err := os.Setenv(childEnv, "plugin"); err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		os.Exit(1)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$") //nolint:gosec // the test binary itself
	if os.Getenv("SPIKE_PDEATHSIG") == "1" {
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	}
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  handshake,
		Plugins:          pluginSet,
		Cmd:              cmd,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           hclog.NewNullLogger(),
	})
	rpc, err := client.Client()
	if err == nil {
		err = rpc.Ping()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		os.Exit(1)
	}
	fmt.Println(client.ReattachConfig().Pid)
	select {}
}

// alive reports whether pid exists and is not a zombie
func alive(pid int) bool {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	// the state follows the command name, which is in parentheses
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	return len(fields) > 0 && fields[0] != "Z"
}

// EXT-5: a go-plugin child does not reliably exit when the host dies;
// Pdeathsig kills it. (The cgroup kill is checked in the lab, AC-10.)
func TestEXT5PluginOutlivesKilledHostUnlessPdeathsig(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pdeathsig bool
		survives  bool
	}{
		{"without-pdeathsig", false, true},
		{"with-pdeathsig", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := "SPIKE_PDEATHSIG=0"
			if tc.pdeathsig {
				env = "SPIKE_PDEATHSIG=1"
			}
			host := child(t.Context(), "host", env)
			out, err := host.StdoutPipe()
			if err != nil {
				t.Fatalf("pipe: %v", err)
			}
			if err := host.Start(); err != nil {
				t.Fatalf("host: %v", err)
			}
			line, err := bufio.NewReader(out).ReadString('\n')
			if err != nil {
				t.Fatalf("plugin pid: %v", err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(line))
			if err != nil {
				t.Fatalf("plugin pid %q: %v", line, err)
			}
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

			_ = host.Process.Kill()
			_ = host.Wait()
			deadline := time.Now().Add(time.Second)
			for alive(pid) && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			if got := alive(pid); got != tc.survives {
				t.Fatalf("plugin alive 1 s after the host was killed: %v, want %v", got, tc.survives)
			}
		})
	}
}

func boottime() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		panic(err)
	}
	return ts.Nano()
}

// runClock answers every line on stdin with its CLOCK_BOOTTIME
func runClock() {
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		fmt.Println(boottime())
	}
}

// EXT-6 (local part): CLOCK_BOOTTIME is the same clock for two processes on
// a host and keeps running while a process is stopped. (Across an LXD VM
// pause: in the lab.)
func TestEXT6BoottimeSharedAndRunsWhileStopped(t *testing.T) {
	clock := child(t.Context(), "clock")
	stdin, err := clock.StdinPipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	stdout, err := clock.StdoutPipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	if err := clock.Start(); err != nil {
		t.Fatalf("clock: %v", err)
	}
	defer func() { _ = stdin.Close(); _ = clock.Wait() }()
	answers := bufio.NewReader(stdout)
	ask := func() int64 {
		if _, err := io.WriteString(stdin, "\n"); err != nil {
			t.Fatalf("ask: %v", err)
		}
		line, err := answers.ReadString('\n')
		if err != nil {
			t.Fatalf("answer: %v", err)
		}
		v, err := strconv.ParseInt(strings.TrimSpace(line), 10, 64)
		if err != nil {
			t.Fatalf("answer %q: %v", line, err)
		}
		return v
	}

	before := boottime()
	theirs := ask()
	after := boottime()
	if theirs < before || theirs > after {
		t.Fatalf("child's BOOTTIME %d outside the parent's [%d, %d]", theirs, before, after)
	}

	const stopped = 2 * time.Second
	if err := clock.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP: %v", err)
	}
	time.Sleep(stopped)
	if err := clock.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT: %v", err)
	}
	if elapsed := time.Duration(ask() - theirs); elapsed < stopped {
		t.Fatalf("child's BOOTTIME advanced %s across a %s stop", elapsed, stopped)
	}
}
