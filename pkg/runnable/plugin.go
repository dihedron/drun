package runnable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"github.com/dihedron/resurgo/pkg/runnable/runnablepb"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

// Plugin is how the node loads a runnable binary.
type Plugin struct {
	// Path is the binary's absolute path.
	Path string
	// SHA256 is the binary's pinned digest, hex (SEC-4): no other binary
	// is executed.
	SHA256 string
	// Logger receives the plugin's stderr, line by line.
	Logger *slog.Logger
	// CheckOwnership refuses a binary, or a directory of it, that is not
	// root-owned or that group or others can write (SEC-4). Only tests turn
	// it off.
	CheckOwnership bool
}

// Load starts a fresh plugin process for one incarnation (ADR-4). The
// process dies with the node (Pdeathsig, SEC-10).
func Load(ctx context.Context, p Plugin) (Instance, error) {
	if p.CheckOwnership {
		if err := checkOwnership(p.Path); err != nil {
			return nil, err
		}
	}
	sum, err := hex.DecodeString(p.SHA256)
	if err != nil || len(sum) != sha256.Size {
		return nil, fmt.Errorf("runnable %s: sha256 %q is not a SHA-256 in hex", p.Path, p.SHA256)
	}
	cmd := exec.Command(p.Path) //nolint:gosec // the configured binary, pinned by SecureConfig
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	stderr := &lineLogger{logger: p.Logger}
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  Handshake,
		Plugins:          plugin.PluginSet{pluginName: &grpcPlugin{}},
		Cmd:              cmd,
		SecureConfig:     &plugin.SecureConfig{Checksum: sum, Hash: sha256.New()},
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		AutoMTLS:         true,
		Stderr:           stderr,
		Logger:           hclog.NewNullLogger(),
	})

	inst := &pluginInstance{client: client, stderr: stderr, release: make(chan struct{})}
	// Pdeathsig fires when the thread that started the child ends, not the
	// process (EXT-5): the child is started from a goroutine that stays on
	// its thread until the instance is killed
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		rpc, err := client.Client()
		if err == nil {
			var raw any
			if raw, err = rpc.Dispense(pluginName); err == nil {
				inst.rpc, _ = raw.(runnablepb.RunnableClient)
			}
		}
		started <- err
		<-inst.release // the thread ends with the goroutine: never unlocked
	}()
	select {
	case err := <-started:
		if err != nil {
			_ = inst.Kill()
			return nil, fmt.Errorf("runnable %s: %w", p.Path, err)
		}
	case <-ctx.Done():
		_ = inst.Kill()
		return nil, ctx.Err()
	}
	return inst, nil
}

// checkOwnership: the binary and its directory are root's, and only
// root can change them
func checkOwnership(path string) error {
	for _, p := range []string{path, filepath.Dir(path)} {
		fi, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("runnable %s: %w", path, err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != 0 {
			return fmt.Errorf("runnable %s: %s is not owned by root", path, p)
		}
		if fi.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("runnable %s: %s is writable by group or others", path, p)
		}
	}
	return nil
}

// lineLogger logs the plugin's stderr line by line, with the epoch once
// the incarnation has started
type lineLogger struct {
	mu     sync.Mutex
	logger *slog.Logger
	buf    []byte
}

func (l *lineLogger) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	for {
		i := -1
		for j, b := range l.buf {
			if b == '\n' {
				i = j
				break
			}
		}
		if i < 0 {
			return len(p), nil
		}
		if l.logger != nil {
			l.logger.Info("runnable stderr", "line", string(l.buf[:i]))
		}
		l.buf = l.buf[i+1:]
	}
}

func (l *lineLogger) with(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logger != nil {
		l.logger = l.logger.With(args...)
	}
}

type pluginInstance struct {
	client  *plugin.Client
	rpc     runnablepb.RunnableClient
	stderr  *lineLogger
	release chan struct{}

	once    sync.Once
	mu      sync.Mutex
	session *pluginSession
}

func (p *pluginInstance) Describe(ctx context.Context) (Description, error) {
	d, err := p.rpc.Describe(ctx, &runnablepb.DescribeRequest{})
	if err != nil {
		return Description{}, fmt.Errorf("runnable: describe: %w", err)
	}
	return fromPBDescription(d), nil
}

func (p *pluginInstance) Validate(ctx context.Context, config []byte) error {
	r, err := p.rpc.Validate(ctx, &runnablepb.ValidateRequest{Config: config})
	if err != nil {
		return fmt.Errorf("runnable: validate: %w", err)
	}
	if r.GetError() != "" {
		return errors.New(r.GetError())
	}
	return nil
}

func (p *pluginInstance) Start(_ context.Context, inc Incarnation, validUntil int64, host Host) (Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session != nil {
		return nil, errors.New("runnable: an instance runs one incarnation")
	}
	p.stderr.with("runnable", inc.Runnable, "epoch", inc.Epoch)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := p.rpc.Run(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("runnable: run: %w", err)
	}
	s := &pluginSession{stream: stream, host: host, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	if err := s.send(&runnablepb.HostMessage{Msg: &runnablepb.HostMessage_Start{Start: &runnablepb.Start{
		Incarnation: toPBIncarnation(inc), ValidUntilBoottimeNs: validUntil,
	}}}); err != nil {
		cancel()
		return nil, fmt.Errorf("runnable: start: %w", err)
	}
	p.session = s
	go s.serve()
	return s, nil
}

// Kill kills the process at once.
func (p *pluginInstance) Kill() error {
	var err error
	p.once.Do(func() {
		if rc := p.client.ReattachConfig(); rc != nil && rc.Pid > 0 {
			err = syscall.Kill(rc.Pid, syscall.SIGKILL)
			if errors.Is(err, syscall.ESRCH) {
				err = nil
			}
		}
		p.client.Kill()
		close(p.release)
	})
	return err
}

// pluginSession serves an incarnation's requests from its stream
type pluginSession struct {
	stream runnablepb.Runnable_RunClient
	host   Host
	ctx    context.Context
	cancel context.CancelFunc
	sendMu sync.Mutex

	exit Exit
	done chan struct{}
}

func (s *pluginSession) send(m *runnablepb.HostMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(m)
}

func (s *pluginSession) Renew(validUntil int64) error {
	return s.send(&runnablepb.HostMessage{Msg: &runnablepb.HostMessage_Heartbeat{Heartbeat: &runnablepb.Heartbeat{ValidUntilBoottimeNs: validUntil}}})
}

func (s *pluginSession) Stop(reason string) {
	_ = s.send(&runnablepb.HostMessage{Msg: &runnablepb.HostMessage_Stop{Stop: &runnablepb.Stop{Reason: reason}}})
}

func (s *pluginSession) Done() <-chan struct{} { return s.done }
func (s *pluginSession) Exit() Exit            { <-s.done; return s.exit }

// serve answers requests until the stream ends; the stream ending without
// Exit is a crash
func (s *pluginSession) serve() {
	defer close(s.done)
	defer s.cancel()
	var requests sync.WaitGroup
	defer requests.Wait()
	for {
		m, err := s.stream.Recv()
		if err != nil {
			if s.exit.Outcome == 0 {
				if errors.Is(err, io.EOF) {
					err = errors.New("the stream ended without an exit")
				}
				s.exit = Exit{Outcome: Crashed, Err: fmt.Errorf("runnable: %w", err)}
			}
			return
		}
		if x, ok := m.GetMsg().(*runnablepb.RunnableMessage_Exit); ok {
			s.exit = exitOfPB(x.Exit)
			continue
		}
		requests.Go(func() { s.answer(m) })
	}
}

func exitOfPB(e *runnablepb.Exit) Exit {
	switch x := e.GetOutcome().(type) {
	case *runnablepb.Exit_Completed:
		return Exit{Outcome: Completed}
	case *runnablepb.Exit_Fenced:
		return Exit{Outcome: Fenced}
	case *runnablepb.Exit_Error:
		return Exit{Outcome: Failed, Err: errors.New(x.Error)}
	}
	return Exit{Outcome: Failed, Err: errors.New("runnable: exit without an outcome")}
}

// answer runs one request on the host and sends its result
func (s *pluginSession) answer(m *runnablepb.RunnableMessage) {
	var reply *runnablepb.HostMessage
	switch x := m.GetMsg().(type) {
	case *runnablepb.RunnableMessage_Checkpoint:
		seq, err := s.host.Checkpoint(s.ctx, x.Checkpoint.GetData())
		reply = &runnablepb.HostMessage{Msg: &runnablepb.HostMessage_CheckpointResult{CheckpointResult: &runnablepb.CheckpointResult{
			Id: x.Checkpoint.GetId(), Seq: seq, Error: toPBError(err)}}}
	case *runnablepb.RunnableMessage_Status:
		err := s.host.Status(s.ctx, x.Status.GetHealthy(), x.Status.GetDetail())
		reply = &runnablepb.HostMessage{Msg: &runnablepb.HostMessage_StatusResult{StatusResult: &runnablepb.StatusResult{
			Id: x.Status.GetId(), Error: toPBError(err)}}}
	case *runnablepb.RunnableMessage_Exec:
		r := &runnablepb.ExecResult{Id: x.Exec.GetId()}
		results, err := s.host.Store().Exec(s.ctx, FromPBStatements(x.Exec.GetStatements()))
		if err == nil {
			r.Results, err = toPBResults(results)
		}
		r.Error = toPBError(err)
		reply = &runnablepb.HostMessage{Msg: &runnablepb.HostMessage_ExecResult{ExecResult: r}}
	case *runnablepb.RunnableMessage_Query:
		reply = &runnablepb.HostMessage{Msg: &runnablepb.HostMessage_QueryResult{QueryResult: s.query(x.Query)}}
	default:
		return
	}
	_ = s.send(reply)
}

func (s *pluginSession) query(q *runnablepb.StoreQuery) *runnablepb.QueryResult {
	r := &runnablepb.QueryResult{Id: q.GetId()}
	rows, err := s.host.Store().Query(s.ctx, q.GetSql(), fromPBValues(q.GetArgs())...)
	if err != nil {
		r.Error = toPBError(err)
		return r
	}
	defer rows.Close() //nolint:errcheck // in memory or the node's
	r.Columns = rows.Columns()
	for rows.Next() {
		vals := make([]any, len(r.Columns))
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return &runnablepb.QueryResult{Id: q.GetId(), Error: toPBError(err)}
		}
		pv, err := toPBValues(vals)
		if err != nil {
			return &runnablepb.QueryResult{Id: q.GetId(), Error: toPBError(err)}
		}
		r.Rows = append(r.Rows, &runnablepb.Row{Values: pv})
	}
	if err := rows.Err(); err != nil {
		return &runnablepb.QueryResult{Id: q.GetId(), Error: toPBError(err)}
	}
	return r
}
