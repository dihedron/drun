package runnable

import (
	"context"
	"errors"
	"sync"
)

// InProcess returns an Instance that runs impl in this process, with the
// same lease and outcomes as a plugin: for deterministic cluster tests and
// the conformance kit (ADR-10). Kill cannot end a goroutine: it cancels
// Run's context and reports the incarnation crashed at once.
func InProcess(impl Runnable) Instance {
	return &inProcess{impl: impl}
}

type inProcess struct {
	impl    Runnable
	started bool
	mu      sync.Mutex
	session *localSession
}

func (p *inProcess) Describe(context.Context) (Description, error) { return p.impl.Describe(), nil }

func (p *inProcess) Validate(ctx context.Context, config []byte) error {
	return p.impl.Validate(ctx, config)
}

func (p *inProcess) Start(_ context.Context, inc Incarnation, validUntil int64, host Host) (Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return nil, errors.New("runnable: an instance runs one incarnation")
	}
	p.started = true
	ctx, l := withLease(context.Background(), validUntil)
	s := &localSession{lease: l, done: make(chan struct{})}
	p.session = s
	go func() {
		err := runSafely(ctx, p.impl, inc, host)
		l.cancel(nil)
		s.finish(exitOf(err))
	}()
	return s, nil
}

func (p *inProcess) Kill() error {
	p.mu.Lock()
	s := p.session
	p.mu.Unlock()
	if s != nil {
		s.lease.cancel(errors.New("runnable: killed"))
		s.finish(Exit{Outcome: Crashed, Err: errors.New("runnable: killed")})
	}
	return nil
}

// localSession is a Session whose Run is a goroutine
type localSession struct {
	lease *lease
	once  sync.Once
	exit  Exit
	done  chan struct{}
}

func (s *localSession) Renew(validUntil int64) error { s.lease.renew(validUntil); return nil }
func (s *localSession) Stop(string)                  { s.lease.cancel(ErrStopped) }
func (s *localSession) Done() <-chan struct{}        { return s.done }
func (s *localSession) Exit() Exit                   { <-s.done; return s.exit }

// finish records the first outcome only
func (s *localSession) finish(e Exit) {
	s.once.Do(func() {
		s.exit = e
		close(s.done)
	})
}
