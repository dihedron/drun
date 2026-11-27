package runnable

import (
	"context"
	"time"
)

// Kind is how a runnable ends (FR-7).
type Kind int

const (
	// Service runs until stopped; any return is a failure.
	Service Kind = iota
	// Task runs to completion once per cluster; a nil return completes it.
	Task
)

func (k Kind) String() string {
	if k == Task {
		return "task"
	}
	return "service"
}

// Description is what a runnable says of itself.
type Description struct {
	// Name MUST match the runnable's name in the node's configuration.
	Name string
	// Version is the runnable's own version.
	Version string
	Kind    Kind
	// Stateful runnables keep state outside drun and fence it with the
	// epoch (§8.5).
	Stateful bool
	// ConfigSchema is an optional JSON Schema of the configuration.
	ConfigSchema []byte
}

// Checkpoint is the latest committed checkpoint of a runnable.
type Checkpoint struct {
	// Seq is per runnable, strictly increasing.
	Seq uint64
	// Epoch is the incarnation that wrote it.
	Epoch uint64
	Data  []byte
	// CommittedAt is the leader's clock: informational only.
	CommittedAt time.Time
}

// Incarnation is one run of a runnable on the leader.
type Incarnation struct {
	Runnable string
	// Epoch is the fencing token.
	Epoch uint64
	// Term is the Raft term of the leader that started it.
	Term    uint64
	Node    string
	Cluster string
	// Config is the node-local configuration, as JSON.
	Config []byte
	// Checkpoint is nil when there is none.
	Checkpoint *Checkpoint
}

// Host is the node, as a running incarnation sees it.
type Host interface {
	// Checkpoint replicates data through Raft; it returns once a quorum
	// has committed it (FR-9), or ErrFenced, after which Run MUST return.
	Checkpoint(ctx context.Context, data []byte) (seq uint64, err error)
	// Status reports health and progress, shown by "runnable show";
	// unhealthy for longer than health.timeout fails the incarnation.
	Status(ctx context.Context, healthy bool, detail string) error
	// Store is the runnable's replicated SQL database.
	Store() Store
}

// Runnable is what a runnable implements; its main is Serve(impl).
type Runnable interface {
	Describe() Description
	// Validate checks a configuration without side effects (FR-16).
	Validate(ctx context.Context, config []byte) error
	// Run does the work. ctx is cancelled when the incarnation must stop
	// (leadership lost, lease expired, shutdown, disable); Run MUST return
	// within the stop grace after that. A nil return completes a task; for
	// a service any return is a failure (FR-7).
	Run(ctx context.Context, inc Incarnation, host Host) error
}
