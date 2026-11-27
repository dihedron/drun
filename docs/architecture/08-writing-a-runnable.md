# Writing a runnable

A runnable is a Go program built with the SDK, `github.com/dihedron/resurgo/pkg/runnable`.
Its `main` calls `runnable.Serve` with a value that implements
`runnable.Runnable`:

```go
type Runnable interface {
	Describe() Description
	Validate(ctx context.Context, config []byte) error
	Run(ctx context.Context, inc Incarnation, host Host) error
}
```

- `Describe` names the runnable (it must match `name` in the node's
  configuration) and its kind: `Service` runs until stopped, `Task` runs
  to completion once per cluster.
- `Validate` checks the configuration, as JSON, without side effects; the
  node calls it at start and before every incarnation.
- `Run` does the work, once per incarnation, in a process of its own.

## Run's contract

- `inc` holds the epoch, the Raft term, the node, the configuration and
  the latest checkpoint (or nil): resume from it.
- `ctx` ends when the incarnation must stop: a planned stop, a lost
  leadership, or the lease expiring. `Run` must return within the
  runnable's `stop_grace` (2 s by default); after that the process is
  killed.
- A nil return completes a task; for a service, any return is a failure,
  restarted after a back-off. Return `ErrFenced` when the host says so.

## The host

| Call | What it does |
| :--- | :--- |
| `host.Checkpoint(ctx, data)` | Replicates up to `checkpoint.max_size` bytes; returns once a majority committed them. A faster caller than `checkpoint.min_interval` is held back. `ErrFenced`: this incarnation is stale |
| `host.Status(ctx, healthy, detail)` | Shown by `runnable show`; unhealthy for longer than `health.timeout` fails the incarnation |
| `host.Store()` | The runnable's SQL database: `Exec` replicates a batch, `Query` reads |

A checkpoint replaces the previous one; keep it small (a position, a
counter, a cursor), and keep larger state in the store.

## The store

`Exec` takes a batch of statements, applied on every node in one
transaction: all or none. Each statement may carry an expectation on the
rows it affects (`runnable.Exactly(1)`), so that a batch checks its own
preconditions. The SQL is SQLite's, minus what would make the nodes
differ: no `random()`, no clock (`'now'`, `CURRENT_TIMESTAMP` and
`localtime` are refused), no `ATTACH`, no transactions of its own, no
extensions. The only clock is `resurgo_now()`, the leader's time of the
batch in Unix milliseconds, the same on every node; `resurgo_epoch()` is
the incarnation's epoch.

The schema is the runnable's: migrate it at the start of `Run` with
`pkg/runnable/migrate`, which moves the version on in the same batch as
each migration and refuses a store newer than the binary knows.

```go
version, err := migrate.Run(ctx, host.Store(), []migrate.Migration{
	{Name: "jobs", Statements: []runnable.Statement{{SQL: "CREATE TABLE jobs (id INTEGER PRIMARY KEY, state TEXT NOT NULL)"}}},
})
```

## The example: the ticker

```{.go include="cmd/resurgo-runnable-ticker/main.go"}
```

## Testing

`pkg/runnable/conformance` checks a runnable as the node needs it:
it describes itself, accepts its configuration, and returns in time when
stopped, when its lease expires and when it is fenced. Run it in process
and against the built binary:

```go
conformance.Check(t, func(*testing.T) runnable.Instance { return runnable.InProcess(ticker{}) },
	conformance.Options{Config: []byte(`{"interval":"20ms"}`)})
```

`conformance.NewHost()` is an in-memory host for unit tests of `Run`.

## Shipping it

Build it statically (`CGO_ENABLED=0`), install it root-owned in
`/usr/libexec/resurgo/runnables`, and pin its SHA-256 in every node's
configuration. Its standard error goes to the node's journal: never write
secrets there.
