# Runnables and the plugin protocol

A runnable is a separate program built with the SDK (`pkg/runnable`): its
`main` is `runnable.Serve(impl)`. The node starts one fresh process for
each incarnation and talks to it over gRPC through hashicorp/go-plugin,
with mutual TLS between the two set up automatically (AutoMTLS).

## Starting a process

- The binary must match its pinned SHA-256; go-plugin checks it before
  starting it. The binary and its directory must be owned by root and not
  writable by group or others.
- The process gets `Pdeathsig = SIGKILL`: it dies with the node. Linux
  delivers it when the *thread* that started the child ends, so the
  loader starts each child from a goroutine locked to its own OS thread,
  which ends only when the instance is killed.
- The plugin's stderr goes to the node's log, one line per record, with
  the runnable's name and epoch.

## One stream per incarnation

`Run` is a bidirectional stream. The node sends `Start` (the incarnation,
its latest checkpoint and the first lease), then heartbeats, the results
of the runnable's requests, and possibly `Stop`. The runnable sends its
requests (`Checkpoint`, `Status`, store `Exec` and `Query`), each with an
ID answered by a result with the same ID, and finally `Exit`: completed,
fenced, or an error. A stream that ends without `Exit` is a crash.

## The lease

Inside the plugin, the SDK cancels `Run`'s context as soon as
`CLOCK_BOOTTIME` passes the latest `valid_until` the node sent. Both
processes read the same kernel clock, which keeps running while either is
stopped, so a node that is paused cannot keep its runnable running. A
heartbeat only ever extends the lease. The context is also cancelled on
`Stop` and when the stream breaks.

## Outcomes

| Outcome | When |
| :--- | :--- |
| Completed | `Run` returned nil (a task completes; for a service it is a failure) |
| Fenced | `Run` returned `ErrFenced`: a newer incarnation exists |
| Failed | `Run` returned another error, or panicked |
| Crashed | The process died or was killed without an `Exit` |

## In-process instances

`runnable.InProcess(impl)` runs a runnable inside the node's process with
the same lease and outcomes, for tests of whole clusters in one process.
The conformance kit (`pkg/runnable/conformance`) runs the same checks
against both kinds of instance: the runnable describes itself, accepts its
configuration, and returns within its stop grace when stopped, when its
lease expires and when it is fenced.

## Signed manifests

A runnable is trusted either by a digest in each node's configuration or
by a manifest its publisher signed (`key`). At start, `serve` verifies
each manifest (`pkg/runnable/manifest`): ownership, the Ed25519
signature against the named key file, the name, drun's version against
the manifest's range (`pkg/semver`, shared with veho), and the protocol.
It then puts the manifest's digest in place of `sha256`. From there,
pinning, `Incarnate`'s digests and drift work as for a configured digest.
A manifest that fails makes the runnable invalid on that node (FR-16),
with the step that failed. The release signs the examples with drun's
key, held as a repository secret, in a goreleaser hook
(`tools/runnable-manifest`).
