# The admin API

`internal/admin` is one gRPC service (`proto/admin/v1/admin.proto`)
served on pluggable endpoints. An endpoint is a listener and the
transport credentials whose server handshake identifies the caller; the
service sees only the resulting caller.

| Endpoint | Listener | Caller |
| :--- | :--- | :--- |
| Socket (always) | unix socket `0660` | `SO_PEERCRED`: root, or a member of `admin.allowed_groups` |
| Remote (`admin.remote`) | TCP, TLS 1.3, mutual | the client certificate's identity, if allowed |
| Peers | the cluster port's admin stream | the forwarding node (authenticated by the cluster port), the original caller in the call's metadata |

A caller that is not allowed gets `PermissionDenied` on every call rather
than a dropped connection, so the CLI can say why.

## Forwarding

An interceptor runs every call. The methods only the leader executes
(membership, transfer, enable/disable, restart, reset, the supervisor's
views) are, on a follower, invoked again on the leader over the cluster
port's admin stream, which is already mutual TLS between members. A
forwarded call that arrives at a node that is not leader fails with
`Unavailable` instead of hopping again. During an election a follower
waits up to 5 s for a leader. Mutations are logged by the node that
executes them.

## Handovers

`leader transfer` and the removal of the leader use the supervisor's
planned handover: the runnables are stopped and released before
leadership moves (FR-12). The removal is then forwarded to the new
leader, which executes it.

## Assembly

`internal/daemon` assembles a node for `drun serve` and for the tests:
store, FSM, Raft node, supervisor, admin endpoints, then the FR-16 check
of every runnable and a watcher that warns of drift (FR-15). Tests run
clusters of real nodes in one process, over real TLS on loopback, with
in-process runnables.

## Operator writes

`store exec` (STO-17) runs on the leader: the supervisor holds the
runnable (a planned stop, released), so that it cannot incarnate between
the operator's `Incarnate` and batch; the leader commits `Incarnate` with
the operator's identity (no digests, allowed while disabled), the batch at
that epoch, and `Release`; then it resumes the runnable, which starts at a
newer epoch without a takeover delay. `--dry-run` applies the batch with
the store's rules to a `VACUUM INTO` copy in a transaction that is rolled
back; nothing on it can stop the node. Statements are split by SQLite's
tokenizer (`sqlite3_complete`), which needs no schema.

`store show` asks every member for its digest over the admin stream;
digests are compared only between members at the same index.

## Metrics

`internal/metrics` writes the Prometheus text format by hand. Gauges are
read at each scrape (Raft state, the FSM, the supervisor's views, drift);
counters and the histogram come from the supervisor's `Events`.

Raft's own metrics (FW-3) come from `hashicorp/go-metrics`, the
process-wide instance that `hashicorp/raft`, `raft-boltdb` and `raft-wal`
report to. With `metrics.listen` set, the daemon installs a `RaftSink` as
that instance before it opens the Raft stores, because raft-wal binds the
instance when it opens. The sink keeps counters cumulative, gauges at
their last value and samples as histograms in milliseconds, all under
one mutex, and `/metrics` renders it after drun's own families. The four
keys that hashicorp/raft names after a peer are blocked through
`BlockedPrefixes`, since each has a twin with a `peer_id` label. Because
the instance is per process, nodes that share a process (as in the
tests) share their Raft series.
