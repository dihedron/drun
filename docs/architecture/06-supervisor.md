# The supervisor

The supervisor (`internal/supervisor`) runs the runnables on the leader,
and nothing on a follower. It follows leadership through raft's leader
channel and checks the node's state at every lease renewal too, so that a
missed signal cannot leave a follower running anything.

## On becoming leader

1. `Barrier`: every entry of earlier leaders is applied first, so the new
   leader sees the latest checkpoint.
2. For each configured runnable, a controller:
   - skips a disabled runnable and a completed task;
   - commits `Incarnate`, which hands out the next epoch;
   - waits `takeover_delay` from the moment it became leader, unless the
     predecessor was released, completed, or one of its own incarnations;
   - confirms leadership (`VerifyLeader`), starts a fresh instance with the
     incarnation, its configuration and the latest checkpoint, leased until
     the `CLOCK_BOOTTIME` reading taken before the confirmation plus
     `lease.duration`.
3. Every `lease.renew_interval`, the controller confirms leadership again
   and renews the lease; it also re-reads the state: a disabled runnable
   is stopped and released, a newer epoch means the incarnation is
   fenced. An incarnation unhealthy for longer than `health.timeout` is
   stopped and counts as a failure.

## Stopping

| Why | How | Committed |
| :--- | :--- | :--- |
| Leadership lost, or not confirmed | `Stop`, then killed after `stop_grace` | nothing |
| Shutdown, handover, restart, disable | `Stop`, then killed after `shutdown_grace` | `Release`, if it stopped in time |
| A newer epoch | as for a lost leadership | nothing |

A released predecessor lets the next leader start at once; otherwise the
next leader waits `takeover_delay`.

## Failures

A failed incarnation (an error, a crash, a service that returned, an
instance that could not start) commits `Fail` and is restarted after a
back-off of `restart.initial` doubled per consecutive failure, at most
`restart.max`, with ±20 % jitter. When this node's consecutive failures
reach `restart.handover_after`, the leader hands leadership to the voter
with the fewest recent failures of the runnable; when every voter has
failed as often, it keeps backing off where it is. A successful checkpoint
or release clears the node's count. A runnable that cannot be loaded on a
node fails the same way, so such a node gives leadership away.

## The incarnation's host

Checkpoints larger than `checkpoint.max_size` are refused; one sent
sooner than `checkpoint.min_interval` after the previous is held back.
Store batches larger than `store.max_batch` are refused. Queries run on
the local database, only while the incarnation's epoch is current, within
`store.query_timeout`.

## Testing

Clusters of in-process nodes (raft's in-memory transport, real stores,
in-process instances) cover failover, handover, failures, tasks, limits
and disabling. `make stress` (2 minutes) and `make soak` (8 hours,
overnight) run such a cluster under random partitions, handovers and
failures, checking throughout that no two incarnations act at once, that
epochs increase and that no acknowledged checkpoint is lost.
