# Metrics

With `metrics.listen` set, each node serves `/metrics` in the Prometheus
text format.

| Metric | Type | Meaning |
| :--- | :--- | :--- |
| `drun_is_leader` | gauge | 1 on the leader |
| `drun_raft_term` | gauge | The Raft term |
| `drun_epoch{runnable}` | gauge | The epoch of the current incarnation, as this node knows it |
| `drun_incarnation_state{runnable,state}` | gauge | 1 for the state this node's supervisor shows (`idle` on a follower) |
| `drun_incarnations_total{runnable,outcome}` | counter | Incarnations this node ran, by how they ended: `completed`, `failed`, `crashed`, `unhealthy`, `fenced`, `lost` (leadership), `stopped` (planned) |
| `drun_failover_seconds{runnable,kind}` | gauge | From becoming leader to the first incarnation, the last time; `planned` if the predecessor was released |
| `drun_takeover_wait_seconds{runnable}` | gauge | The takeover delay waited then |
| `drun_checkpoint_bytes{runnable}`, `drun_checkpoint_seq{runnable}` | gauge | The latest checkpoint |
| `drun_checkpoint_commit_seconds{runnable}` | histogram | Time to commit a checkpoint (leader) |
| `drun_lease_renew_failures_total` | counter | Renewals refused because leadership was not confirmed |
| `drun_config_drift{runnable}` | gauge | 1 if this node's runnable differs from the current incarnation's |

Alert on: no node with `drun_is_leader 1` for longer than an election;
`drun_incarnation_state{state="running"}` at 0 on the leader for an
enabled service; a growing `drun_incarnations_total{outcome="failed"}`;
`drun_config_drift` at 1.

## Raft's own metrics

The same endpoint also serves what `hashicorp/raft` and the Raft log
store (`raft-wal` or `raft-boltdb`) report through `hashicorp/go-metrics`.
They are collected only while `metrics.listen` is set. Their names follow
the library key: `drun_`, then the key in snake case, so
`raft.rpc.appendEntries` becomes `drun_raft_rpc_append_entries`. Each
family's HELP line gives the original key.

- A library counter is a Prometheus `counter`, named `…_total`, counting
  from the node's start.
- A library gauge is a `gauge` holding the last value.
- A library sample or timer is a `histogram`, with buckets 0.1, 0.25,
  0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000 and 10000.
  Timers are in **milliseconds**, as HashiCorp documents them (not
  seconds, unlike drun's own `…_seconds` families).

Per-peer series carry a `peer_id` label. The series of a removed peer
stay until the node restarts.

| Metric | Type | Meaning |
| :--- | :--- | :--- |
| `drun_raft_state_leader_total`, `…_candidate_total`, `…_follower_total` | counter | Transitions into each Raft state: a growing candidate count means elections |
| `drun_raft_transition_heartbeat_timeout_total` | counter | Followers that lost the leader's heartbeat |
| `drun_raft_peers` | gauge | Servers in the latest configuration (leader) |
| `drun_raft_commit_time` | histogram | From dispatch to commit on the leader, ms |
| `drun_raft_apply_total` | counter | Commands applied through `Apply` (leader) |
| `drun_raft_fsm_apply` | histogram | Time the FSM takes to apply one entry, ms |
| `drun_raft_leader_last_contact` | histogram | Time since the leader last heard from each follower, sampled at each lease check, ms |
| `drun_raft_replication_append_entries_rpc{peer_id}` | histogram | AppendEntries round trip to each follower, ms |
| `drun_raft_replication_heartbeat{peer_id}` | histogram | Heartbeat round trip to each follower, ms |
| `drun_raft_snapshot_persist`, `drun_raft_snapshot_create` | histogram | Snapshot timings, ms |
| `drun_raft_thread_main_saturation`, `drun_raft_thread_fsm_saturation` | histogram | Share of time Raft's main and FSM goroutines are busy (0 to 1) |
| `drun_raft_wal_log_appends_total`, `drun_raft_wal_log_entry_bytes_written_total` | counter | raft-wal appends and bytes written |
| `drun_raft_wal_segment_rotations_total` | counter | raft-wal segment rotations |
| `drun_raft_boltdb_store_logs` | histogram | raft-boltdb append time, ms |
| `drun_raft_boltdb_num_free_pages` | gauge | raft-boltdb free pages: grows with compaction |

Alert on: `rate(drun_raft_state_candidate_total[10m]) > 0` (elections
are happening); a p99 of `drun_raft_commit_time` well above the
round-trip time between voters; `drun_raft_thread_main_saturation`
near 1.

The endpoint has no authentication. It shows the peers' IDs and Raft's
timings, but no secrets, addresses or payloads (threat T-15). Bind it
to loopback or to a monitoring network.
