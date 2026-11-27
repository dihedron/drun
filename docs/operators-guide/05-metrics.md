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
