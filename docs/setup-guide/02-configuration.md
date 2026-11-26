# Configuration

Each node reads one file, `/etc/resurgo/resurgo.yaml` (an example ships as
`examples/resurgo.yaml`). Unknown keys are refused, so a misspelt key
stops the node instead of being ignored. All problems in the file are
reported together.

## Node and cluster

| Key | Meaning |
| :--- | :--- |
| `node.id` | The Raft server ID: unique in the cluster, never reused |
| `node.data_dir` | Absolute path; created `0700` if missing; refused if others can enter it or it belongs to another user |
| `cluster.name` | The cluster's name |
| `cluster.bind` | Address and port the cluster port listens on, e.g. `0.0.0.0:7350` |
| `cluster.advertise` | Address and port the other nodes reach this node on; not `0.0.0.0`; must equal this node's entry in `cluster.voters` |
| `cluster.voters` | The initial voters (`id`, `address`), identical on all of them |
| `cluster.tls.cert`, `key`, `ca` | Absolute paths of this node's certificate, key and the CA that signs every node's certificate |
| `cluster.tls.peers` | For every node ID (this one, every voter, every member added later): the identity its certificate carries |

A node without Raft state that is one of `cluster.voters` creates the
cluster from that list; a node that already has state ignores the list.
A node that is not an initial voter waits until a member adds it.

## Timings

| Key | LAN (default) | WAN (`raft.profile: wan`) |
| :--- | :--- | :--- |
| `raft.heartbeat_timeout` | 1 s | 2 s |
| `raft.election_timeout` | 1 s | 2 s |
| `raft.leader_lease_timeout` | 500 ms | 1 s |
| `lease.duration` | 750 ms | 1.5 s |
| `lease.renew_interval` | 250 ms | 400 ms |
| `raft.snapshot_threshold` / `raft.trailing_logs` | 512 / 256 | 512 / 256 |

Any of them can be set on its own; the profile fills in the others. The
node refuses timings that break the leadership lease:
`leader_lease_timeout` and `lease.duration` must be less than
`heartbeat_timeout`, and `renew_interval` at most a third of
`lease.duration`.

## Runnables

| Key | Default | Notes |
| :--- | :--- | :--- |
| `name` | | Letters, digits, `.`, `_`, `-`; at most 64 |
| `kind` | `service` | or `task` |
| `binary`, `sha256` | | Absolute path; the binary's SHA-256, 64 lower-case hex digits |
| `config` | | Passed to the runnable as JSON; name secret files here, never secrets |
| `takeover_delay` | `auto` | `auto` = `lease.duration + stop_grace - heartbeat_timeout + 500ms` (2.25 s on a LAN), or a duration such as `0s` |
| `stop_grace` / `shutdown_grace` | 2 s / 30 s | |
| `restart` | `initial: 1s, max: 2m, handover_after: 5` | |
| `checkpoint` | `max_size: 256KiB, min_interval: 1s` | `max_size` at most `1MiB` |
| `health.timeout` | 30 s | |
| `store` | `max_batch: 1MiB, max_steps: 50000000, max_size: 256MiB, max_result: 1MiB, query_timeout: 30s` | `max_batch` at most `4MiB` |

Sizes are integers with an optional `B`, `KiB`, `MiB` or `GiB`.
