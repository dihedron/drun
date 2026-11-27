# resurgo: backlog

Work agreed for later (`FW-nn` in `resurgo_specification.md`). Nothing here
is part of v1.

| ID | Item |
| :--- | :--- |
| FW-1 | Placement beyond the leader: per-runnable leases in the FSM, spreading runnables over nodes |
| FW-2 | `hashicorp/raft-wal` as the log store, after a benchmark against bbolt. *Trialled 2026-11-27*: opt-in as `raft.log_store: wal` (v0.5.0, experimental upstream); `make bench-logstore` compares the two |
| FW-3 | Export of Raft's own metrics (go-metrics) to Prometheus |
| FW-4 | Encryption at rest of checkpoints and snapshots |
| FW-5 | A separate UID (and systemd sandbox) per runnable |
| FW-6 | *Done in v0.2.0 of the specification*: the SQL store (`resurgo-store.md`) |
| FW-7 | Signed runnable manifests, as in veho, for third-party runnables |
| FW-8 | Non-voter promotion helpers (autopilot-like health checks) |
| FW-S-1 | Leader-local interactive transactions shipped as SQLite changesets (session extension) |
| FW-S-2 | Group commit of concurrent batches into one Raft entry |
| FW-S-3 | Stale reads from followers for administrators and reporting |
| FW-9 | Preferred-leader policy (e.g. the region closest to the runnable's external systems), by leadership transfer after an election |
| FW-10 | A fence helper in the SDK for runnables that also write to an external database (§8.5), e.g. `pkg/runnable/fence/postgres` |
