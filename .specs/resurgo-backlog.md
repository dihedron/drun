# resurgo: backlog

Work agreed for later (`FW-nn` in `resurgo_specification.md`). Nothing here
is part of v1.

| ID | Item |
| :--- | :--- |
| FW-1 | Placement beyond the leader: per-runnable leases in the FSM, spreading runnables over nodes |
| FW-2 | `hashicorp/raft-wal` as the log store, after a benchmark against bbolt |
| FW-3 | Export of Raft's own metrics (go-metrics) to Prometheus |
| FW-4 | Encryption at rest of checkpoints and snapshots |
| FW-5 | A separate UID (and systemd sandbox) per runnable |
| FW-6 | A small key/value namespace per runnable instead of one blob |
| FW-7 | Signed runnable manifests, as in veho, for third-party runnables |
| FW-8 | Non-voter promotion helpers (autopilot-like health checks) |
