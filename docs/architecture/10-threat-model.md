# Security

## Requirements

| ID | Requirement |
| :------ | :---------------------------------------- |
| SEC-1 | The cluster port uses TLS 1.3 with mutual authentication; there is no plaintext mode |
| SEC-2 | A peer must present the identity configured for the node ID it claims; every Raft request's sender is checked against the connection's identity |
| SEC-3 | Certificates come from files, reloaded on change |
| SEC-4 | A runnable binary runs only if its SHA-256 matches and it and its directory are root's |
| SEC-5 | A runnable reaches only its own incarnation's stream: not another runnable, not the admin API |
| SEC-6 | The admin socket admits root and the members of `admin.allowed_groups`; every change is logged with the caller |
| SEC-7 | `data_dir` is `0700`, the service user's |
| SEC-8 | No secret in the cluster configuration, the log, a checkpoint of the shipped runnables, a log line or a metric |
| SEC-9 | The service runs confined by systemd |
| SEC-10 | A runnable's process dies with the node |
| SEC-11 | The remote admin endpoint, when configured, uses TLS 1.3 with mutual authentication and admits only listed identities |

## Threats

| ID | Threat | Mitigation | Residual |
| :---- | :------------------------ | :-------------------- | :---- |
| T-1 | A host on the network joins as a peer or injects Raft requests | SEC-1, SEC-2 | R-1 |
| T-2 | One node's certificate is used to impersonate another node | SEC-2: one identity per node ID | R-1 |
| T-3 | A runnable binary is replaced on disk | SEC-4: digest pinning, root-owned directory | — |
| T-4 | A malicious or buggy runnable attacks the node | separate process, SEC-5, SEC-9; same user as the node | R-3 |
| T-5 | Checkpoints holding secrets are replicated to every node and written to disk | SEC-7, SEC-8 | R-2 |
| T-6 | A runnable floods the log with checkpoints | size and rate limits, log compaction | — |
| T-7 | Split brain: two incarnations write external state | the epoch fence, the lease, the takeover delay | R-4 |
| T-8 | A local user drives the admin socket | SEC-6 | — |
| T-9 | An operator denies a destructive admin action | SEC-6: the audit log | R-5 |
| T-10 | A runnable that crash-loops on one node leaves the cluster without it | handover after repeated failures, back-off | — |
| T-11 | A tampered package is installed | signed packages and checksums | — |
| T-12 | A runnable's standard error leaks secrets into the journal | the runnable's responsibility | R-6 |
| T-13 | A partitioned node with a stale log disrupts the cluster | Raft pre-vote | — |
| T-14 | A host on the network drives the remote admin endpoint | SEC-11: off by default, mutual TLS, identity allowlist | R-7 |
| T-15 | A host that reaches the metrics endpoint reads the peers' IDs and Raft's timings | metrics off by default; bind to loopback or a monitoring network; no secrets in any series | — |
| T-16 | A stale incarnation writes to the runnable's external PostgreSQL after a newer one started | the fence helper: a claim, and a locked check in every writing transaction | R-4, R-8 |
| T-17 | An operator promotes a member still catching up, and commits stall | promotion only after a stable run of healthy checks; `--force` is explicit | — |
| T-18 | A runnable binary is replaced by another, or by an older signed one with a known flaw | the signed manifest's digest; drun's version must be in the manifest's range; root-owned files | R-9 |
| T-19 | A key added to the key directory trusts a malicious publisher | root-owned directory and files; a key signs only the runnables that name it | — |


## Accepted risks

- **R-1** A stolen node key lets an attacker act as that node until the
  certificate is revoked or expires: prefer short-lived certificates.
- **R-2** Checkpoints and stores are not encrypted on disk.
- **R-3** A runnable runs as the service user: it can read the node's
  configuration, keys and data.
- **R-4** Side effects outside fenced systems may repeat during an overlap.
- **R-5** The audit trail is each node's journal.
- **R-6** A runnable can log what it wants.
- **R-7** A stolen admin client key gives full admin access through the
  remote endpoint until its identity is removed or its certificate
  expires; there are no per-command roles.
- **R-8** Restoring a backup of a runnable's external database can put
  back an older fence epoch: restart the runnable afterwards.
- **R-9** A publisher's older signed runnable can be put back within the
  drun versions the publisher signed for: narrow ranges, or a new key,
  revoke old manifests.
