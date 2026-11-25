# resurgo: the replicated SQL store of a runnable

- **Document Version:** `0.2.0` (2026-11-25): engine `mattn/go-sqlite3`, operator writes (STO-17), limits confirmed
- **Status:** Draft, for review; not yet approved for implementation (`AGENTS.md`, rule 1)
- **Parent:** `resurgo_specification.md` (conventions §0, identifiers, invariants, threat model)
- **Identifiers:** `STO-nn` requirements, `INV-S-nn` invariants, `T-S-nn` threats, `AC-S-nn` scenarios, `EXT-S-nn` assumptions, `OI-S-nn` open issues

---

## 1. Purpose

Every runnable gets its own SQL database, a SQLite file that is replicated
through the Raft log of the cluster and fenced by the runnable's epoch. A
stateful runnable can keep its state there instead of in an external
database: a write from a stale incarnation is refused by the state machine
in the same step that would have applied it, so there is nothing to fence
by hand, and no overlap between two incarnations can corrupt the data.

The store is meant for coordination and moderate application state (tens
to hundreds of MiB, hundreds to low thousands of writes per second); bulk
data still belongs in an external system.

## 2. Model

- **One database per runnable** (`data_dir/store/<runnable>.db`), created
  by the first write. A runnable reaches only its own database; the
  database survives incarnations, leader changes and node restarts.
- **Single writer.** Only the current incarnation writes; it runs on the
  leader, where its reads are local.
- **Writes are batches.** A batch is an ordered list of statements with
  their parameters, replicated as one Raft entry and applied by every node
  in one SQLite transaction: all statements or none. There are no
  interactive transactions: a transaction cannot stay open across a Raft
  round trip.
- **Read, then write.** Because the incarnation is the only writer, the
  pattern "read locally, decide, commit a batch" is serialisable without
  locks; a stale incarnation's reads may be stale, but its batch is
  refused (INV-S-2), so the stale decision never lands. Conditions that
  must hold at apply time are written into the batch (STO-6).

## 3. Requirements

### API (illustrative Go, in `pkg/runnable`)

```go
type Statement struct {
	SQL    string
	Args   []any       // int64, float64, string, []byte, nil, bool
	Expect *Expectation // optional: the batch fails unless it holds
}

type Expectation struct {
	RowsAffected    *int64 // exactly this many rows
	MinRowsAffected *int64 // at least this many
}

type Result struct {
	RowsAffected int64
	LastInsertID int64
	Rows         [][]any // RETURNING rows, bounded by STO-9
}

type Store interface {
	// Exec replicates a batch and applies it atomically; it returns once
	// a quorum has committed it and the leader has applied it.
	// ErrFenced: the incarnation is stale; *BatchError: the batch was
	// rolled back (statement index, SQLite code, message), on every node.
	Exec(ctx context.Context, batch []Statement) ([]Result, error)
	// Query reads the leader's local copy, read-only.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

type Host interface {
	Checkpoint(ctx context.Context, data []byte) (uint64, error) // STO-12
	Status(ctx context.Context, healthy bool, detail string) error
	Store() Store
}
```

### Writes
- **STO-1 (fenced)** A batch carries the incarnation's epoch; the FSM
  applies it only if the epoch is the runnable's current one (parent
  INV-3), otherwise it changes nothing and returns `ErrFenced`.
- **STO-2 (atomic)** The FSM applies a batch inside one transaction
  (`BEGIN IMMEDIATE … COMMIT`); any failing statement or expectation rolls
  back the whole batch, with the same error on every node.
- **STO-3 (exactly once)** The index of the last applied entry is stored in
  the database (`_resurgo_meta`), in the same transaction as the batch; an
  entry whose index is not greater is skipped. Raft re-applies the log
  from the last snapshot on restart; without this the batches since the
  snapshot would be applied twice (INV-S-3).
- **STO-4 (deterministic)** The FSM MUST refuse, deterministically and on
  the leader before replication too, any statement that can give different
  results on different nodes or affects more than the database:
  non-deterministic functions (`random`, `randomblob`, `changes` is fine;
  `date`/`time`/`datetime`/`julianday`/`unixepoch`/`strftime` with `'now'`,
  `CURRENT_TIME`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`), `ATTACH`,
  `DETACH`, `VACUUM`, `TEMP` objects, `load_extension`, and every `PRAGMA`
  except reading the schema and setting `user_version`. Enforcement uses
  SQLite's authorizer (EXT-S-1), not text matching.
- **STO-5 (time)** The leader stamps every batch with its time; the FSM
  exposes it as `resurgo_now()` (Unix ms) and `resurgo_epoch()`, the only
  clock and identity a statement may use.
- **STO-6 (conditions)** A statement MAY carry an expectation on the rows
  it affects; together with conditional SQL (`UPDATE … WHERE version = ?`,
  `INSERT … ON CONFLICT`, `RETURNING`) this expresses compare-and-set.
- **STO-7 (bounded work)** A batch is limited in size (`store.max_batch`,
  default 1 MiB, at most 4 MiB) and in work: a progress handler counts
  SQLite VM steps and aborts the batch after `store.max_steps` (default
  50 million). The count is a function of the statement and the data, so
  every node aborts the same batch at the same point (EXT-S-2). Without it
  one runaway query (a recursive CTE) would stop the state machine of the
  whole cluster.
- **STO-8 (bounded size)** The FSM sets `max_page_count` from the
  runnable's `store.max_size` (default 256 MiB); a batch that would grow
  the database beyond it fails with `SQLITE_FULL` on every node. The limits
  of STO-7 and STO-8 are replicated in `Incarnate`, not read from each
  node's configuration, so all nodes apply the same ones.
- **STO-9 (results)** `RETURNING` rows are returned to the caller, at most
  `store.max_result` bytes (default 1 MiB); a larger result fails the batch.

### Reads
- **STO-10 (local, current)** `Query` runs on the leader's local database
  on a read-only connection (`query_only`), only while the incarnation is
  current; it sees every batch acknowledged to this incarnation and, after
  the start barrier, every batch committed before it (read your writes).
  It is not linearizable against a deposed leader; STO-1 makes that
  harmless for writes.
- **STO-11 (bounded reads)** A query is limited by a wall-clock timeout
  (`store.query_timeout`, default 30 s) and a result size; rows are
  streamed over the incarnation's stream.

### Checkpoints and schema
- **STO-12 (checkpoints)** `Checkpoint` becomes a convenience over the
  store: an upsert into the reserved table `_resurgo_checkpoint`, with the
  same semantics as parent FR-9; tables starting with `_resurgo_` are
  reserved and read-only to the runnable.
- **STO-13 (migrations are the runnable's)** The runnable owns its schema.
  It migrates it at the start of `Run`, on the leader, before doing any
  work, with ordinary batches (DDL is transactional in SQLite). The SDK
  provides `pkg/runnable/migrate`: ordered migrations embedded in the
  runnable's binary, each applied as one batch that also advances the
  version with an expectation (`UPDATE _schema SET version = n WHERE
  version = n-1`, exactly one row), so that:
  - a failover in the middle leaves the schema at a version boundary, and
    the next incarnation continues from there;
  - a stale incarnation's migration is refused (STO-1);
  - two binaries never race, since there is one writer;
  - a runnable that finds a version newer than it knows MUST refuse to
    start (it is an older binary on a newer schema: parent FR-15, drift).
  A migration larger than `store.max_batch` or `store.max_steps` (a
  backfill) MUST be split into resumable chunks that record their
  progress in the database.

### Snapshots and administration
- **STO-14 (snapshots)** A snapshot holds the FSM state and, per runnable,
  a consistent copy of its database, taken from a read transaction in WAL
  mode so that applies continue while it is persisted (EXT-S-3). Restore
  writes each database to a temporary file, syncs it and renames it over
  the old one.
- **STO-15 (identical replicas)** Every node computes, on demand, a digest
  of each database's logical content at an index; `resurgo store show`
  compares them across nodes (INV-S-1).
- **STO-16 (CLI)** `resurgo store show [NAME]` (size, tables, last index,
  digests), `resurgo store query NAME SQL` (read-only; on the leader after
  `VerifyLeader` by default, with `--local` on the local node, flagged as
  possibly stale on a follower), `resurgo store exec NAME [SQL | --file
  F]` (STO-17), `resurgo store backup NAME FILE`, `resurgo store reset
  NAME` (committed, refused while the runnable runs). Admin access as
  parent SEC-6; every query and batch is logged with the caller, not with
  its results.
- **STO-17 (operator writes)** An operator MAY write to a runnable's store
  with `resurgo store exec`. The batch goes through the Raft log and the
  FSM like a runnable's, under the same rules (STO-2 to STO-9), and is
  recorded with the caller's identity (UID, node) in the entry and in the
  audit log. To keep the runnable the only writer while it runs, an
  operator batch is an incarnation of its own: the leader commits an
  `Incarnate` for the operator (a new epoch), then the batch, so the
  running incarnation is fenced (its next batch fails with `ErrFenced`),
  stops, and is started again with a newer epoch and the operator's
  changes visible. With `--disable` the runnable is disabled first and
  stays so (repairs before a restart); `--dry-run` applies the batch in a
  rolled-back transaction on the leader and shows the results. Operator
  schema migrations are ordinary batches: they MUST keep the runnable's
  `_schema` version consistent, or the runnable refuses to start
  (STO-13).

## 4. Engine

| ADR | Decision | Rationale | Alternatives |
|-----|----------|-----------|--------------|
| S-1 | `mattn/go-sqlite3` (cgo), with the SQLite amalgamation it bundles, never the system library (no `libsqlite3` tag), built with `sqlite_omit_load_extension`; the node is linked statically against musl (§4.1) | Upstream SQLite, the most used Go driver; the bundled version is compiled into the release, so every node of a release runs exactly the same SQLite, which deterministic replay needs | `modernc.org/sqlite` (no cgo: the fallback if EXT-S-1 or EXT-S-2 fail); `ncruces/go-sqlite3` (Wasm translated to Go, no cgo); purego bindings (see below) |
| S-2 | One file per runnable | Isolation, per-runnable limits, reset and snapshot | One file with a schema per runnable (`ATTACH`: shared limits, weaker isolation) |
| S-3 | Batches, no interactive transactions | Raft entries are the unit of replication and fencing | Leader-local transactions shipped as changesets (session extension): richer, but the changeset applies differently if replicas diverge, and the driver support is uncertain (FW-S-1) |

**purego.** Bindings through `ebitengine/purego` call a SQLite shared
library without cgo. They load the library at run time, so the binary is
no longer self-contained: the library must be installed with the
package, and every node must load the same build, or the replicas may
diverge after an upgrade of the system's `libsqlite3`. As far as we know,
purego on Linux also needs a dynamically linked binary. The libraries that
exist today are early (2026). Not chosen; reconsidered if S-1 fails the
spike.

### 4.1 Static linking with cgo

The node stays a single static binary (parent NFR-6):

- C toolchain: musl, for both architectures (`zig cc -target
  x86_64-linux-musl` / `aarch64-linux-musl`, or the musl cross compilers);
  glibc is avoided because a static glibc still loads NSS modules at run
  time.
- Build: `CGO_ENABLED=1 CC=<musl cc> go build -tags
  "osusergo,netgo,sqlite_omit_load_extension" -ldflags "-linkmode external
  -extldflags -static"`; `file` reports "statically linked" and `ldd`
  "not a dynamic executable", checked by a packaging test.
- goreleaser gets one build per architecture with its `CC`; CI installs
  the toolchain; the race detector needs cgo anyway.
- Only the node links SQLite: runnables reach the store through the
  plugin protocol, so they and the SDK stay `CGO_ENABLED=0`.
- `govulncheck` does not see the C code: the SQLite version bundled by the
  pinned driver is checked against SQLite's own advisories at every bump
  (`make vuln` prints it).

## 5. Invariants

| ID | Invariant | Tests |
| :--- | :--- | :--- |
| INV-S-1 | Two nodes that applied the same entries hold databases with the same logical content (same digest). | property test; AC-S-6 |
| INV-S-2 | A batch with a stale epoch changes no database. | FSM unit; AC-S-2 |
| INV-S-3 | Each committed batch is applied exactly once to each node's database, across restarts and snapshot installs. | AC-S-3 |
| INV-S-4 | A runnable cannot read or write another runnable's database, or the reserved tables. | unit; AC-S-5 |

## 6. Threats

| ID | Threat | Mitigation | Residual |
| :--- | :--- | :--- | :--- |
| T-S-1 | (D) A batch that never ends stops the FSM on every node | STO-7 deterministic step budget | — |
| T-S-2 | (D) A runnable fills the disk of every node | STO-8, parent §9.3 compaction | — |
| T-S-3 | (T) A non-deterministic statement makes replicas diverge | STO-4 authorizer, STO-5, INV-S-1 digests | R-S-1 |
| T-S-4 | (I) The store holds application data in clear on every node and in every snapshot | parent SEC-7; documentation | parent R-2 |
| T-S-5 | (E) SQL reaches files or extensions (`ATTACH`, `load_extension`) | STO-4; extensions not compiled in | — |
| T-S-6 | (T) SQL injection in a runnable | parameters only in the SDK's helpers; the runnable's responsibility | — |
| T-S-7 | (I) An administrator reads application data with `store query` | admin group only (parent SEC-6), logged | R-S-2 |
| T-S-8 | (T) An administrator changes application data, or an attacker with admin access does | admin group only; every operator batch is in the Raft log with its caller and in the audit log; `--dry-run` | R-S-2 |
| T-S-9 | (T) A bug or a vulnerability in the bundled C SQLite | pinned version, checked against SQLite's advisories at every bump (§4.1); SQL comes only from the runnable and administrators | — |

- **R-S-1** A SQLite behaviour that is non-deterministic in a way the
  authorizer does not see would make replicas diverge; the digests of
  STO-15 detect it, and the soak checks them.
- **R-S-2** Administrators can read and change the store, as they could
  change the files; the Raft log and the audit log record who did.

## 7. External assumptions (spike, parent chunk C0)

| ID | Assumption |
| :--- | :--- |
| EXT-S-1 | `mattn/go-sqlite3` exposes an authorizer (`RegisterAuthorizer`) that sees every function call and pragma |
| EXT-S-2 | A progress handler can be installed on the driver's connections (the driver, or a small cgo shim on the same bundled SQLite); the number of callbacks for a statement on the same data is the same on every node. If neither works, the fallback is S-1's alternative |
| EXT-S-3 | A WAL read transaction gives a consistent copy (backup API or `VACUUM INTO`) while another connection commits |
| EXT-S-4 | hashicorp/raft applies entries after the restored snapshot again on restart (the reason for STO-3), and offers no hook that makes STO-3 unnecessary |
| EXT-S-5 | Throughput on the lab: batches of 10 small rows, 3 nodes, LAN: at least 500 batches/s; on the WAN profile (parent §8.7), at least 25 sequential and 300 concurrent batches/s |

## 8. Acceptance scenarios

| ID | Scenario | Pass when |
| :--- | :--- | :--- |
| AC-S-1 | A runnable migrates from nothing to version 3, then a newer binary to version 5 | each step applied once; `_schema` = 5 on every node |
| AC-S-2 | `SIGSTOP` the leader during writes, then resume it (parent AC-5) | its batches return `ErrFenced`; no row of the old epoch after the new incarnation's first batch |
| AC-S-3 | `kill -9` a node between snapshots, restart it | no batch applied twice (row counts, digests) |
| AC-S-4 | A batch with `random()`, `datetime('now')`, `ATTACH`, a runaway CTE, an oversized result | refused or aborted identically on every node; the FSM keeps going |
| AC-S-5 | Two runnables on one cluster | neither sees the other's tables; `_resurgo_*` cannot be written |
| AC-S-6 | Overnight soak with a writing runnable (parent AC-13) | digests equal on all nodes at every check |
| AC-S-7 | Failover in the middle of a chunked backfill | it resumes from the recorded progress, every chunk applied once |
| AC-S-8 | An older binary is pinned on one node and becomes leader | it refuses to start on the newer schema; leadership moves (parent FR-8) |
| AC-S-9 | `store exec` while the runnable writes | the runnable's next batch fails with `ErrFenced`; it restarts with a newer epoch and sees the operator's change; the entry names the caller |
| AC-S-10 | The release binaries | statically linked (`file`, `ldd`); runnables built with `CGO_ENABLED=0` |

## 9. Open issues

- **OI-S-1** *Decided* (2026-11-25): `mattn/go-sqlite3`, cgo, linked
  statically (S-1, §4.1).
- **OI-S-2** *Decided* (2026-11-25): the default limits stand: 256 MiB per
  database, 1 MiB batches, 50 M VM steps, 30 s queries.
- **OI-S-3** *Decided* (2026-11-25): operators can inspect and modify the
  store through the log and the FSM (STO-16, STO-17).
