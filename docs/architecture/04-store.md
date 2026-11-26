# The store

Every runnable has its own SQLite database, replicated through the Raft
log (`internal/store`): `data_dir/store/<runnable>.db`, `0600`, in WAL
mode. Only the FSM writes to it, one batch per log entry.

## Reserved tables

| Table | Holds |
| :--- | :--- |
| `_resurgo_meta` | `last_index`: the index of the last entry applied to this database |
| `_resurgo_checkpoint` | The runnable's checkpoint: sequence number, epoch, commit time, data |

A runnable can neither read nor write them.

## Exactly once

Every change runs in one `BEGIN IMMEDIATE` transaction that first reads
`last_index`, skips the entry if it is not newer, and writes the entry's
index with the change. Raft replays the log after the last snapshot when a
node restarts: entries the database already holds are skipped.

## Determinism

The same batch must have the same effect on every node:

- **Authorizer.** While runnable SQL runs, SQLite's authorizer refuses
  `random`, `randomblob`, `current_*` and `load_extension`; `ATTACH`,
  `DETACH` (and so `VACUUM`); transactions and savepoints (a batch is the
  transaction); temporary objects; every `PRAGMA` but the schema reads and
  `user_version`; and every `_resurgo_*` table.
- **Time.** `date`, `time`, `datetime`, `julianday`, `unixepoch`,
  `strftime` and `timediff` are replaced on every connection by versions
  that refuse `'now'`, a missing time value and the `localtime` and `utc`
  modifiers, and otherwise return SQLite's own results. The batch's time
  and epoch, set by the leader, are `resurgo_now()` and `resurgo_epoch()`.
- **Work.** A progress handler counts virtual machine steps and aborts a
  batch beyond `store.max_steps`; the count depends only on the statement
  and the data, so every node aborts it at the same point.
- **Size.** `max_page_count` keeps the database within `store.max_size`;
  results are bounded by `store.max_result`.
- **One statement.** A statement holding several statements is refused.

A failure of the batch (SQL error, expectation, limit, refusal) rolls it
back and is returned the same on every node. A failure of the node (I/O,
corruption, a full disk) stops the node instead: carrying on would leave
its copy behind the others'.

The progress handler and the statement count use SQLite's C API through a
small cgo shim on the SQLite that `mattn/go-sqlite3` bundles; the driver's
connection handle is read by reflection and checked against the pinned
driver's layout.

## Reads

Queries, checkpoints and digests use a separate read-only connection, so
they never see an apply's uncommitted changes. A digest is a SHA-256 of the
schema and of every table's rows in a fixed order, with the index it is
at: equal on every node at the same index.

## Snapshots

A snapshot carries a copy of every database, made with `VACUUM INTO` while
applies continue. A copy may be later than the FSM state of the same
snapshot; it carries its own `last_index`, so the entries it already holds
are skipped when replayed. A restore reads every database into temporary
files first (a bad snapshot changes nothing), keeps a local database that
is at least as recent as the snapshot's copy, replaces the others, and
removes databases the snapshot does not have.
