# The state machine

Every node holds the same replicated state machine (`internal/fsm`), fed by
the Raft log. It does little: it hands out fencing epochs, records each
runnable's current incarnation, checkpoint, failures and completion, and
passes store batches to the runnable's SQL store.

## State

| Field | Meaning |
| :--- | :--- |
| epoch counter | The last epoch handed out, cluster-wide |
| current incarnation | Epoch, Raft term, node, start time, binary and configuration digests |
| released | The current incarnation stopped cleanly |
| checkpoint | Sequence number, epoch, commit time and size; the data is in the store |
| store limits | Batch size, steps, database size, result size, as replicated by `Incarnate` |
| completed | For a task: the epoch that completed it, and when |
| disabled | Set by an operator |
| failures | Per node: consecutive failures, last time, last reason |

## Commands

Each log entry is one protobuf `Command` (`proto/fsm/v1/fsm.proto`) with a
format version. A node refuses an entry it cannot decode, an unknown
command or a newer format version, with an error result: mixed versions
during a rolling upgrade fail safe.

| Command | Effect | Refused when |
| :--- | :--- | :--- |
| `Incarnate` | Next epoch; new current incarnation, not released | Disabled, or a completed task |
| `Checkpoint` | Next sequence number; data to the store; clears the node's failures | Stale epoch or released (fenced); more than 1 MiB |
| `StoreBatch` | Applied by the store, in one transaction | Stale epoch or released (fenced) |
| `Release` | Released; clears the node's failures | Stale epoch |
| `Complete` | Completed and released | Stale epoch |
| `Fail` | One more consecutive failure for the node | Stale epoch |
| `SetDisabled` | Disabled or enabled | — |
| `Reset` | Clears the checkpoint, completion or failures | Unknown runnable |
| `Forget` | Removes the runnable and its database | Unknown runnable; an incarnation not released |

## Determinism

`Apply` reads no clock and draws no random numbers: the leader puts every
timestamp in the command. A refused command changes nothing, and a store
failure on one node (an I/O error, not a SQL error) leaves the state as it
was. The state is encoded with runnables sorted by name and failures by
node, so two nodes that applied the same entries write byte-identical
snapshots.

## Snapshots

A snapshot is the magic `RSGO`, the format version, the length of the
encoded state, the state, and then the store's copy of every database,
taken while applies are stopped and written while they run again. A
restore refuses a snapshot that is not resurgo's, is newer than the node,
or is inconsistent (a runnable twice, an epoch beyond the counter), and
then keeps the state it had.
