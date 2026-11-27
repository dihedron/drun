# The command line

Every command but `serve`, `version` and `completion` talks to a node's
admin API: the local socket by default, or another one with `--admin`:

| Option | Meaning |
| :--- | :--- |
| `--admin unix:///path` | A local socket (default `unix:///run/drun/admin.sock`) |
| `--admin tls://host:port` | A node's remote endpoint, with `--cert`, `--key` (your certificate) and `--ca` (the CA of the node's certificate); `--server-identity` checks the identity the node presents |
| `--json` | Print replies as JSON |
| `--timeout` | How long a call may take (default 90 s; a handover waits for the runnables to stop) |

Each option can also come from the environment: `DRUN_ADMIN`,
`DRUN_ADMIN_CERT`, `DRUN_ADMIN_KEY`, `DRUN_ADMIN_CA`,
`DRUN_ADMIN_SERVER_IDENTITY`.

## Commands

| Command | What it does |
| :--- | :--- |
| `cluster show` | Members, their suffrage and identity, the leader, term, commit and applied index, last contact |
| `leader show` | The leader and its term |
| `leader transfer [--to ID]` | A planned handover: the runnables stop and are released, then leadership moves |
| `member show [--max-lag N]` | Every member's suffrage, lag, last contact and health, as the leader judges it, with the reason when unhealthy |
| `member add ID ADDRESS [--identity I] [--non-voter] [--stable D] [--max-lag N] [--force]` | Adds a member; `--identity` records its certificate identity, so no node needs a new configuration. A voter joins as a non-voter and is promoted once healthy (see *Promotion*) |
| `member promote ID [--stable D] [--max-lag N] [--force]` | Makes a non-voter a voter once it is healthy |
| `member demote ID` | Makes a voter a non-voter; demoting the leader hands leadership over first; the last voter cannot be demoted |
| `member remove ID` | Removes a member; removing the leader hands leadership over first |
| `runnable show [NAME]` | State, incarnation, epoch, health, checkpoint, failures; with a name, also the digests and this node's drift and validation |
| `runnable enable\|disable NAME` | Disabling stops the runnable and stays recorded across leader changes |
| `runnable restart NAME` | Stops the incarnation, releases it, starts a new one |
| `runnable reset NAME [--checkpoint] [--completion] [--failures]` | Clears the chosen parts; `--completion` lets a finished task run again |
| `checkpoint show NAME [--data]` | The checkpoint's sequence, epoch, size and age; `--data` writes its bytes to stdout |
| `store show [NAME]` | Size, tables and last index of the runnable's database on the node called, and every member's digest |
| `store query NAME SQL [--local]` | A read-only statement, on the leader; `--local` on the node called, flagged as possibly behind |
| `store exec NAME [SQL…] [--file F] [--disable] [--dry-run]` | An operator's batch, see below |
| `store backup NAME FILE` | A consistent copy of the database on the node called, written to FILE (`0600`) |
| `store reset NAME` | Drops the database, on every node; refused while the runnable runs |

## Promotion

A voter counts in the commit quorum from the moment it is one. A member
still catching up would slow every commit, or stop them if the cluster is
small. `member promote`, and `member add` of a voter, therefore promote
a member only once the leader finds it healthy. A member is healthy when
all four of these hold:

- it answers the leader over the cluster port;
- it follows this leader, in this term;
- it heard from the leader within `raft.heartbeat_timeout`;
- it is at most `--max-lag` entries (256) behind the leader's commit
  index.

The member must stay healthy, check after check (one a second), for
`--stable` (10 s). An unhealthy check starts the count again. If
`--timeout` (90 s) runs out first, the command fails with the last
reason, and the member stays a non-voter. `--force` promotes without
checking. `member show` gives the same judgement for every member.

After a promotion or a demotion, update `cluster.voters` in the nodes'
configuration. It is only read when a node has no data, but a later
rebuild should probe the right voters.

## Operator writes

`store exec` changes a runnable's database the way the runnable does:
through the log, under the same rules (no clock, no randomness, the same
limits), all statements or none. Statements are given as arguments or in
a file (`--file -` for stdin), separated by `;`. The leader:

1. stops the runnable and records that it stopped (as for a restart);
2. with `--disable`, disables it;
3. commits an incarnation of the operator's own (a new epoch, recorded
   with your identity), then the batch, then releases it;
4. starts the runnable again, at a newer epoch, seeing your changes
   (unless disabled: repair, then `runnable enable`).

`--dry-run` runs the batch on a copy of the leader's database in a
transaction that is rolled back, and shows the results; nothing is
committed. A schema change must keep the runnable's `_schema` version
consistent, or the runnable refuses to start.

`store show` lists a digest of the database's content per member. Two
members at the same index must have the same digest; a difference is
flagged `MISMATCH` and means a replica diverged: restore that node from a
snapshot (remove its `data_dir`, add it again).

## Where a command runs

`show` commands answer from the node you call, as far as it has applied
the log; `runnable show` adds what the leader's supervisor sees (state,
health). Every other command is executed by the leader: a follower
forwards it there over the cluster port. If leadership moves while a
command travels, the command fails with "retry" rather than running
twice.

## Drift and invalid runnables

Runnable configuration lives on each node. Each incarnation records the
digests of the binary and configuration it ran with; a node whose digests
differ logs a warning and `runnable show` marks the runnable "drift on"
that node. A node that cannot run a runnable at all (wrong digest, wrong
name, refused configuration) marks it "invalid on" that node and hands
leadership away if it is leader.

## Audit

Every command that changes something is logged by the leader at `info`,
with the caller (`uid=N` on the socket, the certificate identity on the
remote endpoint), the node it came from and the runnable, and whether it
was refused. Queries and batches are logged with their SQL; results are
never logged.
