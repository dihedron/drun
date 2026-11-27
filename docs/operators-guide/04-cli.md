# The command line

Every command but `serve`, `version` and `completion` talks to a node's
admin API: the local socket by default, or another one with `--admin`:

| Option | Meaning |
| :--- | :--- |
| `--admin unix:///path` | A local socket (default `unix:///run/resurgo/admin.sock`) |
| `--admin tls://host:port` | A node's remote endpoint, with `--cert`, `--key` (your certificate) and `--ca` (the CA of the node's certificate); `--server-identity` checks the identity the node presents |
| `--json` | Print replies as JSON |
| `--timeout` | How long a call may take (default 90 s; a handover waits for the runnables to stop) |

Each option can also come from the environment: `RESURGO_ADMIN`,
`RESURGO_ADMIN_CERT`, `RESURGO_ADMIN_KEY`, `RESURGO_ADMIN_CA`,
`RESURGO_ADMIN_SERVER_IDENTITY`.

## Commands

| Command | What it does |
| :--- | :--- |
| `cluster show` | Members, their suffrage and identity, the leader, term, commit and applied index, last contact |
| `leader show` | The leader and its term |
| `leader transfer [--to ID]` | A planned handover: the runnables stop and are released, then leadership moves |
| `member add ID ADDRESS [--identity I] [--non-voter]` | Adds a member; `--identity` records its certificate identity, so no node needs a new configuration |
| `member remove ID` | Removes a member; removing the leader hands leadership over first |
| `runnable show [NAME]` | State, incarnation, epoch, health, checkpoint, failures; with a name, also the digests and this node's drift and validation |
| `runnable enable\|disable NAME` | Disabling stops the runnable and stays recorded across leader changes |
| `runnable restart NAME` | Stops the incarnation, releases it, starts a new one |
| `runnable reset NAME [--checkpoint] [--completion] [--failures]` | Clears the chosen parts; `--completion` lets a finished task run again |
| `checkpoint show NAME [--data]` | The checkpoint's sequence, epoch, size and age; `--data` writes its bytes to stdout |

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
was refused. Results are never logged.
