# Troubleshooting

## No leader

`drun cluster show` shows no leader, or the term keeps growing.

- Fewer than a majority of the voters are up or reachable: start them,
  or fix the network. Port 7350 (`cluster.bind`) must be open both ways.
- The certificates are refused: the journal shows `handshake refused`.
  Check that each certificate is signed by `cluster.tls.ca`, is valid for
  both client and server authentication, and carries exactly the
  identity in `cluster.tls.peers` (*Certificates and identities*).
- The clocks do not matter to drun; the timings do: all nodes need the
  same `raft` and `lease` settings.

## A runnable does not run

`drun runnable show NAME`, on the leader and on the other nodes:

| What it says | Meaning, and what to do |
| :--- | :--- |
| `disabled` | An operator disabled it: `drun runnable enable NAME` |
| `completed` | A task that finished: `drun runnable reset NAME --completion` to run it again |
| `waiting` | The takeover delay after a failover, or the back-off after a failure |
| `invalid on NODE` | That node cannot run it: wrong digest, a manifest that fails (the step is in the error: ownership, signature, name, version, protocol), a binary not owned by root, a name or kind that differs, a configuration it refuses. The journal has the error. The node gives leadership away when it is leader |
| `drift on NODE` | That node's binary or configuration differs from the running incarnation's: align the configurations |
| failures | It keeps failing: the reasons are in `runnable show NAME`; after `restart.handover_after` failures the leader moves to another node |

## Store digests differ

`drun store show NAME` flags `MISMATCH` when two members at the same
index hold different data. That must not happen; if it does, the
minority's copy is wrong: rebuild that node (*Maintenance*) and report a
bug with its journal.

## A command fails with "retry"

Leadership moved while the command travelled to the leader, or there is
no leader yet: try again in a few seconds.

## Permission denied on the socket

Only root and the members of `admin.allowed_groups` may use the socket;
a user added to `drun-admin` must log in again for it to count.
