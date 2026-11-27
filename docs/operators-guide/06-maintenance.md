# Maintenance

## Restarting nodes

Restart one node at a time, followers first, the leader last, waiting for
each to rejoin (`drun cluster show` from another node lists it with a
recent last contact) before the next. Restarting the leader is a planned
handover: its runnables stop, are released, and start on the new leader
without a takeover delay.

## Upgrades

Upgrade the packages one node at a time, as for a restart: followers
first, the leader last. Runnable binaries are upgraded the same way, each
with its new digest in every node's configuration (*Runnable binaries*).

Before version 1.0.0 a release may change the format of the Raft log,
snapshots or the admin API without keeping the old one: read the release
notes, and when they say so, stop every node, upgrade all of them, and
start them again.

## Replacing a member

To replace a host, add the new node and then remove the old one, so the
cluster never drops below its size:

```
drun member add node-d 10.0.1.14:7350 --identity spiffe://example.org/drun/node-d
drun member show
drun member remove node-a
```

`member add` returns once node-d is a healthy voter: it joins as a
non-voter, catches up, and is promoted after 10 s of healthy checks. If
it fails, node-d stays a non-voter: `member show` says why. Fix the
cause, then run `drun member promote node-d`. Remove node-a only after
node-d is a voter.

The new node's configuration lists the current members in
`cluster.tls.peers`. Before it creates a cluster, a node with no data asks
the voters in its `cluster.voters` whether one exists: if any answers, it
waits to be added instead.

## Rebuilding a node

A node whose data is lost or damaged is rebuilt from the others:

1. `systemctl stop drun` on the node, then empty `/var/lib/drun`;
2. `systemctl start drun`: finding that the other nodes hold the
   cluster, it waits, and the leader sends it a snapshot and the log since:
   state, checkpoints and stores;
3. only if it had been removed (`member remove`): `drun member add
   node-a 10.0.1.11:7350` from another node.

Rebuild nodes one at a time, or at least never a majority while the other
nodes are down: nodes that all lack data cannot tell a lost cluster from
a new one, and would create a new, empty one.

## Backups

The cluster keeps every node's data identical, so a backup protects
against mistakes (a bad `store exec`, a runnable that corrupts its data),
not against the loss of a node.

- A runnable's store: `drun store backup NAME FILE` writes a
  consistent copy, a plain SQLite database, from the node called.
- A whole node: stop it, copy `/var/lib/drun`, start it.

To put a backed-up store back, hold the runnable and replace its content
with an operator batch (`store exec --disable`, then `runnable enable`),
or reset it (`store reset`) and let the runnable start empty.

## Disk

Each node keeps the Raft log, the last two snapshots and the stores
under `/var/lib/drun`. A snapshot is taken every 512 log entries and
keeps the last 256 (`raft.snapshot_threshold`, `raft.trailing_logs`); it
holds every runnable's store, so allow at least three times the stores'
total size.
