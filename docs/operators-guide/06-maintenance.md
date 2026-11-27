# Maintenance

## Restarting nodes

Restart one node at a time, followers first, the leader last, waiting for
each to rejoin (`resurgo cluster show` from another node lists it with a
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
resurgo member add node-d 10.0.1.14:7350 --identity spiffe://example.org/resurgo/node-d
resurgo member remove node-a
```

The new node's configuration lists the current members in
`cluster.tls.peers`, and in `cluster.voters` the current voters but not
itself: a node that has no data and finds itself in `cluster.voters`
creates a new cluster instead of waiting to be added.

## Rebuilding a node

A node whose data is lost or damaged is rebuilt from the others:

1. `resurgo member remove node-a` (from another node);
2. on `node-a`: `systemctl stop resurgo`, then empty
   `/var/lib/resurgo`;
3. remove `node-a` from its own `cluster.voters` (see above), and start
   it;
4. `resurgo member add node-a 10.0.1.11:7350` from another node.

The leader sends it a snapshot and the log since: state, checkpoints and
stores.

## Backups

The cluster keeps every node's data identical, so a backup protects
against mistakes (a bad `store exec`, a runnable that corrupts its data),
not against the loss of a node.

- A runnable's store: `resurgo store backup NAME FILE` writes a
  consistent copy, a plain SQLite database, from the node called.
- A whole node: stop it, copy `/var/lib/resurgo`, start it.

To put a backed-up store back, hold the runnable and replace its content
with an operator batch (`store exec --disable`, then `runnable enable`),
or reset it (`store reset`) and let the runnable start empty.

## Disk

Each node keeps the Raft log, the last two snapshots and the stores
under `/var/lib/resurgo`. A snapshot is taken every 512 log entries and
keeps the last 256 (`raft.snapshot_threshold`, `raft.trailing_logs`); it
holds every runnable's store, so allow at least three times the stores'
total size.
