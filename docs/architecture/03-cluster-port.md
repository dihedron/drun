# The cluster port

Raft and the admin calls forwarded to the leader share one TCP port
(`internal/raftnode`).

## Connections

1. TLS 1.3 handshake, mutual. The peer's chain is checked against the
   current CA file, and its certificate must carry exactly one known
   identity: that gives the peer's server ID. Identities come from
   `cluster.tls.peers` and from the `SetPeer` entries the node has applied;
   for the same ID, the configured one wins.
2. The first byte says what the connection carries: `R` for Raft, `A`
   for a forwarded admin call, `P` for a probe. Any other byte closes the
   connection.
3. `R` connections go to hashicorp/raft's `NetworkTransport`; `A`
   connections to the admin server, with the peer's ID attached; a `P`
   connection is answered one byte, whether this node has Raft state, and
   closed.

## Bootstrapping

A node with no Raft state that is one of the configured voters probes the
other configured voters (in parallel, for at most 3 s) before it
bootstraps. If any has state, the cluster exists and this node was wiped:
bootstrapping would write a first log entry that, if `cluster.voters`
changed since the cluster was created, differs from the cluster's at the
same index and term, which Raft cannot detect. It waits instead, with no
configuration: it grants votes and starts no election until the leader
replicates to it. Nodes that all lack state cannot tell a lost cluster
from a new one, and bootstrap.

When a node dials, it looks up the server ID of the address in the
cluster's configuration and checks that the server presents that ID's
identity.

## Identity binding

`NetworkTransport` does not tell raft which connection a request came
from, so on its own a valid certificate could send requests in another
server's name. Every `R` connection therefore passes through a gate that
reads each request before raft does: it decodes the request type and
header with raft's own msgpack codec, checks that the header's server ID
is the connection's and that its address belongs to that server, and only
then hands the request's bytes to raft, unchanged. The decoder reads one
byte at a time from a buffered reader, so it never reads into the next
request. `InstallSnapshot`'s snapshot bytes, which follow the request,
pass through as they are. A request in another name, or one that does
not decode, closes the connection before raft sees any of it.

## Storage

`data_dir/raft/raft.db` holds the log and the stable store (bbolt, through
raft-boltdb/v2); `data_dir/raft/snapshots` the last two snapshots.
`data_dir` is `0700`.
