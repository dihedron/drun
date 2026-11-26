# Certificates and identities

The nodes talk over one TCP port with TLS 1.3 and mutual authentication;
there is no plaintext mode.

## What each certificate needs

- Signed by the CA in `cluster.tls.ca`.
- Usable for both client and server authentication (extended key usages
  `serverAuth` and `clientAuth`).
- The node's identity, exactly as written in `cluster.tls.peers`:
  - a SPIFFE ID such as `spiffe://example.org/resurgo/node-a`, as a URI
    subject alternative name; or
  - a DNS name such as `node-a.resurgo.example.org`, as a DNS subject
    alternative name.
- The identity of one node only: a certificate that carries the
  identities of two nodes is refused.

A node accepts a connection only from a certificate whose identity is in
`cluster.tls.peers`, and then only Raft requests sent in the name of the
node that identity belongs to: one node's certificate cannot speak for
another. When a node connects to a member, it checks that the member
presents the identity configured for that member's ID.

## Adding a member

`cluster.tls.peers` is read when the node starts. Before adding a
member, add its identity to `cluster.tls.peers` on every node and restart
them one at a time; a member whose identity a node does not know cannot
connect to it.

## Rotation

Replace the certificate, key or CA files in place. Each node checks them
at every new connection and loads them when one changed. If the new files
do not load (for example, the key was replaced before the certificate),
the node keeps using the previous ones and logs a warning once; it picks
up the files as soon as they load.

To change the CA, first distribute a CA file that holds both the old and
the new CA, then the new certificates, then a CA file with the new CA
only.
