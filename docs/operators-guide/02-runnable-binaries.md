# Runnable binaries

A node starts a runnable's binary only if one of these holds:

- its SHA-256 equals `sha256` in the runnable's configuration
  (`sha256sum /usr/libexec/drun/runnables/NAME`);
- the runnable names a publisher's key (`key: acme`), and the binary's
  manifest, `NAME.manifest` next to it, is signed by a key in
  `/etc/drun/runnable-keys/acme.pub`, names the runnable, supports this
  drun version, and carries the binary's SHA-256.

The binary, its manifest and their directory, and the key directory and
file, must be owned by root and not writable by group or others
(`chown root:root`, `chmod 0755` or stricter).

A new version of a pinned runnable is a new binary and a new digest:
install it, then update `sha256` on every node. Until a node's
configuration names the new digest, that node refuses to start the new
binary. A runnable with a `key` needs no configuration change: its
package installs the new binary with its new manifest, and the node
picks it up when it restarts.

drun's example runnables are signed with drun's own key,
`/etc/drun/runnable-keys/drun.pub`, which the `drun` package installs;
configure them with `key: drun`. Their manifests support only the drun
release they came with: upgrade `drun` and `drun-runnable-examples`
together.

To trust another publisher, install their public key file as root:

```
install -o root -g root -m 0644 acme.pub /etc/drun/runnable-keys/acme.pub
```

A key signs only the runnables that name it, so adding one trusts
nothing until a runnable says `key: acme`. If you restore the key
directory or a manifest from a backup, an older signed binary may run
again within the drun versions its publisher signed for.

## Signing your own runnables

`tools/runnable-manifest` makes the keys and signs manifests; it needs
only Go:

```
go run github.com/dihedron/drun/tools/runnable-manifest@VERSION genkey acme.key > acme.pub
RUNNABLE_SIGNING_KEY="$(cat acme.key)" go run github.com/dihedron/drun/tools/runnable-manifest@VERSION \
  sign -name invoice-scheduler -version 1.2.0 -drun '[0.5.0,0.6.0)' ./invoice-scheduler
```

`-drun` is the range of drun versions the runnable supports, in interval
notation: `[` and `]` include the bound, `(` and `)` exclude it. Keep the
private key offline. Ship the binary, `NAME.manifest` and
`NAME.manifest.sig` together, owned by root.

The runnable's process runs as the node's service user, in the node's
cgroup, and dies with the node. Its standard error appears in the node's
log as `runnable stderr` records, with the runnable's name and epoch.
