# Runnable binaries

A node starts a runnable's binary only if:

- its SHA-256 equals `sha256` in the runnable's configuration
  (`sha256sum /usr/libexec/resurgo/runnables/NAME`);
- the binary and its directory are owned by root and not writable by group
  or others (`chown root:root`, `chmod 0755` or stricter).

A new version of a runnable is a new binary and a new digest: install it,
then update `sha256` on every node. Until a node's configuration names
the new digest, that node refuses to start the new binary.

The runnable's process runs as the node's service user, in the node's
cgroup, and dies with the node. Its standard error appears in the node's
log as `runnable stderr` records, with the runnable's name and epoch.
