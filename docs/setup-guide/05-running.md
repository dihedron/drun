# Running a node

`drun serve --config /etc/drun/drun.yaml` runs a node (the
packaged systemd unit does this). At start the node:

1. joins or creates the cluster;
2. checks every runnable: the binary matches its `sha256`, or the
   digest of its manifest signed by its `key`, it and its
   directory are root's and writable by root only, it describes itself
   with the configured name and kind, and it accepts its configuration.
   A runnable that fails is reported at `error` level and in `drun
   runnable show`; the node still joins the cluster, and gives leadership
   away if it becomes leader (it cannot run the runnable);
3. opens the admin socket, and the remote endpoint if configured.

On `SIGTERM` (`systemctl stop`) or `SIGINT` the leader stops its
runnables within their `shutdown_grace`, records that they stopped, hands
leadership to another voter and exits; a follower exits at once. It
logs `stopped` with the time the stop `took`. A stop is bounded by
`max(10 s, shutdown_grace + 10 s)`, below the unit's `TimeoutStopSec`
(90 s): a node whose stop outlives it logs `stop timed out` and exits
with status 75 (*Operator's Guide*, Troubleshooting). Status 70 means the
node stopped itself on a store failure.

The node logs at `info` to stderr (the journal) unless
`DRUN_LOG_LEVEL` says otherwise (`debug`, `info`, `warn`, `error`,
`off`).

Check the node with `drun cluster show` (as root, or as a member of
`admin.allowed_groups`).
