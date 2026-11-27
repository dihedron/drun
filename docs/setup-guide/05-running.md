# Running a node

`resurgo serve --config /etc/resurgo/resurgo.yaml` runs a node (the
packaged systemd unit does this). At start the node:

1. joins or creates the cluster;
2. checks every runnable: the binary matches its `sha256`, it and its
   directory are root's and writable by root only, it describes itself
   with the configured name and kind, and it accepts its configuration.
   A runnable that fails is reported at `error` level and in `resurgo
   runnable show`; the node still joins the cluster, and gives leadership
   away if it becomes leader (it cannot run the runnable);
3. opens the admin socket, and the remote endpoint if configured.

On `SIGTERM` (`systemctl stop`) or `SIGINT` the leader stops its
runnables within their `shutdown_grace`, records that they stopped, hands
leadership to another voter and exits; a follower exits at once.

The node logs at `info` to stderr (the journal) unless
`RESURGO_LOG_LEVEL` says otherwise (`debug`, `info`, `warn`, `error`,
`off`).

Check the node with `resurgo cluster show` (as root, or as a member of
`admin.allowed_groups`).
