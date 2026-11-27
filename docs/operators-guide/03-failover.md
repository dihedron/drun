# Failover, handover and failures

## When the leader fails

If the leader crashes, is cut off from the others, or is paused, its
runnables stop: the runnable's own lease expires within `lease.duration`
even if the node cannot act. The remaining nodes elect a leader, which
waits the runnable's `takeover_delay` (2.25 s by default on a LAN) before
starting a new incarnation from the last checkpoint. Expect about 5 s from
the failure to the new incarnation with the default timings.

## Planned handover

Stopping the service on the leader (`systemctl stop drun`) stops every
runnable within its `shutdown_grace` (30 s by default), records that it
stopped cleanly, and only then hands leadership over. The new leader
starts the runnables at once, without the takeover delay.

## A runnable that keeps failing

A runnable that fails is restarted on the same node after a growing
delay (1 s, 2 s, 4 s … up to 2 minutes by default). After 5 consecutive
failures on the leader (`restart.handover_after`), leadership moves to the
voter where the runnable has failed fewest times, in case the fault is
the node's own (a missing file, a full disk). When it has failed on every
node, it keeps retrying with the growing delay.

A task that completes is not run again, on any node, until it is reset.
