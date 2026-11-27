# Introduction

drun runs a program, a *runnable*, on exactly one node of a small
cluster: the leader of a Raft group. When the leader fails, is cut off or
is stopped, another node becomes leader and starts the runnable again
there, from the last state the runnable saved. It is meant for the jobs
that must run once and only once, somewhere: a scheduler, a poller, a
queue consumer that must not run twice, the writer of a single-writer
database.

What a runnable gets:

- **One incarnation at a time.** A runnable runs only on the leader, only
  after its start is committed by a majority of the nodes. A node that
  loses leadership, or cannot confirm it, stops its runnable within a
  bounded time, and the next leader waits until that time has passed.
- **An epoch.** Every incarnation has a number larger than every earlier
  one's. A runnable that also writes to an external database uses it to
  refuse writes from an older incarnation, should one still be running.
- **Checkpoints.** A runnable saves small state through the cluster; the
  next incarnation, on any node, starts from the last one acknowledged.
- **A replicated SQL database.** Each runnable has its own SQLite
  database, replicated to every node and fenced by the epoch.

A cluster has three or five voting nodes (one per failure domain), on one
LAN or across three regions. The runnable is a separate executable built
with drun's Go SDK; two examples ship with drun.

This guide installs a cluster and runs the example ticker on it. The
*Operator's Guide* covers running it day to day; the *Architecture and
Design Guide* explains how it works and how to write a runnable.

## What you need

- Three (or five) Linux hosts, amd64 or arm64, with systemd, that can
  reach each other on one TCP port (7350 by default).
- A certificate for each node, from a CA of your own (see *Certificates
  and identities*).
- Root on each host, to install the packages.
