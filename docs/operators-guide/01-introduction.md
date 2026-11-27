# Introduction

This guide is for the people who run a resurgo cluster: what happens when
nodes fail, the command line, metrics, maintenance and what to do when
something looks wrong. The *Setup Guide* installs a cluster; the
*Architecture and Design Guide* explains the design and how to write a
runnable.

## In one page

- A cluster is three or five nodes; one of them is the **leader**, and
  the runnables run there and nowhere else.
- Each start of a runnable is an **incarnation** with an **epoch**,
  larger than every earlier one's.
- A leader that fails is replaced within seconds; a leader that is
  stopped hands over at once (*Failover, handover and failures*).
- A runnable saves its progress as **checkpoints** and may keep data in
  its own replicated **store**; both survive any node.
- Every command (`resurgo ...`) talks to the local node; commands that
  change something run on the leader wherever you type them.
- Nothing needs a quorum but changes: with one node of three down,
  everything works; with two down, the runnables stop until a majority is
  back.
