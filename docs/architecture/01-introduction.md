# Introduction

drun runs a runnable on the leader of a Raft cluster and nowhere else,
and runs it again on every new leader. This guide explains how: the
replicated state machine, the cluster port, the stores, the plugin
protocol, the supervisor and the admin API, then how to write a runnable
and how to fence state outside drun. The specification
(`.specs/drun_specification.md` and `.specs/drun-store.md` in the
source) holds the requirements, decisions and invariants these chapters
refer to by identifier (FR-, SEC-, INV-, STO-).

![The components of a node](components.pdf)

## The guarantees, and what they rest on

| Guarantee | Mechanism |
| :--- | :--- |
| At most one incarnation acts at a time | It starts only on the leader, after its `Incarnate` is committed; it holds a lease on `CLOCK_BOOTTIME`, enforced inside its own process, renewed only after the leader confirms its leadership; a new leader waits the takeover delay unless the predecessor was released |
| A stale incarnation cannot change state | Every write carries the epoch; the state machine refuses an older one in the step that would apply it |
| Progress survives any node | Checkpoints and the stores are replicated through the log |
| The same result on every node | The state machine reads no clock and no randomness; the stores refuse non-deterministic SQL |

The lease bounds how long a cut-off leader's runnable can go on; the
epoch makes the rare overlap that remains harmless to drun's own state
and, with the fence of *Fencing external state*, to a database outside it.
