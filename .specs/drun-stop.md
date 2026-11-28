# drun: a bounded, diagnosable node stop (FW-11)

- **Document Version:** `0.2.0` (2026-11-28): approved with decisions D-1 to D-3, exit status 75, a leader's handover within the grace; `0.1.0` (2026-11-28): first draft
- **Status:** approved 2026-11-28 (`AGENTS.md` rule 1)
- **Parent:** `drun_specification.md` (FR-12 planned handover, INV-5,
  §10 operations); found by the lab (`drun-lab.md`), chaos run
  `20261128-033051`.
- **Identifiers:** `STOP-nn` requirements, `T-STOP-nn` threats,
  `AC-STOP-nn` scenarios

## 1. The problem

In 1 of 26 graceful restarts, `drun-b2` received `SIGTERM`, logged
`stopping: handing leadership over` (`cmd/drun/serve.go`), and never logged
`stopped`. After `TimeoutStopSec=90` systemd killed it. The node was a
follower: it had lost leadership 24 s before (`leadership lost while
committing log`), had stood as a candidate 16 s before, and had then gone
back to follower. The cluster kept serving, because `drun-a2` led
throughout, so this was not an outage. But a planned restart or upgrade
of that node took 90 s instead of about 1 s, and an operator gets no
evidence of why.

The stop path (`Daemon.Stop`) runs under a grace context of
`max(10 s, shutdown_grace + 10 s)`, but only some of its steps honour it:

| Step | Bounded by the grace? |
| :--- | :--- |
| `Supervisor.Shutdown` (returns at once on a follower) | yes |
| `Admin.Stop`, metrics `Shutdown` | yes |
| `d.done.Wait()`: `Supervisor.Run` (its `lose()` waits for every controller; `lead()` runs a 5 s barrier) and `watchDrift` | **no** |
| `Node.Shutdown`: raft's `Shutdown().Error()` | **no** |
| `Store.Close` | **no** |

No goroutine dump exists for the incident, so it is not known which of
these hung. Guessing would be "coding blind". This specification does two
things. It bounds the stop, so a node always exits within its grace. It
also makes the next hang diagnose itself, and then the root cause is
fixed.

## 2. Requirements

- **STOP-1 (bounded stop)** After `SIGTERM`, the node process MUST exit
  within its grace (`max(10 s, max shutdown_grace + 10 s)`), whatever any
  step of the stop does. The grace stays below the unit's
  `TimeoutStopSec` (90 s), so systemd never has to kill the node.
- **STOP-2 (diagnosis)** When the grace expires before the stop has
  finished, the node MUST log, at `ERROR`:
  - which step it was in (`stop_step`: `handover`, `admin`, `metrics`,
    `supervisor`, `raft`, `store`);
  - the stacks of all goroutines (`runtime.Stack(all)`), as one
    multi-line attribute.

  It then exits with status **75** (`EX_TEMPFAIL`, D-2), which is
  distinct from a configuration error (1) and a store divergence (70),
  so alerting can tell a timed-out stop apart. An exit without
  `Store.Close` and raft's `Shutdown` is no worse than the `SIGKILL`
  systemd would send. The raft WAL, the snapshots and the SQLite stores in
  WAL mode are crash-safe, and the next start recovers them, as after
  `kill`.
- **STOP-3 (each step logged)** The stop MUST log each step at `DEBUG`,
  and its total time at `INFO` (`stopped`, `took`), so that a slow step is
  visible even when the stop does not time out.
- **STOP-4 (root cause)** The step that hung in the lab MUST be found
  (§4 step 2) and fixed so that it honours the grace on its own. A
  regression test MUST reproduce it. STOP-1 is a backstop: it does not
  stand in for the fix.
- **STOP-5 (`SIGQUIT`)** The Operator's Guide documents
  `systemctl kill -s QUIT drun`: Go's runtime prints every goroutine's
  stack to the journal and exits. It is the manual way to diagnose a node
  that hangs outside a stop.

FR-12 does not change. A leader still stops its incarnations within
`shutdown_grace`, releases them, and then transfers leadership. STOP-1
only bounds what comes after. A leader whose handover is still running
when the grace ends is not released first. Its handover already had
`shutdown_grace`, and an exit without a release is an unplanned failover:
the next leader waits the takeover delay (FR-13), as after a crash.

## 3. Design

- `cmd/drun/serve.go` runs `d.Stop(sctx)` in a goroutine and waits for it
  or for `sctx`. When `sctx` expires first, it writes the STOP-2 record and
  exits with status 75, as the store-divergence path exits with 70.
- `Daemon.Stop` keeps the current step in an atomic value (STOP-2,
  STOP-3).
- The fix for STOP-4 depends on §4 step 2. The likely shapes are:
  `lose()` waiting on a controller stuck in a call that ignores its
  context (an `apply`, a plugin `Load` or `Start`, `VerifyLeader`); or
  raft's `Shutdown` waiting on a goroutine of its own. A hang inside
  hashicorp/raft becomes an `EXT-nn` assumption, with a spike test, and is
  worked around in drun. raft is not patched.

## 4. Steps

Decisions (2026-11-28):

- **D-1:** steps 1 and 2 ship first, as v0.2.3; the root-cause fix
  follows in its own release.
- **D-2:** a timed-out stop exits with status 75.
- **D-3:** the lab reproduction (step 2) runs right away, in the day; it
  takes about one hour, not a night (rule 14).

1. STOP-1, STOP-2, STOP-3 and STOP-5, with unit tests (AC-STOP-1,
   AC-STOP-2). Release v0.2.3 (D-1).
2. Reproduce the hang on the lab with a new chaos fault,
   `restart-after-loss`: isolate the leader for 10–20 s, heal, and restart
   that node within 30 s. Run it in a loop for one hour, about 100
   iterations. The STOP-2 dump names the hanging wait.
3. Fix that wait (STOP-4), with a regression test (AC-STOP-3). Lab: the
   loop again, 100 iterations, no stop longer than 5 s.
4. Add the `restart-after-loss` fault to the campaign at weight 3, and
   check every graceful stop's duration from the journal (`stopped`,
   `took`). A stop beyond its grace is a liveness failure.

## 5. Acceptance

| ID | Scenario | Expected |
| :--- | :--- | :--- |
| AC-STOP-1 | A daemon whose stop step blocks forever (a test hook) gets `SIGTERM` | it exits within the grace with status 75; the log has `stop_step` and the goroutine stacks, including the blocked one |
| AC-STOP-2 | A normal stop, leader and follower | `stopped` with `took`, no `ERROR`; FR-12 handover unchanged (existing tests) |
| AC-STOP-3 | The reproduced hang (§4 step 2), as a test | the stop finishes within the grace without the STOP-1 backstop |
| AC-STOP-4 | Lab: 100 × `restart-after-loss` | every stop below 5 s; no `stop timed out` |

## 6. Threat model

| ID | Threat | Mitigation |
| :--- | :--- | :--- |
| T-STOP-1 | (I) The goroutine dump in the journal leaks a secret | Go stacks hold function names, file:line, and arguments as raw words (pointers, integers), never the contents of strings or buffers. Key material, the DSN and the admin token are not printed. The journal is already readable only by root and `adm`. |
| T-STOP-2 | (T) An exit before `Store.Close` corrupts a store | The same as `SIGKILL`, which already happens today after 90 s: SQLite WAL and raft-wal are crash-safe. The applied index lives in each database (INV-S), so replay is exactly once. |
| T-STOP-3 | (D) A shorter forced exit cuts a leader's handover short | The grace is at least `shutdown_grace + 10 s`, which FR-12's handover already fits in. A forced exit of a leader is no worse than a crash, which is an unplanned failover the cluster handles anyway. |
| T-STOP-4 | (E) A runnable process outlives the node | `KillMode=control-group` (INV-5) kills it with the unit, as now. |

## 7. Out of scope

Hangs outside a stop, such as a node that stops serving while running.
STOP-5 covers their diagnosis by hand, and the lab's liveness checks cover
their detection.
