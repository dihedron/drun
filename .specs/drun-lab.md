# drun: the lab and the overnight chaos campaign

- **Status:** approved 2026-11-28 (build the lab, run overnight); as built below.
- **Parent:** `drun_specification.md` (§13 invariants, §14 acceptance,
  §8.7 WAN profile); rules 10, 14 and 15 of `AGENTS.md`.

## 1. Goals

1. A reproducible LXD lab for drun, built from the released packages
   (v0.2.0 and later), on which an unattended campaign can run for hours.
2. A chaos campaign that injects random faults for about six hours:
   crashes, pauses, partitions, latency, jitter and loss, leader rotation,
   demotion, promotion, and removal and re-addition of members. It also
   checks the invariants continuously, so that any split brain, lost
   write, diverged store or stuck failover is caught when it happens.
3. A morning report: what ran, what failed, failover times, and whether
   any invariant broke.

Out of scope: performance benchmarking (the numbers are recorded, not
judged), multi-host hardware, and FW-5 (runnables keep the service user).

## 2. The lab

Copied from veho's lab (`veho/test/lab`, `veho-lab.md`) and cut down to
what drun needs. The copied files keep a header that names their origin.

```
host (LXD 5.21+, KVM)
└── project drun-lab, bridge drun-lab 10.249.0.0/24, pool drun-lab
    └── VM "lxd" 10.249.0.20 (Ubuntu 24.04, 8 vCPU, 10 GiB, 60 GiB; inner LXD; the WAN)
        ├── bridge region-a 10.70.1.0/24 ── drun-a1 .31  drun-a2 .32
        ├── bridge region-b 10.70.2.0/24 ── drun-b1 .31  drun-b2 .32  pg-b .10
        └── bridge region-c 10.70.3.0/24 ── drun-c1 .31  drun-x .39 (spare)
```

- **Lab VM and inner containers** as in veho: lab-specific kernel
  settings (qdiscs, nftables) stay inside the VM; one disk snapshot
  captures the whole lab; `lab.sh reset` returns to the baseline.
- **Nodes:** five voters in three regions (2 + 2 + 1, the §8.7 placement),
  plus a spare `drun-x`. The spare is installed but not a member: the
  campaign adds and removes it. Each node is a system container, 1 GiB,
  running the `drun` and `drun-runnable-examples` packages from the
  release under test, installed from the `.deb` files and checked against
  `checksums.txt`. The runnables are `ticker` (stateless) and
  `fenced-counter` (stateful), configured with `key: drun`, so that the
  signed manifests (§8.8) are exercised too.
- **PKI:** a lab CA; each node a certificate with
  `spiffe://drun-lab/node/<id>` (SEC-2); an admin client certificate for
  the remote admin endpoint (SEC-11), which the campaign uses from the
  lab VM.
- **PostgreSQL 16** in `pg-b`, for the fenced-counter, reachable from
  every region across the WAN. The DSN file is in each node's
  `/etc/drun/fenced-counter/dsn`. It sits in one region on purpose: a
  partition of region B then also cuts the external store, as it would in
  production.
- **Metrics:** every node serves `/metrics` on its region address. The
  campaign scrapes them every 10 s into the state directory (a few MiB
  per hour); there is no Prometheus server.
- **WAN** (veho's `wan.sh`): per pair of regions a `netem` delay, jitter
  and loss on the source-classified band; partitions as nftables drops in
  table `inet drun_wan`. Profiles: `none`, `lan` (0.2 ms), `wan30`
  (15 ms one way, ±3 ms, the §8.7 RTT), `wan-bad` (40 ms ±20 ms, 1 %
  loss), and `--link X-Y=DELAY/JITTER/LOSS` by hand.

### 2.1 Interface: `test/lab/lab.sh`

| Command | Does |
| :--- | :--- |
| `preflight` | KVM, LXD 5.21+, free names and subnets, CPU, memory, disk |
| `up [--release vX.Y.Z]` | builds or completes the lab with that release (default: the latest), then takes the baseline snapshot |
| `down` | destroys everything the lab created, after confirmation |
| `status` | instances, `member show` from a voter, WAN profile, disk use |
| `wan show\|set PROFILE\|set --link …\|partition X[-Y]\|isolate NODE\|heal` | the WAN; `isolate` cuts one node from every other |
| `node start\|stop\|restart\|kill\|pause\|resume\|wipe NAME` | a node's service; `kill` is SIGKILL; `pause` is SIGSTOP on the whole cgroup (a gray failure); `wipe` stops it and empties `/var/lib/drun` |
| `drun NAME args…` | the drun CLI on a node |
| `pg kill\|start` | PostgreSQL |
| `snapshot`, `reset` | the baseline |
| `chaos run [--duration D] [--seed N] [--profile P]`, `chaos stop`, `chaos report` | §3 |

## 3. The chaos campaign

`test/lab/chaos` is a Go program on the host that drives `lab.sh` (the
CLI over `lxc exec`, metrics with one `curl` per node from the lab VM),
run under `systemd-run --user --unit drun-chaos`. It survives the operator's session ending, and every run is
reproducible from its seed. It loops: pick a fault by weight, apply it,
hold it for a random time, heal, wait for recovery, and check.

### 3.1 Faults

| Fault | Weight | Hold | Notes |
| :--- | :--- | :--- | :--- |
| SIGKILL the leader | 8 | until restart, 5–60 s | unplanned failover; restart by systemd |
| SIGKILL a follower | 6 | 5–60 s | |
| SIGSTOP the leader (cgroup) | 6 | 1–20 s | gray failure: the old leader wakes up behind a new one; its runnable must be fenced |
| SIGSTOP a follower | 3 | 1–20 s | |
| Graceful restart of a random node | 4 | — | planned handover when it is the leader (FR-12) |
| `leader transfer --to` a random voter | 8 | — | leader rotation |
| `leader transfer` (any) | 3 | — | |
| Isolate the leader | 6 | 2–30 s | minority leader: it steps down, its runnable is fenced |
| Partition a region (both ways) | 5 | 5–60 s | region B also cuts PostgreSQL |
| Asymmetric cut (one direction) | 3 | 5–30 s | nftables on one direction only |
| Change the WAN profile | 6 | 2–10 min | `lan`, `wan30`, `wan-bad`, random links; jitter to ±50 % |
| Loss burst 5–20 % on one link | 3 | 10–60 s | |
| `member demote` a random voter, then `member promote` | 4 | 30–120 s | never below three voters |
| `member remove` a follower, wipe it, `member add` it (gated) | 3 | — | a rebuild (FR-1, FR-17) |
| Add the spare as a voter, remove another later | 2 | 10–30 min | five voters stay five |
| `runnable restart` | 3 | — | |
| `runnable disable` then `enable` | 2 | 10–60 s | |
| `store exec` an operator batch on fenced-counter's store | 1 | — | STO-17 |
| PostgreSQL killed | 2 | 10–60 s | the runnable fails and backs off, it does not run twice |
| Two faults at once | 5 | as above | any two compatible ones, overlapping |

Guards: at most one region partition at a time; never more than two
voters down at once (quorum of five); no membership change while another
is in progress. The hold times are drawn from the seed.

### 3.2 Checks

Every 10 s, after every heal, and at the end:

| Check | Source | Failure means |
| :--- | :--- | :--- |
| At most one node reports a runnable `running` for each epoch, and no two `running` at once beyond the overlap window | every node's `drun_incarnation_state`, `runnable show` | a split brain (INV-1) |
| Epochs never go back, cluster-wide | `drun_epoch` across scrapes | INV-3 |
| fenced-counter: no row of an older epoch after a newer one | `drun-runnable-fenced-counter check` every 10 min and at the end | external state not fenced (§8.5) |
| Store digests equal at the same index | `store show` every 10 min | INV-S-1 |
| A healthy leader within 30 s of every heal (WAN: 45 s) | `leader show` | a stuck election |
| The runnables running within the failover budget after a heal (record: p50, p99, max) | metrics | liveness; the numbers go in the report |
| No node exits with status 70 (store divergence), no panic, no `fatal` | journals | a product bug |
| `member show` all healthy at every quiet point | the leader | membership stuck |
| Lab health: containers up, PostgreSQL up, VM disk below 80 % | `lab.sh status` | a lab problem, not a product one |

On a product failure the campaign stops injecting, heals everything,
saves the evidence (journals, `runnable show`, `store show`, metrics,
`/var/lib/drun` of every node, the seed and the fault log) under
`test/lab/.state/runs/<id>/`, and leaves the lab as it is for
investigation. A lab or tooling failure is logged, healed, and the
campaign goes on.

### 3.3 Schedule

| Time | Phase |
| :--- | :--- |
| 0:00–0:15 | baseline, profile `lan`, no faults: steady-state numbers |
| 0:15–2:15 | LAN, `raft.profile: lan`: every fault, the full weights |
| 2:15–2:30 | reconfigure every node to `raft.profile: wan` (rolling restart, itself a test) |
| 2:30–4:30 | WAN: profiles `wan30` and `wan-bad`, jitter, every fault |
| 4:30–5:45 | membership-heavy: demotion, promotion, rebuilds and the spare get three times their weight |
| 5:45–6:00 | heal everything, settle, the final checks, the report |

About **six hours** of campaign, plus 30 minutes to reset the lab and
check it beforehand.

### 3.4 Hourly checks (rule 15)

A scheduled check every hour during the run, one line each in the run
log and in the session. Each check asks:

- Is `drun-chaos` still running?
- How many faults have run, and are product or lab failures growing?
- Is every node up, and are all members healthy with a quorum?
- Is PostgreSQL up?
- What is the disk use of the VM and of every container?

On a product failure the run has already stopped (§3.2). The hourly
check decides whether to fix the bug and restart the campaign on the
fixed code, or to leave it for the morning.

## 4. Steps

1. **Daytime, about one working day:** this specification approved; the
   lab copied from veho and adapted (`lab.sh`, `lib/`); `lab.sh up
   --release v0.2.0`; the chaos program with a dry run of 30 minutes,
   one fault type at a time first, then the full mix; `make lint`, the
   chaos program's unit tests (the fault picker's guards, the checks on
   recorded scrapes).
2. **Evening:** `lab.sh reset`; `chaos run --duration 6h --seed <date>`
   started in an unattended session, with the hourly checks scheduled.
3. **Morning:** the report (`chaos report`): faults by type, failures,
   failover p50/p99/max for LAN and WAN, and the invariants. Published
   as a page, and its findings filed in the backlog.

## 5. Threats and limits

- The lab runs on this host's LXD in a project, network and pool of its
  own (`drun-lab`), tagged `user.drun-lab=yes`, and touches none of the
  other labs (ligo, veho).
- Containers share the VM's kernel and clock: there is no clock-step
  fault (it would need a VM per node).
- `netem` jitter reorders packets above the inter-packet gap; TCP absorbs
  it. Same limits as veho's lab.
- SIGSTOP on a cgroup also stops the runnable's process, as a frozen VM
  would.
