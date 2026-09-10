# Evidence bundle - RayJob driver resume via a durable progress ledger

This is the experimental record behind the REP. **Nothing here is proposed for merge.** It is pushed as a draft PR purely so that every number in the REP can be checked against the run that produced it.

## How to read it

| path | what it is |
|---|---|
| `claims.json` | the ledger - every claim, its refutation condition, status, evidence and full note history |
| `experiments/<claim>.md` | the **pre-registered** card for each claim: prediction, thresholds and what result would refute it, committed *before* the run |
| `runs/<timestamp>-<claim>/` | immutable run output - `results.json`, `stdout.log`, and `VOID.md`/`NOTE.md` where a run was discarded |
| `poc/` | the rig: stage A model checker, stage B cost model, stage C reference implementation on real Ray |
| `FINAL-REPORT.md` | conclusion, operating envelope, residual risks |
| `journal.md` | iteration-by-iteration narrative, including every wrong turn |

The design is referred to throughout as **the ledger design**. Some historical run logs and scratch paths under `runs/` still carry an earlier internal label (`r4`); those files are immutable evidence and are deliberately not rewritten.

## Method

Every claim carries a **refutation condition**. Every experiment was **pre-registered** - prediction and thresholds committed to git before the run - and every run carries **negative controls** that had to fail as expected, or the run was discarded.

**5 of 35 runs are retained as void or partial**, each with its diagnosis. Two of them produced better answers than the experiments they invalidated. They are kept because a rig that can only be trusted when it agrees with you cannot be trusted at all.

## Ledger

| status | count |
|---|---|
| proven | 17 |
| refuted | 11 |
| accepted risk | 4 |
| **total** | **32** |

Bold status = load-bearing claim.

| id | status | goal | claim | evidence |
|---|---|---|---|---|
| C1 | **proven** | G2 | The M1 epoch protocol (absent-key CAS + read-back + epoch-scoped immutable state keys) admits exactly one legitimate writer per epoch, under lost acks... | `20260910-042707-C1` |
| C13 | **proven** | G2 | The coordinator constructor is idempotent against the ledger, so a transient duplicate arising from the RESTARTING-at-GCS-death window cannot corrupt ... | `20260910-043934-C13` |
| C17 | **proven** | G2 | C16 restated after the fix: with the carry-forward read reordered (segments first, base_ptr last, bounded retry when base@ptr has vanished) a fenced c... | `20260910-043110-C16` |
| C2 | **proven** | G1 | With coalescing window W, a job of N units costs <= ceil(N/W)+O(1) durable writes, a redo window <= W + in-flight, and a steady-state key count indepe... | `20260910-045229-C2` |
| C20 | **proven** | G2 | With the carry-forward scanning epochs ascending (lowest first, highest readable wins) on top of the C17 within-epoch ordering, N concurrent coordinat... | `20260910-044309-C20` |
| C21 | **proven** | G2 | session_name is stable across a head restart, so a ledger scoped by it is still found by the coordinator that restarts into the same cluster | `20260910-045910-stageC`<br>`20260910-055803-headfail` |
| C23 | **proven** | G2 | A ledger scope key that distinguishes cluster restart from cluster replacement must be injected by the orchestrator (KubeRay RayCluster UID or RayJob ... | `20260910-045910-stageC`<br>`20260910-055447-C23a` |
| C24 | **proven** | G2 | Scoping the ledger by an ORCHESTRATOR-INJECTED key (RayCluster metadata.uid via the downward API) fixes C12 in both directions: the same uid resumes a... | `20260910-055536-C23b` |
| C25 | **proven** | G1 | A detached coordinator must use soft or no node affinity: hard NodeAffinitySchedulingStrategy(soft=False) makes it permanently unschedulable after the... | `20260910-061604-C25pin` |
| C28 | proven | G3 | A coordinator that crashes while GCS is unavailable does not thrash: with max_restarts=-1 the restart storm is bounded, and the compaction sweep keeps... | `20260910-170133-C28storm2` |
| C29 | **proven** | G1 | A GCS outage at or beyond gcs_rpc_server_reconnect_timeout_s (60s default) terminates the cluster rather than the coordinator, and R4 degrades to a co... | `20260910-170133-C28storm2` |
| C3 | proven | G3 | At the target job shape the ledger write rate stays below 10% of the measured GCS durable-write ceiling (~260/s same-key, ~593/s aggregate) with no me... | `20260910-045229-C2`<br>`20260910-181751-C3resolution` |
| C31 | proven | G3 | Once the producer is actually blocked on a full queue, the async redo window is exactly Q*W + 2W, deterministically and independent of when the crash ... | `20260910-175438-C31blocked` |
| C4 | **proven** | G1 | A detached, named, non-head-pinned coordinator survives driver death and head pod replacement under gcs_storage=rocksdb, and is reattachable by name f... | `20260910-045910-stageC`<br>`20260910-055803-headfail`<br>`20260910-062918-C7inplace2` |
| C5 | **proven** | G1 | Coordinator state is fully reconstructible from ledger + live-cluster introspection, with no external storage | `20260910-045910-stageC`<br>`20260910-064230-C5children2` |
| C6 | **proven** | G3 | The M3 compaction protocol is crash-safe over a non-transactional put/del store: no crash sequence loses committed progress, and every orphan is remov... | `20260910-041339-C6` |
| C8 | proven | G4 | The reference implementation needs zero Ray core patches on 2.57+ | `20260910-045910-stageC` |
| C12 | **refuted**<br>-> superseded by C24 | G2 | Scoping ledger keys by session_name prevents a new cluster that reuses an existing gcs_storage_path from adopting a stale ledger, given the KV DB's cl... | `20260910-044833-C12`<br>`20260910-045910-stageC` |
| C15 | refuted | G2 | The uniqueness of compaction base keys (base@n rather than a single overwritten base) is load-bearing ONLY against a concurrent stale writer, not agai... | `20260910-044833-C12` |
| C16 | **refuted**<br>-> superseded by C17 | G2 | Epoch scoping is SUFFICIENT to make a fenced coordinator harmless: a coordinator still running after a higher epoch has been claimed cannot cause the ... | `20260910-042707-C1` |
| C19 | **refuted**<br>-> superseded by C20 | G2 | C17's result - that the reordered carry-forward read protects the new epoch from a fenced writer - extends to more than two concurrent instances | `20260910-043934-C13` |
| C22 | refuted<br>-> superseded by C2 | G1 | The ledger's durable-write count is bounded by ceil(N/W) + O(1), i.e. compaction adds only an additive constant | `20260910-045229-C2` |
| C26 | refuted<br>-> superseded by C31 | G3 | Moving the ledger commit off the unit loop's critical path (an async writer with a bounded queue) lets work continue through a GCS outage, at the cost... | `20260910-070449-C26async3` |
| C27 | refuted<br>-> superseded by C31 | G3 | With an async ledger writer the redo window after a mid-outage crash is exactly Q*W + 2W units: the bounded queue, plus the batch the writer has deque... | `20260910-173047-C27sweep2` |
| C30 | refuted<br>-> superseded by C31 | G3 | The async redo window is Q*W + W + p, where p in [1,W] is the partial batch held by the producer at the instant of death; so Q*W + 2W is a tight upper... | `20260910-174219-C30repeats` |
| C32 | refuted | G3 | An adaptive coalescing policy that sizes W from the observed completion rate to hold a target ledger WRITE RATE bounds both the write rate and the red... | `20260910-183234-C32adaptive` |
| C7 | refuted<br>-> superseded by C31 | G3 | GCS unavailability during head failover degrades to 'work continues, durability pauses' without deadlocking the coordinator or tripping the GCS health... | `20260910-062918-C7inplace2` |
| C9 | refuted | G1 | Resume boundary is intra-cluster: the design survives driver and head loss but not RayCluster replacement, and KubeRay's RayJob retry path cannot be c... | `20260910-050300-C9-kuberay` |
| C10 | accepted risk | G2 | At-least-once commit semantics are acceptable: the realistic target workloads express work as idempotent units | - |
| C11 | accepted risk | G4 | Depending on the private internal_kv API is an acceptable risk given Serve, Jobs, the autoscaler and the dashboard already do | - |
| C14 | accepted risk | G1 | For distributed-training-shaped workloads R4's ledger adds negligible value over plain application checkpointing, because the resume unit is already a... | - |
| C18 | accepted risk | G2 | A fenced coordinator telling its driver that work is committed is acceptable, because at-least-once already holds (C10) and the work is redone by the ... | - |

## Runs retained despite being void or partial

| run | marker |
|---|---|
| `20260910-062429-C7inplace` | `VOID.md` |
| `20260910-064013-C5children` | `VOID.md` |
| `20260910-064613-C26async` | `NOTE.md` |
| `20260910-165139-C28storm` | `VOID.md` |
| `20260910-180327-C3interference` | `VOID.md` |

## Reproducing

Stage A and stage B need only the standard library:

```bash
python3 poc/run_checker.py --experiment C6
python3 poc/costmodel.py
python3 poc/adaptive.py /tmp/out
```

Stage C needs a real cluster (all measurements taken on Ray 2.58.0):

```bash
python3 -m venv .venv && .venv/bin/pip install 'ray[default]==2.58.0'
.venv/bin/python poc/stage_c/run_stage_c.py /tmp/out
```

Harnesses under `poc/stage_c/` start and kill real clusters, `SIGKILL` `gcs_server` and re-exec it from its captured `/proc` argv. **They are destructive to any Ray cluster running on the machine.**
