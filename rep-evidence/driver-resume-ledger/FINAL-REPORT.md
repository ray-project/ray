# Durable progress ledger for RayJob driver resume — final report

**Status:** design loop concluded at iteration 22 of a 100-iteration budget.
**Ledger:** 32 claims — **17 proven, 11 refuted, 4 accepted risk, 0 pending**.
**Evidence:** 35 immutable run directories, of which **6 are retained as void or
partial**. Idea backlog empty.

> ### On the mechanical verdict
>
> `ledger.py status` reports **REFUTED**, because three load-bearing claims
> (C12, C16, C19) are refuted and the backlog is empty. That is the correct
> output for the rule as written and it is **not** the correct reading of this
> loop.
>
> Every refuted load-bearing claim has a **proven successor**: C12→C24,
> C16→C17, C19→C20. The design was not killed; it was **corrected three times**,
> each correction pre-registered *before* the fix was written and then proven on
> its own terms. The tool does not model supersession, so supersession is
> recorded per-claim in `superseded_by` rather than by editing statuses, which
> would destroy the evidence.
>
> The honest one-line verdict: **The ledger design is viable, and it is viable in a shape
> materially different from the one the design doc started with.**

---

## 1. What the design is, after 22 iterations

Three tiers: a disposable driver, a restartable **detached named coordinator
actor**, and a durable progress ledger in GCS `internal_kv` under
`gcs_storage=rocksdb`. No Redis, no external database, no Ray core patches.

Mechanisms, with the ones **added by refutation** marked:

| | mechanism | origin |
|---|---|---|
| **M1** | epoch fencing: `put(overwrite=False)` + **read back the instance id** + epoch-scoped immutable state | original |
| **M1b** | carry-forward reads **segments first, `base_ptr` last**, with bounded retry | **added — C16 refuted** |
| **M1c** | carry-forward scans epochs **ascending**, highest readable wins | **added — C19 refuted** |
| **M2** | commit coalescing on a **fixed** window `W` | original; adaptive alternative tested and rejected (C32) |
| **M3** | compaction: write `base@m` → flip `base_ptr` → delete; startup sweep **after** the flip | original |
| **M4** | ledger scoped by an **orchestrator-injected** key | **added — C12 refuted, C23/C24** |

**M1b and M1c are one rule at two scales:** *read in the order that makes
"I missed X" imply "X's replacement is already published."* Together they are
about six lines of code, and without them the design is silently wrong.

### Requirements that are not optional

1. **Per-incarnation instance ids.** Never actor-derived (NC14).
2. **No hard node affinity.** `NodeAffinitySchedulingStrategy(soft=False)` makes
   the coordinator permanently unschedulable once its node is gone (C25).
3. **Deterministic, scope-prefixed child names.** Reality must be
   self-describing; opaque names orphan children (C5).
4. **An injected cluster-scope key.** No in-cluster identity can distinguish a
   head restart from a cluster replacement on a reused PV (C23).
5. **Every coordinator method idempotent under replay.** `max_task_retries=-1`
   replays in-flight tasks on a restarted actor (C28).

---

## 2. Goals

| goal | verdict |
|---|---|
| **G1** driver loss resumes from committed progress | **met** — C4, C25, C29 proven end-to-end on Ray 2.58 |
| **G2** resume correct under crash at any point | **met, after three repairs** — C6, C1, C13, C17, C20 proven by exhaustive model checking |
| **G3** fits `internal_kv` without degrading the cluster | **met, with a mapped envelope** — C2, C3, C28, C31 |
| **G4** unmodified Ray 2.57+ | **met** — C8 proven; zero core patches |

---

## 3. The operating envelope

- **Write overhead is multiplicative, ×2.79** — not additive (C2 proven, C22
  refuted). Frozen prediction `2 + 3/K`, max error 8.6 %.
- **Key count is flat in `N`** — 12 keys from `N=10` to `N=10⁶`, saturating at
  ≈`K+4`. Recovery costs ≤33 KV ops.
- **No measurable interference at design rate** (C3). At 26 writes/s the victim's
  `internal_kv` p99 moved **+3.0 %** against a **9.8 %** noise floor; nothing
  regressed up to **200 writes/s**. The result is only meaningful because an
  independent positive control — 4 MB values — moved p99 by **42×**.
- **Where the ledger design breaks:** fine-grained, high-throughput, short-duration work.
  `N=1,000` dies above ~100 units/s; `N=10⁶` tolerates 10⁴/s.
- **Outage ceiling: 60 s.** Beyond `gcs_rpc_server_reconnect_timeout_s` the
  *cluster* dies, not the coordinator. The ledger design then degrades to **cold restart with
  zero data loss and zero redo** (C29) — downtime, not damage.
- **A crash during an outage costs one restart and ≤`W` units of redo** (C28).
  There is no restart storm; the one I thought I saw was my own instrumentation.

---

## 4. The three refutations that changed the design

**C16 — epoch scoping protects the writer, not the reader.** A fenced
coordinator keeps compacting and deletes segments the new epoch is midway
through reading. The design doc's safety argument was framed entirely around
*writes*; **both** bugs were about *deletes*.

**C19 — the same bug across epochs.** The startup sweep deletes a whole epoch
unconditionally. It **requires a third coordinator to observe**: with two, the
sweeper and the reader are the same process. 5,448 violations at three
instances, **zero** at two.

**C12 — `session_name` is a storage-path identity, not a cluster identity.**
Refuted by the very measurement that proved C21. C23 then showed something
stronger: **no** in-cluster identity works, because a head restart and a cluster
replacement on a reused PV are *the same physical event* from inside. Nine
candidates probed, zero usable. The scope key must come from the orchestrator
(C24).

---

## 5. What this loop is really evidence for

**Twelve distinct blind spots**, from twelve unrelated causes. Every one was
caught by **a number looking wrong — never by a failure**:

1. controls that only tested the invariant checker, not the fault injectors
2. faults injected into the audit instance
3. a partial-order reduction that starved the second instance
4. a missing liveness invariant (total livelock ≡ success)
5. concurrency degree too low to contain the failure class
6. an inert stale-tail parameter (identical trace counts)
7. a control starved by too small a sweep (NC15 ≈25,000 violations vs NC13 ≈100)
8. `get_if_exists` silently re-adopting, making a control look red
9. a buffer sized in batches while the prediction was in units
10. a redo measurement taken after the backlog had drained
11. **a whole claim manufactured by my own rig** — `max_task_retries=-1` replayed
    the suicide task, inventing an 18-incarnation "restart storm" (C28)
12. a control that was **necessary but not sufficient** — a full queue is not a
    blocked producer (C27→C31)

Three claim-hygiene defects (C1→C16, C2→C22, C26→C27) were all the same error:
a claim broader than its refutation condition. The rule adopted — *every clause
must appear in the refutation condition or it is not being tested* — and false
clauses were **split into new claims and refuted on their own terms**, never by
widening the condition after the fact.

**Six runs are retained as void or partial.** They are evidence too, and two of
them produced better answers than the experiments they invalidated.

---

## 6. Recommendation

**Proceed to an REP**, proposing:

1. The three-tier pattern with M1/M1b/M1c/M2/M3/M4 as specified above.
2. A **durability contract for `internal_kv` under `gcs_storage=rocksdb`**, plus
   per-namespace quota/TTL. This composes with ray#65692 rather than competing.
3. The five non-optional requirements in §1.
4. **Fixed `W`, not adaptive** (C32). Adaptive bounds redo *wall-clock* where
   fixed bounds redo *count*; nothing bounds both while the rate varies, and
   the ledger design's budgets are denominated in units.
5. **Synchronous commit, not async** (C7/C26/C31). Async buys ≈`Q·W` units of
   liveness during an outage and costs exactly `Q·W + 2W` units of redo. For
   long jobs, pausing is better. If async is adopted, `Q` is **not** freely
   choosable: it must be small enough that `Q·W + W` units are produced inside
   the outage being ridden out.

### Accepted risks

| | risk |
|---|---|
| **C10** | at-least-once commit semantics |
| **C11** | dependence on the private `internal_kv` API (Serve, Jobs and the dashboard all do the same) |
| **C14** | negligible value for distributed-training-shaped workloads |
| **C18** | a fenced coordinator may tell its driver work is committed |

### Residual fidelity gaps

`F1`–`F7` remain open at **low/med**; **none is rated high**. `F8` was resolved
in iteration 14. The most substantive remainder is **F3**: GCS restart is modelled
in stage A as operations failing then succeeding rather than as a process
restart — which is exactly why stages B and C exist, and why the head-failover
and in-place-GCS harnesses were built.

The one thing the ledger design **does not** claim: a real head pod returns on a **new IP**. That
is raylet↔GCS reconnection, owned by KubeRay's GCS-FT machinery, and it is not a
property of this design.
