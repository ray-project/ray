# Hash Shuffle ObjectRef Bypass Design

> Status: draft, ongoing design discussion
> Branch: `feat/object-push-bytebuffer`
> Last updated: 2026-06-13

---

## ⚠️ Known Errors / Fix-List (verified against code)

Read this before the design body. Markers: ❌ = verified bug (must fix);
⚠️ = misleading/wrong-as-stated; 🌿 = branch-only (real on
`feat/object-push-bytebuffer`, just not in this checkout — not a bug).

| # | Issue | Where | Status |
|---|---|---|---|
| E1 | **Ownership (path-dependent).** `ray.put` makes the *mapper* the owner (needed ONLY for Bundle 1 = refcount-callback cleanup + Gap-1). Bundle 2 *returns* the handle (caller-owned, reconstruction-ELIGIBLE) and uses path-A cleanup + per-node serving. So "must `ray.put`" holds only for Bundle 1; see §4.10. | §4.1, §4.10, §7 | ✅ clarified (was overstated) |
| E2 | **Pinning.** `InternalPlasmaHandle` holding `shared_ptr<Buffer>` does **not** pin — `Seal` calls `Release` → object becomes LRU-evictable. Need a post-`Seal` `Get` reference. `MutableObject` is not a precedent (it pins via `Get`). | §3.2 (boxed), §2.3, §7 | ❌ verified bug — **primitive withdrawn** (PR3 dropped, A.1 adds no bypass API); §3.2 kept only as analyzed alternative |
| E3 | **`Delete` semantics.** `PlasmaClient::Delete` is a no-op while store `ref_count_>0`; the creator's own `Create`/`Get` ref must be `Release`d first, and the object must be sealed. "Bypass guarantees no other client" is not sufficient. | §2.3, §7 | ⚠️ flagged |
| E4 | **Callback API.** No `SetObjectDeletedCallback`; spill does not use it. Real API `AddObjectOutOfScopeOrFreedCallback` (reference_counter.h:150) fires **under the refcount mutex** → callback must only `post`, never work inline. | §3.3 (fixed), §4.3, §7 | ⚠️ corrected |
| E5 | **Memory truth.** Whole-handle reclaim does not bound peak memory (handle drops only at end of reduce stage in all-to-all). Per-(mapper,partition) early release is the lever, with a `NOT_FOUND`→re-exec policy tradeoff. | §4.3, §4.4 (added) | ✅ documented |
| E6 | **Don't put shuffle logic in `CoreWorker`.** Registry, fetch service, file/cleanup must NOT be `CoreWorker` methods/members. They live in a worker-process `ShuffleManager` (Ray Data layer) with its own gRPC server+port. `CoreWorker` gets at most one *generic* hook (cleanup path B), or nothing (path A). | §2.2, §3.3, §3.4, §3.5 (decoupled) | ✅ fixed |
| E7 | **Recovery model is path-dependent.** Bundle 1 (`ray.put` handle = `INELIGIBLE_PUT`, core_worker.cc:985) → **executor-driven** re-run. Bundle 2 (returned handle, ELIGIBLE iff `max_retries>0`) → **core-automatic** re-run, same task id → same ObjectID, transparent to a borrower's `ray.get` (verified: object_recovery_manager.cc:24-91, task_manager.cc:353-411). Earlier "only executor / core can't" was overstated for Bundle 1. | TL;DR, §4.3, §4.10, §11 Q1 | ✅ corrected |
| E8 | **RELEASED-before-punch invariant.** A hole-punched range `pread`s back **zeros, not an error**, so early release + hole-punch MUST mark the chunk RELEASED (registry → serve returns `NOT_FOUND`) *before* punching, else a re-executed reducer reads silent garbage. | §5.7 (bold invariant) | ✅ elevated |
| B1 | **`ZeroCopyPushServer` / ByteBuffer-writev ZC send** — real on the branch, absent here. ⚠️ user-measured **no clear gain for in-memory data**; design must NOT rest on it. Throughput re-derived in §4.5 (RPC count × payload size, network/disk-bound) — gRPC is fine at PB scale *iff* batches stay large. Fetch service on `ShuffleManager`'s own server/port (E6). | §3.5, §4.5, §7 | 🌿 branch-only |
| B2 | **`MmapRegion` non-page-aligned** (disk fast path) — claimed on the branch's `zero_copy_slice.cc`; not in this checkout, confirm on branch. | §5.4, §7 | 🌿 unverified here |

Resolution for E2/E3 — **the design takes the drop-primitive route**: partition
bytes are mapper-owned `ray.put` objects or explicit mapper-written files (§5), so
there is no bypass core API (PR3 dropped, A.1). The **keep-bypass** route
(`Seal`→`Get` to pin, `Release`→`Delete` to free) survives in §3.2 only as an
analyzed alternative, not the recommended path. §4.3/§4.4 cleanup covers both.
> ⚠️ **Naming:** these "keep-bypass / drop-primitive" labels are a *storage* choice
> and are **orthogonal** to the "path A / path B" *cleanup-trigger* choice in
> §3.3/§4.3. Don't conflate them.

---

## TL;DR

**Problem.** Ray Data hash shuffle creates `N×M` ObjectRefs per (mapper, reducer) pair (N mappers, M = R reducers). At scale this drives:
- Driver-side ref metadata explosion (ReferenceCounter + ObjectDirectory tracking >100K refs).
- Persistent 2× memory pressure during pull (Ray's ObjectDirectory keeps both source and destination plasma copies alive until refcount=0).
- Layout tension between memory and disk (forced into adaptive layout or fragmenting tradeoffs).

**Approach.** Each mapper output is **one** Ray ObjectRef (`ShuffleHandle`) — small
metadata. The N×M partition objects' ids ride inside it as **opaque bytes**, so
the driver/reducers never register them as ObjectRefs (driver stays O(N)). The
**bulk lives in a per-mapper file** (sort-spill-merged into a partition-contiguous
layout, §5.9), and the **OS page cache is the elastic RAM tier** — there is **no
in-memory-vs-disk decision, no memory budget, no plasma tier for bulk** (plasma
holds only the handle; §5.0). Reducers fetch via a **side-channel RPC** (bytes →
user space, never a 2nd plasma copy → §4.6), or, in the cloud regime, read a
remote object store (S3) directly (§16). The plasma-bypass *primitive* of the
original draft is **withdrawn** (unsound — E2/E3).

**Net result.**
- Driver tracks **O(N)** refs, not O(N×M) (opaque ids; no borrow protocol for bulk).
- **No 2× pull copy** — side-channel fetch streams into user space (§4.6).
- **Plasma core untouched** — bulk is ordinary files; plasma only carries the handle.
- **Bulk = one file form**; page cache makes it RAM-resident when it fits, spills
  when it doesn't — kernel-managed, no threshold (§5.0). 1× is a *guardrail* (don't
  go through Ray's copy-on-get), not a tuned metric (§16 note).
- **Unproven empirically** — this is a design + code-verified mechanisms; nothing
  is implemented or benchmarked (§11; the "1×/skew/beats-X" claims are analytical).
- Cleanup and recovery come in two coupled bundles (§4.10): **Bundle 1** —
  `ray.put` handle (mapper-owned) → refcount-callback cleanup + Gap-1 protection,
  but handle is `INELIGIBLE_PUT` (core_worker.cc:985) so recovery is
  **executor-driven**. **Bundle 2** — *returned* handle (reconstruction-ELIGIBLE)
  → **core-automatic** re-run of the producer on node loss (same task id → same
  ObjectID, transparent to a borrower's `ray.get`), with executor (path-A) cleanup
  + per-node serving. Either way, **re-running the mapper regenerates bulk + handle
  together** — recovery is task-granular, not per-byte. Correctness requires a
  **deterministic mapper / pinned partition plan** (§11 Q1).

---

## 0. Design derivation: mutual-exclusion axioms → the spills-to-local-disk corner

The design is not a pile of independent choices — it is (nearly) *derived*. The
method is **not** a Nash equilibrium (there are no strategic agents); it is
**constraint propagation over fundamental mutual exclusions, then Pareto selection
by the workload objective**. Fix the objective and the "X ⊥ Y" axioms chain to a
near-unique corner. This section is the rationale spine; everything below is an
expansion of one corner.

### 0.1 Axioms — fundamental mutual exclusions (what cannot coexist)

`A ⊥ B` = "you cannot have both." **F** = fundamental to Ray's model;
**C** = contingent (holds *because we forbid core changes*; relax that and it lifts).
The **C** axioms — and X9 below — lift once core changes are allowed *back/forward
compatibly* (the real constraint, not "no core change"); **§0.6** gives the core
end-state that dissolves most of this table. **X10 is the exception**: a conservation
tradeoff (store-or-recompute) that **no** core change repeals.

| # | Mutual exclusion | Why | F/C |
|---|---|---|---|
| X1 | mapper **owns** the handle ⊥ handle is **core-reconstructable** | `ray.put` = `INELIGIBLE_PUT` (core_worker.cc:985); only task *returns* reconstruct, and a return's owner is the caller | **F** |
| X2 | plasma **in-memory residency** ⊥ **survives owner-worker death** | a plasma object's lifetime = its owner worker's lifetime | **F** |
| X3 | **no destination-resident copy** ⊥ stays in the **`ray.get`/`Pull` dataflow** | the plasma→task read *is* zero-copy (Arrow view into shm) — **not** the 2×; the 2× is that a cross-node **`Pull` copies the bytes into a 2nd resident plasma object** on the reducer node (pinned till refcount→0). Two ways out: a **side-channel** (bytes→heap, freed on consume — §4.6), **or a move semantic** (relocate to the destination, **free the source** — §8.6, *in progress in the group*). **But move is 1→1**; shuffle is **1→M fan-out**, so move only removes the 2× at per-(mapper,partition) = **O(N×M)** granularity (→ reintroduces X4). At the design's **O(N)** granularity the mapper output is fan-out-read → move can't serve all M → **the side-channel stays the lever here** (move coexists, §8.6). | **C** — a move primitive lifts it for 1→1, but it **stays binding for O(N) fan-out shuffle** |
| X4 | **O(N) driver refs** ⊥ **per-consumable-unit core-reconstructable bulk** | reconstructable *at the granularity reducers fetch* ⟹ one tracked ref per piece = O(N×M). (One returned object **per mapper** is O(N) *and* reconstructable — but coarse; what it loses is per-partition reclaim, which is X9, not X4.) | **F** |
| X5 | **controlled spill/backpressure** ⊥ using **plasma as the store** | Ray spill is raylet-driven, node-global, no rate knob (§4.5/§5 banner) | **C** |
| X6 | **per-partition early reclaim** ⊥ **O(N) files** (one file per mapper) | can't `unlink` part of a file; hole-punch bridges it but with reads-as-zeros + Linux-only (§5.7) | **F** |
| X7 | **refcount-driven cleanup** ⊥ **zero core change** | needs a core callback hook (path B); else executor-driven (path A) | **C** |
| X8 | **per-node serving** (P² conns, survive death) ⊥ reuse the **per-worker CoreWorker gRPC server** | CoreWorker's server is per-worker (§ "why CoreWorker-register was superseded") | **F** given Ray |
| X9 | **core-auto-lineage on the bulk** ⊥ **per-partition early reclaim** (same bytes) | the **lineage↔reclaim trade**, not "reclaim is pending": per-partition reclaim needs a *sub-range-free* primitive — **files have one** (`fallocate` hole-punch = X6, **done**, §5.7), **sealed Ray objects don't** (core-reconstructable ⟹ sealed ⟹ whole-object only; sub-range free would need §8.3 / a §0.6 reconstructable-*and*-reclaimable kind, **not in progress**). So the design takes **file** — per-partition reclaim is **available with today's primitives** (hole-punch, no core change; v1 may still default it off — T2) + **executor-orchestrated recovery** (handle keeps core lineage; bulk doesn't). Having *both* lineage-and-reclaim on the same bytes needs §0.6. | **C** — a new kind dissolves the trade; **not needed** (file already reclaims) |
| X10 | **minimal storage (1×, eager-free)** ⊥ **small recompute blast-radius (cheap recovery)** | conservation: a freed-then-lost byte can only be recomputed. Retain (toward 2×) → a consumer failure re-fetches locally (re-run the consumer only); eager-free (1×) → a later need cascades to a **producer** re-run. A continuous knob, not a boolean ⊥; this is §4.4 generalized. Ray already does exactly this for streaming-generator objects (created→deleted→needed→re-run, task_manager.cc:377-380). | **F** — and **does NOT lift under §0.6** |

### 0.2 Propagation for the spills-to-local-disk objective

**Target band: bigger than RAM, fits on local disk** — ~tens to low-hundreds of TB
aggregate, **per-node bulk ≤ node NVMe**. True PB (per-node bulk > local disk) is a
*different leaf* — the remote object-store / S3 backing of §16 (durable class), **not
this local-file path scaled up** (at PB, local disk is the wrong substrate: capacity +
node-loss recompute cost → industry uses disaggregated shuffle). Objective vector
**O = { ¬fits-memory(but-fits-disk), low-core-invasion, fault-tolerant,
high-throughput, O(N)-driver, no-2× }**. Each step forced by an axiom:

```
¬fits-mem    ─X5/X2→  bulk = local FILES (not plasma)   [¬fits-disk → S3 leaf, §16]
FILES + FT   ─X2→     durable tier is files (in-mem can't be FT for free)
O(N)-driver  ─X4→     bulk not core-reconstructable → recovery = re-run the task
  └ recovery=re-run, want it core-automatic ─X1→ handle is RETURNED (reconstructable) = BUNDLE 2
BUNDLE 2     ─X1/X7→  no owner callback → cleanup = PATH A (executor) → zero core
FILES+FT+scale ─X8→   serve from a PER-NODE ACTOR (not per-worker CoreWorker server)
no-2×        ─X3→     SIDE-CHANNEL fetch → out-of-band → MUST wire resource accounting
per-part reclaim ─X6→ HOLE-PUNCH + RELEASED-before-punch invariant (§5.7)
recovery=re-run  ────  REQUIRES deterministic mapper + pinned partition plan (§11 Q1)
```

**Derived corner** = `files + path-A cleanup + per-node actor + side-channel
(+resource accounting) + hole-punch(+RELEASED invariant) + deterministic mapper`,
**with the handle (Bundle 1 vs 2) left FREE** unless an extra objective term is
added — see the machine-checked result below, which corrected an earlier
over-claim. This is the design in §3–§5/§4.10 — *derived from the axioms +
objective, not chosen ad hoc.*

> ⚠️ **Hand-derivation correction (caught by §0.5's Z3 model).** An earlier prose
> version listed *Bundle 2* as forced by the base objective. It is **not**: the
> base objective forces side-channel, executor-cleanup, per-node serving, file-bulk
> and zero-core, but leaves the **handle choice (Bundle 1 vs 2) free**. Bundle 2 is
> forced **only** when you additionally require *core-automatic* recovery
> (`want_auto_recovery` → X1 → returned/reconstructable handle). This is exactly
> the kind of over-claim a machine check exists to catch.

### 0.2.1 Objective priority — must-have vs nice-to-have (v1 target = T0 + T1)

§0.4 #3 notes there is no scalar optimum — the corner is a *weighting* call. Here is
that weighting: the objectives are **not co-equal**. Tiering them is what makes
"what to build first" decidable (and is the explicit form of "do we start at PB?",
§0.3).

| Tier | Properties | Why this tier | In v1? |
|---|---|---|---|
| **T0 — essential (correctness floor)** | eventual recoverability (*any* form — executor re-run suffices); runs without OOM (spill when it doesn't fit); **integrity — per-chunk checksum** (local disk silently corrupts at scale, §5.10); auth (security.md) | ship-blocking; absence = incorrect / silently wrong | **yes** |
| **T1 — essential at scale, nice-to-have small** | **O(N) driver refs**; **no-2× on read** (side-channel → user space) | the *motivating* pain (§1) — but only bites at scale; small data tolerates O(N×M) + `ray.get`'s 2× | **yes** |
| **T2 — nice-to-have (increments)** | **1× peak / per-partition early reclaim** (hole-punch, §5.7); **automatic *core* lineage** (Bundle-2 reconstruction / §0.6); bounded-peak beyond spill for ¬fits | each has a *ship-without* precedent — **Spark has no object lineage** (its scheduler recomputes the map stage) and **runs at ~2×** (no hole-punch) | **no — later** |

**v1 = T0 + T1.** Concretely: file bulk + per-node `ShuffleManager` + side-channel
fetch (no-2×, O(N) handle) + path-A cleanup + a **Bundle-2 returned handle**, with
**recovery executor-orchestrated**. (The returned handle is already
reconstruction-ELIGIBLE, so core-automatic lineage is a later near-free upgrade, not
a v1 dependency.) **Deferred to T2:** hole-punch early release — so **v1 runs at ~2×
peak disk** for data-preserving ops, exactly like Spark — and core-automatic lineage.

**Capacity ceiling (where this design stops being right):** the local-file path is
bounded by **per-node bulk ≤ node disk** (with the ~2× peak when hole-punch is off →
usable ≈ ½ node disk). Past that — true PB, per-node bulk > local disk — switch to the
**S3 leaf (§16)** (durable class, no local recompute); do **not** scale the local path
into it. So the band is: fits-RAM → page cache; bigger → local file (this design);
bigger-than-local-disk → S3.

**Be honest about the consequence:** with 1× and auto-lineage demoted to T2, the only
property neither Spark nor Daft has is T2's **core-native out-of-band lineage**
(§0.6) — i.e. the sole differentiator sits in the *deferred* tier. So **v1 (T0+T1) is
"Ray Data shuffle brought to Spark/Daft architectural parity"**, a real upgrade over
today's Ray Data shuffle; the differentiation is an explicit later bet, not a v1
claim. This maps onto §10: PR1 (`ray.get` baseline) is a T0 stepping-stone; PR2–PR3
deliver T1 (per-node file serving + side-channel); T2 (hole-punch, auto-lineage) is
PR4+.

### 0.3 The other corner, the frontier, and free variables

- **Small-data / fits-in-memory corner**: `fits ─X2→ plasma in-mem ─→ mapper-owned
  + Gap-1 (Bundle 1) ─X7→ refcount cleanup (path B) ─→ near per-worker serving`.
  This is the §4.10 Bundle-1 / in-memory fast path.
- **Pareto frontier**: parameterized by *the fraction that fits in memory*. **Update
  (§5.0):** rather than forking into a plasma tier for this, the frontier is handled
  by **one form (a file) + the OS page cache** as the elastic RAM tier — fits → hot
  in cache (RAM-speed), doesn't → kernel writes back. No per-task decision, no H.
  So the "in-memory corner" is just "the file's pages happen to be cache-resident,"
  not a separate plasma path.
- **Genuine free variables** (no axiom binds them): chunk/run/sort-buffer size,
  batch granularity, zero-copy send (B1 — pure perf), local-vs-S3 backing (§16).

### 0.4 Limits of the derivation (so it isn't over-trusted)

1. It is Pareto + constraint propagation, **not** a Nash equilibrium.
2. The **C** axioms (X3, X5, X7, X9) hold *only because core change is forbidden*. The
   real constraint is softer — core dataplane changes are allowed when **back/forward
   compatible** (Ray is re-partitioning the dataplane). Relax accordingly and new
   corners open — see **§0.6** for the end-state (externally-backed lineage-eligible
   objects) that lifts X1–X9 almost entirely. (X3 specifically is lifted by a **move
   semantic** for 1→1 transfers — §8.6, in progress — but **stays binding for the
   O(N) 1→M shuffle**.) So the "ideal" is parameterized by **how much b/f-compatible
   core change is allowed** (a product decision, not a law).
3. Objectives are **incommensurable** (core-invasion vs throughput have no common
   unit) → the frontier is real but there is **no scalar optimum**; the final
   corner is a weighting call. For PB-corpus the chain is tight enough to be
   near-unique; OLAP / small-data land on other corners.
4. Soundness holds **only over the enumerated exclusions**. An unlisted fundamental
   exclusion (multi-tenancy, security, a scheduler constraint) would move the corner.

### 0.5 Machine-checked derivation (Z3)

The axioms X1–X8 and the objective are encoded as boolean clauses in
`design/shuffle_design_derivation.py` (readable; one clause per axiom, each
tagged). Z3 **enumerates every satisfying assignment** over the decision variables;
a variable constant across all models is FORCED (derived), one that varies is FREE.
This is deliberately honest: a choice we *claim* is forced but isn't will show up
as FREE — which is exactly what happened to the handle/Bundle choice.

Verbatim result (`python3 design/shuffle_design_derivation.py`):

```
=== Spills-to-disk: doesn't fit mem, low-core, FT, no-2x, per-node ===
  satisfying designs (over reported vars): 10
  FORCED: sidechannel=True, rayget=False, cleanup_executor=True,
          cleanup_refcount=False, serve_pernode=True, serve_perworker=False,
          bulk_file=True, bulk_plasma=False, core_change=False, actor_owns_inmem=False
  FREE:   handle_return, handle_put, holepunch, auto_recovery

=== Spills-to-disk + WANT core-automatic recovery ===
  satisfying designs: 4
  FORCED: ...as above... + handle_return=True, auto_recovery=True
  FREE:   handle_put, holepunch

=== Small data: fits in memory, FT, no-2x (no low-core demand) ===
  satisfying designs: 156
  FORCED: sidechannel=True, rayget=False, bulk_plasma=True
  FREE:   (everything else)
```

**Reading it:**
- The **PB spine holds together** — but be precise about *derived* vs *asserted*:
  side-channel, file bulk, per-node serving and zero-core are objective **inputs**
  (asserted in `objective()`); the model's genuine **derivations** are
  `cleanup_executor` (from ¬core_change + X7) and `¬actor_owns_inmem` (from X2). Its
  most useful output is what it proves **free** (handle/holepunch/auto_recovery).
  (X4/X6/X9 are not yet encoded — see §0.6 status note.)
- The **handle choice is FREE**, and `handle_put`/`handle_return` are **independent
  (at-least-one, not exclusive)** — so "both" = the **double-return hybrid** (§4.10)
  is a real design. 10 models = handle ∈ {put, return, both} × holepunch ×
  auto_recovery (modulo X1). Demanding `auto_recovery` forces `handle_return=True`
  (X1) but leaves `handle_put` free — plain-return *and* double-return both qualify
  (→ 4 models, free on `handle_put`/`holepunch`). **Note `cleanup=executor` in every
  model**: path-B refcount cleanup needs a core hook (X7), which zero-core forbids —
  so even Bundle 1 cleans via the executor here, and its only residual edge is Gap-1
  (redundant under per-node serving). (An earlier model wrote the handle as
  *exactly-one*, which wrongly hid double-return; corrected to at-least-one.)
- **Small-data** with no low-core/scaling demand leaves the space wide (156 models)
  — i.e. the design is *under*-determined there; only side-channel + plasma bulk
  are forced. The PB constraints are what pin the design down.

So the answer to "can we derive the ideal design from the mutual exclusions?" is:
**yes for the forced spine, and the model tells you precisely which choices remain
free and which extra objective term collapses each** — which is more useful than a
single hand-asserted "optimum." Re-run with a different objective (or flip a **C**
axiom by allowing core changes) to derive a different corner.

### 0.6 Core end-state: externally-backed lineage-eligible objects

Everything in §3–§5 routes *around* Ray's object machinery. The cleaner — and likely
upstream — form is to make the capability **native**: let the object store hold only
**control data** (small objects + a descriptor), keep the **bulk out-of-band**, yet
let the object still enjoy **native lineage reconstruction**. Shuffle then becomes
the first *client* of this primitive, not a bypass.

This is admissible because the real constraint is not "no core change" but **core
dataplane changes must be back/forward compatible** (Ray is re-partitioning the
dataplane). An *additive* new object kind — existing sealed-immutable objects
untouched (backward); versioned + graceful fall-back to `ray.get` (forward) —
qualifies, and is designed against the seams the re-partition commits to keep.

**The primitive — an *externally-backed lineage-eligible object*:** a refcount- and
lineage-tracked **control object** in plasma carrying a **backing descriptor**
(out-of-band location + **per-chunk checksum** + durability class). Three new core
behaviors:

1. **Loss detection = liveness *and integrity*, at backing granularity.** Liveness
   alone is too weak: local disk fails *partially* and *silently* while the node is
   alive (sector/UBER errors → wrong bytes; per-file EIO; torn writes). So the
   trigger is "backing unreachable **OR** read-error **OR** checksum mismatch" —
   detected per backing (per file), not only at node death. Integrity (the
   descriptor's checksum, verified end-to-end on read) is what catches the silent
   case that pure reachability cannot. Any of these → loss event → reconstruct that
   producer (§5.10). *(This corrects an earlier over-simple "liveness-only" sketch.)*
2. **Reconstruction regenerates bytes.** On loss, core re-runs the producer task → it
   re-writes the out-of-band bytes and re-registers them under the **same ObjectID**,
   transparent to a consumer's read.
3. **Reclaim is native.** refcount→0 → core frees the out-of-band bytes (this closes
   "returning the handle does not clean files"). With *range*-level refcounting it
   also yields **per-partition early reclaim** natively — **X9 dissolved**.

**Which axioms it lifts** (vs §0.1 — all because it un-fuses *bytes* from
*identity/lineage* at the core level, b/f-compatibly):

| Axiom | Today (fused) | Externally-backed kind |
|---|---|---|
| X1 owns ⊥ reconstructable | `ray.put` = INELIGIBLE_PUT | producer-owned **and** reconstructable |
| X2 in-mem ⊥ survive worker death | plasma life = owner life | bulk out-of-band → survives worker death natively |
| X3 no-2× ⊥ `ray.get` | side-channel = a bypass | side-channel **is** the native read path |
| X4 O(N) ⊥ reconstructable bulk | per-piece refs = O(N×M) | one control object/mapper, internal index → O(N) + reconstructable |
| X5 controlled spill ⊥ plasma | raylet spill, uncontrolled | bulk not in plasma → spill is the data plane's own concern |
| X6 / X9 reclaim ⊥ file/sealed | can't free a sub-range | range-free is a native data-plane op |
| X7 refcount cleanup ⊥ zero core | needs a hook | native (refcount→0 frees the backing) |
| X8 per-node serving ⊥ per-worker server | CoreWorker server is per-worker | serving is a core dataplane role, directory-addressed |

**What does NOT lift (the irreducible residue):**
- **Determinism.** Any lineage requires the producer to deterministically reproduce
  the bytes (or an equivalent partition→records mapping). Ray *assumes*, does not
  *enforce*, this (task_manager.cc:219-224) — and out-of-band bulk makes it the
  common case, so the blast radius is larger (§4.10 precondition).
- **New obligation — a data-plane liveness *and integrity* protocol** feeding the
  directory. This is the hard part and the main risk: a false "alive" → consumer
  cannot fetch but core won't reconstruct → hang; a false "lost" → wasted recompute;
  **and integrity is not optional at this scale** — reading ~hundreds of TB per
  shuffle on commodity disk (UBER ~1e-15) yields ~O(0.1–1) silent bit errors per
  read pass, so per-chunk checksums must gate the read or you return wrong results.
  It is *more* than Spark does (Spark has no object lineage; its scheduler recomputes
  the map stage on fetch failure). **Native lineage + integrity over out-of-band
  bytes is the novel, and harder, step.**
- **The storage ↔ recovery-cost tradeoff (X10).** Native reconstruction makes
  recovery *automatic*, not *free*: *whether* you retain the bytes or free-and-
  recompute is a conservation tradeoff (retain → cheap recovery, more storage;
  free → 1×, but a lost-and-needed byte costs a producer re-run). Core change cannot
  repeal conservation. Ray already implements exactly this for streaming-generator
  objects — one "created, deleted, and then needed again for recovery" triggers a
  whole-task re-run (task_manager.cc:377-380).

**Durability classes.** The descriptor names a backing class: *reconstructable*
(local disk → lineage on loss) vs *durable* (S3 / §16 → no recompute, just re-point).
The directory's loss/reconstruct policy keys off it.

**Scope discipline.** Define the **minimal** primitive first (whole-object
reconstruct, refcount free, opt-in externally-backed kind, local-disk backing);
layer range-reclaim, S3, and partial reconstruction as additive extensions.
"Mostly out-of-band" is a **kind** distinction (a producer declares a dataflow
externally-backed) — **not** a size threshold (the gameable `H` rejected in §5.0).

**Migration.** Ship shuffle as a Ray Data-layer per-node actor now (zero core);
propose this primitive into the dataplane re-partition; shuffle then collapses to a
thin client — wire protocol and operator API unchanged because both are designed
b/f-compatible from day one.

> **Status:** direction, not yet machine-checked. §0.5's Z3 model encodes X1–X8
> only; X4/X6/X9 and a `bf_compatible_core` predicate are future work (X10 is a
> continuous Pareto axis, not a boolean clause — it would enter as a cost objective,
> not an exclusion). The claim here is architectural.

---

## 1. Background

### 1.1 Current Ray Data hash shuffle bottleneck

Ray Data's existing shuffle operator materializes each `(mapper_i, reducer_j)` pair as a separate `ObjectRef` (a "block"). For `N=100, M=1000`:
- 100K ObjectRefs flow through the driver during the shuffle phase.
- Each ref carries ~250-500 bytes of metadata across `ReferenceCounter`, `ObjectDirectory`, and owner protocol tracking.
- Per-ref cross-node ownership RPCs (borrow → return → release notifications) become the throughput ceiling, often dominating actual byte transfer time at scale.

### 1.2 Goals

1. **Streaming write**: mapper RSS bounded; no requirement to materialize total output before exposing it.
2. **Fit in memory when possible**: if total shuffle data fits in cluster plasma capacity, do not spill to disk.
3. **Random-access reads**: each reducer reads only its partition's bytes — no full-object pulls.
4. **Scalable to large M×R**: target M=10K partition × R=10K reducer = 10^8 logical fetches without melting the driver.
5. **Cleanup is deterministic and integrated with Ray's refcount/lineage**: no orphaned plasma objects, no manual GC.

### 1.3 Constraints accepted

- **Linux-only optimizations OK** (Ray runs in production on Linux); macOS dev environment may pay a small fallback cost.
- **No first-class user API change** — `ray.get`/`ray.remote` semantics unchanged. New API is Ray Data scheduler-facing.
- **Plasma core untouched** — any change to plasma's existing object lifecycle is a non-goal; precedent (MutableObject) is followed strictly.

---

## 2. Top-Level Design

### 2.1 Three Invariants

| # | Invariant | Why |
|---|---|---|
| 1 | Driver only sees **O(N) ObjectRefs** for the entire shuffle | Avoid N×M refcount/directory metadata explosion |
| 2 | Plasma core **untouched**: no new object lifecycle state, no Release-after-Seal semantics | Maximize upstream acceptance; avoid touching the most sensitive subsystem |
| 3 | Bulk has **one form: a per-mapper file**; the OS page cache is the RAM tier (§5.0) | No in-memory-vs-disk decision, no `H` budget, no plasma tier for bulk — the kernel slides the residency continuum |

### 2.2 Architecture overview

> **Component boundary (E6):** all shuffle-specific state and RPC live in a
> **per-node `ShuffleManager`** (a long-lived Ray actor) owned by the **Ray Data
> layer** — NOT in `CoreWorker`. One per node, it serves the files of *all* local
> mappers, so a mapper **worker** dying is a non-event (the node keeps serving —
> §4.10). `CoreWorker` is touched by at most one *generic* hook (Bundle 1 / path B),
> or nothing (Bundle 2 / path A). The manager runs its own server (own port, reusing
> `src/ray/rpc/GrpcServer` + auth), owns the registry, and manages files + cleanup.
> (This supersedes the earlier per-worker-singleton framing; §0.6 is the eventual
> move into the core dataplane.)

```
┌──────────────────────────────────────────────────────────────────────┐
│ Driver process                                                       │
│   holds: N × ShuffleHandle ObjectRef (one per mapper output)         │
│   each ShuffleHandle = small metadata describing partition layout    │
└──────────────────┬───────────────────────────────────────────────────┘
                   │ borrowed by reducer tasks (standard Ray ref protocol)
                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Reducer worker (on any node)                                         │
│   1. ray.get(ShuffleHandle) → metadata: {fetch_endpoint, auth,      │
│                                          partition → [chunks]}       │
│   2. rpc.FetchPartition(fetch_endpoint, chunks)                      │
│      → bytes flow into reducer's user-space buffer (NOT plasma)      │
│   3. consume bytes inline (merge/reduce/aggregate)                   │
│   4. reducer task returns its own output as normal ObjectRef         │
└──────────────────────────────────────────┬───────────────────────────┘
                                           │ side-channel gRPC
                                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Mapper node                                                          │
│                                                                      │
│   Mapper worker(s) ── write per-mapper file to session_dir/shuffle/  │
│                       register (handle_id → file, offset index) with │
│                       the node's ShuffleManager, then may be freed.  │
│                       (Bundle 1: also ray.put the handle for Gap-1)  │
│                                                                      │
│   ┌── ShuffleManager  (per-NODE Ray actor, Ray Data layer) ───────┐  │
│   │  registry   handle_id → file paths + offset index + RELEASED  │  │
│   │  FetchService   own server + port + token auth (pread/sendfile)│ │
│   │  background unlink/punch pool                                 │  │
│   │  cleanup trigger   (Bundle 2: executor Release; B: callback)  │  │
│   └───────────────────────────────────────────────────────────────┘ │
│   survives worker death; lost only on NODE loss → lineage re-run      │
└──────────────────────────────────────────┬───────────────────────────┘
                                           │
                                           ▼
              storage (per node): one file per mapper output —
                session_dir/shuffle/<mapper_output_id>.bin   (chunks = offsets)
              OS page cache is the RAM tier (§5.0). ShuffleHandle is a small
              ray.put (Bundle 1) or task-return (Bundle 2) object.
```

### 2.3 Key decisions (with rationale)

| Decision | Rationale |
|---|---|
| **One ShuffleHandle per mapper, not per partition** | O(N) driver metadata. Partition addressing is metadata internal to the handle. |
| **Partition ids ride as opaque bytes (not ObjectRefs)** | Avoid `N×M` ref/owner/directory protocol cost. The bulk is out-of-band; Ray's higher layers never register it. |
| **Bulk = one per-mapper file; page cache is the RAM tier** | O(N) files (chunks = offsets, §5.3); the kernel decides residency (§5.0) — no plasma tier, no `H` budget. The bypass-plasma *primitive* of the draft is withdrawn (E2/E3, §3.2). |
| **Side-channel RPC for fetch** | Cannot reuse Ray's `Pull` (avoids the borrow protocol + the destination plasma copy). Served by a **per-node `ShuffleManager`** (Ray Data layer) on its **own** server — NOT `CoreWorker` (E6, §3.4). |
| **Cleanup + recovery: two bundles (§4.10)** | Bundle 1 (`ray.put` handle) → owner refcount→0 callback frees the file (path B) + Gap-1. Bundle 2 (returned handle) → core-automatic recovery + executor (path A) cleanup. |
| **Per-partition early reclaim via hole-punch (§5.7)** | One file gives O(N) inodes but blocks per-partition `unlink`; `fallocate(PUNCH_HOLE)` returns the blocks while keeping offsets — gated by the RELEASED-before-punch invariant. |

---

## 3. Components

### 3.1 `ShuffleHandle` (Ray-level ObjectRef)

A protobuf serialized into a small (KB-scale) **`ray.put` object** (so the mapper
is its owner — §4.1). One per mapper output. The chunk is the **unified
descriptor** of §5.1 (a `oneof` of file / plasma backing), not a plasma id:

```protobuf
message ShuffleHandle {
  bytes shuffle_id;            // unique per shuffle job
  NodeID mapper_node;          // where the bytes live (locality scheduling)
  string fetch_endpoint;       // ShuffleManager host:port (its own gRPC server, §3.5)
  bytes auth_token;            // per-shuffle, data-level isolation (security.md)
  repeated string files;       // file_id -> path table (file-backed chunks)
  repeated PartitionLocator partitions;
}

message PartitionLocator {
  uint64 partition_id;
  repeated Chunk chunks;       // a partition's bytes; may span several chunks
}

// Unified per §5.1 — backing is a oneof; a single handle may mix file & plasma.
message Chunk {
  uint64 length    = 1;
  fixed32 checksum = 4;        // CRC32C of bytes — verified on read (§5.10)
  oneof loc {
    FileLoc   file   = 2;      // (file_id, offset) into `files[...]`
    PlasmaLoc plasma = 3;      // mapper-owned ray.put object id, pinned (§5.4)
  }
}
message FileLoc   { uint32 file_id = 1; uint64 offset = 2; }
message PlasmaLoc { bytes object_id = 1; }
```

**Default (Bundle 2):** the mapper **returns** the handle proto as its task value →
driver-owned, reconstruction-ELIGIBLE (§4.1, §4.10). *Alternative (Bundle 1):*
`ray.put(handle_proto)` inside the mapper so the mapper owns it — only when you want
path-B refcount cleanup / Gap-1 (§4.10, E1). Either way the driver gets
`ObjectRef[ShuffleHandle]`.

### 3.2 `InternalPlasmaHandle` (bypass primitive) — **WITHDRAWN**

The original draft backed each partition chunk with a plasma object
(`PlasmaClient::Create`+`Seal`, held by `shared_ptr<Buffer>`, unregistered with
`ReferenceCounter`/`ObjectDirectory`). **Dropped (E2/E3; PR3 removed)** for two
reasons, kept here only as the analyzed-and-rejected alternative:

- **It does not pin (verified bug).** `Seal` calls `Release` → store `ref_count_`→0
  → the object is LRU-evictable (object_lifecycle_manager.cc:128-175); holding the
  create buffer does *not* keep the store ref. (`MutableObject` is **not** a
  precedent — it pins via `Get`, client.cc:427-469.) Fixing it would require a
  post-`Seal` `Get` to pin, then `Release`→`Delete` to free (`Delete` is a no-op
  while `ref_count_>0` and requires sealed — object_lifecycle_manager.cc:94-118).
- **It is unnecessary.** Bulk is now mapper-written **files** (§5) — no plasma-core
  interaction, no new primitive. (A mapper-owned `ray.put` object is the other
  no-new-primitive option; both beat the bypass.)

### 3.3 `CoreWorker` surface — kept minimal (E6)

**No shuffle-specific method goes into `CoreWorker`.** `CreateInternalPlasmaObject`
/ `ReleaseInternalPlasmaObject` (the bypass primitive) are dropped — partition
bytes are explicit files or ordinary mapper-owned `ray.put` objects, neither of
which needs a new core API. The registry and fetch service live in
`ShuffleManager` (§3.4 / §3.5), not `CoreWorker`.

The only thing that *could* require a core change is the cleanup trigger, because
the handle's refcount lives behind the private `reference_counter_`. **Decided:
path A (executor-driven) is the default** — it needs zero CoreWorker change and is
forced anyway in the settled corner (Bundle 2 returns a *driver*-owned handle, so
there is no mapper-side refcount callback; and zero-core rules out path B — §4.10,
§0.5 Z3). Path B is kept only as the Bundle-1 / allow-a-core-hook alternative:

**Path A — zero CoreWorker change (executor-driven cleanup).** The Ray Data
executor knows when the reduce stage that consumes a handle has finished; it calls
`shuffle_manager.Release(shuffle_id)` directly (or via the fetch server's control
RPC). Crash safety comes from the session-dir backstop (§5.6). No `CoreWorker`
API, no refcount coupling; cleanup timing is explicit and operator-controlled.

**Path B — one *generic* CoreWorker hook (refcount-driven cleanup).** Add a single
method that is **not shuffle-aware** and reusable by any library:

```cpp
class CoreWorker {
 public:
  // Generic: fire `callback` when an object OWNED by this worker leaves scope
  // (refcount → 0). Thin wrapper over
  // ReferenceCounter::AddObjectOutOfScopeOrFreedCallback (reference_counter.h:150
  // — there is NO `SetObjectDeletedCallback`). The callback fires WHILE the
  // ReferenceCounter mutex is held, so it must only POST work to an executor,
  // never block or re-enter the reference counter (§4.3).
  Status RegisterOwnedObjectDeletedCallback(
      const ObjectID& owned_id,
      std::function<void()> callback);
};
```

`ShuffleManager` (not `CoreWorker`) registers the callback and owns the posted
`Release`. The method stays a small, generic primitive — it does not know the word
"shuffle".

> **Decision:** **path A** (executor-driven, core untouched) is the default — it is
> what the settled Bundle-2 + per-node + zero-core corner forces. Path B (automatic,
> refcount-integrated) is the alternative *only* under Bundle 1, or once a generic
> core hook is allowed (§0.6). §4.3 describes the reclaim mechanics common to both.

### 3.4 `ShuffleManager` + `ShuffleOutputRegistry` (Ray Data layer — NOT CoreWorker)

`ShuffleManager` is a **per-node, long-lived Ray actor owned by the Ray Data
layer** (a Ray Data extension with a thin Python binding), not a `CoreWorker`
member. One per node (e.g. via `NodeAffinitySchedulingStrategy` or a per-node
custom resource), it survives individual **worker** deaths — only **node** loss
takes it down — which is what makes worker death a non-event for fetches (§4.10).
It owns the registry and the fetch server (§3.5) and serves the files of all local
mappers. It does not link CoreWorker internals; it only needs `src/ray/rpc` (for
its server + auth) and the filesystem. (Supersedes the earlier per-worker singleton;
§0.6 is the eventual move into the core dataplane.)

```cpp
// Ray Data extension — e.g. python/ray/data/_internal/shuffle/shuffle_manager.{h,cc}
class ShuffleManager {
 public:
  static ShuffleManager& ForLocalNode();   // handle to this node's actor

  // Called by a mapper task after it has written its partition bytes.
  // `backing` is either file paths (file variant) or mapper-owned ray.put ids
  // (plasma variant). Keeps them alive until Release(shuffle_id).
  void Register(ObjectID shuffle_handle_id, ShuffleBacking backing);

  // Fetch-service membership/auth check (is this chunk one we serve?).
  bool Serves(const ChunkKey& key) const;

  // Drop a registered entry → free its bytes (unlink files / drop ray.put
  // local refs). Invoked by the cleanup trigger:
  //   path A: directly by the Ray Data executor;
  //   path B: by the posted lambda from CoreWorker's generic out-of-scope
  //           callback (§3.3 / §4.3). Idempotent.
  void Release(ObjectID shuffle_handle_id);

  const std::string& fetch_endpoint() const;  // host:port advertised in the handle

 private:
  absl::Mutex mu_;
  absl::flat_hash_map<ObjectID, ShuffleBacking> entries_;
  absl::flat_hash_set<ChunkKey> served_;       // for fetch auth/membership
  std::unique_ptr<rpc::GrpcServer> fetch_server_;   // own port + token auth
  ThreadPool unlink_pool_;                          // offload blocking close/unlink
};
```

A mapper task calls the local node's `ShuffleManager.ForLocalNode().Register(...)`;
no path goes through `CoreWorker`. The handle is then either `ray.put` (Bundle 1) or
task-returned (Bundle 2) per §4.10.

### 3.5 `ShuffleFetchService` (side-channel RPC, hosted by `ShuffleManager`)

A gRPC service **owned by `ShuffleManager`**, running on **its own port** (the
manager stands up its own `rpc::GrpcServer`, reusing Ray's `GrpcServer` + token
auth machinery from `src/ray/rpc` — no `CoreWorker` server involvement, no
coupling into core_worker bootstrap). The mapper advertises `host:port` in the
handle's `fetch_endpoint`. This matches the original "separate per-node service"
shape and keeps shuffle out of `CoreWorker` (E6).

```protobuf
service ShuffleFetchService {
  rpc FetchPartition(FetchRequest) returns (stream FetchReply);
}

message FetchRequest {
  bytes  auth_token;             // per-shuffle, data-level isolation
  uint64 partition_id;
  repeated Chunk chunks;         // the chunks for this partition (from the handle)
}

message Chunk { string path = 1; uint64 offset = 2; uint64 length = 3; }
// (plasma variant: replace `path` with the mapper-owned ray.put object id)

message FetchReply { bytes data = 1; }   // streamed, one message per chunk
```

> 🌿 **Zero-copy is optional, not load-bearing.** If the branch's ByteBuffer/slice
> ZC send is present, the handler can use it; the user measured **no clear gain for
> in-memory data**, so v1 may just copy plasma/file bytes into the message (same as
> the object manager's current `Push`). Do not block the design on ZC.

**Server side**: handler validates `auth_token` + `ShuffleManager::Serves(chunk)`,
then reads each chunk — file variant `pread(path, offset, length)`; plasma variant
`plasma_store_provider`/`PlasmaClient::Get` on the mapper-owned id — and streams
it. Runs on `ShuffleManager`'s own server threads, so blocking I/O does not touch
CoreWorker's io_service.

**Client side**: reducer batches all chunks for one (mapper, partition) into one
RPC. Receives into a user-space buffer (NOT plasma). Consumes inline.

**Server-side request pooling (merge-on-read, §4.12).** The handler does NOT serve
each request with independent random seeks. The `ShuffleManager` owns *one* file per
local mapper output and sees *every* reducer's `FetchRequest` against it. It **pools
the pending ranges across all connections** and reads the file in **ascending-offset
order** (one near-sequential pass), routing each chunk's bytes to whichever reducer
requested it. The coordination point is this single per-node process — no global/driver
barrier, no cross-node epoch (§4.12).

**Race-safety with cleanup**: while serving, the handler takes its OWN
short-lived pin on each requested chunk, independent of the registry's long-lived
hold (§4.3). For plasma it `Get`s the object (store refcount +1) and `Release`s
after the send; for disk it keeps the `fd` open (Linux defers `unlink` until the
last `close`). This guarantees an in-flight fetch completes even if `Reclaim`
runs concurrently — no torn reads, no use-after-free.

---

## 4. Data Flow

### 4.1 Write path (mapper)

```
mapper task body:
  M heap staging buffers (each ~1 MB)
  
  for record in input_stream:
    p = hash(record.key) % M
    staging[p].append(record)
    if staging[p].full():
      // flush staging[p] to the ONE per-mapper file (§5.3) — NO CoreWorker call:
      //   off = out.tell(); out.write(staging[p])     # single append-only file
      //   chunk = (file_id=0, off, len)               # handle index entry
      //   (plasma/S3 are other chunk leaves, §5.1 — not the default)
      partition_chunks[p].append(flush(staging[p], p))
      staging[p].reset()
  
  // task end: flush remaining buffers
  for p in 0..M:
    if staging[p].size() > 0:
      flush as above
  
  // build ShuffleHandle proto
  shuffle_handle = ShuffleHandle {
    mapper_node, fetch_endpoint, auth_token,
    partitions: [partition_chunks[0], ..., partition_chunks[M-1]]
  }
  
  // DEFAULT = Bundle 2: RETURN the handle value (no ray.put). A task return is
  // driver-owned and reconstruction-ELIGIBLE (task_manager.cc:277-282) → core-
  // automatic lineage on node loss (§4.10). The mapper need NOT own the handle,
  // because cleanup is path A (executor), not a mapper-side refcount callback.
  return_id = this_task.return_id()   // known to the worker from the task spec

  // register backing with the per-node ShuffleManager actor (NOT CoreWorker),
  // keyed on the handle's object id.
  ShuffleManager.ForLocalNode().Register(return_id, backing(partition_chunks))

  // cleanup = DEFAULT path A (§3.3 / §4.3): the Ray Data executor calls
  //   ShuffleManager.ForLocalNode().Release(return_id) when the consuming reduce
  //   stage finishes; session-dir backstop (§5.6) covers crashes.
  //   (Alt — Bundle 1: ray.put the handle so the mapper owns it + arm the path-B
  //    out-of-scope callback; only for per-worker serving / a core hook — §4.10.
  //    Double-return = return value + a ray.put anchor; needed only in those corners.)

  return shuffle_handle   // Bundle 2: driver-owned, ELIGIBLE task return (no ray.put)
```

Mapper RSS during execution = `M × staging_size ≈ M × 1MB`. For M=1000 that's ~1GB; for M=10000 with smaller buffers (100KB each), still ~1GB.

### 4.2 Fetch path (reducer)

```
reducer task body, partition_id = k:
  for handle in my_shuffle_handles:
    h = ray.get(handle)   // small metadata, fast
    chunks_for_me = h.partitions[k].chunks
    
    request = FetchRequest {
      auth_token:   h.auth_token,
      partition_id: k,
      chunks:       chunks_for_me   // [(file_id, offset, length), ...] from the handle
    }
    for reply in grpc.FetchPartition(h.fetch_endpoint, request):  // streamed
      consume(reply.data)   // merge/sort/reduce inline
    
  return my_reduced_output  // standard ObjectRef
```

Reducer never materializes the data in its local plasma. Bytes flow: mapper
source (plasma `Get` **or** file `pread`) → gRPC send → TCP → reducer-side gRPC
receive buffer → user-space consume → discarded. (Whether the send side is
zero-copy is a marginal optimization, not the mechanism — see §4.5; v1 may just
copy into the message.) Reducer-side footprint = transient transfer buffer
(MB-scale).

### 4.3 Cleanup path

Both reclamation triggers end in the same call — `ShuffleManager.Release(handle_id)`
— and differ only in *what fires it*. **Decided: path A is the default** (the
settled Bundle-2 + per-node + zero-core corner forces it; §3.3, §4.10); path B is
the Bundle-1 / allow-a-core-hook alternative, documented below for completeness.

**Trigger — path A (zero CoreWorker change, executor-driven) — THE DEFAULT.** The Ray Data
executor knows when the reduce stage that consumes a handle has completed; it
calls `ShuffleManager.Release(handle_id)` directly (or via the fetch server's
control RPC). No refcount coupling, no core method. Crash safety: session-dir
backstop (§5.6). Timing is explicit and operator-controlled.

**Trigger — path B (one generic CoreWorker hook, refcount-driven) — Bundle 1 only.**
In the Bundle-1 variant the handle is `ray.put` by the mapper (§4.10), the mapper owns it, and
`ReferenceCounter::AddObjectOutOfScopeOrFreedCallback` (reference_counter.h:150 —
there is no `SetObjectDeletedCallback`) fires on the mapper when the handle's
refcount — mapper's local ref + driver's and reducers' borrows — reaches 0.
Propagation of a borrower's drop goes through the `WaitForRefRemoved` pubsub
channel (async, ms-scale), not a heartbeat.

> **Hard constraint for path B (verified): the callback fires while the
> ReferenceCounter mutex is held** (reference_counter.cc:835-846). It must NOT do
> work inline — `RemoveLocalReference` re-enters the same mutex (deadlock), and a
> blocking `unlink` stalls the loop. The callback therefore only POSTS
> `ShuffleManager.Release` to an executor. (Path A has no such constraint — the
> executor calls `Release` directly.)

Reclamation itself (common to both paths) runs off the trigger thread, is
idempotent, and lives in `ShuffleManager`:

```
# path B only: bridge the refcount callback to ShuffleManager
on_handle_gone(handle_id):              # still under refcount mutex
    executor.post(ShuffleManager.Release, handle_id)   # POST ONLY (no inline work)

# path A: executor calls ShuffleManager.Release(handle_id) directly

ShuffleManager.Release(handle_id):      # on ShuffleManager's pool, off trigger thread
    entry = entries.take(handle_id)
    if not entry: return                # idempotent → re-fire / crash-retry safe
    # file variant:   offload close()+unlink() of the chunk files to unlink_pool_
    # plasma variant: drop the mapper-owned ray.put local refs → raylet unpin
    #   (legacy bypass variant: destroy InternalPlasmaHandles → PlasmaClient::Delete)
```

Timeline (path B; path A replaces T0–T2 with "executor detects reduce-stage done"):

```
T0  all reducers return; driver drops its handle refs
T1  borrowers→0 propagated to owner via WaitForRefRemoved pubsub (ms-scale)
T2  mapper ReferenceCounter fires on_handle_gone (under mutex → POSTs only)
T3  ShuffleManager.Release(handle_id) runs on its pool, idempotently
T4  file: files closed + unlinked;  plasma: ray.put refs dropped → raylet unpin
```

**Race-safety (in-flight fetch vs reclaim).** A reducer holds its handle ref until
its fetches from that handle finish, so the refcount cannot hit 0 mid-fetch. As
defense-in-depth the fetch handler also takes its OWN short-lived pin per chunk
while serving (§3.5 race-safety): plasma `Get`/`Release` around the send; disk
keeps the fd open (Linux defers the real delete until the last `close`). So an
in-flight fetch completes even if reclaim runs concurrently.

**Failure modes — mapper crash (three backings, all verified; do NOT conflate):**
- *`ray.put` plasma variant:* the raylet frees the mapper's owned objects via the
  **owner-dead subscription** — when the owning worker dies, `ReleaseFreedObject`
  fires for each pinned object (local_object_manager.cc:148-153). No leak.
- *bypass plasma variant:* the mapper's `PlasmaClient` connection drops →
  `PlasmaStore::DisconnectClient` releases all that client's pins
  (store.cc:331-361). No leak.
- *file variant:* files are orphaned (no process to unlink) → reclaimed by the
  session-dir backstop (§5.6) plus the graceful-shutdown unlink for clean exits.

In every case, the **in-flight/subsequent reducer fetch fails** (connection reset
or `NOT_FOUND`) → Ray Data treats it as "this `ShuffleHandle` is unreachable" and
the **Ray Data executor re-runs the producer mapper task** (§11 Q1), regenerating
its file + a fresh handle. This is *application-level* re-execution — **not** Ray
core lineage reconstruction (the handle is a `ray.put` → `INELIGIBLE_PUT`,
core_worker.cc:985; the bulk is files — neither is core-reconstructable). The
bytes being gone is therefore safe *because the executor recomputes them*, not
because Ray rebuilds them.

**Failure mode — reducer crash:** the handle borrow is reclaimed via the standard
owner-borrower failure path (the owner's subscriber sees the borrower die via
`publisher_failed`/`HandlePublisherFailure`); the dead reducer is re-executed and
re-borrows a fresh handle.

**Memory truth — whole-handle reclaim does NOT lower peak footprint.** The
handle's refcount reaches 0 only after *every* reducer has consumed this mapper.
In an all-to-all shuffle each reducer reads every mapper, so a mapper's handle can
only drop at the *end of the reduce stage*. Whole-handle reclaim therefore holds
the entire shuffle dataset (in plasma/spill or on disk) until the reduce stage
finishes. That is fine when the data fits; for the "does not fit" case (the reason
§5 exists), reclaim alone does not bound peak — use §4.4.

### 4.4 Optional: per-(mapper, partition) early release (memory-bound shuffles)

To bound *peak* footprint below the full dataset, release a partition's bytes as
soon as its single consuming reducer has fetched it, instead of waiting for the
whole handle. In hash shuffle mapper *i*'s partition *k* is consumed by exactly
one reducer (reducer *k*), so:

- The fetch service keeps a *delivery counter* per (mapper, partition). When
  reducer *k* finishes fetching partition *k*, the mapper proactively
  `RemoveLocalReference`/`unlink`s that partition's chunks — without waiting for
  the handle refcount.

**Policy to decide explicitly** (this is a real tradeoff, not a free win):
proactive release diverges from Ray's refcount. If reducer *k* is later
re-executed (lineage) and re-fetches an already-released partition, it gets
`NOT_FOUND` → the upstream mapper is re-executed. Mitigate by gating "delivered"
on the reducer having *durably written its own output* (so a re-fetch implies the
reducer truly failed and its mapper-retry is acceptable), or accept occasional
re-execution. **Off by default; enable for memory-pressured shuffles.**

The §4.3 out-of-scope callback stays armed as the correctness backstop even when
early release is on — it reclaims anything early release missed.

**This section is the instantiation of X10 (§0.1) — the storage ↔ recompute-radius knob:**

| Operating point | Retention | Peak disk¹ | Failure blast radius |
|---|---|---|---|
| **A — low storage** (early release **on**) | free source as consumed | **~1×** | **large**: a re-fetch of a freed range → `NOT_FOUND` (§5.7) → **producer** re-run |
| **B — cheap recovery** (early release **off**, default) | hold source to reduce-stage end | **~2×** | **small**: source still present → re-run the **consumer** only |

¹ The ~2× at point B is reached only when **all** of: data-preserving op
(sort/repartition — *not* dedup/aggregation/filter, which shrink), result
materialized to local disk (not streamed onward / to S3), and doesn't-fit (else it
is page-cache-resident, §5.0). It is **not** two copies of the same bytes — it is
source 1× + result 1× (two different datasets) coexisting. **Spark sits at point B
too** (keeps shuffle files whole-stage, no hole-punch); §5.7 early release is the
lever to reach **A** that Spark lacks.

Recovery is task-granular, not byte-granular: a freed object needed again re-runs its
producer (task_manager.cc:377-380), and a failed consumer is re-executed wholesale
(ResubmitTask, task_manager.cc:353-388) — so there is **no partial-consumption state
to reconcile**, the radius is just "which tasks re-run."

> ⚠️ **Interaction with one-file-per-mapper (§5.3).** Early release assumes you can
> free *one partition's* bytes. With one file per mapper, a partition's bytes are
> interleaved ranges **inside a shared file — you cannot `unlink` part of it.**
> Options: (a) accept whole-file lifetime (no per-partition reclaim; file freed
> only at end of reduce stage); (b) **punch holes** for consumed ranges
> (`fallocate(FALLOC_FL_PUNCH_HOLE)`, Linux ext4/xfs — aligns with the Linux-only
> constraint) to return disk blocks while keeping offsets stable; (c) a middle
> layout — one file per *partition-group* instead of per-mapper, trading some file
> count for reclaim granularity. So O(N) files (§5.3) and per-partition early
> reclaim are in tension; pick per workload. The plasma/`ray.put` variant frees
> per-object naturally but pays §4.8's read-side cost.

### 4.5 Transport & throughput (the design does NOT rest on gRPC zero-copy)

A fair challenge: §3.5/§4.2 read as if throughput depends on a ByteBuffer/writev
zero-copy send. The user's own benchmark says ZC gives **no clear gain for
in-memory data**. So the throughput model must be re-derived from what actually
dominates — and it turns out gRPC ZC is *not* the lever at PB scale.

**Count RPCs and payloads, not "logical fetches".** The "M=10⁴ × R=10⁴ = 10⁸"
figure is *logical (mapper, partition) pairs*, not RPCs. The design batches **one
streaming RPC per (reducer, source-node)**: a reducer pulling partition *k* sends
one RPC to each node, carrying every chunk for *k* held by that node's mappers.
With P source-nodes and R reducers that is **R × P** RPCs, not N × R.

**Worked example — 1 PB, N=10⁴ mappers on P=10³ nodes, R=10⁴ reducers, balanced:**
- per mapper output ≈ 100 GB; per (mapper, partition) chunk ≈ 10 MB.
- per reducer input ≈ 100 GB; batched per source-node ≈ 100 GB / P = **100 MB per RPC**.
- total RPCs ≈ R × P = **10⁷**, each moving ~100 MB → 1 PB total.

At ~100 MB per streaming RPC, gRPC framing/dispatch overhead is **amortized to
nothing**, and the plasma/file→message copy is a few % of a transfer that is
network-bound anyway — exactly why ZC shows no gain. **The ceiling is network and
disk bandwidth, not gRPC.** So §4's throughput assumption holds *on plain gRPC*,
provided two things:

1. **Keep per-RPC payloads large.** Over-partitioning (M ≫ data) or tiny chunks
   shrink payloads → per-RPC overhead reappears. Size staging buffers / node-level
   batching so each RPC moves ≥ a few MB. State this as a tuning invariant.
2. **Reuse connections.** Channels are pooled per (reducer-node, source-node) pair
   → **P² channels** cluster-wide, not per-fetch. This is the real scaling pressure
   (connection explosion), and it argues for a **per-node** `ShuffleManager`
   (one server per node, serving all local workers' files + the node-shared plasma
   store) rather than per-worker — P² beats (workers)². *Open decision (§11).*

**Where ZC *might* still pay off: disk, cold cache.** For the file variant,
`sendfile`/mmap avoids pulling bytes through user space (read-copy + page-cache
double-buffer) on a cold read. That is a disk optimization, benchmark-gated
(B1/B2), not a prerequisite. In-memory: skip it.

**If gRPC framing ever does dominate** (small payloads you cannot batch away), the
fallback is a raw-socket / sendfile block-transfer service à la Spark's Netty
transport — more code, deferred unless benchmarks force it. `ShuffleManager`
owning its own server makes swapping the transport a local change, not a core one.

**Bottom line:** the transport concern is a *documentation/tuning* matter, not a
feasibility threat — but only because batching keeps payloads large. The honest
correction is to delete the "ZC zero-copy is the mechanism" framing (done) and
make large-batch + connection-reuse first-class assumptions of §4.

### 4.6 Fetch-side plasma bypass: how the destination copy is eliminated

The design's main memory win, removed *by construction*. The "2×" is **two distinct
1×'s** overlapping at peak, not one dataset stored twice:
- **source 1×** — mapper outputs on mapper nodes until consumed; **inherent to any
  materialized shuffle** (Spark too), shrinkable via §4.4, never removable.
- **destination 1×** — what Ray's `Pull` *additionally* materializes in the reducer
  node's plasma. **This is the avoidable half; the bypass removes it.**

`ray.get` is defined as "object **resident in local plasma**" → on Pull the reducer's
object manager does `CreateObject` + copy + `Seal` + pin until refcount→0; that pinned
object *is* the destination copy. The bypass eliminates it **structurally**: a
partition's id rides in the handle as **opaque bytes**, not a registered `ObjectRef`,
so there is **no Pull path** — the only way to get it is the side-channel
`FetchPartition`, whose bytes land in the reducer's **user-space heap**, consumed
inline (zero-copy Arrow) and freed. The reducer's plasma is never touched.

| | lands in | lifetime | vs store budget | granularity |
|---|---|---|---|---|
| `ray.get` | reducer plasma (pinned) | until refcount→0 | **counts** | whole object resident |
| bypass | user-space heap | freed on consume | **no** | streamed, MB-scale window |

So it is **"no resident object," not "one fewer copy"** (both copy network→landing
once; the difference is residency afterward). Arrow-in-plasma zero-copy is orthogonal
(it makes the *read* zero-copy, not the *residency*) and is kept either way.

**When it pays:** (a) data does **not** fit (don't pin it in plasma competing for the
store / triggering uncontrolled spill), and (b) the reducer can **stream**
(dedup/filter/merge). If it fits *and* must fully materialize (global sort),
`ray.get`-into-plasma is fine and the bypass buys little — state this honestly.
**De-risking:** PR1 ships correctness via `ray.get`; the bypass swaps the transport in
later, so the lineage wiring (a `FetchPartition` failure → typed "re-execute producer"
exception, §11 Q1) is hardened on the baseline first.

### 4.7 Backpressure & flow control

Throughput aside, the worry is *backpressure*: a fast producer/server must not
balloon memory, and a slow consumer must not cause unbounded buffering. Where it
is free, where it must be added, and why the **mapper-side plasma path is the
weak spot**:

**Free — the fetch path (per stream).** gRPC streaming over HTTP/2 has built-in
flow control: a slow reducer's receive window fills → the mapper's stream `Write`
blocks → the handler stops reading more from disk/plasma. *Requirement:* the
handler must **stream incrementally** (read a chunk → write → repeat); if it
pre-reads the whole partition into a buffer it has already lost backpressure.

**Free — produce ⊥ fetch.** Producer and consumer are decoupled by the at-rest
storage (files / plasma). Fetch backpressure never stalls produce — the mapper
finished producing; the bytes are at rest.

**Must add — fan-in / fan-out concurrency caps.** Per-stream flow control does not
bound *how many* streams are open:
- reducer fan-in: cap concurrent source streams, else memory ≈ (#sources)×window
  (cf. Spark `maxReqsInFlight` / `maxBlocksInFlightPerAddress`);
- mapper fan-out: cap concurrent served streams (gRPC max-concurrent-streams or a
  handler semaphore), else memory ≈ (#reducers)×buffer.

**Must add — produce → disk admission.** OS write-blocking / dirty-page throttling
backpressures the writer naturally — **until the disk is full, which is `ENOSPC`
(an error, not backpressure).** So the scheduler needs admission control: do not
let in-flight map output exceed disk capacity.

**The weak spot — mapper-side plasma backpressure (plasma variant).** When the
mapper `ray.put`s into a filling store, backpressure is *blocking inside* `ray.put`
(`CreateAndSpillIfNeeded`), and it is bad on five counts:
1. **Node-global & coarse** — you block on the *shared* object store + *shared*
   `max_io_workers` (4) IO workers; a slow-spilling node head-of-line-blocks
   unrelated co-located tasks. You cannot bound one mapper's plasma footprint.
2. **Unpredictable** — stall timing depends on aggregate node pressure + shared
   spill drain, not on this mapper.
3. **Can hard-fail** — if spill cannot free space, `ray.put` raises
   `ObjectStoreFullError` mid-stream (a failure, not a throttle).
4. **Couples serve ⇆ produce** — an object a reducer is actively fetching is
   pinned un-spillable during the serve, shrinking spillable headroom → more
   produce stalls. Fetch load worsens produce backpressure.
5. **Degenerates under PB** — outputs spill to disk anyway, and serve must then
   read spill files → effectively the file variant **plus** create/spill/restore
   churn.

**The file variant has none of (1)–(5):** produce backpressure is the mapper's own
`write()` (local, predictable, OS dirty-page throttle), uncoupled from a shared
store, no per-object create cost, no spill/restore churn; the only hard wall is
true disk-full, handled by admission control. **This is a primary reason to make
the file variant the default and plasma the optional "fits-in-memory" fast path**
(where the store never fills, so none of this triggers).

### 4.8 Why bulk is a file, not plasma (settled)

Bulk is always a file (§5.0); this is the rationale, not a "pick a variant" menu.

| Axis | **file (page-cache)** — chosen | plasma `ray.put` — rejected for bulk |
|---|---|---|
| OS objects/files | **O(N)** files (one per mapper, §5.3); chunks = offsets | O(N×M) plasma objects |
| In-memory tier | **OS page cache** (global, elastic, no `H` budget) | plasma residency + an arbitrary, gameable budget `H` |
| Mapper backpressure | local `write()`, predictable (§4.7) | node-global `ray.put` stall; can `ObjectStoreFullError` |
| Spill control | operator's own writes | uncontrollable (raylet `SpillObjectUptoMaxThroughput`) |
| Serve when not resident | `pread` | restore-thrash / read spill internals (below) |
| Survives worker death | **yes** (file on node) | no (object dies with owner) → recompute |
| Fragmentation | none | dlmalloc fragments, **cannot compact** (§8.3) |

**Clinching argument — native spill is clean on WRITE, hacky on READ.** `ray.put` +
raylet spill is clean to *write*; but serving a spilled partition wants a *byte range
out to the network*, while Ray spill is built to *restore the whole object on `Get`*
(`AsyncRestoreSpilledObject`) — under the very pressure that caused the spill,
competing for the shared `max_io_workers` (4). Avoiding that restore means reading the
spill file yourself (`SpilledObjectReader` + a non-public `spilled_url`, local-FS
only, Ray's *fused* files ≤2000 objs/100 MB freed only when all co-tenants are) — a
hack. And Ray *will* serve a spilled range via its object manager's cross-node `Push`
(which reads spill files) — **but only through `ray.get`/Pull, which re-creates the 2×
destination copy**. So the plasma variant cannot have *native-spill-ease + no-2× +
non-hacky-read* together. **The file variant gets all three: write your file, `pread`
a range, no destination copy.** Hence **bulk = file; plasma = handle only.**

### 4.9 Native spill — clean WRITE, hacky READ (merged into §4.8)

*Folded into §4.8:* native Ray spill is clean to write but on read forces
restore-thrash / spill-internal hacks / a 2× re-copy — the clinching reason bulk is
a file, not plasma.

### 4.10 Recovery model — owned-handle (executor) vs returned-handle (core-automatic)

How the producer is re-run after loss depends on **how the handle is produced** —
and this is a real, coupled choice (it also fixes the "automatic lineage" the
prior drafts oversold). Both keep O(N) refs and no-2× (bulk stays out-of-band).

**Bundle 1 — owned handle (`ray.put`).** Mapper `ray.put`s the handle → mapper is
owner. Buys: refcount→0 **cleanup callback** (path B) + **Gap-1 idle protection**
via the in-scope owned handle. Costs: the handle is
`LineageReconstructionEligibility::INELIGIBLE_PUT` (core_worker.cc:985) → **core
cannot reconstruct it** → recovery is **executor-driven** (Ray Data detects fetch
failure → re-runs the producer task; §4.3 / §11 Q1).

**Bundle 2 — returned handle (normal task return).** Mapper **returns** the handle
value (caller/driver owns it; the bulk stays in files). Buys: the return is
**reconstruction-ELIGIBLE** (task_manager.cc:277-282, gated on `max_retries>0` +
`lineage_pinning_enabled`, both default-on) → **core auto-recovery**. Verified
mechanics: on the producer **node's** loss the owner marks the value lost and the
object-recovery manager **re-runs the SAME task id → SAME ObjectID** transparently
to a borrower's `ray.get` (core_worker.cc:471-495; object_recovery_manager.cc:24-91;
task_manager.cc:353-411; id.h:267). Re-running the mapper **regenerates the bulk
too** (it writes its files), so the reducer's `ray.get(handle)` triggering
reconstruction recovers metadata **and** bulk in one shot — the genuine "automatic
lineage". Requires: cleanup via **path A** (executor `Release`, since there's no
owner-side callback) and a **per-node `ShuffleManager`** so a worker death is a
non-event (raylet pins the value + the node still serves the files —
core_worker.cc:3076, reference_counter.cc:889-904).

> **Caveat — core auto-recovery covers node loss, NOT live-node disk faults.** Handle
> reconstruction fires when the *handle* is lost (node death). But a file that is
> unreadable or **silently corrupt while its node is alive** leaves the handle intact
> → core sees nothing wrong. That case is caught only on *read* (EIO / short-read /
> **chunk-checksum mismatch**, §5.10) → typed exception → reconstruct that mapper. So
> **even Bundle 2 needs the §5.10 integrity + file-granular path**; "auto-recovery" ≠
> "covers every loss".

**Double-return** = the hybrid: return the reconstructable handle (Bundle 2
recovery) *and* `ray.put` a tiny anchor contained in it (Bundle 1 cleanup/Gap-1).
`handle_put` and `handle_return` are **independent**, so this is a legitimate design
(the §0.5 model admits it once the handle is at-least-one, not exactly-one). Whether
it earns the anchor depends on the corner:
- **per-node + path-A (the settled corner): redundant** — Gap-1 isn't needed (the
  node actor outlives the worker) and path-B refcount cleanup needs a core hook
  anyway (so cleanup is the executor regardless). Just return the handle.
- **per-worker serving: worth it** — the anchor supplies the Gap-1 idle protection
  that keeps the mapper worker alive to serve, while the return still gives
  core-automatic recovery.
- **core change allowed (§0.6): strictly most capable** — `core_change=True`
  re-enables path-B, so double-return yields auto-recovery (return) **and** native
  refcount-driven cleanup (anchor refcount→0) at once.

| | Bundle 1 (owned `ray.put`) | Bundle 2 (returned) |
|---|---|---|
| Cleanup | path B (refcount callback) | path A (executor `Release`) |
| Gap-1 idle protection | via owned handle | not needed (per-node serving) |
| Handle reconstructable? | ❌ INELIGIBLE_PUT | ✅ ELIGIBLE (`max_retries>0`) |
| Recovery | executor re-runs producer | **core auto** re-runs producer (same id) |
| Trigger granularity | fetch failure (worker/node/disk) | **node** loss → core-auto; **disk fault / checksum-fail on a live node** → executor via §5.10 |

**Shared precondition (both):** core reconstruction (B) and executor re-run (1)
are only correct if the **mapper is deterministic** — Ray *assumes* determinism,
it does **not** enforce it (task_manager.cc:219-224). With a sampled/byte-balanced
partitioner (§12) the partition plan **must be pinned** so a re-run reproduces the
same partition→records mapping; else a reducer mixing already-consumed output with
re-fetched output drops/duplicates records. Early release (§4.4/§5.7) must gate on
the reducer's **durable** completion, else a re-run reducer re-fetches a released
chunk → NOT_FOUND → cascades a producer re-run.

### 4.11 Reduce-side prefetch (overlap fetch with compute)

Default fetch (§4.2) is fetch-a-chunk → consume → discard, which **serializes**
network and compute. Reduce-side prefetch overlaps them: keep a **bounded lookahead**
of in-flight chunks per source so the merge/consume loop never stalls on the network.

**Mechanism — per-source sliding window, credit-based.**
- For each source node the reducer pulls from, keep K in-flight chunks. As the reducer
  **consumes** a chunk and frees it, return a credit → prefetch the next. Per-source
  so an external merge never stalls waiting on any single input.
- Staging is tiered (mirrors §5.0): a bounded **heap** ring first; optional
  **page-cache temp-file** overflow for deeper lookahead. Size the window to the
  **bandwidth-delay product + a jitter margin (hundreds of MB) — NOT the whole input.**
- Backpressure is free: window full → stop refilling → gRPC/HTTP-2 flow control
  throttles the source (§4.7). A fast network + slow reducer cannot balloon memory.

**Four invariants (correctness):**
1. **Delivery counts *consumption*, not *fetch*.** Early release / RELEASED (§4.4,
   §5.7) fires only after a partition is **durably consumed** — never on prefetch
   arrival — else a prefetched-but-unconsumed-then-failed reducer frees the source
   prematurely.
2. **Prefetch buffer is volatile** (heap or unlinked temp file) → dies with the task
   attempt; a failed reducer re-runs wholesale (ResubmitTask, task_manager.cc:353-388),
   so prefetch adds **no new recovery state to reconcile** (the §4.4 / atomic-restart
   result holds).
3. **no-2× preserved — but only if bounded.** A reducer-side page-cache prefetch file
   is a transient, kernel-evictable buffer, *not* a resident plasma copy (§4.6 holds).
   **An unbounded "stage the whole partition" prefetch, however, re-creates a
   same-bytes duplicate (source 1× + reducer 1×) on top of X10's retention 2×** —
   so keep the window BDP-bounded; then it is "1× + a small window," not 2×.
4. **Bounded fan-in.** Total in-flight ≤ (per-source window × #sources), capped by a
   global budget (§4.7), independent of dataset size.

**Synergy with source-side sequential reads (§3.5 / §5.3).** Prefetch hands the
per-node `ShuffleManager` a *queue of upcoming ranges ahead of time*, which it can
reorder by offset (scan), coalesce, and `posix_fadvise(SEQUENTIAL/WILLNEED)` —
turning all-to-all random reads into sequential disk access. You cannot reorder reads
you have not received, so **prefetch is the enabler of source-side sequential I/O**,
not just reducer overlap.

**Orthogonal to X10 (§4.4).** Prefetch is a perf lever used at *both* X10 operating
points; it does not change the retain-vs-free choice. (It only interacts if you treat
"prefetched/staged" as "delivered" to free the source early — that is moving to point
A via a *volatile* copy, with the usual recompute exposure.)

**Phasing.**
- **P1** per-source bounded heap window + credit refill + flow-control backpressure —
  pure overlap, no disk. *On by default, shallow.*
- **P2** source-side scan / coalesce / readahead fed by the P1 queue. *Disk-bound win.*
- **P3** page-cache temp-file overflow for deeper lookahead (jitter / large fan-in),
  still BDP-bounded. *Off by default.*
- **P4** adaptive depth (RTT / bandwidth / compute speed), metrics-driven.

**Metrics** (to tune, and to substantiate the speedup): prefetch hit-rate (consume
found ready vs stalled), in-flight bytes, reducer stall time, overflow bytes.

### 4.12 Merge-on-read — induce sequential disk reads at fetch time (NOT a write-side merge)

The bulk file is written in **flush order** (§5.3): under the shared-pool spill policy
(§4.7 / §5.0) the largest bucket is evicted first, so a partition's bytes are scattered
across the file as several chunks, in no particular partition order. Naïvely, each
reducer then `pread`s its partition's scattered ranges → the mapper disk does
**random** seeks as concurrent reducers hit unrelated offsets. Two ways to turn that
back into sequential disk I/O:

1. **Merge-on-write (§5.9, sort-spill-merge).** The mapper does a final merge pass so
   each partition is one contiguous run. Cost: a second I/O pass (read runs + write
   merged file ≈ 2× the bulk) on the **map** side. Benefit: reducers read one
   contiguous range each, zero coordination.
2. **Merge-on-read (this section).** Do **not** merge on write. Instead, realize the
   sequential access **at read time**: the per-node `ShuffleManager` pools the range
   requests it is holding and serves them in **ascending file-offset order** — one
   near-sequential pass over the file, fanning each chunk to its owning reducer. The
   "upstream sequential read" is *induced by the order in which reads are issued*, not
   by the on-disk layout.

**Why the coordination is cheap here (the key result).** A reducer wants partition `k`,
whose chunks are scattered through the file with *other* partitions' chunks in the gaps.
Sorting one reducer's ranges by offset is not enough — to read the whole file in one
sweep you must order ranges **across reducers**. The classic worry (the Spark-style
"zonal randomized fetching" sketch) is that cross-reducer ordering needs a global,
driver-level **wave/epoch coordinator** — which reintroduces a synchronization barrier
and **straggler amplification** (the slowest reducer gates each wave). That worry is
real for a *distributed* coordinator. **We avoid it because the coordination point is a
single per-node process:** the `ShuffleManager` owns the file and already sees *all*
local reducers' requests, so it pools and offset-sorts them **in one process, with no
cross-node barrier and no straggler coupling.** Late/retried reducers just get appended
to the next scan window (or fall back to a direct `pread`); they don't gate anyone.

**Offset-sort dominates partition-zoning.** The zonal scheme tries to induce locality by
having reducers fetch in *partition-number* order (zone 0–49, then 50–99, …), betting
that low partition numbers sit early in the file. That bet needs a **write-side
contract — every mapper must lay partitions out in the same physical order** — which our
flush-order layout (§5.3) does **not** satisfy (it isn't even monotonic in partition
id). Sorting by **physical offset** needs no such contract: it produces sequential reads
directly from whatever order the file happens to be in. So offset-sort is strictly more
general than zoning — it is the right primitive, and it makes the §5.3 "all-mappers-same-
order" contract unnecessary.

**Flow control is orthogonal (and still required).** Offset-sort disperses *where* reads
land; it does **not** bound *how many* are in flight. Incast protection still needs the
§4.7 caps — per-reducer in-flight bytes/requests (cf. Spark `maxReqsInFlight`/
`maxBytesInFlight`) and a per-mapper fan-out cap. Merge-on-read + in-flight caps together
are the complete picture; either alone is not.

**Relationship to the other levers.** Merge-on-read is orthogonal to 1× (§4.6), to the
shared-pool memory bound (§4.7), and to recovery (§4.10). It interacts with §4.11
prefetch (prefetch is what *feeds* the server its queue of upcoming ranges ahead of
time — you cannot offset-sort reads you have not yet received) and is an **alternative
to §5.9** when you would rather not pay the write-side merge: pick merge-on-read when the
map-side 2× write pass is the bottleneck, merge-on-write when the read path must be
coordination-free (e.g. an external consumer that won't pool requests).

**Phasing.** P1: within a single reducer's request, sort its ranges by offset + coalesce
adjacent ones (zero coordination, partial win). P2: pool ranges **across** local reducer
connections in the `ShuffleManager` and serve one offset-ordered scan per file (the full
win, still single-process). P3: `posix_fadvise(SEQUENTIAL/WILLNEED)` ahead of the scan
cursor.

---

## 5. Storage backing — bulk is ALWAYS a file; the page cache is the RAM tier

> ✅ **Canonical model (supersedes the earlier "file vs plasma tier / placement
> policy / #1-#2-#3 / memory-budget H" framing).** Decided after rejecting the
> H threshold as an arbitrary, gameable per-task decision ("you can always ask
> for more memory"). The bulk has **one form: a file** (page-cache-backed). There
> is **no in-memory-vs-disk decision, no H, no mixed-at-rest state, and no plasma
> tier for bulk** — plasma holds only the small handle.

**Why.** The "2× / in-memory tier" question dissolves once you stop deciding:

- Bulk is **always written to a file** (per-mapper, sort-spill-merged into a
  partition-contiguous layout — §5.3 / §5.9). plasma is **fired for bulk** (it is
  used only for the tiny `ray.put` handle).
- The **OS page cache is the elastic RAM tier**: hot pages resident (RAM-speed
  `pread`/mmap, zero-copy cross-process via mmap), cold pages evicted to the file.
  This is a **global, continuous, demand-driven** LRU using all free RAM — not a
  per-task budget. So "does it stay in memory" is answered by the kernel under
  global pressure, **with no threshold and nothing to game**.
- **Fits + consumed fast → no disk I/O**: write → reducers `pread` from cache →
  `unlink` before writeback fires → dirty pages discarded (the data is gone). So
  small/fast outputs are effectively RAM-only **automatically** (the old "#1
  all-memory" benefit, free). **Doesn't fit / slow → the kernel's
  `dirty_background_ratio` (global, principled) writes back to disk** (the old
  "#3 all-disk", free). The transition is continuous and kernel-driven.
- **1× is preserved**: the file is the single copy; the page cache is a *cache of
  that file*, not a second logical copy. The reducer streams via the side-channel
  into user space (§4.6) — never a second plasma copy.

So §5.1 (chunk descriptor), §5.3–§5.8 (file layout, serve, cleanup, holes,
cross-node) and §5.9 (sort-spill-merge write) below still apply — they describe
the **file** form. The earlier §5.2 "placement policy" and the plasma leaf are
**withdrawn for bulk** (kept only as the analyzed-and-rejected alternative, since
plasma-for-bulk costs the §4.8 serve-from-spill hack and dies with the worker).
The chunk `oneof` (§5.1) survives because future durable backings (S3 — §16;
first-class persistent storage — §15) are *additional leaves*, not in-memory tiers.

### 5.1 Unified chunk descriptor

```protobuf
message Chunk {
  uint64 length    = 1;
  fixed32 checksum = 4;      // CRC32C/xxh of the chunk bytes — verified end-to-end
                             // on read; mismatch = loss → reconstruct producer (§5.10)
  oneof loc {
    FileLoc   file   = 2;    // a byte range in one of this output's files
    PlasmaLoc plasma = 3;    // a mapper-owned ray.put object, pinned (§5.4)
  }
}
message FileLoc   { uint32 file_id = 1; uint64 offset = 2; }  // file_id → path in the handle's file table
message PlasmaLoc { bytes object_id = 1; }

message PartitionLocator { uint64 partition_id = 1; repeated Chunk chunks = 2; }
// ShuffleHandle also carries: repeated string files;  // file_id → path table
```

Same proto, same fetch RPC, same refcount/cleanup for both backings. The Ray Data
scheduler never branches on backing.

### 5.2 ~~Placement policy~~ → **there is no placement decision** (withdrawn)

The earlier per-chunk "plasma vs disk" decision gated on a memory budget `H` is
**withdrawn** — `H` is arbitrary and gameable. Bulk is always a file (§5.0); the
kernel's page cache decides residency continuously. The only in-memory thing is the
write path's bounded **sort buffer** (§5.9 — every external sort has one), *not* a
"does it fit" decision. So #1 (all-memory) / #3 (all-disk) are two ends of one
kernel-managed continuum over a single file form; #2 (mixed-at-rest) never exists.

### 5.3 Disk layout — **one file per mapper output** (NOT one per chunk)

> ⚠️ The earlier "one file per chunk" gave **O(N×M) files** (inode blowup). The
> correct layout is **one append-only file per mapper output** (= per map task) →
> **O(N) files**, with an offset index in the handle. This is Spark's sort-shuffle
> data+index layout, and it directly resolves §11 Q7 (object/file count at scale):
> there are **no per-chunk OS objects/files at all** — a "chunk" is just a byte
> range inside the one file.

```
$RAY_SESSION_DIR/shuffle/
  <mapper_output_id>.bin        # one file per map task; all M partitions interleaved
```

**How it's built (streaming, RSS-bounded):** the mapper opens one append-only
file. Whenever `staging[p]` fills, it records the current end-of-file `offset`,
appends the bytes, and pushes `(offset, len)` onto **partition p's** chunk list:

```
flush(staging[p], p):
    off = file.tell()                       # append-only ⇒ running byte offset
    file.write(staging[p])
    handle.partitions[p].chunks.append(Chunk(offset=off, length=len(staging[p])))
```

So the file is chunks in *flush order* (a partition's bytes are scattered through
it, interleaved with others); the **handle is the index**: `partition p → [(offset,
len), …]`. The index is O(N×M) tiny numbers (16 B each) living in handle metadata,
**not** O(N×M) OS files.

- **Reducer read**: partition k's chunk list = scattered ranges in the one file →
  `pread` each (coalesce adjacent ones). At 2 MB chunks each `pread` is a 2 MB
  sequential read — fine. Concurrent reducers issue concurrent `pread`s on the
  same fd (positional, thread-safe — no shared seek).
- **Cleanup**: unlink the single file when the handle is released (§4.3) — one
  `unlink`, not M.
- **Optional**: larger staging ⇒ fewer/larger ranges; an end-of-task compaction
  pass could make each partition contiguous (fewer seeks) at the cost of a rewrite
  — usually not worth it at 2 MB chunk size.
- If M is so large the inline index bloats the handle, spill the index to a sibling
  `<mapper_output_id>.idx` and reference it from the handle (Spark-style).
- Whole tree under `session_dir` → cleaned by Ray's `rm -rf session_dir` backstop.

### 5.4 Plasma leaf + unified serve (one path, leaves differ only at the read)

A `PlasmaLoc` chunk is an ordinary mapper-owned `ray.put` object **pinned via a
held `Get`** so the raylet never spills it — that pin avoids §4.8's restore-thrash
and lets the handler serve it directly. The whole E2/E3 discipline lives inside
this one leaf: `put` → `Get` (pin & serve) → `Release`+`Delete` (free). With the
§5.2 budget respected, the plasma tier never overflows the store; the rest are
file chunks.

The serve handler is **backing-agnostic** — it resolves each chunk to a leaf
`ChunkReader` and streams; only the leaf differs (mirrors Ray's own `IObjectReader`):

```cpp
// one uniform handler; the oneof picks the leaf
void HandleFetchPartition(req, stream) {
  for (const Chunk& c : Lookup(req.partition_id)) {     // registry lookup + auth
    ChunkReader r = c.has_file()
        ? FileReader{files[c.file().file_id()], c.file().offset()}   // pread
        : PlasmaReader{Get(c.plasma().object_id())};                // held-Get buffer
    stream.Write(r.View(c.length()));    // incremental → gRPC flow control (§4.7)
  }                                       // FileReader: pread; PlasmaReader: zero-copy view
}
```

Zero-copy send is optional per leaf (B1): plasma leaf can ByteBuffer-slice the
shared-mem view; file leaf can `sendfile`/mmap on cold reads (benchmark-gated).
v1 may just copy into the message — the user-measured in-memory ZC gain is ~nil.

### 5.5 Unified cleanup (one `ChunkResource` leaf per backing)

`ShuffleManager`'s registry entry holds a list of leaf **resources**; cleanup is
one path, the leaf differs only at the free:

```cpp
struct ChunkResource { virtual void Release() = 0; };
// FileResource:   one per output file; the file is unlink()'d when the handle's
//                 last resource is released (or its ranges hole-punched, §4.4).
// PlasmaResource: Release the held Get, then PlasmaClient::Delete (the §5.4
//                 discipline). Or, if held as a plain ray.put local ref,
//                 RemoveLocalReference → raylet unpin.
```

`Release(handle_id)` (§4.3) is **posted** from the trigger (path A executor / path
B out-of-scope callback) — never run inline (the path-B callback fires under the
ReferenceCounter mutex). `close()`/`unlink()` are further offloaded to a background
thread so a slow filesystem does not stall the serving event loop. On Linux,
unlinking a file whose `fd` a fetch handler still holds open is safe — the delete
defers until the last `close`, so an in-flight cross-node fetch finishes cleanly
(§4.3 race-safety; the plasma leaf gets the same safety from the handler's own
short-lived `Get`).

### 5.6 Orphan disk file cleanup

Failure modes that can leave orphan files:

1. **Mapper worker crash mid-task**: process exits before all chunks
   register. Some disk files may exist with no registry entry to clean
   them up.
2. **Mapper worker crash after registry built but before any reducer
   fetch**: registry vanishes with the process; disk files persist until
   session end.

Defense layers:

1. **Session-dir cleanup (baseline)**: Ray's existing `rm -rf
   session_dir` on cluster shutdown wipes everything under
   `shuffle/`. Bounded by session lifetime (worst case: hours for a
   long-running cluster).

2. **Graceful-shutdown unlink (planned)**: when a worker exits cleanly, its
   `ShuffleManager` (Ray Data layer, E6) walks its registry and unlinks any
   remaining files / releases plasma resources. Handles "graceful shutdown leaves
   no orphans". Does not help the crash case.

3. **Startup orphan scan (future, out of scope for v1)**: raylet
   could scan `session_dir/shuffle/<mapper_output_id>/` on startup and
   compare against alive `ShuffleHandle` refs (which are normal Ray
   objects, recoverable from ReferenceCounter). Mismatch → unlink.
   Mirrors the spilled-object orphan scan mechanism. Adds ~150 lines if
   we adopt it later.

For v1, layer 1 (session-dir backstop) is sufficient. Layer 2 is a
~50-line nice-to-have. Layer 3 is paranoid and can wait.

### 5.7 The two "holes" — memory hole is minor, **file hole is the real one**

The original design feared the **memory hole** (§8.2 chunked-shared: a shared
plasma object can't be freed until *all* its partitions are consumed → straggler
pins memory). In this unified model that fear is **minor**: under memory pressure
Ray spills the offending object to disk, so the "hole" just relocates to disk and
**memory is relieved automatically**. It does not pin RAM for long.

The **file hole is the real constraint**: with one file per mapper (§5.3), a
consumed partition's bytes are interleaved ranges inside a shared file, and **disk
is the last tier — nothing spills it further to relieve it.** A straggler keeps
the whole file's blocks until all its partitions are consumed.

**Resolution — hole-punch (Linux):** `fallocate(fd, FALLOC_FL_PUNCH_HOLE |
FALLOC_FL_KEEP_SIZE, off, len)` on a consumed partition's ranges returns the blocks
to the filesystem while keeping offsets stable (the file becomes sparse). This
gives **O(N) files *and* per-partition reclaim** (§4.4) — the two stop being in
tension. Integration point = §4.4 early release:

```
ShuffleManager.ReleasePartition(handle_id, k):
    1. mark partition k's chunks RELEASED in the registry   # serve now refuses them
    2. drain in-flight serves touching those ranges (§4.3 handler-pin)
    3. for (off,len) in chunks_of(k): fallocate(fd, PUNCH_HOLE|KEEP_SIZE, off,len)
    4. if all partitions released: close(fd); unlink(file)
```

> 🔒 **INVARIANT (RELEASED-before-punch).** A chunk MUST be marked RELEASED in the
> registry (step 1) — and the serve path MUST refuse a RELEASED chunk with an
> explicit `NOT_FOUND` — *before* it is punched (step 3). A punched range `pread`s
> back **zeros, not an error**, so a re-executed reducer that reads it without
> hitting the registry check gets **silent garbage**. This ordering is the
> load-bearing correctness rule of early release + hole-punch, not an optional
> optimization. (It is sharper here than in the old per-file `unlink` design,
> where a stale read failed cleanly.)

**Three gotchas — get these wrong and it silently corrupts:**

1. **A hole reads as ZEROS, not an error (the real trap).** Unlike `unlink`
   (read → file gone → clean NOT_FOUND), a punched range `pread`s back **all
   zeros**. A lineage-re-executed reducer re-reading a punched range would get
   **silent garbage**. So you must **not rely on the read to detect "gone"**: step
   1 marks the chunk RELEASED in the registry and the serve handler **refuses a
   RELEASED chunk with an explicit NOT_FOUND → typed exception → producer
   re-execution (§11 Q1)**. This makes the registry check mandatory, not optional.
2. **Drain before punch (stricter than unlink).** `unlink` is safe with an open fd
   (delete defers to last `close`); **punch does not defer — it zeros immediately.**
   So step 2 must drain any in-flight serve of those exact ranges before punching.
3. **Block alignment for full reclaim.** `fallocate` only frees filesystem blocks
   **fully covered** by the range (typically 4 KB). Back-to-back appended chunks
   aren't 4 KB-aligned, so edge blocks shared with a neighbor partition aren't
   freed (≤ ~8 KB lost per chunk, <1% at 2 MB). To reclaim fully, **pad each
   chunk's write offset to 4 KB** (≤4 KB padding/chunk — negligible at 2 MB). Else
   accept the <1% slack.

Portability: PUNCH_HOLE works on ext4/xfs/btrfs/tmpfs (Linux); on unsupported FS,
fall back to whole-file reclaim at handle release (no per-partition reclaim). Fits
the Linux-first constraint.

So the priority inverts versus the old draft: don't over-engineer to avoid the
memory hole (spill handles it); **do** plan the file-hole reclaim (hole-punch),
with the registry-refuses-RELEASED rule as the load-bearing correctness piece.

Other layout choices ruled out: `tmpfs`/RAM-disk (competes with RAM, no spill
accounting), `O_DIRECT` (alignment cost, loses page-cache hot-reread benefit),
always-disk-with-plasma-read-cache (pays write I/O even when it fits — the plasma
*tier* in §5.2 is the controlled answer to "fit in memory").

### 5.8 Cross-node fetch is backing-uniform

The receiver never knows the backing. A reducer sends one `FetchPartition` to the
source node's `ShuffleManager`; the handler reads each chunk from the file
(`pread`, hot→page cache) and streams. The reducer sees one uniform byte stream
and consumes inline into user space (never its own plasma — §4.6). Zero-copy send
is optional (B1), not the mechanism.

### 5.9 Map write at PB / large M: sort-spill-merge (= Spark sort-shuffle)

§4.1's "M staging buffers" is the **small-M** path (≈ Spark bypass-merge): it
keeps M buffers (memory ≈ M×buf) and writes one file with partitions *interleaved*
→ a partition is **many scattered chunks** → index size = O(total flushes) (can
dwarf O(N×M)), tiny-chunk `pread` random-read amplification, and hole-punch edge
loss. At PB / large M this breaks (M buffers don't fit; index bloats).

**Large-M path (the default for PB): one in-memory sort buffer, spill sorted runs,
merge to one partition-contiguous file.**

```
write:  buffer (partition_id, record) up to one buffer's worth (the only knob);
        buffer full → spill a partition-sorted run to disk; reset; repeat.
finalize (before reduce):
        if never spilled → it fit one buffer → keep as-is (page cache makes it RAM-resident)
        else → merge all runs → ONE file sorted by partition_id + a COMPACT index
               (one contiguous range per partition) + (optional) .idx sibling file
```

Wins: **memory = one buffer** (independent of M); **index = O(M) per mapper**
(one range/partition, not O(flushes)); **sequential reads**; **hole-punch = one
contiguous range per partition** (clean reclaim, no edge loss). Cost: a merge I/O
pass (read runs + write final) — standard for PB external shuffle.

> **Alternative to the merge pass: merge-on-read (§4.12).** The contiguity this
> section buys on the **write** side can instead be induced on the **read** side — the
> `ShuffleManager` serves pooled requests in offset order, getting sequential disk
> reads without the 2× merge write. Merge-on-write still wins when you also need a
> *compact O(M) index* and clean per-partition hole-punch; merge-on-read wins when the
> map-side write pass is the bottleneck. They are not mutually exclusive (you can ship
> merge-on-read first, add the merge pass only where the index/reclaim cost bites).

This is also
what makes the "partition repeats interleaved in a file" reality clean: the merge
consolidates each partition into one contiguous range. The committed form is
**always a file** (§5.0); the in-memory sort buffer is bounded, not a "fits or
not" decision.

### 5.10 Local-disk failure rate & integrity (file-granular FT)

At the target band (tens to hundreds of TB on commodity NVMe, §0.2), local disk fails
**routinely and partially** — not just "node dies." So the FT story is **file-granular
and integrity-checked**, not node-granular. (This is the gap in an earlier
"node-loss-only / liveness-only" framing — §0.6, §4.10.)

**Failure modes while the node is alive:**
- **Silent corruption** — UBER ~1e-15: a read pass over ~100 TB is ~8e14 bits →
  expect ~O(0.1–1) *undetected* bit errors. `pread` returns **wrong bytes, no error**.
  This is the dangerous one — silently wrong shuffle output; reachability cannot see it.
- **Hard read error** — latent sector errors / a failing disk → `pread` `EIO`; whole-
  disk loss → every file on it unreadable (**file-granular**, node still alive).
- **Torn / partial write** — mapper crash mid-flush → file shorter than the index says,
  or tail garbage.
- **ENOSPC** on write — handled at write time by admission control (§4.7).

**Defenses:**
1. **Per-chunk checksum (CRC32C) in the handle index (§5.1), verified end-to-end on
   read.** Mismatch ⇒ chunk is lost. The *only* defense against silent corruption (and
   it also catches torn-write tail garbage within `length`). At this scale it is
   **mandatory — T0** (§0.2.1), not optional.
2. **Length check** — index records each chunk's `length`; short read ⇒ torn file ⇒ lost.
3. **Write-commit protocol** — temp file → `fsync` → atomic `rename` → *then* register
   with `ShuffleManager`. A *registered* file is always complete; a crash mid-write
   leaves an unregistered temp that the orphan scan (§5.6) reaps. No half-written file
   is ever served.

**Recovery is file-granular.** {checksum mismatch, `EIO`/short-read, missing file} on a
**live** node ⇒ typed exception ⇒ reconstruct **that mapper** (re-run → fresh file +
fresh checksums), *without* waiting for node death. This refines §4.10: the trigger is
**node loss OR per-file disk fault OR integrity failure**; core-auto handle
reconstruction (Bundle 2) covers only node loss — the live-node disk cases are
executor-mediated via this path.

**Budget for it — reconstruction is routine, not exceptional.** Fleet-scale disk AFR +
LSE make occasional per-mapper reconstruction a *normal* operating event; the design
makes it cheap (one mapper re-run, file-granular, §5.3) and must not let a single
corrupt chunk fail the job. Note X10: **retaining the source does not protect against
corruption** — a retained-but-corrupt chunk still fails the checksum → reconstruct; so
checksums are needed at *both* X10 operating points.

**S3 leaf (§16) differs:** remote object stores carry their own end-to-end integrity +
~11-nines durability, so the *durable* class needs neither app-level checksums nor
local reconstruction — it trusts the store and re-points. Checksum + file-granular
reconstruction is specifically the **local-disk (reconstructable) class** story.

---

## 6. Lifecycle Gaps

### Gap 1: Worker idle reclamation — **MOOT under the default; CLOSED under Bundle 1**

> **Under the default (Bundle 2 + per-node actor, §4.10) this gap does not arise:**
> the per-node `ShuffleManager` holds the files and outlives any mapper worker, so a
> mapper worker being reclaimed is a **non-event**. The analysis below applies only
> to the **Bundle-1 / per-worker** variant, where serving lives in the worker and the
> worker must stay alive.

**Risk (Bundle-1 / per-worker only)**: if the mapper worker is reclaimed after its
task returns but before all reducers fetch, the bytes (its files / pinned objects)
become unreachable and fetches fail.

**Mechanism (verified):** `worker_pool.cc:1174` (`TryKillingIdleWorker`) does NOT
itself check owned objects — but it does not need to. It sends an Exit RPC; the
worker's `HandleExit` replies based on `CoreWorker::IsIdle()`, which is
`reference_counter_->Size() == 0` (core_worker.cc:4405-4410). A non-idle worker
replies "not idle" and is re-queued, not killed (worker_pool.cc:1276-1283).

**The protection is carried by the owned `ShuffleHandle` ref, nothing else.**
Because the mapper `ray.put`s the handle (§4.1), it owns an in-scope object →
`Size() > 0` → the worker is never idle-reclaimed while any consumer still holds
the handle. ⚠️ **This is the load-bearing precondition.** The bypass
`InternalPlasmaHandle` (and likewise raw files) contribute **zero** to `IsIdle` —
they are invisible to `reference_counter_`. So if the design ever stops `ray.put`-ing
the handle (e.g. reverts to a bare task return, E1), **this gap reopens
immediately** and the worker can be killed out from under live fetches.

**No separate predicate needed** as long as the `ray.put` self-hold invariant
stands. (A belt-and-suspenders `HasShuffleOutputs()` on `CoreWorker` is possible
but unnecessary, and would violate E6 — keep the protection in the handle ref.)

### Gap 2: cleanup trigger / callback API (corrected — see E4/E6)

**Correction**: there is **no** `SetObjectDeletedCallback`, and spill does not use
it. The real primitive is `ReferenceCounter::AddObjectOutOfScopeOrFreedCallback`
(reference_counter.h:150), which fires under the refcount mutex.

**Decided — path A is the default (§3.3 / §4.3):**
- **Path A (default)** — no core API at all; the Ray Data executor calls
  `ShuffleManager.Release(handle_id)` when the reduce stage finishes. Forced by the
  settled Bundle-2 + per-node + zero-core corner (driver-owned handle ⇒ no
  mapper-side callback; zero-core ⇒ no path B).
- **Path B (alternative, not default)** — add one *generic*
  `CoreWorker::RegisterOwnedObjectDeletedCallback` (~30 lines C++ + ~50 Cython); the
  callback only POSTS `ShuffleManager.Release` (never inline). Use only under
  Bundle 1, or once a core hook is allowed (§0.6).

**Risk**: Low either way. Path A touches no core; path B is a small additive
generic hook.

### Gap 3: Refcount propagation delay (acceptable)

**Risk**: borrower→owner ref-drop propagates via the `WaitForRefRemoved` pubsub channel (async, ms-scale — *not* a heartbeat; reference_counter.cc:1242-1294). After the driver/reducers drop the `ShuffleHandle` ref, the owner may take ~ms before its refcount hits 0 and the callback fires (Bundle 1 / path B only). During this window the file stays on disk.

**Status**: Inherent to Ray's design. Not specific to this proposal.

**Mitigation**: Acceptable for shuffle workloads (a few ms doesn't affect throughput). For tighter early-release use the per-(mapper,partition) `delivery_counter` of §4.4/§5.7: once a partition's reducer has *durably* consumed it, the `ShuffleManager` punches/releases that partition's ranges without waiting for the handle refcount. **Caveat**: proactive release diverges from refcount; a re-executed reducer re-fetching a released range gets `NOT_FOUND` → producer re-run. Decide policy explicitly (§4.4).

### Gap 4: Reducer death detection (slow path — application-layer mitigation needed)

**Risk**: If a reducer worker dies mid-fetch, its borrow on the `ShuffleHandle` is only reclaimed via GCS heartbeat timeout (10s+). During that window, mapper-side plasma stays alive even if no other reducer needs it.

**Status**: Inherent to Ray.

**Mitigation**: Application-layer health check in `ShuffleFetchService`. Reducer announces intent before fetching; mapper-side tracks active fetches with their own timeout (~5s). On timeout, mapper considers that reducer dead and decrements the application-layer counter. Reducer that died has its task re-executed by Ray, which gets a fresh ShuffleHandle ref.

### Gap 5: Worker memory accumulation across tasks (scheduling concern)

**Risk**: A mapper worker that produces multiple shuffle outputs (across multiple mapper tasks reused on the same worker) accumulates `ShuffleOutputRegistry` entries until each one's refcount hits 0. Total worker RSS = sum over all live shuffle outputs.

**Status**: Application concern. Ray scheduler doesn't know about this implicit memory.

**Mitigation**: Mapper worker reports `bytes_in_shuffle_registry` to raylet as a "soft resource". Raylet's resource view becomes aware. Ray Data scheduler honors it when placing new mapper tasks. ~80 lines + scheduler config.

---

## 7. Confirmed Facts (from codebase inspection)

| Fact | Evidence |
|---|---|
| `PlasmaClient::Release(object_id)` exists | `src/ray/object_manager/plasma/client.h:90` |
| `PlasmaClient::Delete(vector<object_id>)` exists | `src/ray/object_manager/plasma/client.h:222` |
| ⚠️ **MISLEADING** — `Delete` is no-op while the object's store `ref_count_>0`. The *creator itself* holds a ref from `Create`/`Get`, so "no other client" is not enough: you must `Release` your own ref before `Delete` frees it; object must also be sealed | object_lifecycle_manager.cc:94-118 |
| ❌ **FALSE** — `MutableObject` is NOT a bypass precedent. It pins via `Get` (store ref_count +1), not by holding the create buffer | client.cc:427-469 |
| ❌ **FALSE** — there is no `SetObjectDeletedCallback`, and spill does not use it. Real API: `AddObjectOutOfScopeOrFreedCallback` / `AddObjectRefDeletedCallback`, fires under the refcount mutex | reference_counter.h:150-157; reference_counter.cc:835-846 |
| ✅ `PlasmaClient` socket disconnect → plasma store releases that client's pins (orphan cleanup) | plasma store IPC design |
| 🌿 **BRANCH-ONLY** — exists on `feat/object-push-bytebuffer`, NOT in this checkout (grep here finds no `zero_copy_push_server.*`). Real per user; do not treat as in-tree | branch `feat/object-push-bytebuffer` |
| 🌿 **BRANCH-ONLY (real)** — ByteBuffer/slice/writev ZC send exists on the branch. ⚠️ user-measured: **no clear perf gain for in-memory data** (bottleneck is gRPC/syscall/batching, not the memcpy); do NOT make the design's value depend on it | branch; user benchmark |
| 🌿 **BRANCH / UNVERIFIED HERE** — `MmapRegion` + non-page-aligned offsets claimed on the branch's `zero_copy_slice.cc`; not present in this checkout, confirm on branch | branch `feat/object-push-bytebuffer` |
| ✅ Standard Ray ObjectRef ownership protocol propagates ref drops back to owner. A task *return* is **caller-owned + reconstruction-ELIGIBLE** (the **default**, Bundle 2); `ray.put` makes it **mapper-owned + INELIGIBLE_PUT** (Bundle 1 only) — §4.1/§4.10 | task_manager.cc:277-282, 294; core_worker.cc:974-993 |

## 8. Open Confirmations

| Question | How to verify |
|---|---|
| Does Ray's worker pool refuse to reclaim workers with active owned objects? | grep `WorkerPool::TryKillingIdleWorker` for predicate; trace owned-object check; if absent, this is a fix-needed |
| ~~Can `SetObjectDeletedCallback` be invoked from application code…~~ **RESOLVED:** no such API; the real one is `AddObjectOutOfScopeOrFreedCallback` (reference_counter.h:150), and path A avoids needing any core hook at all (§3.3, Gap 2). | done |
| What's the actual latency of borrower → owner refcount sync? | benchmark: create + drop ObjectRef cross-process, measure callback fire delay |
| Does plasma's eviction LRU treat our internal plasma objects identically to ObjectRef-backed ones? | inspect `PlasmaStore::EvictObjects`; check pinned-by-Get flag is honored |
| Does the GCS need to know about internal IDs? (We expect NO — bypass means no GCS) | trace: ObjectDirectory access points |
| Can a `ShuffleManager` run its own `rpc::GrpcServer` (own port + token auth) cleanly? | **RESOLVED:** multiple `GrpcService`s per server is well-precedented (node_manager.cc:269-274, gcs_server.cc); `ShuffleManager` uses its own server (E6/§3.5) |
| Multi-tenant safety across concurrent shuffles | per-shuffle `auth_token` in the fetch RPC + node-scoped registry; needs an explicit test (§11 Q4) |
| **(file leaf)** `pread` random-read throughput on scattered 2 MB ranges vs sequential | microbenchmark; informs whether end-of-task compaction (§5.3) is ever worth it |
| **(file leaf)** hole-punch full-reclaim with 4 KB-aligned chunks; reads-as-zeros guarded by registry (§5.7) | unit test: punch a range, confirm serve refuses it (NOT_FOUND), confirm blocks freed |
| **(plasma leaf, optional)** does a held `Get` reliably keep an object un-spillable? | inspect `PlasmaStore::EvictObjects` / in-use pin; confirm §5.4 |
| `posix_fadvise(WILLNEED)` prefetch worth it on cold-cache `pread` serve? | benchmark cold vs warm page-cache fetch latency |

## 9. Alternatives Considered (and Why Rejected)

### 8.1 Per-partition ObjectRef (N×M Ray refs)

**Description**: Each partition is a first-class Ray ObjectRef. Mapper task returns M tuple of refs.

**Why rejected**: Driver tracks N×M refs → metadata explosion at large scale (N=100, M=1000 → 100K refs, observable driver slowdown).

### 8.2 Chunked-shared layout (multiple partitions packed in one plasma object)

**Description**: Each plasma object holds bytes from multiple partitions interleaved; index maps `partition_id → list of (chunk_id, offset, length)`.

**Why rejected (originally)**: a shared chunk can't be freed until *all* its
partitions are consumed → straggler pins memory ("memory hole").

**Re-assessment (§5.7):** the memory hole is now judged **minor** — under pressure
Ray spills the offending object, relocating the hole to disk and relieving RAM
automatically. The framing that mattered turned out to be the **file hole**
(one-file-per-mapper, §5.3), which *is* real (disk is the last tier) and is handled
by **hole-punch** (§5.7). So this layout isn't rejected for the memory-hole reason
anymore; it's simply superseded by the unified file/plasma design (§5), where
one-file-per-mapper + offset index gives O(N) files without packing-into-one-object.

### 8.3 In-plasma reclaim (MADV_REMOVE / compaction) — and why the allocator can't help

**Description**: keep bulk in plasma and try to reclaim freed regions in place
(madvise(MADV_REMOVE)/`fallocate(PUNCH_HOLE)` on the /dev/shm file), or compact.

**Why rejected — plasma's allocator fundamentally cannot do this (verified):**
- Plasma allocates from a **dlmalloc over a pre-mmap'd /dev/shm file**
  (`plasma_allocator.h:31-50`; `dlmalloc.cc` includes thirdparty `dlmalloc.c`).
- **No compaction, and cannot have it**: objects live at fixed offsets in a file
  that *multiple processes mmap by offset, zero-copy*; relocating a live object
  would break every client's pointer. Compaction ⊥ zero-copy-cross-process. No
  userspace allocator (dlmalloc/jemalloc/tcmalloc) compacts; only moving GCs do
  (managed refs + barriers), which plasma's raw offsets preclude.
- **`mremap`/`MREMAP_DONTUNMAP` does not help**: it remaps *one process's* virtual
  addresses; it doesn't change the object's file offset (the shared identity) or
  other clients' mappings, and it's restricted to private-anonymous mappings —
  plasma's is shared file-backed.
- **Page reclaim is deliberately disabled**: dlmalloc's `munmap` is faked and trim
  is off (`dlmalloc.cc:48-53,287`); only `MADV_DONTDUMP` is used
  (`shared_memory.cc:69`). Freed space stays in the pool free-list for reuse.
- Fragmentation is a real, Ray-acknowledged failure mode
  (`object_lifecycle_manager.cc:186-188`: dlmalloc fragmentation can fail
  allocation even with free bytes).
- The only in-RAM "punch" that *would* work is `fallocate(PUNCH_HOLE)`/`MADV_REMOVE`
  on the tmpfs file — but that **is** modifying plasma's store (this alternative),
  which is the most contentious change (~20% acceptance) and we forbid.

**Net**: don't pack in plasma (its hole is unrecoverable: no compaction, no
mremap, page-reclaim disabled). The file variant packs into O(N) files and
reclaims via `fallocate(PUNCH_HOLE)` on *our own* files (§5.7) — the clean path.

### 8.4 Buffer all in heap, seal-time commit

**Description**: Mapper accumulates all output in heap, writes one immutable plasma object at task end.

**Why rejected**: Mapper RSS = total mapper output. Defeats streaming write goal. For large outputs (GB-scale per mapper), OOM-kills mapper before commit. Same memory problem just moved from plasma to heap.

### 8.5 Adaptive layout selection (small M → per-partition, large M → chunked-shared)

**Description**: Choose layout at runtime based on M and expected output size.

**Why rejected**: Two code paths to maintain, complex scheduler logic. Bypass eliminates the reason we needed adaptive selection (the driver metadata cost was the only forcing function for chunked-shared, and bypass removes it).

### 8.6 Move semantic (colleague's parallel work)

**Description**: Atomic transfer of object ownership from one node to another; source frees its copy.

**Why doesn't fit (for the O(N) shuffle)**: Hash shuffle is 1→M fan-out, not 1→1 transfer. Move transfers ownership singularly (frees the source) → can't serve M consumers from one source. It *would* remove the destination-resident 2× (X3) **within** the `ray.get`/Pull dataflow — but only at per-(mapper,partition) = **O(N×M)** granularity (each chunk → exactly one reducer), which reintroduces **X4**'s driver-ref explosion. At the design's **O(N)** granularity the mapper output is fan-out-read, so move can't substitute for the side-channel here. **It is the reason X3 is contingent (C) rather than fundamental, but X3 stays binding for this design.** Compatible / coexists (different use case).

---

## 10. Implementation Plan / PR Sequencing

File-variant-primary sequencing (the old PR2 `PullObjectRange` and PR3 bypass
primitive are **dropped** — see Appendix B / E2/E3). Each PR is independently
mergeable; core footprint is 0 until the optional path-B callback.

### PR 1: `ShuffleHandle` + Ray Data integration, baseline via `ray.get` (no core changes)

- Unified `ShuffleHandle` proto (§3.1) + Python wrapper; mapper **returns** it
  (Bundle 2 default, driver-owned/ELIGIBLE; `ray.put` only for Bundle 1 — §4.10).
- Ray Data shuffle operator emits N handles; reducers initially read partitions via
  ordinary `ray.get` (through plasma — correctness baseline, still pays the 2×).
- Benchmark: driver metadata O(N×M)→O(N). Hardens the operator + lineage wiring on
  a simple transport before the bypass.
- **Acceptance ~90%.** Pure Ray Data layer.  **~400 LOC + ~600 tests.**

### PR 2: `ShuffleManager` + file write/serve + cleanup (the main path)

- `ShuffleManager` worker(or node)-process singleton, Ray Data layer (NOT CoreWorker, E6):
  registry, own `rpc::GrpcServer` (own port + token auth), background unlink pool.
- File leaf: one-file-per-mapper append + offset index (§5.3); `ShuffleFetchService`
  serves via `pread`, incremental stream (flow control, §4.7); handler self-pin (§4.3).
- Cleanup: **path A** (executor calls `ShuffleManager.Release`) — zero core; or
  **path B** — one generic `CoreWorker::RegisterOwnedObjectDeletedCallback` (decide
  before merge). Orphan defense: session-dir backstop + graceful unlink (§5.6).
- **Acceptance ~75%.** 0 core (path A) / 1 generic hook (path B).  **~700 LOC + ~800 tests.**

### PR 3: Fetch-side plasma bypass + per-partition reclaim

- Reducer reads via the side-channel (raw opaque ids → no Pull path), bytes into
  user space — **kills the destination 2×** (§4.6).
- `ReleasePartition` + hole-punch reclaim with the registry-refuses-RELEASED rule
  + 4 KB alignment + drain-before-punch (§5.7); `ReduceShard` sub-splitting (§12).
- **Acceptance ~70%.** No core change. **~500 LOC + ~500 tests.**

### PR 4 (optional): zero-copy send + S3 backing

- Optional ByteBuffer/`sendfile` zero-copy send (B1, benchmark-gated — §4.5).
- **S3 backing leaf (§16)** for the cloud/disaggregated regime: per-partition merge
  (push-based), reducers read S3 directly (no per-node actor), durable→no recompute.
- (A plasma *bulk* tier is NOT a PR — it was withdrawn in §5.0; bulk is always a file.)
- **Acceptance ~60%.** Regime/perf extensions. **~400 LOC + ~400 tests.**

### Sequencing rationale

PR 1 delivers the **O(N) driver-metadata** win on a trivial transport (still 2×),
de-risking operator + lineage wiring. PR 2 delivers the **primary file path** (PB
storage, controlled backpressure, O(N) files) and is independently shippable. PR 3
delivers the **no-2× memory win** + per-partition reclaim. PR 4 is pure
optionality (in-memory fast path, ZC). Nothing touches plasma/raylet; the only
possible core line is one generic callback.

---

## 11. Open Questions

1. **Recovery model & correctness** (see §4.10 for the two bundles).
   - **Wiring**: Bundle 1 needs a typed fetch-failure exception the Ray Data executor
     treats as "ShuffleHandle unreachable → re-run producer". Bundle 2 gets the
     producer re-run automatically from core (borrower `ray.get(handle)` on a
     node-lost, reconstruction-ELIGIBLE return), but the reduce *task* retry is
     still the executor's job.
   - **Recovery-correctness invariants (BOTH bundles, else silent data loss):**
     (a) **Mapper determinism** — Ray assumes, does not enforce (task_manager.cc:219-224);
     a re-run must reproduce the same partition→records mapping.
     (b) **Pin the partition plan** — a sampled/byte-balanced partitioner (§12) must
     reuse the exact boundaries on re-run, or a reducer mixing already-consumed +
     re-fetched data drops/duplicates records.
     (c) **`max_retries>0` + `lineage_pinning_enabled`** for Bundle 2's core path
     (defaults on; if a job disables retries the handle is `INELIGIBLE_NO_RETRIES`).
     (d) **Early release gated on durable reducer completion** (§4.4/§5.7) — else a
     re-run reducer re-fetches a released/punched chunk → NOT_FOUND → cascades a
     producer re-run (and a punched range reads as zeros — §5.7 RELEASED-before-punch).

2. **Locality scheduling**: ShuffleHandle carries `mapper_node`. Should Ray Data's scheduler use this to co-locate reducers on mapper nodes (minimizing cross-node fetches)? Probably yes, but the integration with Ray scheduler is non-trivial. Phase 2 work.

3. **Disk** — ~~fallback~~ **RESOLVED**: disk is the *primary* backing (file variant, §5), not a fallback. The unified `Chunk{oneof}` (§5.1) carries per-chunk backing; the same fetch RPC serves file (`pread`) and plasma (`Get`) chunks. The open part is reclaim policy (§4.4/§5.7 hole-punch) and the §5.2 placement budget if the plasma tier is enabled.

4. **Multi-tenancy**: can two simultaneous shuffles in the same cluster interfere? Auth token + node-scoped registry should handle this, but needs explicit test.

5. **Two-level hash for extreme M (>10K)**: out of scope for v1, but should not be precluded by the API. ShuffleHandle should compose recursively if needed.

6. **GPU**: out of scope. ShuffleHandle could carry device_num metadata for future GPU shuffle, but plasma's current GPU object support is limited.

7. **Object/file count at scale** — **RESOLVED**: bulk is one file per mapper +
   offset index (§5.0/§5.9) = **O(N) files, no per-chunk OS object**, so the
   N×M-small-plasma-objects concern does not arise (plasma is fired for bulk). The
   remaining knob is keeping per-RPC payloads large (§4.5) and chunk/run sizing.

8. **Ray Object Store (the Python-facing API)**: should `ShuffleHandle` be visible to users via `ray.put_partitioned()` or stay an internal Ray Data primitive? Recommendation: internal-only for v1; promote to public API only after Ray Data integration stabilizes.

9. **`ShuffleManager` granularity — RESOLVED: per-node Ray actor (local regime).**
   Per-node (one actor per node, serving all local workers' files on shared local
   disk) gives **P² connections** vs (workers)², survives individual worker death
   (serving decoupled from the producer worker — §4.10), and matches Spark's shared
   block-transfer service. Make it a **Ray actor** (placement-pinned one-per-node)
   so it participates in Ray's placement/restart, touching `CoreWorker` zero (E6).
   Per-worker (reuse the CoreWorker gRPC server) was the early idea but is
   superseded: it can't node-batch (→ N×R connections) and dies with the worker.
   **Note:** in the **S3 regime (§16) there is no serving actor at all** — reducers
   read S3 directly.

10. **Transport floor benchmark (before committing to plain gRPC).** §4.5 argues
    gRPC is fine because PB-scale payloads are ~100 MB/RPC. Validate: measure
    achieved fetch bandwidth vs NIC line rate at the target chunk size; confirm
    framing overhead is negligible at the planned batch size; only then rule out a
    raw-socket/`sendfile` transport. This gates §4's throughput assumption.

---

## 12. Skew Handling (PB-scale corpus preprocessing)

At PB scale, skew is the norm, not the exception: a handful of keys dominate (a
few giant domains/languages, a boilerplate document duplicated millions of times,
an oversized MinHash-LSH band). Plain `hash(key) % M` drops a hot key entirely
into **one** partition → one reducer receives TB-scale input → OOM + straggler.

**Design advantage exploited here:** because `partition_id` is an *opaque label*
inside the handle (the transport only addresses `chunks`), all skew handling lives
in the **partitioner + reduce-scheduling + operator** layers. None of it touches
Ray's O(N) handle/refcount machinery — unlike per-partition-ObjectRef designs,
where re-sharding a hot partition would mint more driver refs.

### 12.1 Skew detection is (almost) free

The handle already records the byte length of every `(mapper, partition)` chunk.
After the map stage and *before* launching reducers, the scheduler sums lengths
per `partition_id` across the N handles — metadata only, no data movement, no
extra sampling pass:

```python
sizes = defaultdict(int)
for h in map(ray.get, handles):                 # N small metadata objects
    for loc in h.partitions:
        sizes[loc.partition_id] += sum(c.length for c in loc.chunks)
hot = [p for p, b in sizes.items() if b > SHARD_THRESHOLD]   # e.g. > 2 GB
```

`hot` drives the re-sharding decisions below. Cost: N small `ray.get`s + O(N×M)
additions (sub-second even at N=M=10⁴).

### 12.2 Toolbox, ordered by leverage

**(a) Map-side combine — shrink at the source (decisive for dedup/count).**
Pre-aggregate the hot key *inside each mapper* before writing partition files.
Exact dedup: each mapper keeps one representative per content hash → a
million-times-duplicated doc arrives at the reducer as ≤ N copies, not millions.
No transport change — one `combine` step in `map_task` before the partition
flush. *Required for aggregation operators.*

**(b) Reducer-side sub-splitting of hot partitions — for per-record operators
(near-free).** Chunks are individually addressable, so a hot partition `k` is
split across S reduce sub-tasks, each fetching a disjoint subset of mappers'
chunks. The reduce-task identity changes from a bare `partition_id` to:

```protobuf
message ReduceShard {
  uint64 partition_id = 1;
  uint32 num_shards   = 2;   // S for hot partitions; 1 otherwise
  uint32 shard_index  = 3;   // which mapper-subset / chunk-range this task fetches
}
```

For `filter` / `clean` / `tokenize` / reformat (no cross-record dependency) the S
shards produce independent outputs — **correct with no merge.** Scheduler change:
emit S reduce tasks for each `partition_id` in `hot`. Driver refs stay O(N).

**(c) Salting + two-stage merge — for per-key operators on hot keys.** When
records of one key must be processed together (dedup-within-key, group, count),
chunk-splitting alone scatters a key. For *hot keys only* (from 12.1 / a map-side
sketch), repartition by `(key, salt ∈ [0,S))` in stage 1 → S partial aggregates →
a small stage-2 merges partials per key (dedup is idempotent, count sums, group
concatenates). The long tail stays on plain hash; stage-2 volume ≈ S × |hot keys|.

**(d) Heavy-hitter isolation + pluggable, byte-balanced partitioner.** Because the
partition function is opaque to transport, swap `hash % M` freely: a map-side
Count-Min/SpaceSaving sketch finds the top-H keys and gives each its own
`partition_id` (then handled by (b)/(c)); the tail uses a partitioner balanced by
**bytes** (boundaries from the 12.1 histogram), not by key count. `M` may grow
slightly but the handle is still O(M) per mapper, driver still O(N).

**(e) MinHash-LSH giant buckets — operator responsibility.** A hot LSH band is
both large *and* O(n²) internally; sub-splitting the reducer only parallelizes
candidate generation, not convergence. The shuffle layer guarantees the bucket is
splittable and delivered; recursive re-banding / intra-bucket LSH belongs to the
operator, not here.

### 12.3 Memory / cleanup interaction (PB requires external-memory reducers)

- Even after sub-splitting, a hot shard can be large → the reducer must be
  **streaming / external-merge**, never fully in RAM. The fetch path already
  streams chunks into user space for inline consume; the reducer spills its own
  working set (explicit-file variant: to its temp files; `ray.put` variant: Ray
  spills) as needed.
- §4.4 per-(mapper, partition) early release matters here: cold partitions are
  freed as soon as their reducer drains them, so only the hot partition's bytes
  linger through sub-splitting → markedly lower peak.
- Serving load is **naturally balanced**: a hot partition's bytes are spread
  across all N mappers, each serving only its slice; the bottleneck is the
  *receiver*, which is exactly what (b)/(c) split.

### 12.4 What to add (summary)

| Change | Layer | Size |
|---|---|---|
| Hot-partition detection (sum handle chunk lengths before reduce launch) | scheduler | ~40 lines |
| `ReduceShard` (partition_id + num_shards + shard_index) replaces bare partition_id | proto + reduce task signature | ~30 lines |
| Pluggable partitioner + map-side heavy-hitter sketch | map side | ~150 lines |
| Hot-key salting + stage-2 merge (per-key operators only) | operator layer | ~120 lines |
| Map-side combine hook | operator layer | operator-dependent |

**Invariant preserved:** every mechanism above lives in the partitioner / reduce
scheduler / operator. None changes the handle count, the refcount path, or the
fetch transport — the driver stays **O(N)** regardless of how aggressively hot
partitions are re-sharded.

**Operator → technique cheat sheet:**

| Operator | Skew shape | Technique |
|---|---|---|
| filter / clean / tokenize / format | uneven partition bytes | (d) balanced partitioner + (b) sub-split |
| exact dedup | one content-hash duplicated massively | (a) map-side combine → residual handled by (c) |
| fuzzy dedup (MinHash LSH) | giant LSH band | shuffle: (b) sub-split delivery; convergence: (e) operator |
| group-by / count by domain/lang | one giant key | (a) combine + (c) salting + 2-stage |

---

## 13. Ray Data integration — operator I/O contract

The shuffle is a self-contained **AllToAllOperator**: **RefBundles in, RefBundles
out**, with the out-of-band machinery (files, side-channel, ShuffleManager) hidden
inside. This keeps it idiomatic — swappable with `push_based_shuffle`.

- **Upstream → in**: RefBundles of Arrow blocks (raw data). Standard.
- **Out → downstream**: the reduce stage emits **normal Ray blocks** (`ray.put`/
  task-return RefBundles). The out-of-band-ness is *internal only* — downstream
  sees ordinary blocks. So nothing leaks; idiomatic.
- **group-by composes via three hooks**: `(partition_fn, map_combiner,
  reduce_aggregator)` — the standard Ray Data `aggregate()` shape. Map-side combine
  (§12.2a) is the combiner; final aggregation is reduce-side. **Synergy**: the
  sort-spill-merge (§5.9) gives *sorted-within-partition* input → reduce can do
  **streaming sorted aggregation** (no hash table, low memory). A giant group →
  §12 skew (salting + 2-stage / sub-split).
- **hash vs range partition**: `partition_fn` = hash for group-by/dedup/repartition/
  random_shuffle; = range (sampled, pinned plan — §11 Q1) for `sort()`. Same I/O
  contract; downstream needing global order must sort (hash output is only
  partition-local).

**The one real seam (must wire, else it feels bolt-on):** the internal map→reduce
edge carries **handles (KB)** while the real bytes are out-of-band **files (PB)**.
So the streaming executor's resource model sees ~0 data and ~0 memory for the
shuffle — but the actual footprint is **page cache + disk** (which Ray does not
track). **Wire the shuffle's page-cache+disk footprint into Ray Data's resource /
backpressure / OOM accounting**, or the executor over-admits and mis-schedules.
This is the price of the out-of-band data plane (§5.0). Also: the shuffle is a
**barrier + optimization boundary** (no fuse/pushdown across it — normal), and the
**output side also needs skew block-sizing** (a giant group → huge output blocks).

## 14. Competitive positioning vs Spark (honest)

- **Isolated PB shuffle throughput**: we will **not** beat Spark — we are
  re-implementing classic sort-shuffle and trail its ~15 yr per-byte tuning
  (compression, codegen, serialized sort). State of the art for the *random-read-
  bound* regime is **push-based** (Magnet/Cosco/Celeborn: pre-merge per partition →
  few large sequential reads). We are classic **pull**.
- **But push-based is regime-specific**: it trades extra network/coordination +
  replica-FT for sequential reads. **If the network is the bottleneck (common
  local/cloud all-to-all), pull suffices** and push's extra hop is counterproductive
  (per § our analysis). Push wins in the **fast-fabric + per-request-latency-bound**
  regime — notably **S3 (§16)**, where N small ranged-GETs are death.
- **FT is (engineering-)free and simpler**: recovery = re-run the producer via Ray
  lineage (§4.10); **no replica storage, no replica-consistency** — whereas Spark
  *built* shuffle FT (ESS, push-merge replicas) and push-based **creates** replica
  complexity we don't have. (Honest: recompute cost on loss is comparable to
  Spark's; "free" = no mechanism + no replica storage, not free of recompute.)
- **End-to-end is where we win**: for a Ray/Python/GPU corpus pipeline, staying
  native avoids the **Ray↔Spark ETL boundary** and the **PySpark JVM↔Python UDF
  tax** (corpus UDFs are Python/ML-bound). So end-to-end wall-clock + ops can win
  even if isolated shuffle is slower.
- **Don't wage war on the shuffle microbenchmark**; win on "PB corpus processed
  inside Ray, no system hop." To close the shuffle gap itself in the random-read
  regime, the **per-node ShuffleManager can evolve into a push-based per-partition
  merger** — leave room in the handle/fetch protocol; only enable when network is
  not the bottleneck.

## 15. Forward-compat: first-class persistent storage as a backing leaf

If Ray makes persistent storage first-class, it is **an opportunity, not a threat
— if the seams stay clean**:

- A **durable** first-class store becomes a new `Chunk` leaf (`PersistentLoc`) +
  an adapter (§5.1 oneof, §3.5 pluggable transport). Durable bulk **removes the
  recompute-on-node-loss cost** (the Q2/§4.10 cost) — node death → re-read, no
  recompute — *without us building replication*. So it improves FT for free.
- **Don't over-invest in the at-risk bespoke machinery** (the out-of-band file
  store, per-node serving, opaque-id hack, self-transport) — those are what a
  first-class store could replace. Invest in the durable parts: handle abstraction,
  O(N)-driver trick, operator integration, skew, recovery reasoning, the §0 axioms.
- **Treat §0's axioms as the requirements spec** we want Ray's persistent storage
  to satisfy (range-addressable, no-2×-read, O(handle) metadata, controlled
  placement, per-partition reclaim, durable). If it does, our shuffle becomes a
  thin client; if not, we keep the out-of-band plane. Our shuffle layer is, in
  effect, a scoped prototype of what that store should be — lead it, don't fight it.
- **Risk**: if it ships incompatible with our access pattern (forces ray.get
  materialization / per-object refs), two data planes coexist awkwardly.

## 16. Remote object-store (S3) backing — the disaggregated regime

S3 (or a remote durable store) is a **second regime**, not just another leaf. The
§5 page-cache-file design is **local-disk-specific**; S3 changes five things:

1. **No page cache** → the "let the kernel tier it, no decision" property is gone;
   you need an explicit local-SSD/in-mem read-through cache or you eat S3 latency.
2. **Per-request latency + cost → small/scattered reads are death** → you **must**
   have one large contiguous object per *partition* (so a reducer does one ranged
   GET, not N). That means **per-partition merge = push-based** (Celeborn/Cosco),
   not classic per-mapper pull. **S3 reopens push-based** — exactly the high-per-
   request-cost regime where it wins.
3. **Reducers read S3 directly → no per-node ShuffleManager actor / side-channel**
   (S3 *is* the shared serving layer). Simpler topology, different data path.
4. **Durable → node death loses nothing → no recompute** (the §15 win, realized).
   The handle just holds S3 keys.
5. **Cost/network**: mapper→S3→reducer = **2 network hops** + PUT/GET request +
   storage cost → batch into **large objects** (multipart upload), few not many.

The abstractions (Chunk oneof, transport, handle, operator, skew) carry over, but
S3 is a different **storage topology + read path + recovery** — a second shuffle
mode for cloud/elastic/spot/over-local-disk-capacity, sharing the handle/operator/
skew machinery. **When to use which**: local disk (on-prem/dense/network-bound) →
pull + page cache + per-node actor + recompute-FT; S3 (cloud/elastic/durable/
unbounded) → push-based merge + direct reads + durable-no-recompute, paying network
2-hop + request cost.

> Note on "1×": 1× is a **guardrail, not a KPI** — it correctly forbids routing
> shuffle through Ray's copy-on-get (which is 2× *and* N×M refs), which is the real
> value. At PB the first-order bottleneck is I/O (disk/network/S3 requests), not
> peak memory; the clean file design has 1× *for free*, so don't contort the design
> (bypass primitives, allocator tricks) to chase the memory factor further.

---

## Appendix A: Concrete API surface

### A.1 New C++ APIs

**In `CoreWorker`: nothing (path A), or one generic hook (path B).** The bypass
`CreateInternalPlasmaObject` / `ReleaseInternalPlasmaObject` are **removed** (E2/E3
— unsound, and unnecessary once partitions are files or `ray.put` objects). All
shuffle logic lives in `ShuffleManager` (E6), not here.

```cpp
// src/ray/core_worker/core_worker.h
// Path B ONLY. Generic — not shuffle-aware. Omit entirely for path A.
class CoreWorker {
 public:
  /// Fire `callback` when an object OWNED by this worker leaves scope
  /// (refcount → 0). Thin wrapper over
  /// ReferenceCounter::AddObjectOutOfScopeOrFreedCallback (reference_counter.h:150;
  /// there is NO `SetObjectDeletedCallback`). The callback fires WHILE the
  /// ReferenceCounter mutex is held → it must only POST to an executor, never
  /// block or re-enter the reference counter (§4.3).
  Status RegisterOwnedObjectDeletedCallback(
      const ObjectID& owned_id,
      std::function<void()> callback);
};
```

```cpp
// python/ray/data/_internal/shuffle/shuffle_manager.{h,cc}  — Ray Data layer, NOT core
// Per-node Ray actor (one per node). See §3.4 for the full interface:
//   ShuffleManager.ForLocalNode().Register(handle_id, files, index);
//   ShuffleManager.ForLocalNode().Release(handle_id);   // idempotent
//   ShuffleManager.ForLocalNode().fetch_endpoint();      // this node's actor addr
// Owns its own rpc::GrpcServer (own port + token auth) hosting ShuffleFetchService.
```

### A.2 New protobufs

See section 3.1 and 3.5 above.

### A.3 Python-facing (Ray Data internal)

```python
# python/ray/data/_internal/shuffle/handle.py

class ShuffleHandle:
    """Opaque handle representing one mapper output. Internal to Ray Data."""
    def __init__(self, ref: ObjectRef, partition_index: PartitionIndex):
        ...
    
    @classmethod
    def from_mapper(cls, partition_file: str, index: "PartitionIndex",
                    fetch_endpoint: str) -> 'ShuffleHandle':
        """Called inside a mapper task after it has written its per-mapper
        file (§5.3). Registers (handle_id → file, offset index) with the
        node's ShuffleManager actor and returns a serializable handle."""
        ...

def fetch_partitions(handles: List[ShuffleHandle],
                     partition_id: int) -> Iterator[bytes]:
    """Called inside a reducer task. Issues batched FetchPartition RPCs to
    each unique source node's ShuffleManager and yields raw bytes for inline
    consumption."""
    ...
```

---

## Appendix B: Comparison Table

| Approach | Driver refs | OS objects/files | Streaming write | Holes | Ray-core changes | Verdict |
|---|---|---|---|---|---|---|
| Current Ray Data hash shuffle | O(N×M) | O(N×M) | yes | no | none | 100% (status quo); 2× pull memory |
| Per-partition ObjectRef | O(N×M) | O(N×M) | yes | no | none | rejected: driver metadata explosion |
| Chunked-shared in plasma | O(N) handle | O(N×K) | yes | memory (minor — spill relieves, §5.7) | none | superseded by unified §5 |
| Single plasma per mapper + MADV_REMOVE | O(N) handle | O(N) | needs new API | resolved | major (new sealed-state) | ~20% |
| Bypass + per-partition `InternalPlasmaHandle` (old draft) | O(N) | O(N×M) plasma | yes | no | "none" but unsound pin/Delete (E2/E3) | dropped |
| **Unified file/plasma (this design, §5)** | **O(N)** | **O(N) files** (file leaf) / O(N×M) objs (plasma leaf) | **yes** | file hole → **hole-punch** (§5.7) | **0** (path A) / 1 generic hook (path B) | **recommended; file default, plasma optional** |

---

*End of design draft. Comments and verification results to be appended as the design discussion progresses.*
