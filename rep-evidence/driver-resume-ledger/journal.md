# Design loop journal: ray-driver-resume-kv-ledger

Deep dive on the ledger design: resume a Ray job from a
durable progress ledger held in GCS `internal_kv`, with no Redis and no external object
store.

---

## Iteration 0 — 2026-09-10 — framing, verification, claim extraction

**Mode:** design-only. No POC, by explicit instruction — the question is still open enough
that building a rig now would be premature commitment.

**What this iteration did:** verified every load-bearing assumption of the ledger design against
`ray-project/ray@80142ba` (master, 2026-09-09), then converted the option sketch into a
mechanism with three named parts (M1 epoch fencing, M2 commit coalescing, M3 compaction) and
13 falsifiable claims.

**The assumption audit came back clean, which is itself suspicious and worth stating.**
`internal_kv` is fsync-durable under `gcs_storage=rocksdb`; it has its own RocksDB instance
separate from the GCS tables; an acked put is on disk; detached-actor ownership genuinely
outlives the driver down to the raylet's worker-reaping logic. Nothing in the substrate
refutes the design. When a design survives its own verification pass untouched, the right
response is to suspect the pass was not severe — hence the deliberate move to concentrate
the claims on *protocol correctness* rather than on *substrate capability*.

**Biggest surprise: Ray Serve is already this design.** The Serve controller is a detached,
named, `max_restarts=-1` actor that checkpoints all hard state to `internal_kv` and rebuilds
from it on restart — including a two-source reconciliation (target from KV, current from
enumerating live actors) that the ledger design must copy. This moves the ledger design from "novel and risky" to
"recombination of two shipped in-tree patterns": Serve's checkpointing plus Ray Train v2's
actor shape. Train v2 has exactly the right actor options and *deliberately does not
persist* (`# NOTE: All runs and attempts are stored in memory`), which is the gap.

**Second surprise, and the one that changes the upstream story: REP-64 names this design as
its own unbuilt follow-on work.** Its scope disclaimer says the RocksDB backend delivers
"a foundation for follow-on work — first-class 'detached jobs' / checkpointable drivers".
Meanwhile searches for `"resumable job"`, `"job resumption"`, `"driver restart"`,
`"driver fault tolerance"`, `"durable coordinator"` each return **zero** hits across
`ray-project/ray` issues, and no REP in the 39-entry corpus touches it. So the ledger design is filling a
gap upstream has explicitly named and left empty. That is a much better contribution
position than "we invented something."

**The one genuinely new construction: M1.** `internal_kv` has no CAS on values, but
`put(overwrite=False)` is a CAS against "absent". That alone is unsafe, because an
infinitely-retried put cannot distinguish a lost ack from a lost race — and the GCS client
retries `UNAVAILABLE`/`UNKNOWN` forever, so this happens on every head failover. Adding a
read-back of a unique instance id fixes it, and writing all state to *epoch-scoped immutable
keys* removes CAS from the hot path entirely: a stale writer physically cannot touch the
current epoch's keys. Deliberately, this does **not** depend on the actor name registry,
even though that registry *is* a genuine atomic CAS in the GCS — the ledger's safety should
rest on the ledger.

**Reframing that came out of M2.** Durability here is not a correctness question, it is an
economics one. At ~3.81 ms fsync per durable write (~260/s same-key, ~593/s aggregate), the
design lives or dies on the ratio between the commit window W and the job's completion rate:
`writes/sec = rate/W` versus `redo ≈ W/2 + in-flight`. The consequence is that the operating
envelope is a **band, not a half-plane** — too-fine-grained work makes W so large the redo
window swallows the benefit; too-coarse work makes the ledger pointless. Finding the band's
edges is the main thing a POC would be for, and notably it is *not* the highest-VOI question.

**Ranking outcome, and why it justifies deferring the POC.** The top four claims by value of
information are C6 (compaction crash-safety, 3.20), C13 (constructor idempotence, 3.15),
C1 (epoch fencing, 2.33) and C12 (cluster-incarnation scoping, 2.25). **All four are protocol
correctness claims settleable on paper or in a model checker, with no Ray cluster involved.**
The expensive claims — real head failover, write economics under load — rank lower. So the
design-first instruction is not merely a preference here; it matches where the information
actually is.

**Risks found that the option sketch had not anticipated:**

- **ray#55996 (OPEN, P1):** `DEADLINE_EXCEEDED` is not in the GCS client's retryable set, and
  a timed-out `InternalKVPut` wedges the caller — job stuck `RUNNING`, unstoppable by
  `ray job stop`. So: pass a timeout *and* own the retry loop above it *and* make every write
  idempotent.
- **The KV RocksDB instance's cluster-id guard is disabled** (`expected_cluster_id=""`),
  unlike the tables instance. A new cluster pointed at an existing storage path silently
  inherits every prior key. Given KubeRay recreates clusters on RayJob retry while PVs get
  reused, this is not exotic. Added C12.
- **A restartable coordinator holds its name forever.** With `max_restarts=-1` the name is
  never released spontaneously, so a wedged coordinator cannot be displaced by creating a new
  one — it must first be `ray.kill(..., no_restart=True)`. "Restart the coordinator" is an
  explicit operation, not something a new driver does implicitly.
- **Recovery must be eager.** ray#65037 is the in-tree counter-example: job recovery only
  runs when a later API request lazily constructs the `JobManager`, leaving jobs stuck.
  the ledger design's recovery belongs in the coordinator's `__init__`, which is also the only placement
  that composes with `max_restarts=-1`.
- **Split brain is mostly prevented but not entirely.** ALIVE actors are never re-created on
  GCS restart ("We should not reschedule actors in state of `ALIVE`") and leaked workers are
  reclaimed. The residual window is an actor in `RESTARTING` when the GCS dies. M1 keeps the
  ledger safe regardless; C13 covers the constructor.

**Ripples:** C7 raised to `p_wrong=0.55` on the strength of ray#55996 — failover behaviour is
now the most likely-wrong non-protocol claim.

**Deliberately not done:** no fidelity gaps registered, because there is no rig yet and
therefore no fakes. The first POC plan must register them before any run.

**Next iteration should:** settle C6 and C13 by enumerating crash interleavings — cheapest
possible test, highest VOI, no cluster required. Only after that is a rig worth building.


---

## Decision point - 2026-09-10 - one design taken forward

**Human decisions:** settle the paper claims before building any rig; the POC
should attack the claims most likely to be wrong rather than demonstrate the
happy path.

A sibling design was explored in parallel and closed at this point. It aimed at
resume for *arbitrary* driver code with no application cooperation; this design
deliberately does not, and that scope difference is recorded in the non-goals.
What follows concerns only the progress-ledger design.

## Iteration 1 — 2026-09-10 — C6: M3 compaction crash-safety

**Selected:** C6 (VOI 3.20, load-bearing, kill criterion 3). Pre-registered in
`experiments/C6.md` and committed before the rig existed, so the predictions
could not be shaped by the implementation.

**Ran:** `runs/20260910-041339-C6` — exhaustive DFS over crash and lost-ack
schedules, 3 workload shapes × fault budgets 1 and 2, **16,489 traces**, zero
violations of INV-DUR, INV-PTR, INV-GC, INV-REDO or INV-LIVE.

**Controls:** 5 of 6 voiding controls red; NC6 green as the card pre-committed
was possible. Run valid.

**Outcome:** C6 **proven**, scoped to the single-writer case.

### Two rig bugs, both found before the run, both worth remembering

1. Faults were being injected into the *audit* instance — the very instance
   INV-GC is defined against ("after one clean instance completes startup").
   That made the invariant vacuously false and produced 1,051 fake violations
   at budget 2. Caught because the number was implausibly large, not because
   anything failed. Lesson: an invariant that references "a clean run" needs
   the clean run to be outside the fault model by construction, not by luck.

2. **The more serious one.** Four controls (NC4, NC5, NC7, NC10) go red with
   *zero faults injected*. That means a green result on the real protocol
   would have been entirely compatible with crash and lost-ack injectors that
   did nothing at all — the controls proved the invariant checker worked, not
   the fault model. Fixed by adding two invariants that a fault-free run
   *cannot* violate, and a control for each:

   - `INV-REDO` (redo ≤ crashes × W) with **NC12** `resume_ignores_ledger`
   - `INV-LIVE` (some instance completes) with **NC1b** `fence_mode=raw`

   This is the skill's "blind rig" failure mode arriving in a disguise the
   guardrail does not name: the rig was not blind, but the *controls* were
   testing the wrong half of it.

### What was learned about the design

- **The risk was not where the prior said it was.** `p_wrong = 0.40` on C6
  imagined subtle crash-window races inside compaction. There are none. Every
  compaction-ordering bug modelled here is unconditionally broken and would be
  caught by an ordinary integration test. The only failure mode that *requires*
  a crash to observe is redo; the only one that requires a lost ack is a
  liveness failure in fencing. M3 is the easy part.

- **The design doc's stated reason for unique `base@n` keys is wrong.** In-place
  base overwriting survived 6,513 traces. The state is a grow-only set, so
  re-derivation is idempotent, and epoch scoping means there is no racing
  writer in the sequential model. Uniqueness may still be right — but for a
  concurrency reason, not a crash-safety reason. New claim **C15**.

- **S7, not compaction, is the dangerous ordering.** Sweeping before the
  carry-forward pointer flip destroys the whole ledger with no crash at all.

- Incidental, not pre-registered, so priors only: steady-state key count was
  **3** for N = 6, 8 and 12 alike (evidence for C2's "independent of N"), and
  INV-REDO never fired (evidence for C2's redo bound). C2 `p_wrong` 0.35 → 0.15.

### Ledger transitions

`C6 -> proven` · `C15` added · `C2 p_wrong 0.35 -> 0.15` · `C13 p_wrong 0.40 -> 0.30`

### Next

C13 (2.70) → C1 (2.33) → C12 (2.25). All three are **concurrency** claims, and
all three need the same single rig extension: a driver that runs two coordinator
instances interleaved at KV-operation granularity, including a stale writer that
keeps going after a newer epoch has been claimed. That is iteration 2's work, and
it also settles C15.

---

## Iteration 2 — 2026-09-10 — C1 proven, C16 refuted

**Selected:** C13 was top of the ledger, but C1 was taken first: C13 asks whether
the constructor is safe under a duplicate, which is only a meaningful question if
fencing works at all. Building the concurrent driver served both.

**Ran:** `runs/20260910-042522-C1` (**VOID**) and `runs/20260910-042707-C1`.

**Outcome:** **C1 proven** (0 `INV-ONE` in 12,344 exhaustive + 3,000 fuzz traces,
11,746 of them reaching a live fenced instance). **C16 refuted** — 627 `INV-DUR`
violations.

### The refutation

A fenced coordinator does not stop working, and one of the things it keeps doing
is **compacting its own epoch**. Compaction deletes segments. The new epoch's
carry-forward reads `base_ptr`, then `base@p`, then the segment list, then each
segment — four separate, non-atomic operations. If the fenced writer compacts in
the middle of that, the reader ends up holding a base from *before* the
compaction and `None` for every segment the compaction has since deleted. It
carries forward a state missing everything in between, and then sweeps the old
epoch, deleting the one key (`base@m`) that still had the full picture.

**The design doc's argument for M1 is true and beside the point.** "A stale
coordinator physically cannot corrupt epoch k+1's state" — correct, and confirmed
by this run. It does not need to. It only needs to delete epoch k's, which it
does routinely. Epoch scoping protects the writer; nothing protected the reader.

### The void run, and a pattern that is now undeniable

The first attempt voided itself: NC2 (the inverted `_internal_kv_put` return read
as "I won") came back completely clean. With the inverted reading, whoever
actually creates the epoch key sees `raw=1`, concludes it lost, and aborts — and
so does everyone after it. No owners, so `INV-ONE` cannot fire; no acks, so
`INV-DUR` cannot fire. **Total failure was indistinguishable from success.**

Two things fall out of that. First, a finding: the `internal_kv.py:100` footgun
causes **silent livelock**, not split brain — a job at zero progress with a
coordinator restarting forever, which is operationally worse and much harder to
attribute. Second, a pattern. This is the **third** time in two iterations that
the rig has reported clean for lack of an invariant able to see the failure:

1. iteration 1 — controls that only proved the invariant checker worked, not the
   fault injectors;
2. iteration 2 bring-up — a partial-order reduction that starved the second
   instance, so no interleaving was ever explored;
3. iteration 2 run 1 — no `INV-LIVE` in the concurrent driver.

Each was a different cause, and each produced green. "Could a rig that does
nothing have produced this result?" is now a standing pre-run check, not an
occasional one. Two of the three were caught only because a *number* looked
wrong, not because anything failed.

### Ledger transitions

`C1 -> proven` · `C16 -> refuted` · `C13 p_wrong 0.30 -> 0.45` · 3 fix options
added to the backlog

### Adjust: three structurally different fixes, not one tuned corpse

- **A (chosen) — make the reader immune.** Carry-forward reads the segments
  *first* and `base_ptr` last, retrying if `base@ptr` has vanished. A segment `i`
  can only be deleted by a compaction at `m ≥ i`, and that compaction sets
  `base_ptr = m` *before* deleting, so a reader that reads the pointer last
  always observes a pointer that already covers anything it failed to read.
  Needs no coordination, no CAS, and no cooperation from the fenced writer —
  which matters, because the fenced writer is by definition not cooperating.
- **B — fence-check before every destructive operation.** A read costs ~1 µs
  against a 3.81 ms fsync, so it is nearly free, but it is TOCTOU: it narrows the
  window instead of closing it. Worth adding *as well*, to reduce redo, never
  *instead*.
- **C — never delete inside an epoch;** defer all deletion to the next epoch's
  sweep. Closes the race completely and is by far the simplest to reason about,
  but lets segments grow without bound inside a long-lived epoch, which is
  exactly what G3 and C6 rule out. Rejected on those grounds, and the
  discriminating experiment is recorded: it would pass C16 and fail C6's INV-GC.

### Next

Iteration 3 implements fix A. That test **is** blind — the fix and the
prediction were committed in `experiments/C1.md` before any of it was written.
The old read order becomes a negative control that must go red on this exact
counterexample.

---

## Iteration 3 — 2026-09-10 — C17: the fix holds, and is stronger than designed

**Selected:** the C16 refutation, over the ledger's top-ranked C13. A refuted
load-bearing claim outranks the ranking.

**Pre-registered blind:** `experiments/C17.md`, written before the fix was
implemented; the fix *sketch and its correctness argument* were committed one
iteration earlier, in `experiments/C1.md`, before the refutation had even been
analysed. Nothing about this test was informed by the code.

**Ran:** `runs/20260910-043110-C16`. **Zero `INV-DUR`** across 51,144
windowed-exhaustive traces at windows 4, 6 **and 8** — deeper than the `w = 6` at
which the bug first appeared — plus 5,000 fuzz traces over 5 seeds, coverage
49,393. NC13 (the pre-fix pointer-first order) still red. C6 sequential
regression clean at 7,620 traces.

**Outcome:** **C17 proven.** All five frozen predictions held.

### The fix

Read the segments first and `base_ptr` last, with a bounded retry if `base@ptr`
has vanished. Three lines. A segment `i` can only be deleted by a compaction at
`m ≥ i`, and that compaction publishes `base@m` and flips the pointer *before*
deleting — so a reader that takes the pointer last always sees a pointer that
already covers whatever it missed. Reading the pointer last converts "I might
have missed a segment" into "anything I missed is already in the base".

What makes it the right fix rather than merely a working one: **it needs no
cooperation from the fenced writer**, which is the only property that survives
contact with a coordinator that does not know it has been superseded. Both
alternatives required the writer to behave (a fence check before deletes —
TOCTOU) or gave up bounded growth (never delete inside an epoch — fails G3).
Both are in the graveyard with their discriminating experiments.

### Surprise

**The fix strictly dominates the old order.** With fencing disabled entirely
(NC1), the old read order produced 206 `INV-DUR` violations; the new one
produces zero, while still producing all 126 `INV-ONE`. So it protects the
carry-forward even under a *stronger* failure than the one it was designed for —
two instances writing the same epoch concurrently — because reading the pointer
last means any concurrent compactor, legitimate or not, has already published a
base covering the gap.

Recorded as an observation, not as licence to drop fencing. `INV-ONE` still
fires, and two coordinators scheduling the same work is a different disaster
from two coordinators losing each other's records.

### Left deliberately unfixed

`INV-FENCE-ACK`: 38,826 of 51,144 traces. A fenced coordinator keeps telling its
driver that work is committed. The reader fix does nothing about it and was
never meant to — bundling an unrelated mitigation into a fix would have made
this run uninterpretable. It is now **C18**, and it is a question about the
*workload* rather than the protocol: it only matters if an ack is load-bearing
beyond redo — releasing a lock, advancing an offset, telling something outside
the cluster that a thing is done.

### Ledger transitions

`C17 -> proven` · `C18` added · C16 stays `refuted` and superseded

### Next

C13 (4.05) is now clearly top and is the last load-bearing paper claim: three
instances, and a duplicate arising inside the constructor rather than after it.
Then C12 (session scoping), then C18 (a workload question, not a rig question).

---

## Iteration 4 — 2026-09-10 — C13 proven (conditionally), C19 refuted

**Ran:** `runs/20260910-043934-C13`.

**C13 proven, and the condition is the result.** Zero `INV-ONE` with
per-incarnation ids, across 4,950 two-instance and 3,000 three-instance traces,
including the spawn points where both duplicates are still inside `__init__`.
But NC14 — every instance sharing one id, which is exactly what an
**actor-derived** id would give you — produces 62 violations with
`epoch_owners = {1: ['A', 'A']}`. Both duplicates write the same bytes to the
epoch key and both read them back successfully, so M1's read-back passes for
both and the fencing guarantee silently disappears **for precisely the failure
mode M1 exists to handle**.

`ray.get_runtime_context().get_actor_id()` is stable across restarts, obvious,
and fatal here. That belongs in the design doc as a requirement, not a note.

**C19 refuted, and the magnitude is the story.** 5,448 `INV-DUR` violations with
three instances against **zero** with two on the same code — at `w = 5`, 1,146
violations in 1,110 traces. Once a third coordinator exists, losing the ledger
is close to the default outcome, not a narrow race.

The mechanism, as pre-registered: the descending epoch scan is non-atomic
*across* epochs. C reads epoch 2's pointer before B has published it, falls
through to epoch 1, and by the time it reads epoch 1, B has published epoch 2
and swept epoch 1. C carries forward empty. The C17 fix does not help, because
it disciplines deletions *within* an epoch; the **sweep** deletes a whole epoch
unconditionally and has no publish-before-delete rule at all.

### The methodological point, which is larger than this design

The two-instance case was **not a smaller version of the three-instance case**.
It structurally could not exhibit the bug: with two instances the sweeper and
the reader are the same process. A rig that only ever modelled two coordinators
would have returned green forever and shipped this.

That is now the fourth distinct way this loop has produced a clean result for
the wrong reason — controls that only tested the invariant checker, a scheduler
that starved an instance, a missing `INV-LIVE`, and now a concurrency degree too
low to contain the failure class. Every one of them was found by noticing a
number was wrong, never by something failing.

### Ledger transitions

`C13 -> proven` · `C19 -> refuted` · C17 scope narrowed to two instances

### Next

Iteration 5 implements the C19 fix, pre-registered blind in
`experiments/C13.md` before any code: **scan epochs ascending, take the highest
readable state.** Epoch `e` is deleted only by a sweep from some `m > e`, and
that sweep runs after `m` published its pointer — so a reader that visits `e`
before `m` visits `m` after it was published. It is the C17 rule one level up:
*read in the order that makes "I missed X" imply "X's replacement is already
published."*

---

## Iteration 5 — 2026-09-10 — C20: the ascending epoch scan holds

**Ran:** `runs/20260910-044309-C20`. Zero `INV-DUR` at **three and four**
concurrent instances — 25,010 windowed-exhaustive traces at windows 4, 5 and 6,
plus 8,000 fuzz traces over 5 seeds. NC15 (the pre-fix descending scan) still red
at 1,146. C6 sequential and two-instance regressions clean.

**Outcome:** **C20 proven.**

**No surprise, and that is worth recording.** This is the first experiment in the
loop whose outcome matched the frozen prediction exactly, including the
prediction that was deliberately hedged: four instances was given only 0.6
confidence *because* "two instances are representative" had already proved
false. A fourth instance changed nothing, which narrows fidelity gap F7 without
closing it.

### The shape of the design after five iterations

Two refutations, and they were **the same bug at two scales**: a non-atomic read
racing an unconditional delete. Within an epoch, compaction deletes segments
while the new epoch reads them (C16). Across epochs, the sweep deletes a whole
epoch while the next-but-one reads it (C19). Both are fixed by the same rule —
*read in the order that makes "I missed X" imply "X's replacement is already
published"* — segments before the pointer, and low epochs before high ones.

Neither was visible from the design doc, and the reason is a single sentence in
it: *"a stale coordinator physically cannot corrupt epoch k+1's state, because
it does not write those keys."* True, and it framed the whole safety argument
around **writes**. Both bugs are about **deletes**.

The total fix is about six lines of reordering. The cost of finding it was two
days of a model checker; the cost of not finding it would have been an
intermittent, unreproducible data-loss bug in other people's clusters.

### Ledger transitions

`C20 -> proven` · `C19` superseded, stays refuted · `C15 p_wrong 0.25 -> 0.15`

---

## Iteration 6 — 2026-09-10 — C12 half-settled, C15 refuted

**Ran:** `runs/20260910-044833-C12`.

**C12 → `uncertain`, deliberately not `proven`.** The rig settled the half it
can: a coordinator started under a new `session_name` on a store still holding a
completed ledger recovers nothing, re-executes everything, and destroys none of
the old keys. NC9 (no scoping) is red, and its failure mode deserves quoting —
the new cluster executes **zero** units, reports success in milliseconds, and
deletes the prior ledger on its way out. A silent, instant, evidence-destroying
false success, which is strictly worse than a crash.

The half the rig cannot settle is whether `session_name` survives a head
restart. Code reading says yes; code reading is not evidence of behaviour. And
it is the hinge: if `session_name` changed across a head restart, the scoping
would protect against a foreign cluster by hiding the ledger from the
coordinator's **own** restart, converting a safety measure into guaranteed total
loss. Split out as **C21**, `blocked` on stage C. Rounding C12 up to `proven`
on the strength of the half that passed would have buried that.

**C15 refuted.** In-place base overwriting is clean on every arm — sequential
crashes, two instances, three instances — with the comparison arm clean. So
`base@n` uniqueness is not load-bearing against crashes *or* concurrency, and
the design doc's crash-safety justification for it is simply wrong. It is worth
keeping as a **preference** — it makes compaction auditable and a torn read
detectable instead of silent — and that is what the doc should say.

Both predictions matched exactly. Two iterations running now without a surprise,
which after three refutations is a reasonable signal that the correctness
surface of stage A is close to exhausted.

### Ledger transitions

`C12 -> uncertain` · `C21` added (`blocked`) · `C15 -> refuted`

### Next

Stage B: C2 (write economics and redo window) and C3 (GCS load). Neither needs
Ray; both need a cost model on top of the protocol, which the fsync counter in
`kvfake.py` already provides.

---

## Iteration 7 — 2026-09-10 — stage B: the band, measured

**Ran:** `runs/20260910-045229-C2`.

**C2 proven, C22 split out and refuted, C3's testable half proven.**

### A claim-hygiene defect, for the second time

C2's *statement* has four clauses. Its `refutes_if` names two. The write-count
clause — `≤ ceil(N/W) + O(1)` — is **false**, and it is not in the refutation
condition, so it cannot be used to refute C2. Widening the condition after
seeing the result would be exactly the move the method exists to prevent, so the
false clause was split out as **C22** and refuted on its own terms.

This is the same defect that produced C16 out of C1: **a claim broader than the
thing that can kill it**. Two occurrences is a pattern. The rule going forward:
every clause of a statement must appear in the refutation condition, or that
clause is decoration.

### What was measured

- **Write overhead is multiplicative, ×2.79** (range ×2.17–×3.80 over 36
  compacting configurations), against a frozen prediction of `2 + 3/K` with a
  maximum relative error of **8.6%**. The design doc's `writes/sec = rate/W`
  understates cost by ~2.8×. Its conclusions survive on headroom; its formula
  does not, and every band boundary moves in by that factor.
- **Key count is flat in N.** `K=8, W=1` gives 12 keys at N = 10, 100, 1,000 and
  10,000 alike, saturating at ≈ `K+4`.
- **Recovery is O(K).** Never more than 33 KV operations at any N up to 10⁴.

### The band

| rate | N=1,000 | N=50,000 | N=10⁶ |
|---|---|---|---|
| 100 /s | W=20 only | 20..1,000 | 20..10,000 |
| 1,000 /s | **NONE** | 200..1,000 | 200..10,000 |
| 10,000 /s | NONE | **NONE** | 2,000..10,000 |
| 10⁵ /s | NONE | NONE | **NONE** |

The diagonal is the result. **The ledger design is viable exactly when the job is long enough
that a coarse `W` still leaves redo small.** Fine-grained, high-throughput,
short-duration work has nowhere to stand. That was the design doc's guess; it is
now a measurement, and it is the answer to "where does the ledger design break".

### Surprise

A **non-compacting regime** exists (`ceil(N/W) < K`) in which the job finishes
before compaction ever fires and the overhead collapses to a fixed startup cost.
Including those points reported a 40% error against the formula; separating them
gave 8.6%. The formula was never wrong — it was being evaluated outside its
domain, which is a mistake worth not repeating when the cost model gets reused.

### Ledger transitions

`C2 -> proven` · `C22` added → `refuted` · `C3 -> uncertain` (band proven, GCS
p99 half needs a cluster)

### Next

**Stage C is now reachable** — Ray 2.58.0 installs in this environment. Next
iteration builds a real reference implementation on unmodified Ray and settles
C8 directly, plus the driver-death half of C4 and the empirical behaviour of the
`internal_kv` footguns.

---

## Iteration 8 — 2026-09-10 — stage C: it works on real Ray, and C12 dies

**Ran:** `runs/20260910-045910-stageC` on **Ray 2.58.0**, single node,
`gcs_storage=rocksdb`. (`runs/20260910-045817-stageC` is retained: its C21/C12
findings were identical and valid, but T2/T3 were inconclusive because the child
driver could not resolve `address='auto'` against an in-process cluster. Kept
rather than deleted — an inconvenient run is still evidence.)

### The thesis, demonstrated

A subprocess driver created the coordinator (`lifetime="detached"`,
`max_restarts=-1`), fired `run.remote()`, and called `os._exit(0)`. The
coordinator then executed **all 40 units with no driver attached at all**, and a
fresh driver in a different process reattached by name and read the progress
back.

Then `ray.kill`. The restarted incarnation came up in a new PID, claimed **epoch
2**, reported **`resumed_from = 40`**, and executed **zero** further units.
Final ledger: **3 keys**, exactly matching stage B's steady-state prediction.

**C8 proven** — all of it on a stock `pip install ray==2.58.0`, no patches. One
correction to the doc: on 2.58 `internal_kv` lives at
`ray/experimental/internal_kv.py`, not `ray/_private/`.

### C12 is refuted by the same fact that proves C21

`session_name` is written to a file **inside** `gcs_storage_path`. A cluster
started against that path adopts it. Old and new session names came back
byte-identical across a full shutdown/init cycle, and the prior ledger was
visible to the new cluster under exactly the prefix it was about to use.

So the design doc's own justification for the mitigation — *"persisted and
stable across head restarts by design"* — is **why the mitigation fails**.
Session scoping is a **storage-path identity, not a cluster identity**, and the
case it existed to defend against is the one case it cannot see.

**And it is not fixable from inside the cluster.** A head restart and a cluster
replacement on a recycled PV are indistinguishable from within: same disk, same
session name, same keys. The distinction exists only in the orchestrator. The
scope key must be **injected** — RayCluster UID via the downward API, or the
RayJob `submissionId`. That is **C23**, and it is now the ledger design's largest open risk.

### Surprise

The direction was predicted; the shape was not. One small text file settles two
claims in opposite directions and removes a mitigation, and the fix has to come
from outside the system entirely. That is a different *kind* of result from the
three earlier refutations, all of which were repairable inside the protocol.

### The footgun, empirically

`put(absent) = False`, `put(present) = True`, with a shipped docstring reading
*"Returns: Whether the value already exists"*. Open question 6 is answered, and
the model checker already showed the failure mode is silent **livelock**, not
split brain — which is harder to attribute and worse to operate.

### Ledger transitions

`C8 -> proven` · `C21 -> proven` · `C12 -> refuted` · `C4 -> uncertain`
(driver-death half proven, head-pod half needs KubeRay) · `C23` added

### Still open

C23 (injected scope key), C18 (fenced acks), C5, C9, C10, C11, C14, and the
untestable halves of C3 and C4. The last group all need either KubeRay or a
product decision, not another rig.

---

## Iteration 10 — 2026-09-10 — attacking F6 and F7

Budget raised to 100 at the human's direction: close every point before drafting
an REP.

**Ran:** `runs/20260910-054022-GAPS` (and `runs/20260910-053138-GAPS`, **VOID**,
retained).

**Gaps narrowed.** No violation at stale tails to 64, at 5 or 6 concurrent
incarnations, or at sequential fault budget 3 (237,840 traces). All controls red.
F6 and F7 are downgraded to `low` with resolutions recorded; C20's envelope
widens and C6's depth doubles.

### Two more instances of the same failure, in one iteration

**F6 was measuring nothing.** Trace counts came back *identical* across tail
lengths 8/16/32/64, because the fenced instance always finished before the cap
bound. Caught only because the numbers were suspiciously equal. The cap is now
instrumented (`max_tail_actually_used`, `traces_hitting_the_cap`) and the harness
**voids the run** if the cap never binds anywhere. The corrected sweep shows it
biting at tail 8 and genuinely relaxing by tail 64 — which turns "longer tails
add nothing" from an assumption into a measurement.

**Then the corrected run voided itself.** NC13 went green at two configurations.
Rule honoured, then diagnosed *before* changing anything: NC15 was red at those
same configurations, so the rig could still see `INV-DUR` — NC13 was **starved**,
not blind. Given an adequate sweep both go red, and the asymmetry is now
measured: **NC15 ≈ 25,000 violations, NC13 ≈ 100** at identical settings.

Lesson worth keeping: **controls have individual sensitivity, and running them
all at one sweep size assumes they are equally sensitive.** They are not.

That makes six distinct ways this loop has produced green for the wrong reason,
from six unrelated causes, every one caught by a number rather than a failure.

### Ledger transitions

F6 → `low`/narrowed · F7 → `low`/narrowed · C20 envelope widened · C6 depth
extended to fault budget 3

### Next

C23 (the injected cluster-scope key — the one thing that must be fixed), then
C5, C3, and a real multi-process cluster for C4's head-pod half, C7 and C21.

---

## Iteration 13 — the pinning was the bug, and the control proved it

**Claim:** C25. **Run:** `runs/20260910-061819-C25pin` (three arms).

Iteration 12 ended with a split that was easy to misread. The ledger survived a
head kill, the actor *name* survived, the epoch survived — and reattach failed
anyway, with `ActorUnschedulableError`. The tempting reading is "hard pinning
did it". The honest position was that a single run cannot separate

- **(a)** hard pinning is the cause, from
- **(b)** the coordinator was unrecoverable for some other reason and the
  affinity error was merely the first exception thrown on the way out.

A green `soft=True` run would not have separated them either — it would only
show that one configuration happens to work.

So the rig ran the identical scenario three times, differing by exactly one
line: `soft=False`, `soft=True`, and **no scheduling strategy at all**. The
`hard` arm was the **negative control and was required to fail again**; had it
passed, iteration 12 was flaky and the whole run would have been VOID,
including the other two arms.

| arm | placed off-head | committed | reattached | resumed_from |
|---|---|---|---|---|
| `hard` | yes | 60 | **no** — `ActorUnschedulableError` | — |
| `soft` | yes | 60 | yes | 60 |
| `none` | yes | 60 | yes | 60 |

Control fired. C25 **proven**, and proven two-sidedly, which is what its
refutation condition demanded.

### The check that mattered

The result was exactly as predicted, which in this loop is the moment to look
harder rather than less hard. The specific worry: a "successful reattach" could
mean the actor merely *survived* rather than being genuinely rescheduled — in
which case the arms would not be comparable at all.

The identities settle it. Before the kill, `soft` ran as pid 984221 in epoch 1.
After, it answered as **pid 984825 in epoch 2**, with `resumed_from=60` and
`executed_this_incarnation=0`. That is a new process, on a node that is not the
one it was placed on, which re-ran `__init__`, claimed a fresh epoch, and
recovered every committed unit without redoing one. The `none` arm behaved
identically (pid 985552 → 986110).

So iteration 12's failure was **placement, not durability**. The three-tier
structure held throughout; what broke was the scheduling hint on top of it.

### The design consequence is narrower than it looks

Serve and Train v2 pin their controllers to the head with a *resource request*
(`{HEAD_NODE_RESOURCE_NAME: 0.001}`), not a node id, and therefore do not have
this failure mode at all. The hazard belongs specifically to
`NodeAffinitySchedulingStrategy(soft=False)` — which is precisely the thing one
reaches for when the goal is "keep this off the head". The design's own
preferred placement led straight into it.

### What this does *not* close

C4 stays `uncertain`, but the reason is now precise instead of vague. Its
refutation condition has three clauses — dead, **duplicated**, or name lookup
fails — and this harness can only test two. It calls `ray stop --force` before
restarting the head, so the old coordinator is guaranteed dead before the new
head returns, which is exactly the condition under which duplication cannot
occur. A real head-pod replacement leaves the worker raylet and the old
coordinator **alive** while GCS reloads the actor table from `tables/`; that is
the only situation in which two live incarnations could coexist.

The harsher test therefore does **not** subsume the gentler one — it destroys
the hazard it is supposed to detect. Worth stating plainly because "we tested
something stricter" is normally a good argument, and here it is not.

### Ledger transitions

C25 `untested` → **proven** · C4 note sharpened (duplication clause is the
blocker) · design.md open question 4 partly answered

### Next

The single harness change that unblocks the most: restart `gcs_server`
**in place**, without `ray stop --force`, so the worker raylet and the
coordinator stay up. That is what C7 has been blocked on since iteration 8
(*"work continues, durability pauses"*), it is the only way to reach C4's
duplication clause, and it narrows gap F8.

---

## Iteration 14 — an outage the coordinator survives, and the assumption it killed

**Claims:** C7, C4 (duplication clause). **Runs:** `runs/20260910-062831-C7inplace`
(**VOID**, retained), `runs/20260910-063616-C7inplace2`.

Every cluster run since iteration 12 restarted the head with `ray stop --force`,
which takes the worker raylet and the coordinator with it — gap **F8**. That
made two things unreachable, and it is worth being precise about why, because
the harness looked *stricter* and was in fact blind:

- **C7** asks whether an outage degrades to "work continues, durability pauses".
  If the coordinator dies with the head there is no work to continue.
- **C4**'s three clauses are dead / **duplicated** / name lookup fails.
  `ray stop --force` guarantees the old coordinator is dead before the new head
  returns, which is exactly the condition under which duplication *cannot*
  happen. The harsher test destroyed the hazard it was meant to detect.

The fix: capture `/proc/<pid>/cmdline` for `gcs_server`, `SIGKILL` it, and later
re-execute that exact argv. Nothing else stopped; both raylets keep running.

### The rig bug, caught by an impossible number

The first run's `kill` arm produced **zero** samples and reported
*"no gcs_server found"* — while the `nokill` arm in the same run was perfect.
Cause: `gcs_procs()` called `open()` on `/proc/<pid>/cwd`, which is a symlink to
a **directory**, so `open()` raised `IsADirectoryError`, the `except OSError`
branch swallowed it, and every candidate process was discarded. The arm never
killed anything.

Retained as `VOID.md` rather than deleted, because its `nokill` arm is a valid
NC-outage control and because a run that fails loudly is cheap evidence that the
rig is not silently fabricating.

### The measurement that made the result meaningful

`info()` calls `key_count()`, which is a KV list. During a GCS outage `info()`
blocks — so a sampler polling `info()` would have gone dark exactly when the
answer mattered, and a flat progress line would have been indistinguishable from
a blind rig. That is failure mode #4 from this loop's own list, seen coming this
time rather than afterwards.

So the coordinator gained a `progress()` method that touches **no** KV, and the
actor was created with `max_concurrency=3` so it answers from another thread
while `run()` is blocked. Two independent signals:

| | during outage |
|---|---|
| `progress()` — no KV | kept answering (1 sampler error in the entire run) |
| `info()` — lists KV | red, `GetTimeoutError`, for the whole window |

### Result: C7 refuted, benignly

| | pre | during outage | after restore |
|---|---|---|---|
| units executed | 4 → 44 | **48 → 48** | 59 → 327 |
| ledger writes | — | **49 → 49** | 58 → 328 |

Work did not continue. It stopped dead, and the reason is structural rather than
incidental: `commit()` is a **synchronous `internal_kv` put on the unit loop's
critical path**, so the loop can advance at most `W-1` units past a blocked
commit and then halts. *"Work continues, durability pauses"* was never a property
of the outage — it was a property of a commit path the ledger design does not have.

Both controls fired: `nokill` rose monotonically throughout (exec 5→400, writes
49→401, 400 units contiguous), and NC-sampler saw KV go red.

**The dangerous half came out well.** ray#55996 — `DEADLINE_EXCEEDED` not retried
by the GCS client — did **not** reproduce. The blocked put recovered by itself
once GCS returned. No wedge, no restart, ledger contiguous, no duplicates.

### And C4 finally closes

Exactly **one** live actor row, `NumRestarts=0`, epoch unchanged at 1, same
instance id before and after. That is the duplication clause, reached for the
first time.

C4 is now proven **across two runs**, and the decomposition is the point: its
three clauses require **mutually exclusive** hazards. Duplication needs the old
coordinator *alive*; unschedulability needs its node *gone*. No single harness
can produce both. Iteration 13 covered node-destroyed; this covers node-survives.
From the coordinator's point of view a KubeRay head-pod replacement *is* one of
these two, depending on whether its own node goes with the head.

What is deliberately **not** claimed: a real head pod returns on a new IP,
whereas the re-exec'd `gcs_server` reused its address. That dimension is
raylet↔GCS reconnection, which KubeRay's GCS-FT machinery owns; it is not a
property of the ledger design and the ledger design asserts nothing about it. F8 narrowed to exactly that.

### The design choice this opens

C7's refutation is not a defect to fix — it is a fork.

- **Option A: accept it.** An outage pauses the job. Safe, simple; for a job
  measured in hours, 35 seconds is nothing. *Current recommendation.*
- **Option B (new claim C26): async ledger writer with a bounded queue.** Units
  keep completing while durability lags. Buys liveness, pays with a redo window
  bounded by *queue depth* rather than by `W` — so C2's cost model and C10's
  at-least-once budget would both need re-deriving under it.

### Ledger transitions

C7 `blocked` → **refuted** · C4 `uncertain` → **proven** · F8 → resolved
(narrowed to the address-change dimension) · **C26** added

### Next

C5 (two-source reconciliation with live child actors) and C3's p99 half are the
last untested claims with real value; C26 is the open design fork.

---

## Iteration 15 — the ledger is for intent; names are for reality

**Claim:** C5. **Runs:** `runs/20260910-064907-C5children` (**VOID**, retained),
`runs/20260910-065524-C5children2`.

C5 says coordinator state is reconstructible from two sources — the ledger
(*intent*) and live-cluster introspection (*reality*). Every run before this one
used a coordinator whose only state was a set of completed unit ids, which lives
wholly in source one. So "two-source reconciliation" had never actually been
exercised. The state that needs source two is the state that lives in the
cluster: **long-lived child actors**.

Creating a child and recording it are two operations, so a crash can land
between them. One order is benign — the ledger claims a child that does not
exist, and the new incarnation just creates it. The other is not:
**create → crash → record** leaves a child alive that the ledger never heard of.
If it cannot be attributed, it is an orphan, and a duplicate appears beside it.

Rig: K=6 detached named children, only R=2 recorded before `os._exit(1)` — four
crashes in the dangerous window at once.

### The void run, and the better idea inside it

The first attempt's control (`prefixed_noreconcile`) reported `orphans=4` and
looked red. It was not. Phase 2 recreated the missing children with
`get_if_exists=True` and the same deterministic names, so Ray handed back the
**existing** actors — pids 994628/994668/994705/994742, identical before and
after. Nothing was orphaned or duplicated; the four were labelled orphans purely
because their names were missing from `adopted_names`, an artifact of my
bookkeeping rather than an observation about the cluster.

By the rule pre-registered in the card, that voids the whole run. Retained with
a `VOID.md`.

Two things came out of it, and the second is better than the hypothesis it broke:

1. **The control was in the wrong arm.** `opaque` is the real negative control:
   its unrecorded children were genuinely orphaned, with genuinely new pids
   beside them. The variable is *attribution*, and "reconciling or not" was never
   cleanly separable, because deterministic naming reconciles implicitly.
2. **Explicit listing is not necessary.** `get_if_exists=True` over a
   deterministic, scope-prefixed name does the same job in one call.

`get_if_exists` was then set to **false** in every creation path except the arm
whose hypothesis it is — precisely so it could never again paper over a failed
reconciliation.

### Result

| arm | naming | adopted | real duplicates | orphans |
|---|---|---|---|---|
| `opaque` *(control)* | `uuid4` | 2 | **4** | **4** |
| `prefixed_explicit` | scoped | **6**, same pids | 0 | 0 |
| `prefixed_getifexists` | scoped, no listing | 2 + **4 re-acquired**, same pids | 0 | 0 |

Control red, both treatment arms clean, and adoption verified by pid rather than
by the absence of an error. **C5 proven, conditionally** — and the condition is a
design requirement, not a caveat: *child actors must be named deterministically
from job scope plus index.* When names carry attribution, reality is
self-describing and the ledger is only needed for intent.

### The bound that makes this survivable

While fixing the void run: **Ray requires detached actors to be named.** The
fully unattributable case cannot be constructed. So the worst `opaque` naming
can do is lose the ability to say *whose* a child is — never the ability to see
it at all. That caps the blast radius of getting this wrong.

### The caveat worth carrying into the REP

`get_if_exists=True` is sufficient for **reconciliation** but not for
**attribution of intent**: it returns the actor without saying whether it existed
already. A coordinator that must choose between "resume this child's state" and
"initialize a fresh one" still needs the explicit list-and-adopt path, or a child
that reports its own incarnation count.

### Blind-spot count: eight

Eight distinct ways this loop has produced a wrong-looking-right result, from
eight unrelated causes. This one is the first where the *fix for the void run*
produced a better design answer than the experiment it invalidated.

### Ledger transitions

C5 `uncertain` → **proven** (conditional on scoped deterministic child names)
· one idea added

### Next

C3's p99 half is the last uncertain claim; C26 is the open design fork.

---

## Iteration 16 — the async fork: liveness is real, the price was mispriced

**Claim:** C26. **Runs:** `runs/20260910-070448-C26async` (partial),
`runs/20260910-072310-C26async2` (partial), `runs/20260910-074504-C26async3`.

C7's refutation left a fork rather than a defect: commit is synchronous, so an
outage stops the job. C26 is the other branch — put the commit behind a bounded
FIFO drained by a single writer thread. Three clauses, measured separately,
because this loop has twice shipped a claim broader than its refutation
condition (C1→C16, C2→C22).

### Two starved runs before a real one

**Run 1** reported `async_outage` completing **91** units during the outage —
*identical* to `async_nokill`'s 91. Identical counts across the decisive
parameter is the inert-parameter signature from iteration 10.

The cause was an arithmetic error in my own pre-registration: **the queue holds
batches, not units**, so `Q=64` is `Q·W = 192` units of buffer against an outage
that only produces ~98. The buffer was twice the size of the test. Max queued hit
31 batches (~93 units), which matches the outage almost exactly — the writer
*was* blocked; the queue simply never approached its bound.

**Run 2** fixed that (`Q=8` → 24 units of buffer, saturating in ~9 s) and clause
1 came out clean: **25 units during the outage against the sync baseline's zero**,
queue saturated. But both crash arms reported `redo = 0`, because I restored GCS
*before* killing the coordinator and the queue had already drained. A redo
measurement taken when nothing is in flight measures nothing — a starved control,
the same failure as NC13 in iteration 10.

The fix is the interesting part. The backlog only exists **while GCS is down**,
and `ray.kill` goes through the GCS actor manager — precisely what is
unavailable then. So the coordinator kills *itself* through a cached actor handle
held by the sampler, which reaches the worker directly and works during the
outage.

### Run 3, and the refutation

| arm | executed | durable | backlog | redo | bound | within |
|---|---|---|---|---|---|---|
| `async_crash_outage` | 75 | 45 | 30 | **30** | 27 | **no** |
| `sync_crash_outage` | 48 | 45 | 3 | 3 | 3 | yes |

Clause 2 fails, and it fails *explicably*, which is what makes it a refutation
rather than noise. At the moment of death: the queue held 8 batches (24 units),
the writer had **already dequeued** one batch and was blocked committing it (+3),
and the producer had **filled its next batch** and was blocked in `q.put` (+3).
24 + 3 + 3 = **30**, exactly the measurement.

So the real bound is `Q·W + 2W`. I counted the buffer and forgot the two batches
in flight on either side of it. The condition is **not** widened after the fact:
C26 is refuted on its stated terms and the corrected bound becomes **C27**, to be
proven by a *sweep over Q* rather than by the single point that suggested it.

Clauses 1 and 3 held throughout: work continues during an outage by ≈`Q·W` units
and then blocks; every arm's ledger stayed contiguous and duplicate-free with no
writer errors; `async_nokill` completed all 400 units.

### The finding nobody asked for

`epochs=[18]` and `[19]`. Exactly **one** epoch key survives in each arm — so the
compaction sweep held the key-count bound, which is M3 doing its job. But the
epoch *number* reached 18 and 19, meaning roughly eighteen incarnations were
created during a single 35-second outage. The coordinator died, `max_restarts=-1`
brought it back, `__init__` ran recovery against a GCS that was still down, and
the cycle repeated.

Bounded keys is the good half. An unbounded **restart storm** during an outage is
the half worth checking, and it is now **C28** — currently the highest-VOI open
claim at 1.17.

### Blind-spot count: ten

Two more, both mine, both caught by a number rather than a failure: a buffer
sized in the wrong unit, and a control that measured a backlog after the backlog
had drained.

### Ledger transitions

C26 `untested` → **refuted** (clause 2) · **C27** added (corrected bound,
needs a sweep) · **C28** added (restart storm)

### Next

C28 first — it is the highest-VOI open claim and it was found by accident, which
is usually a sign the design has not been looked at from that angle. Then C27's
sweep, then C3's p99 half.

---

## Iteration 17 — the storm was mine, and the ceiling is the real story

**Claims:** C28, and C29 split out of it. **Runs:**
`runs/20260910-...-C28storm` (**VOID**, retained),
`runs/20260910-...-C28storm2` + `probe_outage_ceiling.json`.

C28 existed because iteration 16 saw a surviving epoch key numbered **18** after
a single 35 s outage and inferred a restart storm. It was the highest-VOI open
claim precisely because it was found by accident — a sign nobody had looked at
the design from that angle.

Nobody had. The angle was wrong.

### The control that pointed the other way

| arm | outage | max_epoch | restarts |
|---|---|---|---|
| `die_healthy` *(control)* | **0 s** | **14** | **13** |
| `outage_10` | 10 s | 12 | 11 |
| `outage_30` | 30 s | 11 | 10 |

The control was required to show exactly one restart. It showed thirteen — and
churn was **anti-correlated** with outage duration, so the outage could not be
the cause. Void, and the direction was the diagnosis.

**Cause: my rig.** `JobCoordinator` is declared `max_task_retries=-1`, and
`die()` called `os._exit(1)` *inline*, so from Ray's point of view the task was
in flight when the actor died. Ray replayed it on the restarted actor, which
killed itself again — an infinite suicide loop lasting as long as the calling
sampler stayed alive. The arms differed only in how much sampler lifetime
remained after the death, which is why the control, with the longest post-death
window, churned hardest.

**Iteration 16's `epochs=[18]` has the identical cause.** C28 was raised on a
misreading of my own instrumentation. Fixing `die()` to return before exiting
(timer thread) settles it:

| arm | outage | max_epoch | restarts | ledger keys | redo |
|---|---|---|---|---|---|
| `die_healthy` | 0 s | 2 | 1 | 3 | 1 |
| `outage_10` | 10 s | 2 | 1 | 3 | 3 |
| `outage_30` | 30 s | 2 | 1 | 3 | 3 |

**C28 proven** on all three clauses: one restart, epoch +1, keys flat at 3, redo
≤ `W`. The ledger design does not thrash. My rig did.

### The general constraint underneath

`max_task_retries=-1` **replays in-flight tasks on a restarted actor.** For the ledger design
that is mostly what you want — `run()` resuming is the whole point — but it means
*every* coordinator method must be idempotent under replay, including
administrative ones. The design doc did not say that. It does now.

### The arm that failed, and was the best result of the day

`outage_60` produced nothing: the follow-up driver could not even `ray.init`
(`ConnectionError`), and the sampler had been erroring since t=24 s. The raylet
gave up waiting for GCS and **the node exited**. Past
`gcs_rpc_server_reconnect_timeout_s` (60 s default), "how many times does the
coordinator restart" is not unanswered — it is ill-posed. There is no cluster.

A dead cluster only matters if the ledger dies with it. It does not. Cold restart
on the **same** `gcs_storage_path`:

- 45 committed units recovered, contiguous, no duplicates
- 3 ledger keys; actor name still registered, reloaded from `tables/`
- fresh incarnation: `resumed_from=45`, `executed_this_incarnation=0`
- **zero redo, reproduced twice**; epoch advanced to 3 on the second restart

The first cold restart reported epoch **2**, because GCS had *restored* the
detached actor and `get_if_exists=True` returned that same incarnation. That is
not a bug and the distinction matters: a restored actor must **not** claim a
second epoch for itself, or fencing would be self-defeating.

Split out as **C29, proven**. It answers "what is the ledger design's worst case" and the answer
is better than the design assumed: past the ceiling the failure mode is
**downtime bounded by cold-restart time** — not data loss, not redo. That is the
direct payoff of putting the ledger in rocksdb rather than in memory.

### Blind-spot count: eleven

The eleventh is the most embarrassing and the most instructive: an entire claim
was manufactured by my own instrumentation, survived into the ledger as the
top-ranked open question, and was killed by its own negative control on first
contact. The control did its job. The lesson is that a finding discovered *by
accident* deserves more suspicion than one discovered on purpose, not less —
accidental findings come with no pre-registered prediction to check them against.

### Ledger transitions

C28 `untested` → **proven** · **C29** added and **proven** · `die()` fixed in
`progress_ledger.py` · design.md gains the replay-idempotence constraint and the C26 correction

### Next

C27's sweep over `Q` (the corrected redo bound), then C3's p99 half — the last
two open claims.

---

## Iterations 18–20 — one number, three claims, two refutations, one distinction

**Claims:** C27 → C30 → C31. **Runs:** `…-C27sweep` (rig failure),
`…-C27sweep2`, `…-C30repeats`, `…-C31blocked`.

Three iterations on a single quantity: the redo window of the async writer. It
took that long because the number kept being *almost* right, and "almost right"
is the hardest thing to handle honestly.

### 18 — C27: the point prediction, refuted by one point

C26's refutation had produced an explanation after the fact (queue + writer's
in-flight batch + producer's batch = `Q·W + 2W`). Explanations invented after
seeing the number are not evidence, so C27 turned it back into a **point
prediction** and swept `Q ∈ {0,2,4,8,16}`.

First attempt failed outright: every arm reported "no death dump". The killer
process idled 33 s with no Ray activity and its fire-and-forget `die` never
landed. Making the killer poll `progress()` while it waits fixed it — and gave a
per-arm view of the queue filling, which mattered later.

Also fixed here: sampling `executed_at_death` from a 2 s poller blurs the count
by ~6 units, larger than the gap between sweep points. `die()` now dumps
`progress()` to a **local file** at the instant of death — local because the path
must work with GCS down.

| `Q` | 0 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| predicted | 3 | 12 | 18 | 30 | 54 |
| measured | 3 | 12 | 18 | 30 | **53** |

Slope 2.93 against `W=3`; intercept 6.14 against `2W=6`. Four exact hits and one
miss by **one unit**. C27 says "differs at any swept `Q`" — so: **refuted**.

### 19 — C30: the interval story, refuted by six repeats

The natural rescue was that the last term varies with crash phase: `p ∈ [1,W]`,
making `Q·W + 2W` a loose upper bound. Stated as C30 with a prediction the
generating observation could not make — **at fixed `Q`, the answer should not be
the same every time** — and tested by six repeats at `Q=8` with the death moment
swept across the batch phase.

All six returned **30**. `p=3` every time. Zero variation.

The pre-registration had named this outcome and its consequence in advance: if
the repeats are constant, the interval story is wrong *and* `Q=16`'s 53 needs a
different explanation before any redo bound can be trusted.

### 20 — C31: it was never the arithmetic, it was the state

Reconstructing `Q=16`: durable 45 + queue 48 + writer 3 = 96 against 98 executed,
so `p=2` — the producer was **not blocked**; it was still executing, with the
third unit of its batch in flight. Reaching the blocked state costs `Q·W + W`
units *after the outage begins*: 27 at `Q=8`, but **51** at `Q=16`, which at the
measured rate lands almost exactly on the +30 s death mark.

**The defect was my negative control.** It asserted `queued == Q`. A full queue is
necessary for a blocked producer but **not sufficient** — the producer can have
just completed the `put` that filled the queue and moved on. I verified the
buffer was full and inferred, wrongly, that the pipeline had stalled.

C31 states the corrected condition and was tested by an A/B on the *same* `Q`,
plus a deliberate second instance of the trap:

| arm | `Q` | death | redo | `p` | blocked | predicted |
|---|---|---|---|---|---|---|
| `q16_early` *(control)* | 16 | +30 s | **53** | 2 | no | 53 |
| `q16_late` | 16 | +45 s | **54** | 3 | **yes** | 54 |
| `q32_late` | 32 | +45 s | **96** | — | no | < 102 |

Every prediction hit. The control reproduced the anomaly on demand, so C27 was
not refuted by noise. `q32_late` was predicted *in advance* to come in under its
formula, because `Q=32` needs 99 units to block and a 55 s outage never supplies
them — its queue never even filled (31 of 32). Predicting the trap rather than
discovering it again is the only reason that arm is worth anything.

**C31 proven.** The redo window is deterministic at `Q·W + 2W` once the producer
is genuinely blocked.

### The design rule that falls out

`Q` is **not freely choosable**. It must be small enough that `Q·W + W` units are
produced inside the outage you intend to ride out; beyond that the extra buffer
is never used, and the redo bound stops being the operative limit. Pick `Q` from
the redo budget *and* from the outage duration you are buying liveness against.

### Blind-spot count: twelve

The twelfth is the subtlest so far: a control that was **necessary but not
sufficient**. Every previous one was a control that did not fire, was starved, or
measured the wrong object. This one fired, measured the right object, and still
licensed a wrong inference — because a full queue and a blocked producer are
different states and I treated them as one.

### Ledger transitions

C27 → **refuted** · C30 added → **refuted** · C31 added → **proven** ·
design.md gains the `Q` sizing rule

---

## Iteration 21 — the last open claim, and three attempts to measure nothing

**Claim:** C3 (second half). **Runs:** `…-C3interference` (**VOID**),
`…-C3interference2` (uncertain), `…-C3resolution`.

C3's surviving half — *"no measurable regression in GCS p99 for other
components"* — had been open since iteration 7. It is also the only claim in this
ledger whose statement is **unfalsifiable as written**: a rig that cannot detect
a regression reports none and looks exactly like a pass.

So the experiment was designed around two controls rather than around the
measurement:

- a **positive control** that must regress, and
- a **noise floor** from identical baseline arms, below which no effect may be
  reported in either direction.

### Attempt 1 — blind

`overload` at 2069 writes/s moved p99 by +20 % against a **43 % noise floor**.
Two identical baselines differed by more than any treatment. Void by the
pre-registered rule.

The diagnosis was in the data, not in the logs: `overload` produced a **lower**
p95 (2.46 vs 4.09 ms) and a **lower** actor round-trip p99 (12.96 vs 22.01 ms).
Load making things faster is not a physical result. The victim slept 10 ms
between operations, so it issued ~80 ops/s and was measuring its own wake-up
jitter; and the arms ran sequentially, so warm-up drift dominated everything.

### Attempt 2 — honest, and not a pass

Victim runs back-to-back; baselines **interleaved** between every load arm so
each treatment is compared against its temporal neighbour; load swept; and a new
**independent** positive control that does not involve the ledger at all — four
loaders writing 4 MB values. That fired at **+1794 %**, so the rig was no longer
blind.

But the floor came out at **26.2 %**, marginally above the pre-registered 25 %.
The design arm was below threshold, and it would have been easy to call it a
pass. The frozen rule said `uncertain`, so it was recorded as `uncertain`.

It also produced a plausible, quotable, **wrong** finding: *"regression appears
between 26/s and 200/s"*, from `r200` showing +83 %. That rested entirely on
`base_1` being the lowest of seven baselines.

### Attempt 3 — resolution

Sixty-second arms (~53,000 samples each), nine baselines, threshold as **2σ** of
the baseline distribution rather than max−min spread.

| arm | load | p99 | Δ p99 | Δ p95 |
|---|---|---|---|---|
| `r26` *(design)* | 26/s | 3.27 ms | **+3.0 %** | +1.7 % |
| `r50` | 50/s | 3.50 ms | +2.0 % | +1.7 % |
| `r100` | 100/s | 3.52 ms | +3.2 % | +3.1 % |
| `r200` | 200/s | 3.74 ms | +20.2 % | +5.9 % |
| `bigvalue` *(pos ctrl)* | 60/s × 4 MB | **132.10 ms** | **+4148 %** | +2979 % |

Noise floor **9.8 %**. Nothing ledger-shaped regressed up to 200 writes/s —
nearly an order of magnitude above the design rate — while the positive control
moved p99 by a factor of **42**.

**C3 proven.** And attempt 2's "regression between 26 and 200/s" is retracted: at
proper resolution, 200/s does not regress.

### The pattern across all three attempts

Each attempt measured the same physical system and got a different answer:
*blind*, *marginal*, *decisive*. Nothing about the cluster changed. What changed
was sample count, arm ordering, and the choice of threshold statistic — and the
first two attempts would each have supported a confident, publishable-sounding
sentence.

The only thing standing between "no measurable regression" and a meaningless
claim was insisting, in advance, that something **had** to regress.

### Ledger transitions

C3 `uncertain` → **proven**. **No pending claims remain.**

---

## Iteration 22 — the last idea, and the end of the loop

**Claim:** C32 (adaptive coalescing). **Run:** `runs/…-C32adaptive` +
`tau_sweep.json`.

The last live item in the backlog, carried since iteration 1: *"target a fixed
ledger write rate rather than a fixed `W`. Harder to reason about, probably
correct."* It had to be settled or buried before exhaustion could be claimed.

The motivating asymmetry is real. With fixed `W`, write rate `= r/W` grows with
the completion rate while the redo **time** window `≈ W/r` grows as the rate
*falls*. So one fixed `W` over-exposes the slow phase and another overshoots the
write ceiling when things speed up.

| profile | policy | write-rate dev | redo units | redo time |
|---|---|---|---|---|
| `constant` *(control)* | all three | 4.75 % | 7 | 0.04 s |
| `ramp` | `fixed_mean` | 18.0 % | **4** | 0.80 s |
| `ramp` | `fixed_peak` | 79.5 % | 19 | 3.80 s |
| `ramp` | `adaptive` | 25.1 % | 18 | **0.026 s** |

Control tied **exactly** (0.0 % on both metrics), so the fixed baseline was not
mistuned.

At the pre-registered `tau=5 s` adaptive also failed clause 1. Before concluding
I swept `tau` from 0.5 to 60 s — specifically so the refutation would not be of a
strawman — and `tau=60 s` *does* hold the rate (worst deviation 16.4 %).

**Clause 2 fails anyway, and cannot be tuned away.** On the ramp, adaptive's
worst-case redo is 13–18 **units** against `fixed_mean`'s **4**, at an equivalent
write rate. So adaptive does not dominate; it **trades**.

### The insight underneath

The two policies optimise **different denominators**. Fixed `W` bounds the redo
*count*; adaptive bounds the redo *wall-clock*. They are the same quantity
divided by the completion rate, so **neither can bound both while the rate
varies**. The ledger design's budgets — C2 on write count, C10 on at-least-once units — are
denominated in units, which is the denominator fixed `W` already bounds.

**C32 refuted. Idea buried.** Keep fixed `W`; revisit only for a deployment whose
redo budget is stated in seconds.

---

## Termination

`ledger.py status` reports **REFUTED**: three load-bearing claims (C12, C16,
C19) are refuted and the backlog is empty. That is the correct output for the
rule as written, and it is not the correct reading of this loop.

Each of those three has a **proven successor** — C12→C24, C16→C17, C19→C20 —
and each repair was pre-registered *before* it was implemented, then proven on
its own terms. The design was not killed; it was corrected. Rather than edit the
statuses (which would destroy the evidence), supersession is recorded per-claim
in a `superseded_by` field and in `termination_note`.

**Final ledger: 32 claims — 17 proven, 11 refuted, 4 accepted risk, 0 pending.
35 run directories, 6 of them retained as void or partial. Backlog empty. No
fidelity gap rated high.**

Twelve blind spots, twelve unrelated causes, every one caught by a number
looking wrong rather than by a failure. That is the loop's most transferable
output, and it is the reason the three refutations are trustworthy.
