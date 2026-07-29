# REP-64 — a Ray-core node-death lost-wakeup hangs generator reconstruction

**This branch is a self-contained proof, not a feature** — meant for review and
discussion, not necessarily merge. It shows, deterministically, that
`test_dynamic_generator_reconstruction_nondeterministic[None-*]` can hang
**permanently** because of a bug in **Ray core**, independent of any storage backend.

The bug was surfaced while adding a RocksDB GCS backend (REP-64): RocksDB's strict
per-write `fsync` makes the GCS death-notification publish slow enough to expose it.
But **nothing here uses RocksDB** — every experiment runs on the in-memory GCS and
injects the timing deterministically, so the bug and its fix belong in Ray core.

- Base: vanilla upstream Ray at `606b434212`; the diff is purely this experiment.
- Reproduce: see [§5](#5-reproduce).

---

## 1. The bug

`test_dynamic_generator_reconstruction_nondeterministic[None-*]`
(`test_generators.py:447`): a dynamic generator runs on a worker node and yields 10
objects stored there; the node is killed (`allow_graceful=False`), destroying those
objects; `list(gen)` must detect the loss, reconstruct the generator on the head node,
and return. **Observed under RocksDB GCS:** `list(gen)` hangs permanently.

## 2. Root cause (code-cited)

The driver owns the generator's objects. It reacts to node death **only** from a
single pushed GCS pub/sub notification:

```cpp
// core_worker.cc  on_node_change(node_id, data)   — the ONLY object-recovery trigger
if (data.state() == rpc::GcsNodeInfo::DEAD) {
  reference_counter->ResetObjectsOnRemovedNode(node_id);
  raylet_client_pool->Disconnect(node_id);
  core_worker_client_pool->Disconnect(node_id);   // fails the in-flight generator RPC
}                                                  // -> task resubmission == the heal
```

The GCS publishes that notification from **inside the node-table write's completion
callback** (`gcs_node_manager.cc::InternalOnNodeFailure` — *publish-after-persist*).
Under a slow durable write (RocksDB strict fsync) the publish is delayed, and **the
consumer has no fallback** — no timeout, no re-poll. So if that one push is late or
lost, `ResetObjectsOnRemovedNode`/`Disconnect` never run, recovery never starts, and
`Wait` blocks forever on objects that no longer exist.

Two supporting facts (both shown empirically in §3):

- **It is the *node* channel, not the actor channel.** A fresh subscriber to actor
  state does a fetch-on-subscribe that reads the synchronously-set in-memory DEAD, so
  the actor path self-heals even if its publish is delayed forever. The node→recovery
  trigger has no such fetch fallback.
- **The heal is the client *disconnect*, not object recovery.** On the driver,
  `ResetObjectsOnRemovedNode` matches zero objects here; disconnecting the dead node's
  core-worker client fails the in-flight streaming-generator RPC, which triggers task
  resubmission — the path that actually produces the result.

## 3. Evidence

All arms run the exact scenario on the in-memory GCS, injecting timing via env knobs
(§5). A 5-min injected delay ≫ the 90s per-arm timeout, so each delayed arm models
"this notification never arrives", deterministically (no flakiness).

| Arm | Injected condition | Verdict | Shows |
|-----|--------------------|---------|-------|
| `control` | none | **FAST** (~1.6s, n=9) | baseline heals |
| `actor_publish_delayed` | actor-death publish delayed forever | **FAST** (~0.25s) | actor channel is **not** the trigger (self-heals) |
| `node_publish_delayed` | node-death publish delayed forever | **PERMANENT-HANG** | **node channel is the trigger** |
| `node_persist_delayed` | node persist (fsync) delayed forever | **PERMANENT-HANG** | **reproduces the real bug** (publish gated on slow write) |
| `node_persist_delayed_f3` | persist delayed **+ F3** | **FAST** (~1.65s) | **F3 fixes it** |
| `node_publish_delayed_f5` | publish delayed **+ F5** | **FAST** (~2.5s) | **F5 fixes it** (heals without the push) |

## 4. Fix options and recommendation

All three fixes resolve the same root cause; they differ in **which one property each
sacrifices**:

| Fix | Where | Sacrifices | Robust to a *dropped* push? | Blast radius | Validated |
|-----|-------|-----------|-----------------------------|--------------|-----------|
| **F4** (soft-durability, already shipped) | storage | **durability** (`sync=false` → memtable-only on crash) | No | config flag | — |
| **F3** publish node-death **before** persist | GCS | **broadcast-after-durable ordering** (crash-window ≈ persist latency; node-only, re-derivable) | No (fixes *slow*, not *dropped*) | small, GCS-local | §3 |
| **F5** owner-side fallback re-poll of GCS DEAD nodes | core worker | **neither GCS invariant** (adds an owner-side poll) | **Yes** | medium, consumer-side | §3 |

**Recommendation.** Ship **F3 for the node channel** as the primary fix: it is the
smallest change, removes the proven cause at its source, keeps strict durability, and
is backend-agnostic. The one invariant it relaxes — broadcast-after-durable — is safe
*specifically* for the terminal, restart-re-derivable node-death transition; **do not**
extend it to the actor channel, whose state machine can resurrect across a GCS crash.
If resilience to a genuinely *dropped* (not merely slow) notification is wanted, add
**F5** as a complementary backstop — it preserves both GCS invariants. Whether this
lands in the RocksDB PR or a follow-up is the open question this branch is meant to
inform.

> A reviewer might suggest a submitter-side lease timeout or snapshot-on-subscribe;
> neither engages here — recovery never starts (no lease is requested), and the driver
> is a steady-state subscriber whose push is *delayed*, not dropped-for-lack-of-subscriber.

---

## 5. Reproduce

### Test-only knobs (inert by default)

Additive, env-gated edits in the GCS / core worker. With no env var set, behavior is
identical to upstream. They are read via `std::getenv` (not `RayConfig`) so the
raylet/core-worker config validator never sees them.

| Env var | File | Effect |
|---------|------|--------|
| `RAY_TESTING_GCS_NODE_PUBLISH_DELAY_MS` | `gcs_node_manager.cc` | delay only the node-death publish |
| `RAY_TESTING_GCS_NODE_PERSIST_DELAY_MS` | `gcs_node_manager.cc` | delay the persist `on_done` (models slow fsync) |
| `RAY_TESTING_GCS_NODE_PUBLISH_BEFORE_PERSIST` | `gcs_node_manager.cc` | **F3**: publish on the in-memory transition |
| `RAY_TESTING_GCS_ACTOR_PUBLISH_DELAY_MS` | `gcs_actor_manager.cc` | delay only the actor-death publish |
| `RAY_TESTING_ENABLE_NODE_DEATH_FALLBACK` | `core_worker.cc` | **F5**: owner-side re-poll of GCS DEAD nodes |

### Build and run

The knobs compile into `gcs_server` + `_raylet`; build Ray from source (see
`doc/source/ray-contribute/development.rst`), then:

```bash
source <your ray dev venv>
bash python/ray/tests/run_rep64.sh   # 6 process-isolated arms -> /tmp/rep64_summary.txt
```

Expected verdicts match the table in §3. Each arm runs in its own pytest subprocess
(a hung arm parks a daemon thread in native `CoreWorker::Wait()`; reusing the process
SIGSEGVs the next `ray.init`); hung arms are bounded by a 200s wall-clock timeout.
