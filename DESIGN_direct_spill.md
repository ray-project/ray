# Design: Direct (proactive) spill for task return objects

**Status:** IMPLEMENTED on branch `claude/sleepy-diffie-a9f794`, on fresh **upstream/master**
(`47a2089dc7`, 2026-06-27). Built (full bazel C++ + Cython) in conda `myenv` and verified
e2e — see §10.
**Scope (decided):** Task return values only — `@ray.remote(_spill_immediately=True)` (and `.options(_spill_immediately=True)`). `ray.put()` is intentionally out of scope for the first cut; the proto/raylet plumbing is reusable for it later.

---

## 1. Goal

Today object spilling is **reactive**: an object lives in the plasma store until memory
pressure crosses `object_spilling_threshold` (default 0.8), at which point the raylet
picks victims and spills them to disk. The object occupies plasma memory the entire time
in between.

We want an **opt-in, per-object "direct spill"**: certain task return objects should be
written to the spill backend (disk / external storage) **immediately after they are
created**, rather than waiting for memory pressure. The plasma copy can then be released,
freeing primary memory right away. Reads transparently restore from disk via the existing
restore path (no change needed there).

Use case: large, write-once / read-later objects (e.g. Ray Data shuffle map outputs /
intermediate blocks) that we know will sit around consuming object-store memory and pressure
everything else, when we'd rather pay the disk write up front and keep memory free.

---

## 2. How reactive spill works today (the path we piggy-back on)

Verified call chain:

1. Task returns a value → `_raylet.pyx::store_task_outputs` → `store_task_output`
   (`python/ray/_raylet.pyx:4258`).
2. → C++ `CoreWorker::AllocateReturnObject` (allocates plasma buffer or inlines small objects)
   then `CoreWorker::SealReturnObject` (`src/ray/core_worker/core_worker.cc:3044`).
3. `SealReturnObject` → `SealExisting(return_id, /*pin=*/true, generator_id, owner_address)`
   (`core_worker.cc:3056`).
4. `SealExisting` seals in plasma, then sends the **`PinObjectIDs`** RPC to the local raylet
   (`core_worker.cc:1201`).
5. Raylet `NodeManager::HandlePinObjectIDs` (`src/ray/raylet/node_manager.cc:2603`)
   → `GetObjectsFromPlasma` → `LocalObjectManager::PinObjectsAndWaitForFree`
   (`src/ray/raylet/local_object_manager.cc:32`), which inserts into `pinned_objects_`.
6. **Spilling is decoupled in time:** `NodeManager::SpillIfOverPrimaryObjectsThreshold`
   (`node_manager.cc:2412`) runs periodically and on object-local; only when usage ≥ threshold
   does it call `LocalObjectManager::SpillObjectUptoMaxThroughput`.

**The key lever:** `LocalObjectManager` already exposes a public method to spill a *specific*
set of objects immediately, bypassing the threshold and victim-selection policy:

```cpp
// src/ray/raylet/local_object_manager.h:145
void SpillObjects(const std::vector<ObjectID> &objects_ids,
                  std::function<void(const ray::Status &)> callback) override;
```

`SpillObjects` → `SpillObjectsInternal` moves the named objects from `pinned_objects_` into
`objects_pending_spill_` and hands them to an IO worker — exactly what we want. So "direct
spill" = **tag the object, and have `HandlePinObjectIDs` call `SpillObjects` right after
pinning instead of waiting.**

**The proven plumbing pattern:** `tensor_transport` is an existing per-task/per-object option
threaded Python → TaskSpec proto → C++ → object lifecycle. We mirror it with a boolean
`spill_immediately`.

---

## 3. Design overview

Two pieces of information have to travel:

- **Tag the task** so its return objects are marked: a `spill_immediately` bool on the
  `TaskSpec` proto, set from the Python option.
- **Carry the tag to the spill decision point:** the core worker reads the flag off the
  *currently executing task spec* (no need to thread it through the Python execute callback),
  and forwards it on the `PinObjectIDs` RPC. The raylet acts on it.

```
@ray.remote(_spill_immediately=True)
        │  (submission)
        ▼
TaskSpec.spill_immediately = true                     [common.proto]
        │  (task delivered + executed on worker)
        ▼
CoreWorker::SealReturnObject
   reads worker_context_.GetCurrentTask()->SpillImmediately()    [core_worker.cc]
        │
        ▼
SealExisting(..., spill_immediately)
   → PinObjectIDsRequest.spill_immediately = true     [node_manager.proto]
        │
        ▼
NodeManager::HandlePinObjectIDs
   if request.spill_immediately() && spilling_enabled:
       local_object_manager_.SpillObjects(object_ids, cb)   [node_manager.cc]
        │
        ▼
LocalObjectManager::SpillObjects → SpillObjectsInternal      (existing path)
   → IO worker writes to disk, plasma copy released
```

Why read off `worker_context_` instead of threading through `execute_task`: the Python
execute callback chain (`execute_task` → `store_task_outputs` → `store_task_output`) is long
and would need a new param at every hop. But `SealReturnObject` is C++ and runs inside the
task-execution context, where `worker_context_.GetCurrentTask()`
(`src/ray/core_worker/context.h:97`, returns `shared_ptr<const TaskSpecification>`) already
holds the spec. One read, zero Python-callback churn.

---

## 4. Per-layer changes (exact diffs)

### 4.1 Proto — `src/ray/protobuf/common.proto`

Add to the `TaskSpec` message (last field is `num_objects_per_yield = 46`, so use 47):

```proto
  // If true, return objects of this task are spilled to external storage
  // immediately after creation instead of waiting for memory pressure.
  optional bool spill_immediately = 47;
```

### 4.2 Proto — `src/ray/protobuf/node_manager.proto`

Add to `PinObjectIDsRequest` (after `generator_id = 3`):

```proto
  // If true, the raylet spills these objects to external storage immediately
  // after pinning instead of waiting for the spill threshold.
  optional bool spill_immediately = 4;
```

### 4.3 TaskSpec accessor — `src/ray/common/task/task_spec.{h,cc}`

Mirror `TensorTransport()` (`task_spec.cc:535`):

```cpp
// task_spec.h (near line 395)
bool SpillImmediately() const;

// task_spec.cc
bool TaskSpecification::SpillImmediately() const {
  return message_->spill_immediately();
}
```

### 4.4 TaskSpec builder — `src/ray/common/task/task_util.h`

In `BuildCommonTaskSpec` (around `task_util.h:317`, where `tensor_transport` is set):

```cpp
// add param: bool spill_immediately = false
if (spill_immediately) {
  message_->set_spill_immediately(true);
}
```

### 4.5 Core worker — `src/ray/core_worker/core_worker.cc`

**`SealExisting`** (`core_worker.cc:1193`) — add a `bool spill_immediately = false` param and
forward on the RPC:

```cpp
Status CoreWorker::SealExisting(const ObjectID &object_id,
                               bool pin_object,
                               const ObjectID &generator_id,
                               const std::unique_ptr<rpc::Address> &owner_address,
                               bool spill_immediately) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
        spill_immediately,           // <-- new
        [this, object_id](...) { ... });
  }
  ...
}
```

(Update the `RayletClient::PinObjectIDs` wrapper signature in
`src/ray/raylet_rpc_client/raylet_client.{h,cc}` to accept `spill_immediately` and
`request.set_spill_immediately(...)`; and the `SealExisting` decl in `core_worker.h`. The
`SealOwned` caller at `core_worker.cc:1180` passes `false`.)

**`SealReturnObject`** (`core_worker.cc:3044`) — read the flag off the current task spec:

```cpp
bool spill_immediately = false;
auto task_spec = worker_context_.GetCurrentTask();
if (task_spec != nullptr) {
  spill_immediately = task_spec->SpillImmediately();
}
status = SealExisting(return_id, /*pin=*/true, generator_id,
                      owner_address_ptr, spill_immediately);
```

### 4.6 Raylet — `src/ray/raylet/node_manager.cc::HandlePinObjectIDs` (line 2603)

After the existing `local_object_manager_.PinObjectsAndWaitForFree(...)` call, add:

```cpp
if (request.spill_immediately() &&
    RayConfig::instance().enable_direct_spill() &&
    !RayConfig::instance().object_spilling_config().empty()) {
  local_object_manager_.SpillObjects(
      object_ids, [](const Status &status) {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Direct spill request failed: " << status.ToString();
        }
      });
}
```

Notes:
- `object_ids` here is the already-filtered list (nulls / pending-deletion removed) — the same
  vector passed to `PinObjectsAndWaitForFree`, so every id is guaranteed to be in
  `pinned_objects_`. `SpillObjectsInternal` requires this (it errors if an id isn't pinned).
- Guard on `object_spilling_config` non-empty: if spilling is disabled there are no IO
  workers and `SpillObjects` would be a no-op-with-error; we skip cleanly and the object just
  stays in plasma (graceful degradation).

### 4.7 Config kill-switch — `src/ray/common/ray_config_def.h`

```cpp
/// If false, the per-object direct-spill hint is ignored and objects follow the
/// normal reactive spilling path. On by default; lets ops disable the feature globally.
RAY_CONFIG(bool, enable_direct_spill, true)
```

### 4.8 Python option plumbing

Mirror an existing simple task option. Two surfaces:

- **`@ray.remote(_spill_immediately=True)`** and **`.options(_spill_immediately=True)`** in
  `python/ray/remote_function.py` (and `actor.py` for actor methods if we want it there too —
  can defer).
- Thread the option through the Cython submission path (`_raylet.pyx` task submission →
  `CoreWorker.SubmitTask`, `libcoreworker.pxd`, `common.pxd`) to `BuildCommonTaskSpec`'s new
  `spill_immediately` param. `tensor_transport` is the reference plumbing
  (`python/ray/includes/libcoreworker.pxd:435`, `common.pxd:403/422`).

The user-facing name uses the `_`-prefix convention for experimental/internal options
(consistent with `_tensor_transport`, `_generator_backpressure_num_objects`).

---

## 5. Edge cases & caveats

1. **Inlined small objects don't spill.** `AllocateReturnObject` inlines objects below
   `max_direct_call_object_size`; those never enter plasma and `SealReturnObject` skips the
   plasma branch (`core_worker.cc:3055`). Direct spill only applies to plasma-backed returns.
   Fine — small objects aren't the target. Worth a doc note.

2. **Fusion / many small files.** `SealReturnObject` → `SealExisting` pins **one object per
   RPC**, so direct spill writes one object per spill file, bypassing the reactive path's
   object fusion (`min_spilling_size`, `max_fused_object_count`). For large Ray Data blocks
   this is fine; for many tiny returns it's inefficient (file-per-object). Acceptable for v1;
   note as a known limitation. A later optimization could batch returns of one task into a
   single `PinObjectIDs` + `SpillObjects` call.

3. **Restore is already handled.** Reading a spilled object transparently restores it via the
   existing `restore_spilled_objects` path — no changes needed. Direct-spilled objects are
   indistinguishable from reactively-spilled ones once on disk.

4. **Spilling disabled.** If `object_spilling_config` is empty (spilling off entirely), the
   raylet guard skips direct spill and the object stays in plasma. No error surfaced to the
   task.

5. **Generator / dynamic returns.** They funnel through the same `store_task_outputs` →
   `SealReturnObject`, so they inherit the flag automatically. Streaming-generator returns
   should be validated explicitly in testing.

6. **Ownership / ref counting unaffected.** We only change *when* the object is spilled, not
   ownership, lineage, or reconstruction. Reactive spill already supports spilling pinned
   objects, so reconstruction-on-failure semantics are unchanged.

7. **Double-spill race.** If memory pressure also fires, an object could be picked by both
   paths. `SpillObjectsInternal` already guards via `objects_pending_spill_` (an object
   already pending spill is skipped), so this is safe.

---

## 6. Testing plan

- **C++ unit test** (`src/ray/raylet/local_object_manager_test.cc` style): a pinned object with
  the flag is moved to pending-spill immediately, without crossing the threshold.
- **Python e2e** (new test in `python/ray/tests/test_object_spilling.py`):
  - Start Ray with a small object-store and a spill dir, threshold high (e.g. 0.99) so
    reactive spill won't fire.
  - Run `@ray.remote(_spill_immediately=True)` returning a large array.
  - Assert via `ray.experimental.get_object_locations` / spill metrics
    (`spilled_objects_total`, `spilled_bytes_total`) that the object is spilled shortly after
    the task completes, while object-store memory stays low.
  - Control task without the flag → object stays in plasma, not spilled.
  - `ray.get` on the direct-spilled ref returns correct data (restore path).
- Flip `RAY_enable_direct_spill=0` → behaves exactly like today.

---

## 7. Build requirement

This touches proto + C++ (core worker, raylet, task spec) + Cython. It requires a **full
Ray C++ build** (`bazel`), not a Python-only wheel build. The python-only env is insufficient
to test this end-to-end.

---

## 8. Files touched (summary)

| Layer | File |
|---|---|
| Proto | `src/ray/protobuf/common.proto`, `src/ray/protobuf/node_manager.proto` |
| TaskSpec | `src/ray/common/task/task_spec.{h,cc}`, `src/ray/common/task/task_util.h` |
| Core worker | `src/ray/core_worker/core_worker.{h,cc}` |
| Raylet RPC client | `src/ray/raylet_rpc_client/raylet_client.{h,cc}` |
| Raylet | `src/ray/raylet/node_manager.cc` |
| Config | `src/ray/common/ray_config_def.h` |
| Cython | `python/ray/_raylet.pyx`, `python/ray/includes/{common,libcoreworker}.pxd` |
| Python API | `python/ray/remote_function.py` (+ optionally `actor.py`) |
| Tests | `python/ray/tests/test_object_spilling.py`, `src/ray/raylet/local_object_manager_test.cc` |

---

## 9. Open questions

1. **API name:** `_spill_immediately` vs `_spill_on_create` vs `_direct_spill`. Proposed
   `_spill_immediately`.
2. **Actor methods too?** First cut is plain tasks; actor-method support is the same mechanism
   (set the flag on the actor task spec) but adds `actor.py` plumbing — defer unless wanted.
3. **Batched fusion** (caveat #2) — ship v1 file-per-object, or build the batching optimization
   now? Recommend v1 first.
4. **`ray.put()` support** — explicitly deferred; same proto/raylet plumbing applies via
   `SealOwned`/`SealExisting` with the flag sourced from the put option instead of the task spec.

---

## 10. Implementation notes (as built)

Scope shipped: **task return values only** — `@ray.remote(_spill_immediately=True)` and
`.options(_spill_immediately=True)`.

Two deviations from the original design, both discovered during testing:

1. **Spill-timing accounting moved to the shared path.** `memory_summary` / dashboards gate
   the "Spilled N MiB" line on `spill_time_total_s > 0`, but that counter was only updated in
   the *reactive* callback (`TryToSpillObjects`). Direct spills went through `SpillObjects` →
   `SpillObjectsInternal` with a different callback, so they spilled correctly and incremented
   `spilled_bytes_total_` but stayed invisible in stats. Fix: `SpillObjectsInternal` now records
   `spill_time_total_s_` / `last_spill_finish_ns_` itself (covering both reactive and direct
   paths), and the duplicate update was removed from `TryToSpillObjects`. `test_spill_stats`
   still passes.

2. **Eviction is lazy (Ray's normal spill semantics), not immediate.** After a direct spill the
   object is written to disk and the raylet drops its pin, but the plasma *cache copy* lingers
   until memory pressure actually evicts it — same as reactive spill. So in steady state with no
   pressure, the plasma footprint does not drop the instant the task returns. The win is that
   the object is **pre-spilled**: when pressure hits it can be evicted instantly with no spill
   stall, and it is reflected in spill stats. If a future requirement is to *force* the primary
   copy out of plasma immediately, that is a separate change (delete the primary copy after
   spill and rely on restore-on-read) and was intentionally not done here.

### Verification
- `pytest ray/tests/test_object_spilling.py -k direct_spill` → 2 passed
  (`test_direct_spill_immediately`, `test_direct_spill_disabled_by_config`).
- Regression: `test_spill_stats`, `test_spilling_not_done_for_pinned_object`,
  `test_spill_remote_object` → all pass.
- Raylet debug log confirms the path: `node_manager.cc: Directly spilling 1 object(s)
  requested by the owner.` followed by `Object ... spilled at ...?offset=0&size=52429153`
  (full 50 MiB on disk).

### Build environment gotchas (macOS arm64, this machine)
- Apple clang 21 is newer than the pinned abseil expects. A native build needs
  `BAZEL_ARGS="--copt=-Wno-error=deprecated-builtins --copt=-Wno-error=deprecated-declarations
  --copt=-Wno-error=deprecated-pragma"`. `-Werror` is kept for everything else.
- The editable build was installed into conda `myenv`. A pre-existing
  `site-packages/ray` (stale `setup-dev.py` symlinks pointing at the *limit-shuffle-fusion*
  worktree) conflicts with it and was moved aside to
  `site-packages/ray.disabled_editable_conflict`. Run tests from `<worktree>/python` (cwd
  takes precedence). To restore the previous python-only dev setup, move that dir back — but
  it cannot coexist with this worktree's editable build.
