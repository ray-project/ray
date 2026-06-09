# Plasma Move Semantics

Branch: `karticam/plasma-move-semantics`

Created 2026-05-14, after the basic move-semantics machinery was already in place on this branch (eoakes' WIP commits + `bd9a10db8e` adding the `enable_plasma_move_semantics` config flag + `994a229c82` turning it on by default). This doc evolved as we diagnosed two follow-up bugs and applied fixes; it now serves as the consolidated reference covering:

1. What move semantics is and what the baseline implementation looked like.
2. Two bugs surfaced when running it under chaos: a lineage-reconstruction failure on spot preemption, then a put-object regression.
3. Two patches that fixed them (`5e1b2923c6` and `86dccc2748`).
4. What is still left before this can be considered production-ready.

---

## 1. What plasma move semantics is

Goal: when a raylet pushes an object to a remote raylet, the producer should be allowed to free its local primary copy immediately, instead of waiting for the owner to publish an eviction. Reduces peak object-store usage during pipelines where the producer no longer needs its own copy.

### Baseline implementation (pre-fix)

- `RayConfig::enable_plasma_move_semantics` flag (`bd9a10db8e`, on by default per `994a229c82`).
- `ObjectManager::SetOnPushComplete(fn)` callback fires after all chunks of a push are acked.
- `push_ack_tracking_` map in `ObjectManager::PushObjectInternal` counts acked chunks per `(object_id, peer_node_id)`; invokes the callback on success.
- `NodeManager` wires the callback to:
  ```cpp
  local_object_manager_.ReleaseFreedObject(object_id, /*local_only=*/true);
  ```
- `LocalObjectManager::ReleaseFreedObject` gained a `local_only` parameter. With `local_only=true`:
  - Marks the object `is_freed_`, removes from `pinned_objects_`.
  - Flushes through `on_objects_freed_(objects, local_only=true)` → `ObjectManager::FreeObjects(local_only=true)`.
  - `local_only=true` makes `FreeObjects` only call `buffer_pool_.FreeObjects(...)` locally; it does **not** broadcast `FreeObjectsRequest` to other nodes — preserving the consumer's copy.
- `moved_out_pending_broadcast_` set in `LocalObjectManager`: when the owner later publishes the real eviction, the producer broadcasts a `FreeObjectsRequest` cluster-wide so other raylets clean up.

### What the baseline did NOT touch

These were the gaps the fixes had to close:

- Owner-side state (`ReferenceCounter` on the owner core-worker): `pinned_at_node_id_` is never updated to follow the move.
- Consumer-side raylet pinning (`LocalObjectManager::local_objects_` / `pinned_objects_`): the consumer holds the only copy in plasma but doesn't register it as a primary pin.
- Subscription to `WORKER_OBJECT_EVICTION` on the consumer: never set up; consumer learns about owner-driven eviction only via the producer's broadcast hop above.
- Unsubscribe on the producer after release: producer keeps a dangling subscription that self-cleans only when the owner finally publishes evict.

---

## 2. The bugs

### Bug A — spot preemption + lineage reconstruction failure

**Symptom:** Ray Data workload with spot preemptions fails partway through:

```
ray.exceptions.ObjectFetchTimedOutError: Failed to retrieve object <id>.
Fetch for object <id> timed out because no locations were found for the object.
This may indicate a system-level bug.
```

User-visible time is exactly `fetch_fail_timeout_milliseconds` (default 10 min) after the consumer raylet's first "no locations" observation.

**Root cause.** The owner's lineage-reconstruction trigger in `ReferenceCounter::ResetObjectsOnRemovedNode` (`reference_counter.cc:893`):

```cpp
for (auto it = object_id_refs_.begin(); ...) {
  if (it->second.pinned_at_node_id_.value_or(NodeID::Nil()) == node_id ||
      it->second.spilled_node_id == node_id) {
    UnsetObjectPrimaryCopy(it);
    if (!it->second.OutOfScope(...)) objects_to_recover_.push_back(object_id);
  }
  RemoveObjectLocationInternal(it, node_id);
}
```

Recovery is keyed on `pinned_at_node_id_` matching the dead node. That field is set once, in `TaskManager::HandleTaskReturn` (`task_manager.cc:565`), to the producer's worker node. Move semantics never updates it — the `REMOVED` location report from the producer only edits the `locations` set, not the pin.

After a move:
- `locations = {consumer}`
- `pinned_at_node_id_ = producer` (stale)

When the consumer dies:
- `ResetObjectsOnRemovedNode(consumer)` → no match on `pinned_at`. **Not queued for recovery.**
- Locations get consumer erased → empty.
- Borrower tries to fetch, `pull_manager.cc:499` logs `Object neither in memory nor external storage`, arms the 10-min timer, fires `OBJECT_FETCH_TIMED_OUT` → user-visible exception.

### Evidence from a failing run

Logs under `prodjob_vj56b6ri4stg9hj35t6c5sywsj/logs/.../session_2026-05-14_10-59-31_486856_2906/`.

Lost object: `1df82a0f913a6c3dffffffffffffffffffffffff0300000006000000` (return idx 6 of producer task `1df82a0f913a6c3d...03000000`).

Key lines (file paths relative to the session dir):

- `head-10.0.17.65-.../gcs_server.out:2583` — `Node is dead because the health check failed`, node `a21f47cc...` (`10.0.122.240`), 11:08:21.072.
- `head-10.0.17.65-.../gcs_server.out:2616` — Second death, `d36d97c3...` (`10.0.105.14`), 11:08:25.895.
- `head-10.0.17.65-.../python-core-driver-03000000...3972.log:360` — `Node failure ...`, first dead node observed at 11:08:21.074.
- `…3972.log:363` — `Attempting to recover 12 lost objects` at 11:08:21.144.
- `…3972.log:385` — Second node failure observed at 11:08:25.895.
- `…3972.log:395` — `Attempting to recover 24 lost objects` at 11:08:25.946.
- `…3972.log:394` — Predictor actor task `f620654c...` resubmit at 11:08:25.897. Its `dependencies={...}` list includes `1df82a0f...06000000`.
- `worker-10.0.73.55-.../raylet.out:106` — Consumer pull-manager warning `Object neither in memory nor external storage 1df82a0f...06000000` at 11:08:44.318.
- `worker-10.0.73.55-.../raylet.out:115` — Same warning for sibling `1df82a0f...04000000` at 11:08:49.505.

Smoking-gun observation: searching the driver log for `task_id=1df82a0f913a6c3d` returns **no matches**. Recovery successfully fired for 36 other objects across the two deaths, but the producer task `1df82a0f...` was never resubmitted, even though the lost object is listed as a dependency of the resubmitted Predictor actor task at line 394.

### Bug B — `OBJECT_UNRECONSTRUCTABLE_PUT` (surfaced after fix A)

After fix A landed and the lineage path started firing correctly, the chaos test failed differently:

```
ray.exceptions.ObjectReconstructionFailedError:
  [OBJECT_UNRECONSTRUCTABLE_PUT] The object cannot be reconstructed because
  it was created by ray.put(), which has no task lineage.
```

**Root cause.** `ray.put()` objects have no producing task — `LineageReconstructionEligibility::INELIGIBLE_PUT` is set in `CoreWorker::Put` (`core_worker.cc:986`). With fix A in place, the owner correctly tracks the moved pin for these objects. When the new primary node dies, recovery is queued, `ReconstructObject` runs, `GetLineageReconstructionEligibility` returns `INELIGIBLE_PUT`, and the user sees `OBJECT_UNRECONSTRUCTABLE_PUT`.

The lineage path is working correctly. The bug is that **move semantics should never have moved a put object in the first place** — without lineage, once the primary leaves the put-er's node and that destination dies, the object is permanently lost.

---

## 3. The fixes

### Fix A — consumer→owner pin update (commit `5e1b2923c6`)

Make the owner's `pinned_at_node_id_` follow the move, with the consumer driving the signal.

```
producer raylet                                  consumer raylet                       owner
─────────────────                                ───────────────                       ─────
on_push_complete_(obj, peer) fires
  ↓
NotifyMoveCompleted(obj, peer, owner_addr)  ───→ HandleMoveCompleted
ReleaseFreedObject(obj, local_only=true)         ↓
                                                 GetObjectsFromPlasma + PinObjectsAndWaitForFree
                                                   (consumer now owns the pin, LRU-safe,
                                                    subscribed to WORKER_OBJECT_EVICTION)
                                                 ReportObjectPrimaryMoved(obj, self, owner_addr)
                                                                                       ↓
                                                                                       UpdateObjectLocationBatch
                                                                                         with primary_moved_to_node_id
                                                                                         = consumer
                                                                                       ↓
                                                                                       reference_counter_->
                                                                                         UpdateObjectPinnedAtRaylet(
                                                                                           obj, consumer)
```

**Why we routed through the consumer rather than producer→owner directly.** Producer→owner direct is the strict MVP and would have fixed Bug A alone. Routing through the consumer adds two side benefits "for free" via `PinObjectsAndWaitForFree`:
- Consumer's plasma copy is now pinned, preventing LRU eviction of the only live copy.
- Consumer subscribes to `WORKER_OBJECT_EVICTION` directly, so the owner's eviction publish reaches it without the producer's broadcast hop.

These closed two latent issues called out as gaps in §1 above.

#### Code changes (21 files)

- **Proto (2):** added `optional bytes primary_moved_to_node_id = 5` to `ObjectLocationUpdate`; added `MoveCompleted` RPC + `MoveCompletedRequest`/`MoveCompletedReply` to `ObjectManagerService`.
- **RPC client/server (4):** new `MoveCompleted` method on `ObjectManagerClient` + interface + fake; new server-handler macro in `object_manager_server.h` and the corresponding pure-virtual.
- **ObjectManager (3 + mock):** callback `on_push_complete_` signature extended to `(ObjectID, NodeID peer)`; new `on_move_completed_` callback registered by NodeManager on the consumer side; new public method `NotifyMoveCompleted(id, peer, owner_addr)` that uses `GetRpcClient`; new `HandleMoveCompleted` server handler that posts to `main_service_` and invokes `on_move_completed_`. Mock updated.
- **OwnershipBasedObjectDirectory (3):** new `ReportObjectPrimaryMoved(id, node, owner_addr)` that stages an `ObjectLocationUpdate{primary_moved_to_node_id = node}` on the per-owner buffer, reusing the existing in-flight gating in `SendObjectLocationUpdateBatchIfNeeded`.
- **LocalObjectManager (3 + test fake):** new `GetOwnerAddress(id) → optional<rpc::Address>` reading from `local_objects_[id].owner_address_`.
- **NodeManager (1):** rewired the move-semantics block — producer-side lambda calls `NotifyMoveCompleted` then `ReleaseFreedObject(local_only=true)`; consumer-side `on_move_completed_` lambda does `GetObjectsFromPlasma` + `PinObjectsAndWaitForFree` + `ReportObjectPrimaryMoved`.
- **Reference counter / core worker (2):** owner-side `HandleUpdateObjectLocationBatch` reads `primary_moved_to_node_id` and calls `reference_counter_->UpdateObjectPinnedAtRaylet(id, new_node_id)` (which already handles the dead-node race by unsetting the pin and queuing recovery). Downgraded the `"already has a primary location"` warning at `reference_counter.cc:930` from INFO to DEBUG, since with move semantics this now fires every move.
- **Incidental unblock (2):** removed unused `task_id` lambda captures in `ordered_actor_task_execution_queue.cc` and `unordered_actor_task_execution_queue.cc` that were left over from the `27d8c43e81` "Commenting out logs for benchmarking" commit and tripping `-Werror,-Wunused-lambda-capture`.

#### Race analysis

Four messages can interleave at the owner: (a) `REMOVED` from producer, (b) `ADDED` from consumer, (c) `PrimaryMoved` (the new one), (d) `NODE_DEAD` for consumer from GCS. Walked every ordering of (d) landing:

- (c) before (d): `pinned_at = consumer` when death arrives → recovery queued. ✓
- (d) before (c): pin update arrives with consumer already dead → `UpdateObjectPinnedAtRaylet` sees `is_node_dead_(consumer)=true`, falls into the else branch at `reference_counter.cc:942`, unsets pin and pushes to `objects_to_recover_`. Recovery still queued from the late pin update itself. ✓
- (c) lost because producer died first: producer's death triggers `ResetObjectsOnRemovedNode(producer)` which matches stale `pinned_at == producer` → queues recovery. Recovery manager finds consumer in locations, `PinExistingObjectCopy(consumer)` → `pinned_at = consumer`. Wasteful but correct. ✓

There is one race window not addressed: see §5 item 1.

### Fix B — gate move semantics on `ObjectID::IsForPut` (commit `86dccc2748`)

Skip move semantics entirely for `ray.put()` objects on the producer raylet. Producer keeps the only authoritative copy.

#### Detection mechanism

Reused Ray's existing put-index convention rather than threading an explicit flag through the pin path. From `WorkerContext::GetNextPutIndex` (`context.cc:58`):

```cpp
return num_returns + max_num_generator_returns_ + ++put_counter_;
```

So put indices are always strictly greater than `max_num_generator_returns_` (default 100,000,000 per `ray_config_def.h:331`). New helper:

```cpp
bool ObjectID::IsForPut(const ObjectID &object_id) {
  return object_id.ObjectIndex() >
         RayConfig::instance().max_num_generator_returns();
}
```

Verified on the failing object id `006173568f892e7c84ee4d3aa2054455128691600300000003e1f505`:
- Index bytes (little-endian) = `0x05f5e103` = 100,000,003.
- `100,000,003 > 100,000,000` → `IsForPut = true`.
- That's `max_num_generator_returns + 3` — the third `ray.put()` from this driver.

Property: conservative. Every put is detected. The only misclassification is a generator return whose emission index lands in `(NumReturns, NumReturns + max_num_generator_returns]` — absurd in practice, and would only forgo a runtime optimization (never lose data).

#### Why this over an explicit flag

Two approaches were on the table:

- **Option 1 (explicit flag):** add `optional bool is_put_object` to `PinObjectIDsRequest`, plumb from `CoreWorker::Put` through `HandlePinObjectIDs` → `LocalObjectInfo`, expose `IsPutObject(id)` getter on `LocalObjectManager`. ~6 files.
- **Option 3 (index check, chosen):** derive from `ObjectID` index encoding. 4 files.

| | Option 1 (explicit flag) | Option 3 (index check, chosen) |
|---|---|---|
| Surface area | ~6 files | 4 files |
| Failure mode if convention/flag breaks | **Silent** — forgotten flag → data loss | **Loud, bounded** — at worst missed optimization for absurd generator-return indices |
| Source-of-truth coupling | Worker flag must stay in sync with owner's `INELIGIBLE_PUT` | Both derive from the same `CoreWorker::Put` call site |
| Robustness to new put call sites | Each new site must remember to set the flag | Encoding is intrinsic; can't be forgotten |

#### Code changes (4 files)

- `src/ray/common/id.h` — declared `ObjectID::IsForPut(id)` with a doc comment explaining the convention, the conservative property, and why move semantics needs it.
- `src/ray/common/id.cc` — one-line impl; added `#include "ray/common/ray_config.h"`.
- `src/ray/common/BUILD.bazel` — added `:ray_config` to the `id` cc_library's `deps` so the new include resolves.
- `src/ray/raylet/node_manager.cc` — early-return at the top of `on_push_complete_` when `ObjectID::IsForPut(object_id)`, with a comment pointing at `OBJECT_UNRECONSTRUCTABLE_PUT` as the failure mode it prevents.

#### Behavior matrix after both fixes

| Object class | Index range | Move semantics |
|---|---|---|
| Task return | `[1, NumReturns]` | applies — release + MoveCompleted RPC; owner tracks moved pin; recovery works on death |
| Generator return | `(NumReturns, NumReturns + max_num_generator_returns]` | applies normally; absurd >100M emission index conservatively skipped (no data loss, just missed optimization) |
| `ray.put()` object | `> NumReturns + max_num_generator_returns` (>100M with defaults) | **skipped** — producer keeps its primary; original Ray contract for puts is preserved |

---

## 4. What got built

All targets compile clean with both patches applied:

```
//src/ray/protobuf:core_worker_cc_proto         ✓
//src/ray/protobuf:object_manager_cc_proto      ✓
//src/ray/object_manager_rpc_client:*           ✓
//src/ray/object_manager:object_manager         ✓
//src/ray/raylet:raylet_lib                     ✓
//src/ray/raylet:raylet                         ✓ (binary links cleanly)
//src/ray/core_worker:reference_counter         ✓
//src/ray/core_worker:core_worker_lib           ✓
//src/ray/raylet/tests:node_manager_test        ✓ (compile; link blocked by pre-existing gRPC envoy issue on Darwin/arm64)
//src/ray/common:id                             ✓
```

End-to-end chaos test rerun is still pending validation.

---

## 5. What's still left for production

Ordered by priority. Tags: **[correctness]** can lose or corrupt data; **[observability]** affects on-call triage but not behavior; **[hygiene]** is quality-of-life.

### Must fix

1. **Consumer-dies-mid-handoff race  [correctness].** The producer fires `NotifyMoveCompleted` and releases its local copy without waiting for an ack. If the consumer dies between receiving the bytes (plasma seal → `ADDED` to owner) and processing the `MoveCompleted` callback, the owner ends up with `locations={C}`, `pinned_at_node_id_=P` (still alive). `ResetObjectsOnRemovedNode(C)` doesn't match P → no recovery. Same shape as Bug A, just a narrower window. **Fix:** turn the RPC into callback-bearing; release only on success reply; keep the producer's copy on failure. Modest change in the producer-side `on_push_complete_` lambda.

2. **Tests  [correctness — coverage].** Nothing added yet. Minimum set:
   - Unit: `ObjectID::IsForPut` across boundary indices.
   - Unit: `OwnershipBasedObjectDirectory::ReportObjectPrimaryMoved` — confirms staging and batched send.
   - Unit: `CoreWorker::HandleUpdateObjectLocationBatch` with the new field, including the `is_node_dead_` branch.
   - Integration: push → MoveCompleted → consumer pins → owner updates pin → kill consumer → verify recovery fires.
   - Race test for item 1 above.

3. **Edge cases to confirm  [correctness — verification]:**
   - Fan-out (producer pushes O to C1 and C2). `on_push_complete_` fires per peer; after first completes and producer releases, `GetOwnerAddress(O)` returns `nullopt` for second. Current code logs WARNING and skips. Net: pin moves to C1, C2 has a secondary. Should be safe but warrants a test.
   - Push fails partway: verify `on_push_complete_` doesn't fire on failed pushes.
   - Owner publishes eviction during the move window: should be handled by the consumer's `is_freed_` early-return after pinning, but worth a regression test.
   - Spilling on the consumer after move: verify `ReportObjectSpilled` works with the new `owner_address_` in consumer's `LocalObjectInfo`.

### Should fix

4. **`moved_out_pending_broadcast_` is now mostly redundant  [hygiene].** With the consumer directly subscribed to `WORKER_OBJECT_EVICTION` via `PinObjectsAndWaitForFree`, the owner publishes eviction to the consumer directly — the producer-broadcast hop is no longer the only path. Either remove the broadcast logic entirely (smaller code) or keep with a `// TODO: redundant once X verified` comment (defense-in-depth). Decide and document.

5. **Producer's dangling subscription  [hygiene].** Producer's `WORKER_OBJECT_EVICTION` subscription for the moved object is not torn down. Self-cleans only when the owner eventually publishes evict (and the producer's callback hits `is_freed_` early-return). **Fix:** explicit `Unsubscribe` call inside `on_push_complete_` after a successful move.

6. **Metrics  [observability].** No signal currently. Add counters in `ObjectManager`:
   - `move_semantics.push_completed_total{result=success|failure}`
   - `move_semantics.moves_performed_total`
   - `move_semantics.moves_skipped_total{reason=put|no_owner_addr|disabled}`
   - `move_semantics.bytes_released_total`
   - `move_semantics.consumer_pin_failed_total`
   Required to validate behavior in a canary.

7. **Restore the warning we downgraded  [observability].** Fix A lowered `reference_counter.cc:930` from INFO to DEBUG because it fires on every move (noise). But the warning's original purpose was catching **recovery-time** pin overwrites, which are still INFO-worthy. **Fix:** thread a `bool from_move_semantics` (or enum) into `UpdateObjectPinnedAtRaylet`, log at DEBUG when from move semantics and INFO otherwise. Or add a parallel method.

### Nice-to-have

8. **Rollback story  [operability].** `enable_plasma_move_semantics` is on by default and read at NodeManager ctor (so disabling requires raylet restart — fine for release-blocking issues). Confirm and document the rollback path: `RAY_enable_plasma_move_semantics=0`, behavior with mid-job flag flip, etc.

9. **Code-level design comment  [hygiene].** This `.md` file is useful for the PR but may not survive long-term. Add a short comment block (4–8 lines) at the top of the move-semantics setup in `node_manager.cc` explaining the producer→consumer→owner handshake, and a `// see also` cross-reference at `ObjectID::IsForPut`.

10. **Clean up benchmarking debris  [hygiene].** `27d8c43e81` left commented-out `[karticam]` log statements throughout `object_manager.cc`, `local_object_manager.cc`, `node_manager.cc`, and `core_worker.cc`. Fix A removed two unused captures from these. Either restore the logs gated behind a config flag (if useful) or delete them outright. Currently dead comment lines remain.

11. **Cross-region / high-latency note  [docs].** The wider the `MoveCompleted` RPC latency, the wider the race window for item 1. Worth a sentence in the design doc.

---

## 6. Triage cheat sheet

### Default log levels

C++ raylet/core-worker default is INFO. The INFO-level events already in the code are sufficient for triage without `RAY_BACKEND_LOG_LEVEL=debug`:

- `core_worker.cc:766` — `Node failure. All objects pinned on that node will be lost ...`
- `core_worker.cc:477` — `:info_message: Attempting to recover N lost objects ...`
- `task_manager.cc:406` — `Resubmitting task that produced lost plasma object, attempt #N: ...`
- `object_recovery_manager.cc:80` — `Object has a pinned or spilled location, skipping recovery` (smoking gun for the original stale-pin bug)
- `reference_counter.cc:930` — `Updating primary location ... but it already has a primary location` (now DEBUG; see §5 item 7 about restoring INFO for the recovery path)

### Standard greps

```
DIR=prodjob_.../logs/.../session_...
DRIVER=$DIR/head-*/python-core-driver-<jobid>...log
GCS=$DIR/head-*/gcs_server.out

# Find the producer task of a lost object (first 16 hex of the id):
grep -nE "task_id=<first16hex>" $DRIVER

# All node-death observations on the owner:
grep -nE 'Node failure\. All objects pinned' $DRIVER

# Recovery batches with sizes:
grep -nE "Attempting to recover [0-9]+ lost" $DRIVER

# Skip-recovery (pinned location still valid — the stale-pin symptom pre-fix):
grep -n "skipping recovery" $DRIVER

# Consumer-side pull failure entry:
grep -n "Object neither in memory nor external storage" $DIR/worker-*/raylet.out

# GCS death timestamps + IPs:
grep -nE 'Node is dead because the health check failed|death reason' $GCS
```
