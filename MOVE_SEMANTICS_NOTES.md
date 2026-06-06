# Plasma Move Semantics — State of the Branch

Branch: `karticam/plasma-move-semantics`
Date drafted: 2026-05-14

## 1. What "plasma move semantics" is

Goal: when a raylet has pushed an object to a remote raylet, the producer should be allowed to free its local primary copy immediately instead of waiting for the owner to publish an eviction. This reduces peak object-store usage during pipelines where the producer no longer needs its own copy.

### Implementation surface (existing branch commits)

- `RayConfig::enable_plasma_move_semantics` flag (`bd9a10db8e`, on by default per `994a229c82`).
- `ObjectManager::SetOnPushComplete(fn)` — new callback the node-manager can register. It fires once all chunks of a `Push` to a peer have been acknowledged by that peer (success or failure).
- Ack tracking inside `ObjectManager::PushObjectInternal`: a new `push_ack_tracking_` map keyed by `(object_id, peer_node_id)` counts acked chunks; on the last chunk it calls `on_push_complete_(object_id)` if all acks succeeded.
- `NodeManager` ctor wires `on_push_complete_` to:
  ```cpp
  local_object_manager_.ReleaseFreedObject(object_id, /*local_only=*/true);
  ```
- `LocalObjectManager::ReleaseFreedObject` gains a `local_only` parameter. With `local_only=true`:
  - Marks the object `is_freed_`, removes from `pinned_objects_`.
  - Calls `FlushFreeObjects(local_only=true)` → `on_objects_freed_(objects, local_only=true)` → `ObjectManager::FreeObjects(object_ids, local_only=true)`.
  - With `local_only=true`, `FreeObjects` only does `buffer_pool_.FreeObjects(...)` (local plasma delete) and does **not** broadcast `FreeObjectsRequest` to other nodes — keeping the consumer's copy intact.
- New `moved_out_pending_broadcast_` set in `LocalObjectManager`. When the owner later publishes the real eviction (ref count → 0), the producer raylet broadcasts a `FreeObjectsRequest` cluster-wide at that point so all other raylets clean up their copies.

### What the move-semantics path does NOT touch

- Owner-side state (`ReferenceCounter` on the owner core-worker). Specifically `pinned_at_node_id_` is not updated.
- Consumer-side raylet pinning (`LocalObjectManager::local_objects_` / `pinned_objects_`). The consumer holds the only copy in plasma but does not register it as a primary pin.
- Subscription to `WORKER_OBJECT_EVICTION` for the consumer. The consumer is never told about owner-driven eviction directly — only via the producer's broadcast path above.
- Unsubscribe on the producer. The producer remains subscribed to the owner's eviction channel for this object even after releasing the local copy; the subscription only tears down when the owner finally publishes evict (and then the early-`is_freed_` return fires before re-doing work).

## 2. The bug: spot-preemption + lineage reconstruction

### Symptom

Ray Data workload with spot preemptions fails partway through with:

```
ray.exceptions.ObjectFetchTimedOutError: Failed to retrieve object <id>.
Fetch for object <id> timed out because no locations were found for the object.
This may indicate a system-level bug.
```

User-visible time is exactly `fetch_fail_timeout_milliseconds` (default 10 min) after the first "no locations" observation in the consumer raylet.

### Root cause

The owner core-worker's lineage-reconstruction trigger lives in `ReferenceCounter::ResetObjectsOnRemovedNode` (`reference_counter.cc:893`):

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

Recovery is keyed on `pinned_at_node_id_` matching the dead node. That field is set once, in `TaskManager::HandleTaskReturn` (`task_manager.cc:565`), to the producer's worker node. It is cleared by `UnsetObjectPrimaryCopy` (called from gate-1 of `DeleteReferenceInternal`, or from `ResetObjectsOnRemovedNode` itself, or from `HandleObjectSpilled` on a dead spill node). It is overwritten by `ObjectRecoveryManager::PinExistingObjectCopy` during recovery.

Move semantics never goes through any of those paths. The producer frees its copy and sends a `REMOVED` location update; the owner's `CoreWorker::RemoveObjectLocationOwner` → `ReferenceCounter::RemoveObjectLocation` → `RemoveObjectLocationInternal` only edits `locations`, never `pinned_at_node_id_`.

So after a move:
- `locations = {consumer}`
- `pinned_at_node_id_ = producer` (stale)

When the consumer dies:
- `ResetObjectsOnRemovedNode(consumer)` checks `pinned_at_node_id_ == consumer` → false. Object is **not** queued for recovery.
- Locations get consumer erased → `locations = {}`.
- Borrower raylets subscribed to this object's `WORKER_OBJECT_LOCATIONS_CHANNEL` receive an empty location set via `PushToLocationSubscribers`.
- A consumer task elsewhere needs the object, its raylet's pull manager finds `client_locations` empty and no spilled URL, arms the 10-min `fetch_fail_timeout_milliseconds` timer (logs `Object neither in memory nor external storage` at `pull_manager.cc:499`), and 10 minutes later fires `OBJECT_FETCH_TIMED_OUT`, which `MarkObjectsAsFailed` writes as a plasma error marker, which Python materializes as `ObjectFetchTimedOutError`.

## 3. Evidence from one failing run

Logs under
`prodjob_vj56b6ri4stg9hj35t6c5sywsj/logs/.../session_2026-05-14_10-59-31_486856_2906/`

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

Implication: `ResetObjectsOnRemovedNode` did not enqueue this object for recovery on either death — exactly the stale-pin failure mode predicted by the analysis.

## 4. The fix (minimum viable)

Make the owner's `pinned_at_node_id_` follow the move.

### Mechanics

1. **Plumb peer node through the push-complete callback.** Change `on_push_complete_(object_id)` to `on_push_complete_(object_id, peer_node_id)` in `object_manager.cc::PushObjectInternal` and its callsites.

2. **Add a "primary moved" notification field.** Extend `rpc::ObjectLocationUpdate` in `core_worker.proto` with `optional bytes primary_moved_to_node_id = 5;` (piggyback on the existing `UpdateObjectLocationBatch` flow).

3. **Send it from the producer raylet.** In `node_manager.cc`'s `on_push_complete_` lambda, look up the owner address from `local_object_manager_.local_objects_[id].owner_address_`, then call a new `OwnershipBasedObjectDirectory::ReportObjectPrimaryMoved(id, peer, owner_address)` that buffers an update with `primary_moved_to_node_id = peer` and triggers `SendObjectLocationUpdateBatchIfNeeded`.

4. **Handle it on the owner.** In `CoreWorker::HandleUpdateObjectLocationBatch` (`core_worker.cc:3766`), when `primary_moved_to_node_id` is set, call `reference_counter_->UpdateObjectPinnedAtRaylet(object_id, new_node_id)`. That existing function already handles the dead-node race: if the new node is dead at the time of the update, it calls `UnsetObjectPrimaryCopy` and pushes to `objects_to_recover_`.

5. **Suppress the spurious overwrite warning.** `UpdateObjectPinnedAtRaylet` (`reference_counter.cc:930`) logs `"... but it already has a primary location ... This should only happen during reconstruction"` whenever overwriting. With this fix it fires on every move. Either downgrade to DEBUG or skip when called via this new path.

### Race analysis

The four messages around a move that can interleave at the owner:
- (a) `REMOVED` from producer P
- (b) `ADDED` from consumer C
- (c) New `PrimaryMoved(O, C)` from producer P
- (d) `NODE_DEAD` for C from GCS

All four orderings where (d) lands resolve correctly:
- (c) before (d): `pinned_at = C`, death observation queues recovery.
- (d) before (c): pin update arrives with C already dead. `UpdateObjectPinnedAtRaylet` sees `is_node_dead_(C) == true` and falls into the `else` branch — `UnsetObjectPrimaryCopy` + push to `objects_to_recover_`. Recovery still triggers.
- (c) lost because P died: P's death triggers `ResetObjectsOnRemovedNode(P)` which matches the stale `pinned_at == P` and queues recovery. The recovery manager finds C in locations (assuming b) arrived) and calls `PinExistingObjectCopy(C)` → `pinned_at = C`. Wasteful but correct.

## 5. Additional bookkeeping the consumer is missing

Even with fix (4), the consumer's view of its own copy is incomplete. Three gaps:

1. **No `LocalObjectManager` pin on the consumer.** Pulled-in copies live in plasma but are not in `pinned_objects_` on the consumer raylet. Plasma LRU eviction could silently drop the only live copy under memory pressure.

2. **No `WORKER_OBJECT_EVICTION` subscription on the consumer.** When the owner finally publishes eviction (gate-1 of `DeleteReferenceInternal`), only the original producer (still subscribed) receives it. The current branch handles cleanup via the producer's broadcast `FreeObjectsRequest` after re-receipt, but this depends on the producer still being alive when eviction publishes.

3. **No `owner_address_` recorded on the consumer raylet.** Any code that looks up `local_objects_[id].owner_address_` to know who owns this object will find nothing on the consumer.

### Full fix (extension of the MVP)

In `on_push_complete_` on the producer raylet, send a new raylet→raylet RPC `PinReceivedPrimary(object_id, owner_address)` to the consumer. The consumer handler calls `LocalObjectManager::PinObjectsAndWaitForFree({id}, {RayObject}, owner_address, generator_id)` for the just-received copy. That single call:
- Inserts into `local_objects_` with owner_address.
- Pins via `pinned_objects_` — closes the LRU hole.
- Subscribes to the owner's `WORKER_OBJECT_EVICTION` channel for the object.

Pick where to send the "pin moved" update:
- From the producer (after pin RPC is sent / acked): one extra round-trip but lets the producer drive the whole move atomically.
- From the consumer (after `PinObjectsAndWaitForFree` succeeds locally): mirrors the standard flow (producer-worker pins, then owner is informed via task-return). Safer because the owner only learns about C as primary after C has actually pinned.

Also explicitly unsubscribe the producer from the owner's eviction channel inside `on_push_complete_` once the consumer has taken over. Hygiene only; the existing dangling subscription self-cleans on eventual evict.

## 6. Suggested implementation order

1. **Add an INFO log** in `ResetObjectsOnRemovedNode` that prints `(object_id, pinned_at, spilled_node, queued_recovery)` for every owned ref scanned on death. Rerun the chaos test once; confirm in driver log that the lost object's `pinned_at` is the (still-alive) producer. This proves the diagnosis before code changes.
2. **MVP fix:** sections 4.1 – 4.5 above. Solves the spot-preemption symptom in isolation.
3. **Full fix:** section 5. Closes the LRU and subscription gaps. Needed for correctness in long-running pipelines under memory pressure, not just spot preemption.
4. **Cleanups:**
   - Decide warning policy for `UpdateObjectPinnedAtRaylet` (downgrade or path-aware).
   - Remove `moved_out_pending_broadcast_` if the consumer is now the canonical evict subscriber (the producer broadcast becomes unnecessary).
   - Audit the "second push of an already-released object" case in `ObjectManager::PushObjectInternal` — the in-memory chunk-reader survives plasma deletion, but document the invariant.

## 7. Open questions to resolve before merging

- **Multiple consumers receiving the same push.** If P pushes to both C1 and C2 and frees on the first success, who is the canonical primary? Current `on_push_complete_` fires per peer, so the new RPC would arrive twice; the owner's `pinned_at` would flip C1 → C2. Acceptable (recovery handles either dying), but worth a unit test.
- **Pin-RPC failure on the consumer side.** What does the producer do if `PinReceivedPrimary` RPC fails (consumer raylet rejects, network error)? Most conservative: do not call `ReleaseFreedObject` (keep the producer's copy as primary).
- **Spilling interaction.** If the consumer is asked to spill the moved object, does the existing spill path know about the new owner-address / pin and report `ReportObjectSpilled` correctly? Verify before merging.
- **`UpdateObjectPinnedAtRaylet` already-set warning.** Decide policy and add a regression test asserting that move-semantics overwrites do not log at WARNING.

## 8. Useful greps for triage

```
DIR=prodjob_.../logs/.../session_...
DRIVER=$DIR/head-*/python-core-driver-03000000*_3972.log
GCS=$DIR/head-*/gcs_server.out

# Find the producer task of a lost object id (first 16 hex of the id, with task suffix 03000000):
grep -nE "task_id=<first16hex>" $DRIVER

# All node-death observations on the owner:
grep -nE 'Node failure\. All objects pinned' $DRIVER

# Recovery batches with sizes:
grep -nE "Attempting to recover [0-9]+ lost" $DRIVER

# Skip-recovery (pinned location still valid):
grep -n "skipping recovery" $DRIVER

# Consumer-side pull failure entry:
grep -n "Object neither in memory nor external storage" $DIR/worker-*/raylet.out

# GCS death timestamps + IPs:
grep -nE 'Node is dead because the health check failed|death reason' $GCS
```

## 9. Default log levels

Default backend log level is `INFO`. The INFO-level events already in the code are sufficient for triage:
- `core_worker.cc:766` Node failure
- `core_worker.cc:477` Attempting to recover N
- `task_manager.cc:406` Resubmitting task that produced lost plasma object
- `object_recovery_manager.cc:80` Object has a pinned or spilled location, skipping recovery
- `reference_counter.cc:930` Updating primary location ... already has primary location

Set `RAY_BACKEND_LOG_LEVEL=debug` only if INFO doesn't pinpoint the failure.
