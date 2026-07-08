# Plasma Move Semantics

Branch: `karticam/plasma-move-semantics`

Created 2026-05-14, after the basic move-semantics machinery was already in place (eoakes' WIP commits + `bd9a10db8e` adding the config flag + `994a229c82` turning it on by default). Rewritten 2026-07-07 after a squash-and-simplify rebase onto `origin/master` (`e002fae45f`) which absorbed master's owner-driven FreeLocalObjects refactor (`ce4ccecfe3` + `0c42834064`).

The doc covers:
1. What move semantics is and how the current implementation works on top of master.
2. Two bugs surfaced under chaos: a lineage-reconstruction failure on spot preemption, then a `ray.put` regression. Plus one still-open regression on `video_object_detection`.
3. The two fixes.
4. **The 2026-07-07 rebase — what master changed, what the branch used to do, what we squashed away, what we kept.**
5. What's still left before this can be considered production-ready.

---

## Current status (resume point)

Last touched 2026-07-07.

- Branch tip is the doc commit on top of Fix B (`9d54998e07`) + Fix A (`f3439cbe69`) + Baseline (`dfb6b8407c`), sitting on top of `origin/master` at `e002fae45f`.
- Full history (branch commits only, base → tip), in the historical order the work happened:
  1. `[core] Baseline plasma move semantics + config flag` — the mechanism + its off switch
  2. `[core] Fix A: consumer→owner pin update on move-semantics handoff` — proto + MoveCompleted RPC + all the wiring, fixing the spot-preemption / lineage-reconstruction failure
  3. `[core] Fix B: skip move semantics for ray.put() objects` — IsForPut helper + skip logic in on_push_complete_
  4. `[core] Plasma move-semantics design + fixes writeup` (this doc)
- Safety tag `pre-simplify-squash` points at the pre-rebase tip (`fb56edea8e`) — 24 commits including all the WIPs and `local_only` / `moved_out_pending_broadcast_` machinery. Untouched and available if we need to consult it.
- **Open issue: §2.3 Bug C** — `ObjectFetchTimedOutError` reproduced 2026-06-15 on `video_object_detection` while move semantics is enabled, on a *task return* (index = 2, not a put), so neither Fix A nor Fix B covers it. Three hypotheses recorded; next step is to gather the specific log lines and confirm which path lost the object.

Re-entry order when picking this back up:
1. Read §2.3 Bug C and run the greps in "What to look for in logs".
2. Confirm the deployed wheel actually contains commits from this branch.
3. Decide between hypothesis (a) LRU on consumer, (b) the §5 item 1 race, (c) spill-restore path.
4. Once Bug C is resolved, the next pending item is the §5 item 1 race regardless — it's a known correctness hole.

---

## 1. What plasma move semantics is

Goal: when a raylet pushes an object to a remote raylet, the producer should be allowed to free its local primary copy immediately, instead of waiting for the owner to publish an eviction. Reduces peak object-store usage during pipelines where the producer no longer needs its own copy.

### Implementation on top of master (post-rebase)

Master (as of `e002fae45f`) already does owner-driven eviction — the owner core worker sends `FreeLocalObjects` RPC directly to each raylet in `pinned_at_node_id_ ∪ locations` when the ref count hits 0 (via `CoreWorker::FreeObjectOnNodesAsync` at `core_worker.cc:4855` → `RayletClient::FreeLocalObjects` → `NodeManager::HandleFreeLocalObjects` at `node_manager.cc:3759` → `LocalObjectManager::ReleaseFreedLocalObject`). This replaces the older pubsub-eviction path. Move semantics plugs into that:

- **Config flag** `RayConfig::enable_plasma_move_semantics` (default `true`) gates the producer-side release.
- **`ObjectManager::SetOnPushComplete(fn)`** registers a callback fired once per successful `(object_id, peer_node_id)` push. Chunk-ack tracking lives in `push_ack_tracking_` (map<pair, PushAckState{total_chunks, acked_chunks, failed}>) which is populated in `PushObjectInternal` and drained in the per-chunk `on_complete` callback.
- **Producer-side wiring in NodeManager**: on push complete, if the object is not a `ray.put` (Fix B), the producer sends a `MoveCompleted` RPC to the peer with the owner's address, then calls `local_object_manager_.ReleaseFreedLocalObject(object_id)` to free its local copy. Master's `ReleaseFreedLocalObject` was purpose-built for this — it does the local unpin/spill-cleanup and enqueues the object for the next `FlushFreeObjects` batch without broadcasting anywhere.
- **Consumer-side wiring** (`on_move_completed_` in NodeManager): fetches the newly-received object from plasma, calls `LocalObjectManager::PinObjectsAndWaitForFree` to pin it (so it survives LRU), and calls `object_directory_.ReportObjectPrimaryMoved(id, self_node_id_, owner_address)` to tell the owner the primary pin has moved to this node.
- **Owner-side** (`CoreWorker::HandleUpdateObjectLocationBatch`): when it receives an `ObjectLocationUpdate` with `primary_moved_to_node_id` set, calls `ReferenceCounter::UpdateObjectPinnedAtRaylet(id, new_primary)`. This keeps `pinned_at_node_id_` in sync so lineage reconstruction fires on the *new* primary node if it later dies.
- **Reference counter DEBUG downgrade** (`reference_counter.cc:947`): the `"already has a primary location"` message is now DEBUG instead of INFO because it fires on every successful move-semantics handoff, not just during reconstruction.

### What is NOT in this implementation

Things we deliberately do not do — either because master already handles them, or because they're deferred:

- **No producer→cluster broadcast on release.** Master doesn't have this concept anymore — the owner drives all frees via direct RPCs, so there's no reason for the producer to broadcast anything. This is why the old `local_only` flag and `moved_out_pending_broadcast_` set from the pre-rebase branch are gone (see §4 for the details of what was removed).
- **No consumer subscription to `WORKER_OBJECT_EVICTION` pubsub.** That channel doesn't exist on master anymore — the owner sends `FreeLocalObjects` RPC directly to whoever holds a copy. `PinObjectsAndWaitForFree` still runs on the consumer (to keep the object pinned locally against LRU), but there is no per-object eviction subscription to set up.

---

## 2. The bugs

### Repro setup (image_embedding_chaos — Bug A & Bug B)

The original two bugs were surfaced by the release test **`image_embedding_from_jsonl_fixed_size_chaos`** (`release/release_data_tests.yaml:745`). It is on a `manual` frequency and tagged `fail_on_dead_nodes: 0` — the chaos variant of the ViT image-embedding batch-inference benchmark, expected to be tolerant of node loss.

- Script: `release/nightly_tests/dataset/image_embedding_from_jsonl/main.py`.
- Pipeline: `ray.data.read_json(...)` → `flat_map(decode)` (base64 → PIL Image → ndarray) → `map(preprocess)` (ViT processor → 224×224 tensor) → `map_batches(Infer, num_gpus=1, batch_size=1024, concurrency=(N,N))` (ViT classification on GPU) → `write_parquet`.
- Cluster (`fixed_size_cluster_compute.yaml`, us-west-2): r6a.8xlarge head + 100 r6a.8xlarge CPU workers + 40 g5.4xlarge GPU workers, all `use_spot: false`. Worktree's local diff scales it down to 10 CPU + 4 GPU for cheap repro.
- Input: 10 TiB JSONL set in CI; local diff narrows to 64 shards (~1 TiB) via `NUM_INPUT_FILES`.
- Args in CI: `--inference-concurrency 40 40 --chaos`.

**Chaos mechanism.** Not real spot — instances are on-demand. With `--chaos`, `main.py:65` schedules an `EC2InstanceTerminatorWithGracePeriod` actor onto the head node that periodically terminates a random worker with grace period ~1/min. From Ray's POV it's indistinguishable from spot loss: raylet dies, GCS marks node dead, owners observe node failure, lineage reconstruction must kick in.

### Bug A — spot preemption + lineage reconstruction failure

**Symptom:** Ray Data workload with spot preemptions fails partway through with `ObjectFetchTimedOutError: Fetch for object <id> timed out because no locations were found`. Visible after exactly `fetch_fail_timeout_milliseconds` (default 10 min).

**Root cause.** `ReferenceCounter::ResetObjectsOnRemovedNode` (`reference_counter.cc:912`):

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

Recovery is keyed on `pinned_at_node_id_` matching the dead node. That field is set once, in `TaskManager::HandleTaskReturn`, to the producer's worker node. Pre-Fix-A, move semantics never updated it — the location changes but the pin stayed on the producer.

After a move: `locations = {consumer}`, `pinned_at_node_id_ = producer` (stale). When the consumer dies, `ResetObjectsOnRemovedNode(consumer)` finds no `pinned_at` match, so the object is never queued for recovery. Locations get erased, borrower fetches time out.

### Evidence from a failing run

Logs under `prodjob_vj56b6ri4stg9hj35t6c5sywsj/logs/.../session_2026-05-14_10-59-31_486856_2906/`.

Lost object: `1df82a0f913a6c3dffffffffffffffffffffffff0300000006000000` (return idx 6 of producer task `1df82a0f...03000000`).

- `gcs_server.out:2583` — first node dead 11:08:21.072.
- `driver.log:363` — `Attempting to recover 12 lost objects` at 11:08:21.144.
- `driver.log:395` — `Attempting to recover 24 lost objects` at 11:08:25.946.
- `raylet.out:106` (consumer) — `Object neither in memory nor external storage 1df82a0f...06000000` at 11:08:44.318.

Smoking gun: driver log has zero mentions of `task_id=1df82a0f913a6c3d`. Recovery fired for 36 other objects across the two deaths but the producer task for the lost object was never resubmitted, even though the lost object is a dependency of a resubmitted Predictor task.

### Bug B — `OBJECT_UNRECONSTRUCTABLE_PUT` (surfaced after Fix A)

After Fix A landed and lineage reconstruction started firing correctly, the chaos test failed differently:

```
ray.exceptions.ObjectReconstructionFailedError:
  [OBJECT_UNRECONSTRUCTABLE_PUT] The object cannot be reconstructed because
  it was created by ray.put(), which has no task lineage.
```

**Root cause.** `ray.put()` objects have no producing task — `LineageReconstructionEligibility::INELIGIBLE_PUT` is set in `CoreWorker::Put`. With Fix A tracking the moved pin correctly, when the new primary node dies for a put object, recovery is queued, `ReconstructObject` runs, and the caller sees `OBJECT_UNRECONSTRUCTABLE_PUT`.

The lineage path is doing the right thing. The bug is that **move semantics should never have moved a put object in the first place** — without lineage, once the primary leaves the put-er's node and that destination dies, the object is permanently lost.

### Bug C — `ObjectFetchTimedOutError` on `video_object_detection` (open as of 2026-06-15)

**Symptom (abridged):**
```
2026-06-15 17:42:04 INFO ... Total Progress: 2667/2669
...(~90s stuck)...
2026-06-15 17:43:45 ERROR streaming_executor_state.py:701 -- An exception was raised from
  "FlatMap(explode_features)->Map(crop_image)->MapBatches(drop_columns)->Write". ...
ray.exceptions.ObjectFetchTimedOutError: Failed to retrieve object
  12fe3fb800771024c63fa52f3312878e2a4510ad0200000002000000.
```

Workload: video_object_detection. Pipeline: `ListFiles → ReadFiles→Map(resize_frame) → MapBatches(ExtractImageFeatures) → FlatMap(explode_features)→Map(crop_image)→MapBatches(drop_columns)→Write`. Pre-failure: `ExtractImageFeatures` had emitted `[583/2590 objects local]`, so move semantics was firing. Only 2 of 2669 outputs of the final operator never landed. Stall lasted exactly `fetch_fail_timeout_milliseconds`.

**Object-id decode for `12fe3fb8...0200000002000000`:**
- TaskID: first 48 hex.
- ObjectIndex: last 8 hex little-endian = **2**.
- `2 < max_num_generator_returns (100M)` → NOT a put. Fix B doesn't apply.
- Regular task return.

Same observable shape as Bug A but on a workload where chaos was not mentioned and no `Node failure` event was in the user's paste.

**Hypotheses, ordered by likelihood:**

1. **(a) LRU eviction on the consumer, no chaos involved.** Most likely. Move semantics releases the producer's primary unconditionally. If the consumer's `PinObjectsAndWaitForFree` didn't take (e.g., `GetObjectsFromPlasma` returned null because the object hadn't finished sealing yet), the consumer's copy slips into LRU and can be freed under memory pressure. Producer already released → object gone everywhere.
2. **(b) The §5 item 1 race.** Consumer process dies between plasma-seal (`ADDED` published to owner) and the `MoveCompleted` handler running on its raylet. Owner is left with `locations={C}`, `pinned_at_node_id_=P` (still alive). `ResetObjectsOnRemovedNode(C)` doesn't match P → no recovery. Requires a death event.
3. **(c) Spill-restore loses the moved-pin claim.** If the consumer pinned correctly, then later spilled to S3, then restored — does the restored copy re-register as primary pin under move-semantics rules? Untested.

**What to look for in logs:**

```sh
OID=12fe3fb800771024c63fa52f3312878e2a4510ad0200000002000000
grep -n "$OID" /tmp/ray/session_*/logs/raylet.{out,err} \
                /tmp/ray/session_*/logs/python-core-worker-*.log \
                /tmp/ray/session_*/logs/python-core-driver-*.log

# Did NotifyMoveCompleted fire on the producer?
grep -nE "MoveCompleted|on_push_complete" /tmp/ray/session_*/logs/raylet.*

# Did the consumer's GetObjectsFromPlasma succeed? Look for the WARNING:
grep -nE "consumer could not fetch|no owner address" /tmp/ray/session_*/logs/raylet.*

# Was the consumer node lost between handoff and consumption?
grep -nE "Node failure\. All objects pinned|death reason|Node is dead" \
  /tmp/ray/session_*/logs/python-core-driver-*.log \
  /tmp/ray/session_*/logs/gcs_server.*

# Plasma evictions on the consumer raylet — a positive hit = hypothesis (a):
grep -nE "Eviction|LRU|object store full" /tmp/ray/session_*/logs/raylet.*

# Was the object spilled — a positive hit = hypothesis (c):
grep -nE "Spilled|RestoreSpilledObject|object_spilling" \
  /tmp/ray/session_*/logs/raylet.*
```

**Verifying the deployed binary actually has both fixes:**
```sh
python -c "import ray; print(ray.__version__, ray.__commit__)"
# Expect a sha at or after this branch's tip.

strings $(python -c "import ray, os; print(os.path.dirname(ray.__file__))")/_raylet.so \
  | grep -E "MoveCompleted|IsForPut|primary_moved_to_node_id" | head
# Expect ≥3 hits: the RPC name, the helper, the proto field.
```

**Open questions:**
1. Was `--chaos` enabled in this run?
2. Deterministic repro? Same 2 objects each time, or different?
3. Consumer node's plasma high-water-mark just before the stall?
4. Confirmed the running binary has the branch commits?

**Status:** Open. Recommended patch path:
- If (a): add INFO log in `on_push_complete_` success path and both branches of `GetObjectsFromPlasma` in `on_move_completed_`. Re-run; verify consumer pin actually happens.
- If (b): implement the ack-bearing `MoveCompleted` RPC described in §5 item 1.
- If (c): audit `LocalObjectManager::AddSpilledUrl` / restore path to ensure moved-pin metadata survives a round-trip.

---

## 3. The fixes

### Fix A — consumer→owner pin update

Make the owner's `pinned_at_node_id_` follow the move, with the consumer driving the signal via `MoveCompleted` RPC.

```
producer raylet                                  consumer raylet                       owner
─────────────────                                ───────────────                       ─────
on_push_complete_(obj, peer) fires
  ↓
NotifyMoveCompleted(obj, peer, owner_addr)  ───→ HandleMoveCompleted
ReleaseFreedLocalObject(obj)                     ↓
                                                 GetObjectsFromPlasma + PinObjectsAndWaitForFree
                                                   (consumer holds the primary pin, LRU-safe)
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

Race analysis. Four owner-side messages can interleave: (a) `REMOVED` from producer (secondary), (b) `ADDED` from consumer, (c) `PrimaryMoved` (new), (d) `NODE_DEAD` for consumer from GCS. Walked every ordering with (d) as the failure trigger:

- (c) before (d): `pinned_at = consumer` when death arrives → recovery queued. ✓
- (d) before (c): pin update arrives with consumer already dead → `UpdateObjectPinnedAtRaylet` sees `is_node_dead_(consumer)=true`, unsets pin and pushes to `objects_to_recover_`. Recovery queued from the late pin update itself. ✓
- (c) lost because producer died first: producer's death triggers `ResetObjectsOnRemovedNode(producer)` which matches stale `pinned_at == producer` → queues recovery. Manager finds consumer in locations, `PinExistingObjectCopy(consumer)` → `pinned_at = consumer`. Wasteful but correct. ✓

One race window not addressed by Fix A alone: see §5 item 1 (consumer dies mid-handoff between plasma-seal and processing of `MoveCompleted` — same shape as Bug A, narrower window).

### Fix B — gate move semantics on `ObjectID::IsForPut`

Skip move semantics entirely for `ray.put()` objects. Producer keeps the only authoritative copy.

**Detection.** Reused Ray's put-index convention rather than threading an explicit flag. From `WorkerContext::GetNextPutIndex`:
```
return num_returns + max_num_generator_returns_ + ++put_counter_;
```
So put indices are always strictly greater than `max_num_generator_returns_` (default 100,000,000). Helper:

```cpp
bool ObjectID::IsForPut(const ObjectID &object_id) {
  return object_id.ObjectIndex() > RayConfig::instance().max_num_generator_returns();
}
```

Conservative — every put is detected. The only misclassification would be a generator return with an emission index in `(NumReturns, NumReturns + max_num_generator_returns]`, which is absurd in practice and only forgoes a runtime optimization (no data loss).

**Behavior matrix after both fixes:**

| Object class | Index range | Move semantics |
|---|---|---|
| Task return | `[1, NumReturns]` | applies — release + MoveCompleted → owner tracks moved pin; recovery works on death |
| Generator return | `(NumReturns, NumReturns + max_num_generator_returns]` | applies normally |
| `ray.put()` object | `> NumReturns + max_num_generator_returns` (>100M with defaults) | **skipped** — producer keeps its primary |

---

## 4. The 2026-07-07 rebase — what changed

Master (as of `e002fae45f`) had rewritten the exact code area the branch was iterating on. Specifically, PRs #63218 + #63181 (`ce4ccecfe3` + `0c42834064`) migrated Ray from a pubsub-then-broadcast free path to an **owner-driven `FreeLocalObjects` RPC**. This made a lot of the branch's own machinery redundant. The pre-rebase branch had 24 commits (many WIPs); after squash-and-simplify it has 3 code commits + doc, in the historical order the work happened (baseline → fixes → doc).

### What master introduced (the shape we now build on)

1. **New owner-driven flow.** When ref count hits 0 on the owner core worker, `ReferenceCounter` invokes an injected `free_object_on_nodes_async` callback (`core_worker_process.cc:363`) that maps to `CoreWorker::FreeObjectOnNodesAsync(id, locations)` (`core_worker.cc:4855`). That fires `FreeLocalObjects` RPC directly at each raylet in `pinned_at_node_id_ ∪ locations`.
2. **`NodeManager::HandleFreeLocalObjects`** (`node_manager.cc:3759`) receives the RPC and delegates to `LocalObjectManager::ReleaseFreedLocalObject(object_id)` — a new primitive that does the local unpin/spill-cleanup and enqueues the object for the next `FlushFreeObjects` batch.
3. **`LocalObjectManager::ReleaseFreedLocalObject`** replaces the older `ReleaseFreedObject`. Its signature is single-arg and its semantics are inherently local-only — no broadcast, no pubsub. This is exactly the primitive move semantics needs on the producer side.
4. **`LocalObjectManager::GetLocalObjectsOwnedBy` / `GetLocalObjectsOwnedByOwnersOn`** — new helpers used by the raylet to clean up when GCS reports an owner worker or node dead (replaces the old pubsub `owner_dead_callback`).
5. **`WORKER_OBJECT_EVICTION` pubsub — REMOVED.** The old channel where the owner told the primary raylet to release is gone entirely (`0c42834064`).
6. **The old raylet-to-raylet `FreeObjects` RPC — REMOVED.** No more cluster-wide broadcast on release.

### What the pre-rebase branch had, that we squashed away

All of the following existed on the branch (`pre-simplify-squash` tag preserves it) and were built to work around the old free path. Master's refactor made them unnecessary:

- **`bool local_only` param** on `LocalObjectManager::ReleaseFreedObject`, `FlushFreeObjects`, `on_objects_freed_`, and `ObjectManager::FreeObjects`. Point: distinguish "producer's move-semantics local release" (don't broadcast) from "owner's real eviction" (do broadcast). Redundant because master no longer broadcasts.
- **`moved_out_pending_broadcast_` set** in `LocalObjectManager`. Point: remember objects released locally via move semantics so we could broadcast a `FreeObjectsRequest` later when the owner's pubsub eviction message arrived. Redundant because (a) the owner no longer publishes eviction via pubsub, (b) master doesn't broadcast anyway — the owner sends a direct `FreeLocalObjects` RPC to each copy holder.
- **The whole intermediate-WIP set of commits** iterating on `local_only` (`ed588d2db1`, `5f03c884f8`, `a4eed91c8e`, `f55a9edead`, `df1ea5bc40`, plus six raw `WIP` commits at the base). The design they were iterating on is moot.
- **Debug scaffolding**: `[karticam]`-tagged log statements throughout (~40+ across 11 files). Commented out in `27d8c43e81` "Commenting out logs for benchmarking", then fully removed in `fb56edea8e` on the pre-rebase branch. Not carried forward.

### What we kept (in the 3 squashed code commits + doc)

1. **`[core] Baseline plasma move semantics + config flag`** (`dfb6b8407c`)
   - config `enable_plasma_move_semantics` (default true) in `ray_config_def.h`
   - `ObjectManager::SetOnPushComplete` callback interface + concrete + mock
   - `push_ack_tracking_` map (per-`(obj, peer)` chunk-ack accounting) + `PushAckState` struct
   - `PushObjectInternal` init + per-chunk `on_complete` accounting that fires `on_push_complete_` exactly once when the whole transfer succeeds
   - NodeManager wiring: `on_push_complete_` calls `local_object_manager_.ReleaseFreedLocalObject(object_id)`. **This is the crucial simplification** — we use master's brand-new primitive directly instead of the deleted `local_only` mechanism.

2. **`[core] Fix A: consumer→owner pin update on move-semantics handoff`** (`f3439cbe69`)
   - proto: `ObjectLocationUpdate.primary_moved_to_node_id` field, `MoveCompleted` RPC + request/reply
   - `MoveCompleted` RPC client + fake + server handler wiring
   - `ObjectManager::SetOnMoveCompleted`, `NotifyMoveCompleted`, `HandleMoveCompleted` (+ mocks)
   - `IObjectDirectory::ReportObjectPrimaryMoved` + `OwnershipBasedObjectDirectory` impl (stages `primary_moved_to_node_id` on the per-owner location-update batch)
   - `LocalObjectManager::GetOwnerAddress` accessor (+ test fake update in `node_manager_test.cc`)
   - NodeManager: `on_push_complete_` extended to fire `NotifyMoveCompleted` before releasing; new `on_move_completed_` handler pins on plasma + reports primary moved
   - `CoreWorker::HandleUpdateObjectLocationBatch`: consume `primary_moved_to_node_id` → `ReferenceCounter::UpdateObjectPinnedAtRaylet`
   - `ReferenceCounter` INFO→DEBUG on `"already has a primary location"` (fires every move now)

3. **`[core] Fix B: skip move semantics for ray.put() objects`** (`9d54998e07`)
   - `ObjectID::IsForPut` helper + `:ray_config` dep on the `id` bazel target
   - NodeManager: early-return in `on_push_complete_` for put objects

4. **This doc** (`PLASMA_MOVE_SEMANTICS.md`)

### Files touched (net vs `origin/master`)

- Commit 1 (baseline + config): 5 files, +94 / -23 lines.
- Commit 2 (Fix A): 19 files, +250 / -6 lines.
- Commit 3 (Fix B): 4 files, +34 / -1 lines.

Total: ~374 net insertions of runtime code on top of master (excluding the doc), spread across the RPC surface, ObjectManager, ObjectDirectory, LocalObjectManager, NodeManager, CoreWorker, ReferenceCounter, and the two mocks/fakes. Compared to the pre-rebase branch (~800 net lines), that is roughly half the surface area — the reduction came from dropping the `local_only` param plumbing and the `moved_out_pending_broadcast_` machinery.

### Safety net

`git tag pre-simplify-squash` still points at the pre-rebase tip. If any regression is traced back to something the WIP branch had that this squashed version doesn't, `git diff pre-simplify-squash HEAD -- <file>` will show the delta on that file.

---

## 5. What's still left for production

Ordered by priority. Tags: **[correctness]** can lose or corrupt data; **[observability]** affects triage but not behavior; **[hygiene]** is quality-of-life.

### Must fix

1. **Consumer-dies-mid-handoff race  [correctness].** The producer fires `NotifyMoveCompleted` and releases its local copy without waiting for an ack. If the consumer dies between receiving the bytes (plasma seal → `ADDED` to owner) and processing the `MoveCompleted` callback, the owner ends up with `locations={C}`, `pinned_at_node_id_=P` (still alive). `ResetObjectsOnRemovedNode(C)` doesn't match P → no recovery. Same shape as Bug A, narrower window. **Fix:** turn the RPC into callback-bearing; release only on success reply; keep the producer's copy on failure. Modest change in the producer-side `on_push_complete_` lambda. *Cross-ref: this is hypothesis (b) for the open §2.3 Bug C.*

2. **Tests  [correctness — coverage].** Nothing added yet. Minimum set:
   - Unit: `ObjectID::IsForPut` across boundary indices.
   - Unit: `OwnershipBasedObjectDirectory::ReportObjectPrimaryMoved` — confirms staging + batched send.
   - Unit: `CoreWorker::HandleUpdateObjectLocationBatch` with `primary_moved_to_node_id`, including the `is_node_dead_` branch.
   - Integration: push → MoveCompleted → consumer pins → owner updates pin → kill consumer → verify recovery fires.
   - Race test for item 1.

3. **Edge cases to confirm  [correctness — verification]:**
   - Fan-out (producer pushes O to C1 and C2). `on_push_complete_` fires per peer; after first, `GetOwnerAddress(O)` returns `nullopt` for the second. Current code logs WARNING and skips. Net: pin moves to C1, C2 has a secondary. Should be safe but warrants a test.
   - Push fails partway: verify `on_push_complete_` doesn't fire on failed pushes (`push_ack_tracking_` failure path).
   - Owner publishes eviction during the move window: should be handled by `is_freed_` early-return after pinning, but worth a regression test.
   - Spilling on the consumer after move: verify `ReportObjectSpilled` works with the `owner_address_` in consumer's `LocalObjectInfo`.

### Should fix

4. **Metrics  [observability].** No signal currently. Add counters in `ObjectManager`:
   - `move_semantics.push_completed_total{result=success|failure}`
   - `move_semantics.moves_performed_total`
   - `move_semantics.moves_skipped_total{reason=put|no_owner_addr|disabled}`
   - `move_semantics.bytes_released_total`
   - `move_semantics.consumer_pin_failed_total`
   Required to validate behavior in a canary.

5. **Restore INFO warning path for lineage reconstruction  [observability].** Fix A lowered `reference_counter.cc:947` from INFO to DEBUG because move semantics fires it every handoff. But the warning's original purpose was catching **recovery-time** pin overwrites, which are still INFO-worthy. **Fix:** thread a `bool from_move_semantics` (or enum) into `UpdateObjectPinnedAtRaylet`, log at DEBUG when from move semantics, INFO otherwise.

### Nice-to-have

6. **Rollback story  [operability].** `enable_plasma_move_semantics` is read at NodeManager ctor and in `PushObjectInternal` — disabling requires raylet restart. Confirm and document the rollback path: `RAY_enable_plasma_move_semantics=0`, behavior with mid-job flag flip, etc.

7. **Code-level design comment  [hygiene].** This `.md` file may not survive long-term. Add a short comment block (4–8 lines) at the top of the move-semantics setup in `node_manager.cc` explaining the producer→consumer→owner handshake, and a `// see also` at `ObjectID::IsForPut`.

8. **Cross-region / high-latency note  [docs].** The wider the `MoveCompleted` RPC latency, the wider the race window for item 1. Worth a sentence in this doc once item 1 is fixed.

---

## 6. Triage cheat sheet

### Default log levels

C++ raylet/core-worker default is INFO. INFO-level events already sufficient for triage:

- `core_worker.cc:766` — `Node failure. All objects pinned on that node will be lost ...`
- `core_worker.cc:477` — `:info_message: Attempting to recover N lost objects ...`
- `task_manager.cc:406` — `Resubmitting task that produced lost plasma object, attempt #N: ...`
- `object_recovery_manager.cc:80` — `Object has a pinned or spilled location, skipping recovery` (smoking gun for the original stale-pin bug)
- `reference_counter.cc:947` — `Updating primary location ... but it already has a primary location` (now DEBUG; see §5 item 5 about restoring INFO for the recovery path)
- `node_manager.cc:on_push_complete_ warning` — `Move semantics: no owner address on producer raylet` (fires when producer already released before the callback ran; useful for tracking fan-out edge case)
- `node_manager.cc:on_move_completed_ warning` — `Move semantics: consumer could not fetch object from plasma on MoveCompleted` (fires on hypothesis (a) for Bug C)

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

# Move semantics fired for a specific object:
grep -nE "MoveCompleted|primary_moved" $DIR/worker-*/raylet.out $DRIVER

# GCS death timestamps + IPs:
grep -nE 'Node is dead because the health check failed|death reason' $GCS
```
