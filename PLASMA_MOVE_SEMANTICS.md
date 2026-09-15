# Plasma Move Semantics

Branch: `karticam/plasma-move-semantics`

Created 2026-05-14, after the basic move-semantics machinery was already in place (eoakes' WIP commits + `bd9a10db8e` adding the config flag + `994a229c82` turning it on by default). Rewritten 2026-07-07 after a squash-and-simplify rebase onto `origin/master` (`e002fae45f`) which absorbed master's owner-driven FreeLocalObjects refactor (`ce4ccecfe3` + `0c42834064`).

> **⚠️ The design has evolved — read §7 for how move semantics works today.** The original design used a producer→consumer `MoveCompleted` RPC (see §1's implementation notes and §3 Fix A). On **2026-07-12** that RPC was removed and the consumer now pins the moved object **inline on the push-receive path** (§7). **§1's implementation and §3 (Fix A) are kept only as evolution history — they no longer describe current behavior.** **§5 is the current forward-looking list, and §5 item 1 is the one open correctness gap.** §2 (bugs — including Bug C, the investigation that produced the redesign), §4 (rebase), and §6 (triage) are the historical investigation record; treat their `MoveCompleted`-era details and internal cross-refs as history.

The doc covers:
1. What move semantics is and how the current implementation works on top of master.
2. Two bugs surfaced under chaos: a lineage-reconstruction failure on spot preemption, then a `ray.put` regression. Plus one still-open regression on `video_object_detection`.
3. The two fixes.
4. **The 2026-07-07 rebase — what master changed, what the branch used to do, what we squashed away, what we kept.**
5. What's still left before this can be considered production-ready.

---

## Current status (resume point)

Last touched 2026-07-13.

### Where things stand (cold-start summary)

The implementation has been **redesigned** (§7): the producer→consumer `MoveCompleted` RPC is removed, and the consumer pins the moved object **inline on the push-receive path**, with the producer's release gated on that pin (realizes §2.3's "Fix 2"). This structurally fixes the eviction-before-pin bug (§2.3 Bug C). Key enabler: `PushRequest.owner_address` is already populated, so the `MoveCompleted` RPC only carried redundant data.

**Two repos — they differ, don't confuse them:**
- **`~/ray`, branch `karticam/plasma-move-semantics` — CANONICAL; the redesign lives here.** Master-based; post-#63181 (no raylet-to-raylet `FreeObjects` RPC). Compile-verified: `bazel build //src/ray/raylet:raylet //src/ray/object_manager/... //src/ray/raylet/tests:node_manager_test` is green. Pushed for review.
- **`~/rayturbo`, branch `karticam/plasma-move-semantics`** — the fork used to **build ECR images** (`push_ray_image.sh`); pre-#63181 (still has the `FreeObjects` RPC + `local_only`). The redesign was **reverted here** to the old `MoveCompleted` design; only workload-test scripts remain modified. (Also `karticam/plasma-move-semantics-actor-only` = the jhsu-base variant.) **To validate the fix on a workload, the redesign must be re-applied here and built into an image** — adapt to the pre-#63181 differences (e.g. `FreeObjects` still present; `PullRequest`/service shape differs).

**Verification state:** compile-verified; **workload-tested 2026-07-14** (actor-only branch) on `heterogeneous_memory_batch_inference` (n=6 move-sem-ON) plus a fan-out microbenchmark. Testing **surfaced a raylet-crashing bug in the redesign** — `AbortCreate` aborting a *sealed* deferred-release object — now **FIXED** (full mechanism + fix in **§7.5**); all post-fix runs are crash-free. Still **no unit tests added** (§5 item 2).

**One correctness gap is knowingly left OPEN:** the consumer-dies-before-the-owner-learns race — full description + candidate fixes in **§5 item 1**.

**Ray Data backpressure feature (IMPLEMENTED 2026-07-13, committed both repos, not yet workload-tested):** a "producer physically freed" callback so Ray Data admits the next producer task at the real physical-free moment (M2) instead of Justin's premature submit-time simulation — full design, flow chart, the leak fix found in review, and remaining to-dos in **§8**.

Prior state (2026-07-07):

- Branch tip is the doc commit on top of Fix B (`9d54998e07`) + Fix A (`f3439cbe69`) + Baseline (`dfb6b8407c`), sitting on top of `origin/master` at `e002fae45f`.
- Full history (branch commits only, base → tip), in the historical order the work happened:
  1. `[core] Baseline plasma move semantics + config flag` — the mechanism + its off switch
  2. `[core] Fix A: consumer→owner pin update on move-semantics handoff` — proto + MoveCompleted RPC + all the wiring, fixing the spot-preemption / lineage-reconstruction failure
  3. `[core] Fix B: skip move semantics for ray.put() objects` — IsForPut helper + skip logic in on_push_complete_
  4. `[core] Plasma move-semantics design + fixes writeup` (this doc)
- Safety tag `pre-simplify-squash` points at the pre-rebase tip (`fb56edea8e`) — 24 commits including all the WIPs and `local_only` / `moved_out_pending_broadcast_` machinery. Untouched and available if we need to consult it.
- **Open issue: §2.3 Bug C** — `ObjectFetchTimedOutError` reproduced 2026-06-15 on `video_object_detection` while move semantics is enabled, on a *task return* (index = 2, not a put), so neither Fix A nor Fix B covers it. Three hypotheses recorded; next step is to gather the specific log lines and confirm which path lost the object.
  - **UPDATE 2026-07-09/10 (post-rebase): root cause CONFIRMED.** Reproduced reliably on `image_embedding_from_jsonl` (0.1× scale) — ON failed 4/5 runs incl. a *solo* run; OFF 5/5; clean on `backpressure_training_prefetch` (no pressure). An **instrumented re-run** proved it is **(a) sealed-then-evicted**: on the consumer the pushed copy sealed, was LRU-evicted **21 ms later** (sealed-but-unpinned), and `MoveCompleted`'s pin ran **~200 ms after seal** → object already gone → skip pin; producer had already released → `ObjectFetchTimedOut`. (`rejecting chunk`/`aborting` = 0 → not the never-sealed case.) Fix: pin at seal / hold a store-ref through the pin (gate release on pin-ack). Full timeline + thread analysis in §2.3.

Re-entry order when picking this back up:
1. Current design + rationale: **§7**. The single open issue + candidate fixes: **§5 item 1**. Everything before §5 is historical — see the top-of-doc banner.
2. Validate the eviction fix empirically: re-apply the redesign in `~/rayturbo`, build an image (`push_ray_image.sh`), and run `image_embedding_from_jsonl` (0.1× scale) with move semantics ON — it should now pass (it failed ~4/5 pre-fix). Then re-check `backpressure_training_prefetch` for the memory-relief benefit.
3. Add the tests in §5 item 2 (none exist yet).
4. Decide whether to close §5 item 1 (recommended: gate the producer release on an acked owner primary-moved notification).

---

## 1. What plasma move semantics is

Goal: when a raylet pushes an object to a remote raylet, the producer should be allowed to free its local primary copy immediately, instead of waiting for the owner to publish an eviction. Reduces peak object-store usage during pipelines where the producer no longer needs its own copy.

### Implementation on top of master (post-rebase)

> **Historical (pre-2026-07-12).** This describes the original `MoveCompleted`-RPC design. It has been superseded by the push-inline design in §7 — read that for current behavior. Kept here to show what the redesign replaced.

Master (as of `e002fae45f`) already does owner-driven eviction — the owner core worker sends `FreeLocalObjects` RPC directly to each raylet in `pinned_at_node_id_ ∪ locations` when the ref count hits 0 (via `CoreWorker::FreeObjectOnNodesAsync` at `core_worker.cc:4855` → `RayletClient::FreeLocalObjects` → `NodeManager::HandleFreeLocalObjects` at `node_manager.cc:3759` → `LocalObjectManager::ReleaseFreedLocalObject`). This replaces the older pubsub-eviction path. Move semantics plugs into that:

- **Config flag** `RayConfig::enable_plasma_move_semantics` (default `true`) gates the producer-side release.
- **`ObjectManager::SetOnPushComplete(fn)`** registers a callback fired once per successful `(object_id, peer_node_id)` push. Chunk-ack tracking lives in `push_ack_tracking_` (map<pair, PushAckState{total_chunks, acked_chunks, failed}>) which is populated in `PushObjectInternal` and drained in the per-chunk `on_complete` callback.
- **Producer-side wiring in NodeManager**: on push complete, if the object is not a `ray.put` (Fix B), the producer sends a `MoveCompleted` RPC to the peer with the owner's address, then calls `local_object_manager_.ReleaseFreedLocalObject(object_id)` to free its local copy. Master's `ReleaseFreedLocalObject` was purpose-built for this — it does the local unpin/spill-cleanup and enqueues the object for the next `FlushFreeObjects` batch without broadcasting anywhere.
- **Consumer-side wiring** (`on_move_completed_` in NodeManager): fetches the newly-received object from plasma, calls `LocalObjectManager::PinObjectsAndWaitForFree` to pin it (so it survives LRU), and calls `object_directory_.ReportObjectPrimaryMoved(id, self_node_id_, owner_address)` to tell the owner the primary pin has moved to this node.
- **Owner-side** (`CoreWorker::HandleUpdateObjectLocationBatch`): when it receives an `ObjectLocationUpdate` with `primary_moved_to_node_id` set, calls `ReferenceCounter::UpdateObjectPinnedAtRaylet(id, new_primary)`. This keeps `pinned_at_node_id_` in sync so lineage reconstruction fires on the *new* primary node if it later dies.
- **Reference counter DEBUG downgrade** (`reference_counter.cc:947`): the `"already has a primary location"` message is now DEBUG instead of INFO because it fires on every successful move-semantics handoff, not just during reconstruction.

### How the producer's copy actually leaves plasma (and how the owner learns)

`ReleaseFreedLocalObject` does NOT delete the object immediately — it enqueues and the delete propagates in stages:

1. **Enqueue.** `ReleaseFreedLocalObject` unpins locally (`local_objects_.erase`, `pinned_objects_.erase`) and adds the id to `objects_pending_deletion_` (`local_object_manager.cc:101`). It flushes right away only if the batch is full (`free_objects_batch_size`) or `free_objects_period_ms == 0`; otherwise it waits for the periodic tick. **So there is a bounded delay** between the producer thinking "I released" and the object actually being gone from its plasma store.
2. **Flush.** `FlushFreeObjects` (`local_object_manager.cc:136`) hands the batch to `on_objects_freed_` → `ObjectManager::FreeObjects(ids)` → `buffer_pool_.FreeObjects(ids)` → `PlasmaClient::Delete(ids)` (IPC to plasma store).
3. **Plasma-side delete + notification.** The plasma store's `ObjectLifecycleManager::DeleteObject` gates on `PLASMA_SEALED` state + `ref_count_ == 0`. Only then does `DeleteObjectInternal` run, which physically deletes the object AND fires `delete_object_callback_(object_id)` (`plasma/object_lifecycle_manager.cc:256`). If either gate fails, the id is stashed in `earger_deletion_objects_` and retried when the last reader releases.
4. **Callback hops back to raylet main.** The plasma store runs on its own thread (`ObjectStoreRunner`'s `store_thread_`), so `delete_object_callback_` posts to `main_service` (`main.cc:812-820`) before invoking `ObjectManager::HandleObjectDeleted(id)`.
5. **Owner notification.** `HandleObjectDeleted` (`object_manager.cc:207`) calls `object_directory_->ReportObjectRemoved(id, self_node_id_, object_info)`. On `OwnershipBasedObjectDirectory`, this stages an `ObjectLocationUpdate{plasma_location_update: REMOVED}` on the per-owner batch and sends it via `UpdateObjectLocationBatch` RPC.
6. **Owner-side effects.** The owner's `CoreWorker::HandleUpdateObjectLocationBatch` calls `RemoveObjectLocationOwner(id, node)`, which (a) updates the `locations` set in `ReferenceCounter`, and (b) **publishes the change to `WORKER_OBJECT_LOCATIONS_CHANNEL`** so any raylet currently pulling this object (borrowers) sees the location list shrink and re-plans its fetch.

Implication for move semantics: after `on_push_complete_` fires, the owner's view of `locations` for the moved object doesn't instantly become `{consumer}`. Instead there's a window where it's `{producer, consumer}` — until the producer's next `FlushFreeObjects` batch actually deletes the plasma entry, `HandleObjectDeleted` runs, and the REMOVED update reaches the owner. Fix A's `primary_moved_to_node_id` update (which flips `pinned_at_node_id_` immediately) travels through the *same* batched RPC, so ordering with the eventual REMOVED update depends on which one lands in the buffer first at the consumer/producer respectively.

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

### Bug C — `ObjectFetchTimedOutError` on `video_object_detection` (RESOLVED by §7 redesign — historical investigation)

> **Resolved by the §7 redesign (2026-07-12).** This is the bug that motivated the redesign, root-caused to eviction-before-pin. The investigation below is kept as history — its "Fix 1 / Fix 2" analysis and any "§5 item 1" references **predate** the redesign: Fix 2 became the §7 design, and the one race that remains open is §5 item 1 (now the batched-owner-update variant, not the versions described below).

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

**Status:** ROOT-CAUSED 2026-07-09 — hypothesis (a) confirmed post-rebase on `image_embedding_from_jsonl` (see UPDATE at end of section). Original triage notes:
- If (a): add INFO log in `on_push_complete_` success path and both branches of `GetObjectsFromPlasma` in `on_move_completed_`. Re-run; verify consumer pin actually happens.
- If (b): implement the ack-bearing `MoveCompleted` RPC described in §5 item 1.
- If (c): audit `LocalObjectManager::AddSpilledUrl` / restore path to ensure moved-pin metadata survives a round-trip.

**UPDATE 2026-07-09/10 — reproduced post-rebase on `image_embedding_from_jsonl` and CONFIRMED as (a) sealed-then-evicted via an instrumented re-run. It is the producer-releases-before-consumer-is-durably-pinned race: the sealed-but-unpinned pushed copy is LRU-evicted in the seal→pin window.**

Post the 2026-07-07 rebase, the ON build (`enable_plasma_move_semantics=true`; master + the 3 move-sem commits; image `karticam-raytr-plasma-move-sem-on`) failed the 0.1×-scaled `image_embedding_from_jsonl` benchmark **4 of 5 runs**, including a **solo run** (no concurrency, no `--chaos`, on-demand nodes → **no node death**). The OFF build (flag gated off) succeeded **5/5**. Lost object was a task return, **index 2** — same signature as the original `video_object_detection` repro.

What the failed job's per-node raylet logs (`prodjob_p3325vygp1d5kp4luxjhb5dnk5`) **do** establish:
- The consumer's `on_move_completed_` found the object **not in its plasma** (`GetObjectsFromPlasma` null, `node_manager.cc:289`) → **pin skipped** (`:291`) → and the producer had **already released** (`on_push_complete_` → `ReleaseFreedLocalObject`, fired once all chunks were acked). So the producer freed its copy while the consumer held **no pinned copy** → object exists **nowhere**.
- **Not spilled** — 0 hits in `local_object_manager` / spill+restore workers; borrowers log `pull_manager.cc:500` *"neither in memory nor external storage"*. **No node death** → no recovery → borrowers hit `ObjectFetchTimedOutError`.
- Fan-out edge also present: `node_manager.cc:273` *"no owner address on producer raylet; skipping MoveCompleted RPC"* on a 2nd push (see §5 edge cases).

**Instrumented re-run (2026-07-10) — (a) confirmed, (b) ruled out.** Added INFO logs at the plasma seal (`object_buffer_pool.cc`), plasma eviction (`object_lifecycle_manager.cc::EvictObjects`), the consumer pin path, and the chunk-reject reasons (image `karticam-raytr-master-sem-on-logs`, prod). A failed run (`prodjob_1vriq39in5repiu9qshl7mbj79`, lost object index 2) gave this timeline — **all on the consumer node `10.0.172.69`**:
- `14:07:10.058` — **sealed received object** (push fully received + sealed)
- `14:07:10.079` — **plasma evicting object** — **21 ms later**, the sealed-but-unpinned copy is LRU-evicted
- `14:07:10.277` — **consumer could not fetch … skipping pin** — **219 ms after seal**, `MoveCompleted`'s `GetObjectsFromPlasma` finds it gone

The `rejecting chunk` / `aborting object creation` counts were **0** → **not** the never-sealed case (b). So it is definitively **(a): sealed-then-evicted in the seal→pin window** — ~200 ms wide here, with eviction landing just 21 ms after seal under memory pressure — while the producer had already released on push-complete → object nowhere → `ObjectFetchTimedOutError`. (The earlier "can't distinguish (a)/(b) from INFO logs" was correct for the *un*-instrumented bundle; the instrumentation settles it.)

**Workload-dependence:** `backpressure_training_prefetch` (peak ~50 GB, no spilling, low pull-memory pressure) → move-sem ON clean (reproducible ~9% lower peak); `image_embedding` (heavy spilling / pull-budget pressure) → ON fails 4/5. Confirmed as (a) eviction under pressure (see the instrumented timeline above).

**Root cause (confirmed):** the producer releases its copy on push-*completion* (all chunks acked), but a completed push does **not** mean the object is durably **pinned** on the consumer — the sealed copy is an unpinned secondary until `on_move_completed_` pins it (~200 ms later here), and the plasma store LRU-evicts it in that window under pressure. **Fix:** pin the pushed copy at seal (before the last-chunk ack) and/or hold a store-level reference continuously from seal through the pin so it is never in the refcount-0 evictable state; equivalently, gate the producer release on a consumer pin-ack. (`HandlePush` also replies OK even for cancelled chunks, so a pin-ack is the robust signal regardless.)

**Two candidate fixes:**
1. **Ack-bearing `MoveCompleted` (§5 item 1).** Producer releases its copy only after the consumer replies that `GetObjectsFromPlasma` + `PinObjectsAndWaitForFree` succeeded; on failure (evicted / not fetchable) it keeps its copy and falls back to normal ownership. Correct and minimal, but it does **not prevent** the eviction — it makes it *harmless* by keeping the producer copy — so the move (and its memory relief) is forfeited for exactly the objects evicted under pressure, plus a round-trip of added latency before the producer can release. Still needs the fan-out no-owner-address case handled.
2. **`is_move` flag on `PushRequest` + pin-at-seal (recommended).** The producer tags the move-target push. On that push the consumer pins the object and calls `ReportObjectPrimaryMoved` (owner primary-location update) **as part of the receive path, right after seal — before acking the last chunk**. The last-chunk ack must be **gated on pin-success** (defer `send_reply_callback` until the posted pin completes), so the producer releases only once the consumer is durably pinned; on pin failure the producer keeps its copy (move falls back, no loss). Removes the separate async `MoveCompleted` RPC and its fan-out no-owner edge (only the flagged push pins as primary), and preserves the memory benefit under pressure. Costs: one proto field on `PushRequest`, plus pinning from the OM receive thread (hop to `main_service_` for `PinObjectsAndWaitForFree`). **Caveat — not window-free by default:** seal runs on `rpc_service_` but the pin hops to `main_service_`, so a *residual* seal→pin window remains where the plasma store can still evict the unpinned secondary (see thread map + analysis below). It's safe (gated ack ⇒ no loss) and much shorter than Fix 1 (no extra RPC); to *guarantee* the move (not just avoid loss), hold a store-level reference continuously from seal through the pin hand-off. **Still the stronger fix.**

**Recommendation (2026-07-10): Fix 2.** Fix 1's failure mode is anti-correlated with the feature's value — under memory pressure the copy is evicted in the window, the pin fails, and the producer *keeps* its copy, so the move (and its memory relief) is forfeited exactly when it's needed. Fix 2 keeps the move succeeding under pressure (shorter window, no extra RPC) and simplifies the design. Fix 1 is a fine correctness *stopgap* (it does stop the loss), not the destination.

**Thread map (why a residual window exists even in Fix 2):**

| Operation | Thread |
|---|---|
| `HandlePush` → `ReceiveObjectChunk` → `WriteChunk` (**seal**) | `rpc_service_` — ObjectManager's multi-threaded RPC pool (`ObjectManagerGrpcService` is registered on it, object_manager.cc:167) |
| `HandleMoveCompleted` (today) | `rpc_service_`, then `main_service_->post(...)`, and **acks immediately — before the posted work runs** |
| `on_move_completed_` → `GetObjectsFromPlasma` + `PinObjectsAndWaitForFree` + `ReportObjectPrimaryMoved` | `main_service_` (main raylet thread; LocalObjectManager is main-thread-only) |
| Spilling (`SpillObjects` / `FlushFreeObjects`) | `main_service_` (init), spill_worker procs (I/O) |
| Plasma **LRU eviction** of unpinned secondaries | plasma **store thread** (`store_thread_`), async, on any Create-under-pressure |

**Residual-race analysis for Fix 2.** Seal is on `rpc_service_` but the pin must run on `main_service_`, so the pin is necessarily `main_service_->post(...)`. Between seal (create-ref dropped ⇒ the copy is a refcount-0, unpinned *secondary*) and the posted pin, the store thread can LRU-evict the copy — independently of both threads, whenever a Create needs memory. The loss mechanism is **eviction (drop), not spilling**: spilling only applies to *primaries* the raylet owns; an unpinned secondary is simply dropped (matches the observed logs — 0 spill hits, "neither in memory nor external storage"). Consequences:
- With the ack **gated on pin-success**: evicted-in-window ⇒ pin fails ⇒ ack reports failure ⇒ producer keeps its copy ⇒ **no data loss**; the move just falls back for that object. The window is much shorter than Fix 1's (no extra `MoveCompleted` RPC between seal and pin), so far fewer fall-backs.
- To **eliminate** the window (guarantee the move, not merely avoid loss): hold a store-level reference continuously from seal until the main-thread pin takes it over — i.e., don't drop the buffer-pool create-ref until a Get/pin ref is held; defer only the LocalObjectManager bookkeeping + `ReportObjectPrimaryMoved` to `main_service_`. Then the copy is never in the refcount-0 evictable state and the store thread can't drop it regardless of timing.

Note: Fix 1 has the same *kind* of seal→pin window but larger (it inserts a full `MoveCompleted` RPC round-trip between them), and is likewise only safe because its release is gated on the consumer's pin-success reply.

---

## 3. The fixes

### Fix A — consumer→owner pin update

> **Historical (pre-2026-07-12).** Fix A introduced the producer→consumer `MoveCompleted` RPC. The redesign in §7 **removed** that RPC (the consumer now pins inline on the receive path); the `pinned_at_node_id_` update via `ReportObjectPrimaryMoved` is retained. This section documents the original mechanism for history.

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

1. **Consumer-dies-before-the-owner-learns race  [correctness] — OPEN (post-§7 redesign).** After the §7 redesign the producer releases its copy only once the consumer is durably *pinned*, but **not** once the *owner knows* the pin moved. `ReportObjectPrimaryMoved` reaches the owner via the **batched, fire-and-forget** `UpdateObjectLocationBatch` path (`ownership_object_directory.cc:202` → `SendObjectLocationUpdateBatchIfNeeded`); the owner applies it in `HandleUpdateObjectLocationBatch` → `UpdateObjectPinnedAtRaylet(consumer)` (`core_worker.cc:3924-3930`) — the only thing that moves `pinned_at_node_id_` from producer to consumer. The producer's `ReleaseFreedLocalObject` sends **no** removal report (`FreeObjects(local_only=true)`, `main.cc:884`; report methods are only Added/Spilled/PrimaryMoved).

   **The race:** consumer pins → acks producer → producer frees its copy, while the owner still has `pinned_at_node_id_ = producer` (the batched update hasn't flushed). The consumer then **dies before the batch flushes** → the owner never runs `UpdateObjectPinnedAtRaylet(consumer)`; `ResetObjectsOnRemovedNode(consumer)` (`reference_counter.cc:910`) finds nothing pinned at the consumer → no recovery; the producer is alive (no node-death trigger) but has freed the object → **permanent `ObjectFetchTimedOutError`** (a failed pull does not clear an owner location).

   This **supersedes** the pre-redesign "consumer dies *before pinning*" race: the redesign's gated ack closes that one (the producer won't release until the consumer has pinned+acked); this batched-flush variant survives because the *owner notification* is still async. *(Historically = §2.3 Bug C hypothesis (b).)*

   **Candidate fixes (none implemented):**
   1. **Gate the producer release on the owner durably recording the new primary (recommended).** After pinning, the consumer sends an *acked* primary-moved notification to the owner and only then acks the producer; the owner sets `pinned_at_node_id_` before replying (`core_worker.cc:3930` before `:3934`), so the reply is a durable ack. Needs a dedicated **synchronous** primary-moved RPC for moved objects (cleanest), or a per-object completion callback on the batch reply (today it is per-batch). Cost: one owner RTT before the producer can release. `UpdateObjectPinnedAtRaylet` already handles "target already dead" (`:960-965`: unset primary + queue recovery), so once the owner is told, a later consumer death recovers. Only option that never leaves a false-positive owner location.
   2. **Producer reports its removal to the owner.** Producer's release also sends a location-removed / primary-cleared update; even if the consumer's add is lost, the owner converges to "no primary" → reconstructs (move-eligible objects are reconstructable; puts are excluded by Fix B). Weaker: eventual-consistency, and can momentarily hit "no locations" out of order (spurious reconstruction) if the removed-update races ahead of the consumer's add.
   3. **Producer keeps a fallback until the owner confirms.** Producer retains its copy until it observes the owner's `pinned_at_node_id_` flip (poll/subscribe), then frees. More moving parts than (1).

2. **Tests  [correctness — coverage].** Nothing added yet. Minimum set (updated for the §7 design):
   - Unit: `ObjectID::IsForPut` across boundary indices.
   - Unit: `ObjectBufferPool::WriteChunk(defer_release=true)` seals without releasing (object stays refcount>0) + `ReleaseObject` drops it; and the `defer_release=false` path is unchanged.
   - Unit: `HandlePush` for a sealed move chunk — reply is deferred until the posted pin runs; not-OK ack on pin failure so the producer keeps its copy.
   - Unit: `OwnershipBasedObjectDirectory::ReportObjectPrimaryMoved` — confirms staging + batched send.
   - Unit: `CoreWorker::HandleUpdateObjectLocationBatch` with `primary_moved_to_node_id`, including the `is_node_dead_` branch.
   - Integration: push (`is_move`) → consumer pins inline → owner `pinned_at_node_id_` updates → kill consumer → verify recovery fires.
   - Race test for item 1 (kill the consumer before its batched primary-moved update flushes).

3. **Edge cases to confirm  [correctness — verification]:**
   - Fan-out (producer pushes O to C1 and C2, both with `is_move`). Each consumer pins as primary and reports the move to the owner (last write wins on `pinned_at_node_id_`); the producer frees after each push's ack. The old producer-side `GetOwnerAddress`/no-owner-address edge is gone (the owner address rides the push), but marking **more than one** destination as the move is still semantically muddy (two "primaries"). Ideally the producer should set `is_move` on only one destination; warrants a test either way.
   - Push fails partway: verify `on_push_complete_` doesn't fire (producer keeps its copy) on failed pushes, including the not-OK pin-failure ack (`push_ack_tracking_` failure path).
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

7. **Code-level design comment  [hygiene].** This `.md` file may not survive long-term. Add a short comment block (4–8 lines) at the move-semantics setup in `node_manager.cc` explaining the current handshake (producer stamps `is_move` → consumer pins inline before acking → producer frees on the gated ack), and a `// see also` at `ObjectID::IsForPut`.

8. **Cross-region / high-latency note  [docs].** The wider the consumer's batch-flush delay, the wider the item-1 window (the owner learns the new primary later). And if fix 1 (synchronous owner ack) is adopted, that owner round-trip lands on the move critical path — worth a sentence on the cross-region cost once item 1 is fixed.

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

---

## 7. Redesign 2026-07-12 — remove `MoveCompleted`, pin inline in the push path

Supersedes the Fix A `MoveCompleted` handshake (§3 Fix A) and makes §2.3's "Fix 2" the primary design. Two things motivated it: (1) the confirmed eviction-before-pin bug (§2.3 Bug C), and (2) the realization that the producer→consumer `MoveCompleted` RPC carries nothing the consumer doesn't already have.

### 7.1 What changes

Old (Fix A) flow: producer `on_push_complete_` → `NotifyMoveCompleted` RPC → consumer `on_move_completed_` does Get + pin + `ReportObjectPrimaryMoved`, while the producer released its copy **immediately** after sending the RPC (before the consumer pinned).

New flow — the consumer pins inline on the receive path, and the producer's release is gated on the consumer's ack:

```
producer raylet                     consumer raylet (HandlePush, is_move set)          owner
────────────                        ────────────────────────────────────────          ─────
PushObjectInternal(is_move) ─chunks─► ReceiveObjectChunk / WriteChunk
                                        seal (last chunk) — DEFER buffer-pool release
                                        DEFER send_reply_callback for this chunk
                                        post → main_service_:
                                          GetObjectsFromPlasma            (ref++)  ← pin's ref
                                          PinObjectsAndWaitForFree                 ← durable pin
                                          ReportObjectPrimaryMoved ───────────────────────► (batched)
                                          release buffer-pool create-ref  (ref-- ; still ≥1)
                                          send_reply_callback(OK)   ◄── the deferred ack
push_ack_tracking_ all acked
  ↓
on_push_complete_: if (is_move) ReleaseFreedLocalObject   ← now safe: consumer is pinned
(NotifyMoveCompleted DELETED)
```

Concretely:
- **`is_move` flag on `PushRequest`** (new proto field). The producer sets it when `enable_plasma_move_semantics && !IsForPut(object_id)` for the move-target push, and stores it in `PushAckState` so `on_push_complete_` releases using the *same* flag. The `IsForPut` decision moves off the producer's `on_push_complete_` onto the push-initiation decision.
- **Consumer pins inline, before acking.** On the sealing chunk of a move push: seal, then (on `main_service_`) Get + `PinObjectsAndWaitForFree` + `ReportObjectPrimaryMoved`, then release the buffer-pool create-ref, then send the deferred chunk reply. The ordering `Get(ref++) → … → buffer-pool release(ref--)` keeps refcount ≥1 the whole time.
- **Producer release gated on the ack.** `on_push_complete_` already fires only when every chunk is acked (`push_ack_tracking_`). Holding the sealing chunk's ack until the pin completes means `on_push_complete_` → `ReleaseFreedLocalObject` runs only after the consumer is durably pinned. On pin failure the consumer replies not-OK → `push_ack_tracking_` marks the push failed → producer **keeps** its copy (fallback, no loss).
- **DELETED:** `NotifyMoveCompleted`, `HandleMoveCompleted`, `on_move_completed_` / `SetOnMoveCompleted`, and the `MoveCompleted` RPC + `MoveCompletedRequest/Reply` proto.

### 7.2 Why it works (and why the RPC was redundant)

- **The consumer already has the owner address.** `PushRequest.owner_address` (field 4) is set by the producer from the object's real owner (`object_manager.cc:414-421` → `:576`) and read on the consumer in `HandlePush` (`:627`) → `ReceiveObjectChunk`. That owner address was the *only* substantive payload of the `MoveCompleted` RPC — so the consumer can pin + `ReportObjectPrimaryMoved` with no extra RPC.
- **The producer already waits for all chunk acks.** `push_ack_tracking_` fires `on_push_complete_` only when `acked_chunks == total_chunks` (`object_manager.cc:544-550`). Deferring the sealing chunk's ack until the pin completes turns "push complete" into "consumer durably pinned" for free — the gating machinery already exists.
- **Pin-before-release closes the eviction window structurally.** Refcount on the consumer: `create=2 → seal internal release=1 → pin Get=2 → buffer-pool release=1` — never 0, so the store thread's LRU can never select it (eviction is driven purely by store refcount==0; a "pin" is just RAII holding of the plasma buffer via `pinned_objects_`, keeping the store ref alive — there is no separate is-pinned flag the eviction policy consults). This *eliminates* §2.3 Bug C's seal→pin window rather than merely making the loss harmless.

Bonus: removes Fix A's fan-out "no owner address on producer" edge — the owner address rides the push, so there is no post-hoc `GetOwnerAddress` that can return `nullopt` on a 2nd push.

### 7.3 The bug that motivated this — eviction before pin (recap of §2.3 Bug C)

The producer released its copy on push-*completion*, but a completed push does not mean the consumer's copy is *pinned*: the sealed copy is an unpinned, refcount-0 secondary until the (old) `MoveCompleted`-driven pin ran ~200 ms later. Under memory pressure the plasma store LRU-evicted it ~21 ms after seal (instrumented timeline, §2.3). Producer had already released → object nowhere → `ObjectFetchTimedOutError`. The redesign's pin-before-release fixes exactly this.

### 7.4 Known open issue

The redesign closes the eviction-before-pin window, but it does **not** close one correctness gap: if the consumer dies *after* pinning but *before* its **batched** `ReportObjectPrimaryMoved` reaches the owner, the owner's `pinned_at_node_id_` stays on the freed-but-alive producer, so lineage reconstruction never fires → permanent `ObjectFetchTimedOutError`. The producer now releases only once the consumer is *pinned*, but not once the owner *knows*.

Full description, why it's permanent, and the candidate fixes live in the single "what's left" list: **§5 item 1**. (It supersedes the pre-redesign "consumer dies before pinning" race, which the gated ack closes.)

### 7.5 Crash found in workload testing — `AbortCreate` on a sealed deferred-release object (FIXED 2026-07-14)

Workload-testing the redesign with move semantics ON crash-looped raylets on a fatal `RAY_CHECK` (fired ~70×/run, **only** with move semantics on — the OFF baseline was clean):

```
NodeManager::HandleObjectLocal → CancelPull → ObjectBufferPool::AbortCreate
  → AbortCreateInternal → plasma::PlasmaClient::Abort()
      Check failed: object_entry != objects_in_use_.end()
      "Plasma client called abort on an object without a reference to it"  (client.cc:606)
```

**Root cause — the deferred release leaves a *sealed* object in `create_buffer_state_`, a state `AbortCreate` was never written to handle.** The plasma client ref-counts each object (`objects_in_use_[id].count`): `Create` → count 2 (an extra +1 held until seal, `client.cc:193`), `Seal` internally calls `Release` → count 1 (`client.cc:598`). The **non-move** `WriteChunk` then immediately `Release`s (1→0, dropping it from `objects_in_use_`) **and erases** the `create_buffer_state_` entry — all inside one `pool_mutex_` critical section, across the `Seal` call. The **move** path (`defer_release=true`) seals but keeps both the count-1 client reference and the `create_buffer_state_` entry; that held reference is exactly what keeps the object refcount>0 through the seal→pin window (§7.2).

`AbortCreateInternal` does `Release` then `Abort`, which is correct only for an *unsealed, count-2* in-progress create (`Release` 2→1 leaves it present, `Abort` discards it). On a *sealed, count-1* deferred entry it does `Release` 1→0 — removing it from `objects_in_use_` — then `Abort` finds nothing → fatal check. (`Abort` on a sealed object is illegal regardless; the following `RAY_CHECK(!is_sealed)` would also fire.)

**Why the non-move path never hit this:** `Seal`+`Release`+`erase` all run inside one `pool_mutex_` critical section, and `AbortCreate` also needs `pool_mutex_`, so any `AbortCreate` the seal triggers is forced to wait until the entry is already erased → it finds nothing → no-op. The move path breaks that atomicity: it holds `pool_mutex_` only across `Seal` and defers the release+erase to a *separate* `main_service_` task (`ReleaseObject`, posted from `HandlePush`). `AbortCreate` — also posted to `main_service_`, by the seal's "object added" notification (`main.cc:806`) — then races `ReleaseObject` for the lock. `AbortCreate`'s post is enqueued *during* `Seal` (before `HandlePush` posts the release), so on the single-threaded `main_service_` it runs first ~every time → it aborts the sealed entry.

**Trigger paths.** `AbortCreate` runs whenever any pull for the object is cancelled after it becomes local, all funneling through `ObjectManager::CancelPull → PullManager::CancelPull → cancel_pull_request_ → AbortCreate`:
- the **wait** pull — `HandleObjectLocal → CancelPull` (Ray Data's streaming executor calls `ray.wait` on block refs, so every block has one; this is the stack seen in the crashing Ray Data run),
- the **task-argument** pull — `LeaseDependencyManager::RemoveLeaseDependencies`,
- the **`ray.get`** pull — `CancelGetRequest`.

Because they share one chokepoint, the fix lives there rather than in `HandleObjectLocal`.

**Threads (validated empirically).** `HandlePush`/`WriteChunk`/`Seal` run on the object-manager `rpc_service_` pool; `HandleObjectLocal`, `AbortCreate`, and the posted `ReleaseObject` all run on the single `main_service_` (raylet main) thread. A local 2-node `cluster_utils` cluster with per-function `std::this_thread::get_id()` logs confirmed `HandlePush` on `rpc_service_` pool threads and `HandleObjectLocal == AbortCreate` on one `main_service_` thread, disjoint from `HandlePush`.

**The fix (`ObjectBufferPool::AbortCreateInternal`):** if the found entry is already sealed (`num_seals_remaining_ == 0` — which, since the non-move path erases at seal, uniquely identifies a deferred-release move object), **skip the `Release`+`Abort` and return**, leaving the entry for `ReleaseObject` to drop after the pin. Chosen as a *no-op* rather than "release + erase here" deliberately: releasing the create-reference at abort time would drop the refcount to 0 before the pin's `Get` reference is held, reopening the exact seal→pin eviction window §7 exists to close. Correct regardless of which of `AbortCreate` / `ReleaseObject` wins the race.

**Verification.** With the fix, `heterogeneous_memory_batch_inference` (actor-only) ran crash-free across n=6 move-sem-ON runs, plus a fan-out microbenchmark (14 ON runs); zero recurrences of the check. Unit/regression test still to add (§5 item 2): `AbortCreate` on a sealed deferred-release entry must be a no-op (must not call `Abort`).

---

## 8. Ray Data backpressure integration — "producer physically freed" callback

**Status: IMPLEMENTED (2026-07-13), committed in both repos.** Core in `~/ray` (commit `aa6ec723d0`, "Add function using which ray data can register callback to be fired when object is freed from producer", + the callback-lifetime fix in §8.6). Ray Data wiring in `~/rayturbo` (`resource_bank.py` steps 8–9 + the same core fix). Compile-verified; not yet workload-tested (see §8.5 / §8.7).

### 8.1 Goal & the problem being fixed

Move semantics frees the producer's copy the moment the object is handed off, so the producer's output-buffer slot opens up *earlier* than under the copy-based baseline (which holds the producer copy until the object is fully consumed downstream, refcount→0). We want Ray Data to admit the next producer task as soon as that slot is genuinely free — **but not before the bytes are physically gone**, or peak memory regresses.

Justin's `RAY_DATA_MOVE_SEMANTIC` flag (rayturbo `resource_bank.py`) forces `is_cross_node_transfer()→False`, an optimistic **submit-time** simulation: it relaxes the producer's accounted footprint when the task is *dispatched*, before the move has actually happened. Result: the producer admits new work while its copy is still resident and the move is in flight → **peak memory higher than baseline**. We replace that simulation with a signal tied to the *real* physical free.

### 8.2 The trigger: producer physical free (M2), not logical free (M1)

Two producer-side moments (see §1 "How the producer's copy actually leaves plasma"):
- **M1 (logical):** `ReleaseFreedLocalObject` runs in `on_push_complete_` — marks freed, unpins, enqueues for deletion. **Bytes still resident.**
- **M2 (physical):** flush → plasma `Delete` → `HandleObjectDeleted`/`HandleObjectMissing`. **Bytes reclaimed.** ← fire here.

We fire on **M2**. Firing on M1 (or earlier, like Justin's submit-time) lets the producer's not-yet-reclaimed bytes overlap the next task's output.

### 8.3 End-to-end flow (new pieces marked ★)

```
PRODUCER raylet (ObjectManager)   OWNER core worker (= Ray Data driver)        RAY DATA (rayturbo)
───────────────────────────────   ─────────────────────────────────────       ───────────────────
on_push_complete_(obj, node, is_move)                            ── on_block_produced() earlier had:
  │  if is_move:                                                    ★ add_object_freed_on_producer_
  ├─ ReleaseFreedLocalObject(obj)   (mark freed, unpin, enqueue)       callback(block_ref, cb)
  └─ ★ move_freed_object_ids_.insert(obj)                              registered on the driver
  ▼
FlushFreeObjects → plasma Delete
  │
  ▼  M2: plasma physically deletes → delete_object_callback (main.cc:812)
HandleObjectDeleted(obj)          ← the ONLY hook we touch
  │                                 (HandleObjectMissing also fires here — untouched)
  └─ ReportObjectRemoved(obj, self_node, object_info,
     │                   ★ freed_by_move = move_freed_object_ids_.erase(obj) > 0)
     │      stages ONE ObjectLocationUpdate:
     │        plasma_location_update      = REMOVED           (existing)
     │        ★ freed_on_producer_node_id = self_node         (new — set iff freed_by_move)
     └────────── batched UpdateObjectLocationBatch RPC ──────────►
                                                 │
                                                 ▼
                                    HandleUpdateObjectLocationBatch
                                    ★ if has_freed_on_producer_node_id():
                                        reference_counter_->OnObjectFreedOnProducer(obj)
                                                 │  fire + clear per-object cbs (fire-once),
                                                 │  posted on object_freed_callback_service_
                                                 ▼
                                    ★ _invoke_object_freed_on_producer_callback(id_bytes)
                                                 └──────────────────────────►  cb(id_bytes):
                                                                                 ★ _consumed_refs
                                                                                    .append(id.hex())
                                                                                      │
                                                                    (executor thread) ▼
                                                                    drain_consumed_blocks →
                                                                    stats.on_block_consumed →
                                                                    producer output-byte budget --
                                                                                      │
                                                                                      ▼
                                                                    PER_ACTOR_OUTPUT_BYTES budget
                                                                    frees → pool admits next task
                                                                    to this producer actor
```

Note: at M2 the delete callback (`main.cc:812`) fires **both** `HandleObjectDeleted` (ObjectManager) and `HandleObjectMissing` (NodeManager) — that's existing Ray code. We only touch `HandleObjectDeleted`, which *already* sends the `REMOVED` update; we just set one extra flag on it. `is_move` is known on the producer via `push_ack_tracking_`, so ObjectManager records the move-freed ids itself — no NodeManager map, no separate RPC, no owner-address plumbing (`ReportObjectRemoved` already resolves the owner from `object_info`).

Contrast of *when the producer's output-byte budget is released*:

```
Justin's flag (simulate):   at task SUBMIT          ── too early → peak memory ↑
baseline out-of-scope cb:   at refcount→0 (fully consumed downstream) ── too late → lose the win
★ this plan:                at M2 (producer PHYSICAL free)            ── correct
```

### 8.4 What was implemented

**Core (`~/ray`, and ported into `~/rayturbo`)** — a new per-object callback mirroring `add_object_out_of_scope_callback`. The producer→owner signal rides the **existing** REMOVED update (one hook, one flag), not a new RPC:
1. `core_worker.proto`: `optional bytes freed_on_producer_node_id = 6` on `ObjectLocationUpdate` (sibling of `primary_moved_to_node_id`).
2. `object_manager.{h,cc}`: `move_freed_object_ids_` set on ObjectManager; insert in the `on_push_complete_` firing path when `is_move` (the flag is already in `push_ack_tracking_`). In `HandleObjectDeleted`, `move_freed_object_ids_.erase(obj) > 0` gives `freed_by_move`, passed to `ReportObjectRemoved`. Only the move path populates the set → no false positives from evictions / owner-driven final frees. `HandleObjectMissing` is **not** touched.
3. `object_directory.h` + `ownership_object_directory.{h,cc}` + mock: add `bool freed_by_move` param to `ReportObjectRemoved`; when true it also sets `freed_on_producer_node_id = node_id` on the same staged `ObjectLocationUpdate` it already builds for REMOVED.
4. `core_worker.cc` `HandleUpdateObjectLocationBatch`: on `has_freed_on_producer_node_id()` → `reference_counter_->OnObjectFreedOnProducer(obj)`.
5. `reference_counter.{h,cc}`: `on_object_freed_on_producer_callbacks` + `freed_on_producer` bool on `Reference`; `AddObjectFreedOnProducerCallback` (returns false if unknown or already fired) + `OnObjectFreedOnProducer` (fire-once). **See §8.6 for the callback-lifetime fix — this is where a leak would live.**
6. `core_worker.{h,cc}`: wrapper posting to the existing `object_freed_callback_service_`.
7. `libcoreworker.pxd` + `_raylet.pyx`: `add_object_freed_on_producer_callback(object_ref, callback)` (+ GIL/Py_INCREF wrapper).

**Ray Data (`~/rayturbo`, `resource_bank.py`)**:
8. `on_new_output`: when `RAY_DATA_MOVE_SEMANTIC` is on, `_register_freed_on_producer_callback(ref)` registers the core callback; `cb(id_bytes)` appends `id_bytes.hex()` to `_consumed_refs` (the buffer `on_block_consumed` already drains). Idempotent vs. the later downstream `on_block_consumed` (unknown-ref skip in `drain_consumed_blocks`); thread-safe (that path is built for off-executor-thread calls); catches `ValueError` for not-owned blocks (materialized-dataset `InputDataBuffer`) → falls back to normal consumption-driven release.
9. Removed Justin's `is_cross_node_transfer→False` simulation. `RAY_DATA_MOVE_SEMANTIC` is now **repurposed** to gate the real M2 callback path.

### 8.5 Open question to validate (during workload test)

Popping the ref from `live_block_refs` at M2 means the object's existence on the *consumer* node isn't counted in the M2→downstream-consume window. Confirm that's either correct (the consumer op accounts it as its own input) or display-only — not a second admission gate that would then under-count.

### 8.6 Callback-lifetime fix (leak found in review)

The pyx `add_object_freed_on_producer_callback` does `Py_INCREF(callback)`; the balancing `Py_DECREF` runs only in `_invoke_object_freed_on_producer_callback` (i.e. **when the callback fires**) or the `!registered` early-out. But the callback fires only via `OnObjectFreedOnProducer`, i.e. **only when the object is actually moved**. Step 8 registers on *every* output block, so blocks that register but never move (local consumption, `ray.put`, final-stage outputs, move-disabled) would destroy the callback vector on `Reference` erase without ever invoking it → the `Py_INCREF` is never balanced → **leaked Python closures on the driver** (potentially millions in a large Data job). (`add_object_out_of_scope_callback` doesn't have this because out-of-scope always eventually fires; ours doesn't.)

**Fix** (`ReferenceCounter::OnObjectOutOfScopeOrFreed`, both repos): after firing the out-of-scope callbacks, also fire + clear `on_object_freed_on_producer_callbacks`, guarded by `freed_on_producer`. Guarantees exactly-once firing (whichever of `OnObjectFreedOnProducer` / `OnObjectOutOfScopeOrFreed` runs first fires + clears; the other finds an empty vector), so `Py_DECREF` always runs. Semantically sound (object fully out of scope ⇒ producer copy is gone too), idempotent for Ray Data (extra `_consumed_refs` append deduped in `drain_consumed_blocks`), and closes the register-after-out-of-scope race (a late `AddObjectFreedOnProducerCallback` sees `freed_on_producer == true` → returns false → pyx `Py_DECREF`s).

### 8.7 Still to do

- Workload test: run `image_embedding_from_jsonl` / `video_object_detection` (move semantics ON) and confirm peak object-store memory ≤ baseline and no regression vs. Justin's flag; validate §8.5.
- Unit tests: `add_object_freed_on_producer_callback` fires on move, fires exactly once, and fires (for cleanup) on out-of-scope for a never-moved object (leak regression test).
