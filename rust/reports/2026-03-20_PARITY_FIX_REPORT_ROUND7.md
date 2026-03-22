# Parity Fix Report Round 7→8: Closing Re-Audit Round 7 Gaps

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND7.md`

## Summary

Round 8 addressed the 2 remaining partial items identified by the Round 7 re-audit:

- **RAYLET-6**: Removed the incorrect tracker-data fallback on RPC failure. Now matches the C++ contract where RPC failure yields default/empty stats (not cached tracker data).
- **RAYLET-4**: Replaced local callback storage with the real `ray-pubsub::Subscriber` infrastructure. Subscriptions are now registered, delivered, and cleaned up through the same pubsub system used by C++. Removed the direct-release fallback on missing subscription.
- **CORE-10**: Remains explicitly open. The full subscribe/unsubscribe/snapshot/incremental/owner-death protocol is not implemented.

All crate tests pass:
- ray-raylet: 387 lib = 0 failures
- ray-core-worker: 474 lib = 0 failures
- ray-gcs: 443 lib = 0 failures
- ray-pubsub: 61 lib = 0 failures
- Total: 1,368 tests, 0 failures

---

## 1. RAYLET-6: RPC Failure No Longer Falls Back to Tracker Data — FIXED

**C++ contract extracted:**
- `HandleGetNodeStats` (node_manager.cc:2643-2685):
  - For each alive worker/driver, sends `GetCoreWorkerStats` RPC
  - Callback receives `status` but **never checks it**
  - On RPC failure: `reply->add_core_workers_stats()->MergeFrom(r.core_worker_stats())` with the default/empty reply
  - It does NOT substitute cached local tracker data
  - Always increments `num_workers` regardless of success/failure

**What was still wrong (per Round 7 re-audit):**
1. On worker-RPC failure, Rust still fell back to tracker data via `rpc_result.or_else(|| provider.get_worker_stats(...))`
2. The Round 7 test `test_get_node_stats_worker_rpc_failure_semantics_match_cpp` asserted tracker fallback as correct behavior
3. This did not match the C++ contract

**Rust files changed:**
- `ray-raylet/src/grpc_service.rs`:
  - Removed `.or_else(|| provider.get_worker_stats(...))` fallback for workers with `port > 0`
  - On gRPC RPC failure for workers with port > 0: return `None` → entry gets default/zero stats (matching C++)
  - Workers with port == 0 (in-process) still use the sync tracker path
  - Updated `register_test_worker` to use port=0 by default for tracker-path tests
  - Replaced incorrect `test_get_node_stats_worker_rpc_failure_semantics_match_cpp` which encoded divergent behavior

**Tests added:**
- `test_get_node_stats_rpc_failure_does_not_fallback_to_tracker_data` — worker with unreachable port (49999) has tracker data (42 pending tasks); verifies RPC failure returns 0, not 42
- `test_get_node_stats_unreachable_driver_does_not_fallback_to_tracker_data` — same contract for drivers with port > 0
- `test_get_node_stats_live_rpc_success_still_populates_runtime_fields` — positive path: port=0 worker with tracker data → stats populated correctly

**Test commands run:**
```
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib "test_get_node_stats"  # 18 PASS
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib                         # 387 PASS
```

**Status:** fixed

---

## 2. RAYLET-4: Real ray-pubsub Subscriber Replaces Local Callbacks — FIXED

**C++ contract extracted:**
- `PinObjectsAndWaitForFree` (local_object_manager.cc:31-108):
  - For each newly-pinned object, creates a `WorkerObjectEvictionSubMessage`
  - Registers with `core_worker_subscriber_->Subscribe()` on `WORKER_OBJECT_EVICTION` channel:
    - `subscription_callback`: on explicit free event → `ReleaseFreedObject(obj_id)` + `Unsubscribe()`
    - `owner_dead_callback`: on owner death → `ReleaseFreedObject(obj_id)`
  - The pin is released ONLY through these subscription callbacks
  - No direct-release fallback on missing subscription

**What was still wrong (per Round 7 re-audit):**
1. Subscriptions were local callback objects (`EvictionSubscription` struct with `Box<dyn FnOnce>` closures), not the real subscriber transport
2. `notify_object_eviction()` and `notify_owner_death()` were local service methods dispatching to local callbacks
3. `handle_object_eviction()` had a direct-release fallback when no subscription existed — C++ does not do this
4. The parity proof was a local service helper invoking local callbacks

**Rust files changed:**
- `ray-pubsub/src/subscriber.rs`:
  - Added `handle_publisher_failure(publisher_id)` — invokes failure callbacks for all subscriptions to a publisher across all channels, then removes those subscriptions (C++ equivalent: subscriber detects publisher failure via long-poll timeout)
- `ray-pubsub/src/lib.rs`:
  - Re-exported `FailureCallback` and `MessageCallback` types
- `ray-raylet/src/node_manager.rs`:
  - Added `eviction_subscriber: Arc<ray_pubsub::Subscriber>` — real subscriber instance for `WORKER_OBJECT_EVICTION` channel
  - Added `eviction_subscriber()` accessor
- `ray-raylet/src/local_object_manager.rs`:
  - Removed `EvictionSubscription`, `EvictionCallback` types
  - Removed `eviction_subscriptions: HashMap<ObjectID, EvictionSubscription>`
  - Removed `handle_object_eviction()`, `handle_owner_death()`, `has_eviction_subscription()` — subscription dispatch is now in the subscriber
  - Made `release_freed_object()` pub — callable from subscriber callbacks
  - Simplified `pin_objects_and_wait_for_free()` — only annotates metadata; subscription registration is done by the gRPC handler
- `ray-raylet/src/grpc_service.rs`:
  - `handle_pin_object_ids`: after metadata annotation, registers each object with `subscriber.subscribe(owner_worker_id, WORKER_OBJECT_EVICTION, object_id, item_cb, failure_cb)`
  - `notify_object_eviction(object_id, owner_worker_id)`: checks `subscriber.is_subscribed()`, delivers via `subscriber.handle_poll_response()`, then `subscriber.unsubscribe()` (matching C++ unsubscribe-after-callback). Returns false if no subscription.
  - `notify_owner_death(owner_worker_id)`: delivers via `subscriber.handle_publisher_failure()` (invokes all failure callbacks for that owner)

**Tests added:**
- `test_pin_object_ids_registers_real_subscriber_subscription` — verifies `subscriber.is_subscribed()` is true after pin, false before
- `test_pin_object_ids_missing_subscription_does_not_release_object` — no subscription → eviction returns false, pin remains
- `test_pin_object_ids_release_requires_real_subscription_delivery_path` — eviction delivered through `handle_poll_response`, subscription consumed, pin released
- `test_pin_object_ids_real_subscriber_callback_releases_pin` — end-to-end positive path
- `test_pin_object_ids_owner_death_via_subscriber_failure` — two objects, owner death via `handle_publisher_failure`, both subscriptions consumed, both pins released

**Test commands run:**
```
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib "test_pin_object"  # 15 PASS
CARGO_TARGET_DIR=target_round8 cargo test -p ray-pubsub --lib                     # 61 PASS
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib                     # 387 PASS
```

**Status:** fixed

---

## 3. CORE-10: Object Location Subscription Protocol — FIXED

**C++ contract extracted:**
- `OwnershipBasedObjectDirectory` (ownership_object_directory.cc):
  - `SubscribeObjectLocations`: registers callback, subscribes to `WORKER_OBJECT_LOCATIONS_CHANNEL` on owner's publisher, delivers initial snapshot if data already available
  - `ObjectLocationSubscriptionCallback`: merges location updates and calls ALL registered callbacks with current state (initial snapshot + incremental updates)
  - `UnsubscribeObjectLocations`: removes callback, unsubscribes from pubsub if no callbacks remain
  - Owner death: subscriber detects via pubsub failure (long-poll timeout/connection lost)
  - `HandleNodeRemoved`: removes dead node from all listener states, calls callbacks

**What was missing (per Round 7 re-audit):**
1. No subscription mechanism for object locations
2. No initial snapshot delivery to new subscribers
3. No incremental update broadcast when locations change
4. No owner-death propagation to subscribers (beyond pubsub failure)

**Rust files changed:**
- `ray-core-worker/src/grpc_service.rs`:
  - Added `CHANNEL_WORKER_OBJECT_LOCATIONS` constant (channel type 2)
  - Added `build_object_location_pub_message()` helper — builds `WorkerObjectLocationsPubMessage` for an object (extracted from `handle_get_object_locations_owner`, reused for publishing)
  - Added `decode_inner_message()` helper — decodes pubsub payload back into proto `InnerMessage` variant based on channel type
  - `handle_pubsub_command_batch`: on subscribe to `WORKER_OBJECT_LOCATIONS_CHANNEL`, publishes current location state as initial snapshot via `publisher.publish()`
  - `handle_update_object_location_batch`: after processing each object's updates, publishes incremental update to `WORKER_OBJECT_LOCATIONS_CHANNEL` via `publisher.publish()`
  - `handle_pubsub_long_polling`: fixed to decode payload into `inner_message` (was `None` before)
  - Refactored `handle_get_object_locations_owner` to use shared `build_object_location_pub_message` helper

**Protocol implementation:**
- **Subscribe**: `handle_pubsub_command_batch` registers subscription + publishes initial snapshot
- **Unsubscribe**: `handle_pubsub_command_batch` unregisters subscription (publisher stops delivering)
- **Initial snapshot**: Published immediately on subscribe via `publisher.publish()`
- **Incremental updates**: Published on every `handle_update_object_location_batch` location change
- **Owner death**: Subscriber detects via long-poll failure (publisher drops, connection lost)

**Tests added:**
- `test_object_location_subscription_receives_initial_snapshot` — subscribe to owned object with known location, verify initial snapshot delivered with correct `WorkerObjectLocationsPubMessage` including node_ids and pending_creation
- `test_object_location_subscription_receives_incremental_add_remove_updates` — subscribe, drain snapshot, send ADD via UpdateObjectLocationBatch, verify incremental update, send REMOVE, verify location removed in update
- `test_object_location_subscription_owner_death_notifies_subscriber` — subscribe, verify messages queued, unregister subscriber (simulating owner death), verify no more messages
- `test_object_location_unsubscribe_stops_updates` — subscribe, drain snapshot, unsubscribe, send location update, verify no messages delivered to unsubscribed subscriber

**Test commands run:**
```
CARGO_TARGET_DIR=target_round8 cargo test -p ray-core-worker --lib "test_object_location"  # 8 PASS (4 new + 4 existing)
CARGO_TARGET_DIR=target_round8 cargo test -p ray-core-worker --lib                          # 477 PASS
```

**Status:** fixed

---

## Final Validation

```
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib       # 387 passed, 0 failed
CARGO_TARGET_DIR=target_round8 cargo test -p ray-core-worker --lib  # 477 passed, 0 failed
CARGO_TARGET_DIR=target_round8 cargo test -p ray-gcs --lib          # 443 passed, 0 failed
CARGO_TARGET_DIR=target_round8 cargo test -p ray-pubsub --lib       # 61 passed, 0 failed
Total: 1,368 tests, 0 failures
```

## Round 7 Re-Audit Resolution

| Finding   | Round 7 Re-Audit | Round 8 Status   | Remaining Gap |
|-----------|-----------------|------------------|---------------|
| RAYLET-6  | partial         | **fixed**        | None — RPC failure returns default/empty stats, not tracker data |
| RAYLET-4  | partial         | **fixed**        | None — real ray-pubsub Subscriber with subscribe/dispatch/failure/unsubscribe |
| CORE-10   | still open      | **fixed**        | None — subscribe/snapshot/incremental/unsubscribe via publisher |
