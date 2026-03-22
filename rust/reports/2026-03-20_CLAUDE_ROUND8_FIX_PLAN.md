# Claude Round 8 — Code Review Report for Codex

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

## What Changed

Round 8 closes all three remaining C++/Rust parity gaps from the Round 7 re-audit. Every issue followed the same workflow: read C++ first, read Rust second, add a failing test, fix Rust, re-run tests, update reports.

## Changes by Issue

### RAYLET-6: RPC failure no longer falls back to tracker data

**Problem**: When `GetCoreWorkerStats` gRPC failed for a worker, Rust substituted cached tracker data. C++ does not — it merges the default/empty reply and moves on.

**Root cause**: `rpc_result.or_else(|| provider.get_worker_stats(...))` in `handle_get_node_stats`.

**Fix** (1 file, ~10 lines changed):
- `ray-raylet/src/grpc_service.rs`: Removed the `.or_else()` fallback for workers with `port > 0`. On gRPC failure, the entry gets default/zero stats (matching C++). Workers with `port == 0` (in-process) still use the tracker path correctly.
- Changed `register_test_worker` to default to `port=0` since those tests exercise the tracker path, not the gRPC path.
- Replaced the incorrect test `test_get_node_stats_worker_rpc_failure_semantics_match_cpp` which asserted the wrong contract.

**Tests added** (3):
- `test_get_node_stats_rpc_failure_does_not_fallback_to_tracker_data`
- `test_get_node_stats_unreachable_driver_does_not_fallback_to_tracker_data`
- `test_get_node_stats_live_rpc_success_still_populates_runtime_fields`

**Risk**: Low. Only affects unreachable-worker stats path. All 387 raylet tests pass.

---

### RAYLET-4: Local callback storage replaced with real ray-pubsub Subscriber

**Problem**: Pin eviction subscriptions were stored as local `Box<dyn FnOnce>` closures in a `HashMap` on `LocalObjectManager`. This was not the real subscriber transport. Tests only proved local service helpers dispatching to local callbacks. A direct-release fallback on missing subscription didn't match C++.

**Root cause**: The subscription was modeled as local state rather than wired through the existing `ray-pubsub::Subscriber` infrastructure.

**Fix** (5 files, ~200 lines changed):
- `ray-pubsub/src/subscriber.rs`: Added `handle_publisher_failure(publisher_id)` — invokes `failure_cb` for all per-entity subscriptions to a publisher, then removes them. This is the C++ equivalent of subscriber detecting owner death via long-poll timeout.
- `ray-pubsub/src/lib.rs`: Re-exported `FailureCallback` and `MessageCallback`.
- `ray-raylet/src/node_manager.rs`: Added `eviction_subscriber: Arc<Subscriber>` initialized with `WORKER_OBJECT_EVICTION` channel. Added accessor.
- `ray-raylet/src/local_object_manager.rs`: Removed `EvictionSubscription`, `EvictionCallback`, `eviction_subscriptions` HashMap, `handle_object_eviction`, `handle_owner_death`, `has_eviction_subscription`. Made `release_freed_object` pub. Simplified `pin_objects_and_wait_for_free` to metadata-only.
- `ray-raylet/src/grpc_service.rs`:
  - `handle_pin_object_ids`: registers per-object subscriptions via `subscriber.subscribe()` with `item_cb` (calls `release_freed_object`) and `failure_cb` (same).
  - `notify_object_eviction`: checks `subscriber.is_subscribed()`, delivers via `subscriber.handle_poll_response()`, then `subscriber.unsubscribe()`. Returns false if no subscription (no fallback).
  - `notify_owner_death`: delivers via `subscriber.handle_publisher_failure()`.

**Tests added** (5):
- `test_pin_object_ids_registers_real_subscriber_subscription`
- `test_pin_object_ids_missing_subscription_does_not_release_object`
- `test_pin_object_ids_release_requires_real_subscription_delivery_path`
- `test_pin_object_ids_real_subscriber_callback_releases_pin`
- `test_pin_object_ids_owner_death_via_subscriber_failure`

**Things to scrutinize**:
- The `item_cb` closure holds `Arc<Mutex<LocalObjectManager>>`. This is not a reference cycle — the Subscriber doesn't hold a ref back to NodeManager. When NodeManager drops, the Subscriber drops, callbacks drop, Arc refcount goes to 0.
- `notify_object_eviction` signature changed: now requires `owner_worker_id` to identify the publisher. This is needed to route through the subscriber.
- `notify_owner_death` return type changed from `Vec<ObjectID>` to `Vec<Vec<u8>>` (key_ids from the subscriber).

**Risk**: Medium. This is a structural refactor of the eviction path. All 387 raylet + 61 pubsub tests pass.

---

### CORE-10: Object location subscription protocol implemented

**Problem**: The Rust core worker had point-in-time queries and unidirectional update batches, but no subscription protocol for object locations. C++ `OwnershipBasedObjectDirectory` implements subscribe/unsubscribe/snapshot/incremental/owner-death.

**Root cause**: The owner never published location changes to `WORKER_OBJECT_LOCATIONS_CHANNEL`. The long-poll handler discarded payload (set `inner_message: None`).

**Fix** (1 file, ~120 lines changed):
- `ray-core-worker/src/grpc_service.rs`:
  - Added `CHANNEL_WORKER_OBJECT_LOCATIONS = 2` constant.
  - Added `build_object_location_pub_message()` helper — extracted from `handle_get_object_locations_owner` into a reusable function that builds `WorkerObjectLocationsPubMessage` with node_ids, spilled_url, spilled_node_id, pending_creation, object_size, ref_removed.
  - Added `decode_inner_message()` — decodes pubsub `payload` bytes back into the correct proto `InnerMessage` variant based on `channel_type` (supports channels 0, 1, 2).
  - `handle_pubsub_command_batch`: when a subscribe command for `WORKER_OBJECT_LOCATIONS_CHANNEL` arrives, publishes current location state as initial snapshot via `publisher.publish()`.
  - `handle_update_object_location_batch`: tracks which objects changed, then publishes incremental `WorkerObjectLocationsPubMessage` for each via `publisher.publish()`.
  - `handle_pubsub_long_polling`: fixed to call `decode_inner_message` on payload instead of setting `inner_message: None`.
  - Refactored `handle_get_object_locations_owner` to use shared helper.

**Protocol coverage**:
| C++ Feature | Rust Implementation |
|---|---|
| Subscribe | `handle_pubsub_command_batch` registers + publishes initial snapshot |
| Unsubscribe | `handle_pubsub_command_batch` unregisters (publisher stops delivering) |
| Initial snapshot | Published immediately on subscribe |
| Incremental updates | Published on every `handle_update_object_location_batch` change |
| Owner death | Subscriber detects via long-poll failure (publisher drops) |

**Tests added** (4):
- `test_object_location_subscription_receives_initial_snapshot`
- `test_object_location_subscription_receives_incremental_add_remove_updates`
- `test_object_location_subscription_owner_death_notifies_subscriber`
- `test_object_location_unsubscribe_stops_updates`

**Things to scrutinize**:
- `decode_inner_message` only handles channels 0-2 (core worker channels). GCS channels (3-10) are not decoded. This is correct — core worker only publishes on channels 0-2.
- The `handle_pubsub_long_polling` fix (payload → inner_message) is a behavioral change for ALL long-poll responses, not just object locations. Previously all inner_messages were `None`. Existing tests still pass because they don't inspect `inner_message`.
- The initial snapshot is published to ALL subscribers for that key (via `publisher.publish`), not just the new subscriber. This matches C++ behavior where `ObjectLocationSubscriptionCallback` calls ALL listener callbacks.

**Risk**: Medium. Touches the pubsub delivery path. All 477 core-worker tests pass.

---

## Files Changed (Complete List)

| File | Lines | What |
|---|---|---|
| `ray-pubsub/src/subscriber.rs` | +52 | `handle_publisher_failure()` |
| `ray-pubsub/src/lib.rs` | +3 | Re-export types |
| `ray-raylet/src/node_manager.rs` | +15 | `eviction_subscriber` field + init + accessor |
| `ray-raylet/src/local_object_manager.rs` | -80 | Removed local callback types and methods |
| `ray-raylet/src/grpc_service.rs` | +180, -60 | Subscriber wiring, RPC fallback fix, tests |
| `ray-core-worker/src/grpc_service.rs` | +300, -40 | Subscription protocol, helpers, tests |
| Reports (3 files) | updated | Status changes |

## Full Test Results

```
ray-raylet:      387 passed, 0 failed
ray-core-worker: 477 passed, 0 failed
ray-gcs:         443 passed, 0 failed
ray-pubsub:       61 passed, 0 failed
Total:         1,368 passed, 0 failed
```

## Verification Commands

```bash
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib
CARGO_TARGET_DIR=target_round8 cargo test -p ray-core-worker --lib
CARGO_TARGET_DIR=target_round8 cargo test -p ray-gcs --lib
CARGO_TARGET_DIR=target_round8 cargo test -p ray-pubsub --lib
```

## Remaining Parity Status

| Finding | Status |
|---|---|
| GCS-4 | fixed (prior rounds) |
| RAYLET-4 | **fixed** (this round) |
| RAYLET-6 | **fixed** (this round) |
| CORE-10 | **fixed** (this round) |

## Round 9 Addendum — Transport Lifecycle and Failure Path Proof

Round 8 re-audit found two remaining gaps:
1. RAYLET-4: tests used helper injection (notify_object_eviction/notify_owner_death) instead of exercising the subscriber transport directly
2. CORE-10: owner-death test used publisher-side subscriber removal instead of subscriber-side failure mechanism

Round 9 added 6 tests to close both gaps:

### RAYLET-4 (3 tests — no code changes, transport lifecycle proof):
- `test_pin_object_ids_subscriber_commands_are_drained_for_owner` — subscriber.drain_commands() returns subscribe command
- `test_pin_object_ids_long_poll_delivery_releases_pin_without_notify_helper` — subscriber.handle_poll_response() directly releases pin
- `test_pin_object_ids_owner_death_via_real_subscriber_failure_path` — subscriber.handle_publisher_failure() directly releases pins

### CORE-10 (3 tests — replaced weak owner-death test):
- `test_object_location_subscription_owner_death_via_publisher_failure` — subscriber-side handle_publisher_failure fires failure callback
- `test_object_location_subscription_failure_path_uses_subscriber_failure_mechanism` — multi-object failure fires all callbacks
- `test_object_location_subscription_real_long_poll_failure_path` — long-poll timeout + subscriber failure = failure callback fires

### Final test counts:
```
ray-raylet:      390 passed, 0 failed
ray-core-worker: 479 passed, 0 failed
ray-gcs:         443 passed, 0 failed
ray-pubsub:       61 passed, 0 failed
Total:         1,373 passed, 0 failed
```

All previously-identified parity gaps from the backend port review are now closed.
