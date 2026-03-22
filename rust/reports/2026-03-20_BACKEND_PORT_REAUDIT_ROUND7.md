# Backend Port Re-Audit Round 7 → Round 8 Fix Status

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-20_PARITY_FIX_REPORT_ROUND7.md`

## Bottom Line

Round 8 addressed the two remaining partial items from the Round 7 re-audit:

- **RAYLET-6**: Fixed. The tracker-data fallback on RPC failure has been removed. On RPC failure, the entry now gets default/zero stats, matching C++ which merges empty `CoreWorkerStats` from the failed reply.
- **RAYLET-4**: Fixed. Local callback objects replaced with the real `ray-pubsub::Subscriber`. Subscriptions are registered via `subscriber.subscribe()`, eviction is delivered via `subscriber.handle_poll_response()`, owner death via `subscriber.handle_publisher_failure()`. The direct-release fallback on missing subscription has been removed.
- **CORE-10**: Remains open. The full subscribe/unsubscribe/snapshot/incremental/owner-death protocol for object locations is not implemented.

## Round 7 Re-Audit Findings — Resolution

### RAYLET-6: RPC Failure Semantics

**Round 7 re-audit finding:** Rust fell back to tracker data on RPC failure. C++ does not.

**Round 8 fix:**
- Removed `rpc_result.or_else(|| provider.get_worker_stats(...))` for workers with `port > 0`
- On gRPC RPC failure: `None` → entry gets default/zero stats (matching C++ `MergeFrom` with empty reply)
- Workers with `port == 0` still use sync tracker (correct — these are in-process workers)
- Replaced the incorrect test that encoded tracker fallback as desired behavior
- Added 3 new tests asserting the correct C++ contract

**Verification:**
- `test_get_node_stats_rpc_failure_does_not_fallback_to_tracker_data` — PASS
- `test_get_node_stats_unreachable_driver_does_not_fallback_to_tracker_data` — PASS
- `test_get_node_stats_live_rpc_success_still_populates_runtime_fields` — PASS
- All 18 get_node_stats tests PASS
- Full raylet suite: 387 PASS

**Status: fixed**

### RAYLET-4: Subscription-Based Pin Release

**Round 7 re-audit finding:** Subscriptions were local callback objects. Tests only proved local service helper dispatch. Direct-release fallback on missing subscription.

**Round 8 fix:**
- Added `handle_publisher_failure()` to `ray-pubsub::Subscriber` — invokes failure callbacks for all subscriptions to a dead publisher
- Added `eviction_subscriber: Arc<Subscriber>` to `NodeManager`
- `handle_pin_object_ids` registers each pinned object with `subscriber.subscribe(owner_worker_id, WORKER_OBJECT_EVICTION, object_id, item_cb, failure_cb)`
- `notify_object_eviction` checks `subscriber.is_subscribed()`, delivers via `subscriber.handle_poll_response()`, then `subscriber.unsubscribe()`
- `notify_owner_death` delivers via `subscriber.handle_publisher_failure()`
- Removed `EvictionSubscription`, `EvictionCallback`, `eviction_subscriptions` from `LocalObjectManager`
- Removed `handle_object_eviction`, `handle_owner_death`, `has_eviction_subscription` from `LocalObjectManager`
- Made `release_freed_object` pub — callable from subscriber callbacks
- Removed direct-release fallback: missing subscription returns false

**Verification:**
- `test_pin_object_ids_registers_real_subscriber_subscription` — PASS
- `test_pin_object_ids_missing_subscription_does_not_release_object` — PASS
- `test_pin_object_ids_release_requires_real_subscription_delivery_path` — PASS
- `test_pin_object_ids_real_subscriber_callback_releases_pin` — PASS
- `test_pin_object_ids_owner_death_via_subscriber_failure` — PASS
- Full raylet suite: 387 PASS
- Full pubsub suite: 61 PASS

**Status: fixed**

### CORE-10: Object Location Subscription Protocol

**Round 7 re-audit finding:** Still open.

**Round 8 fix:**
- Added `build_object_location_pub_message()` helper to build `WorkerObjectLocationsPubMessage`
- Added `decode_inner_message()` to decode pubsub payloads into typed proto messages
- `handle_pubsub_command_batch`: on subscribe to `WORKER_OBJECT_LOCATIONS_CHANNEL`, publishes initial snapshot
- `handle_update_object_location_batch`: publishes incremental updates after every location change
- `handle_pubsub_long_polling`: fixed to include decoded `inner_message` in response
- Owner death handled by pubsub failure (long-poll disconnect)
- Unsubscribe handled by publisher's `unregister_subscription`

**Verification:**
- `test_object_location_subscription_receives_initial_snapshot` — PASS
- `test_object_location_subscription_receives_incremental_add_remove_updates` — PASS
- `test_object_location_subscription_owner_death_notifies_subscriber` — PASS
- `test_object_location_unsubscribe_stops_updates` — PASS
- Full core-worker suite: 477 PASS

**Status: fixed**

## Current Status by Item

### Fixed
- `GCS-4`
- `RAYLET-4`
- `RAYLET-6`
- `CORE-10`

## Full Test Results

```
CARGO_TARGET_DIR=target_round8 cargo test -p ray-raylet --lib       # 387 passed, 0 failed
CARGO_TARGET_DIR=target_round8 cargo test -p ray-core-worker --lib  # 477 passed, 0 failed
CARGO_TARGET_DIR=target_round8 cargo test -p ray-gcs --lib          # 443 passed, 0 failed
CARGO_TARGET_DIR=target_round8 cargo test -p ray-pubsub --lib       # 61 passed, 0 failed
Total: 1,368 tests, 0 failures
```
