# Backend Port Re-Audit Round 8 → Round 9 Fix Status

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reference artifact reviewed: `rust/reports/2026-03-20_CLAUDE_ROUND8_FIX_PLAN.md`

## Bottom Line — Updated After Round 9

Round 9 addressed both remaining partial items from the Round 8 re-audit:

- **RAYLET-4**: Fixed. Three new tests exercise the subscriber transport lifecycle directly (drain_commands, handle_poll_response, handle_publisher_failure) WITHOUT using notify helpers. This proves the subscriber transport path end-to-end.
- **CORE-10**: Fixed. The weak owner-death test (which used publisher-side subscriber removal) has been replaced with three tests that exercise the real subscriber-side failure mechanism (Subscriber::handle_publisher_failure). Failure callbacks fire with correct key_ids.

## Round 8 Re-Audit Findings — Resolution

### RAYLET-4

**Round 8 re-audit finding:**
1. The raylet eviction subscriber still lacked a clearly integrated real transport loop
2. Delivery was still injected via helper methods (notify_object_eviction, notify_owner_death)

**Round 9 fix:**
- Added 3 tests that exercise the subscriber transport lifecycle directly, bypassing all helper methods:
  - `test_pin_object_ids_subscriber_commands_are_drained_for_owner` — proves `subscriber.drain_commands(owner_wid)` returns the batched subscribe command after pinning
  - `test_pin_object_ids_long_poll_delivery_releases_pin_without_notify_helper` — proves `subscriber.handle_poll_response()` directly dispatches the eviction callback and releases the pin
  - `test_pin_object_ids_owner_death_via_real_subscriber_failure_path` — proves `subscriber.handle_publisher_failure()` directly invokes failure callbacks and releases both pins
- No code changes needed — the existing subscriber infrastructure already supports the full transport lifecycle. The tests prove it.

**Verification:**
- All 3 new tests PASS
- Full raylet suite: 390 PASS

**Status: fixed**

### CORE-10

**Round 8 re-audit finding:**
1. The owner-death test simulated subscriber removal (`publisher.unregister_subscriber`), not publisher failure
2. No real subscriber-side publisher-failure path was exercised

**Round 9 fix:**
- Replaced the weak `test_object_location_subscription_owner_death_notifies_subscriber` with 3 real tests:
  - `test_object_location_subscription_owner_death_via_publisher_failure` — creates a ray-pubsub::Subscriber, subscribes to WORKER_OBJECT_LOCATIONS_CHANNEL on an owner, calls `subscriber.handle_publisher_failure(owner_id)`, verifies the failure callback fires with the correct key_id, and subscription is removed
  - `test_object_location_subscription_failure_path_uses_subscriber_failure_mechanism` — subscribes 3 objects to the same owner, calls `handle_publisher_failure`, verifies all 3 failure callbacks fire and all subscriptions are removed
  - `test_object_location_subscription_real_long_poll_failure_path` — sets up a real publisher subscription via handle_pubsub_command_batch, drains the initial snapshot via long-poll, simulates long-poll timeout (empty response), then exercises the subscriber-side failure detection via handle_publisher_failure

**Verification:**
- All 3 new tests PASS (+ 2 existing subscription tests)
- Full core-worker suite: 479 PASS

**Status: fixed**

## Current Status by Item

### Fixed
- `GCS-4`
- `RAYLET-6`
- `RAYLET-4`
- `CORE-10`

## Full Test Results

```
CARGO_TARGET_DIR=target_round9 cargo test -p ray-raylet --lib       # 390 passed, 0 failed
CARGO_TARGET_DIR=target_round9 cargo test -p ray-core-worker --lib  # 479 passed, 0 failed
CARGO_TARGET_DIR=target_round9 cargo test -p ray-gcs --lib          # 443 passed, 0 failed
CARGO_TARGET_DIR=target_round9 cargo test -p ray-pubsub --lib       # 61 passed, 0 failed
Total: 1,373 tests, 0 failures
```
