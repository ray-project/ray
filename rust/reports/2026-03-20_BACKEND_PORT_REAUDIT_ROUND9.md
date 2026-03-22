# Backend Port Re-Audit Round 9 → Round 10 Fix Status

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reference artifact reviewed: `rust/reports/2026-03-20_CLAUDE_ROUND9_FIX_PLAN.md`

## Bottom Line — Updated After Round 10

Round 10 addressed both remaining gaps from the Round 9 re-audit by implementing a **production subscriber transport driver** (`SubscriberTransport`) and testing both issues through it.

The transport driver encapsulates the C++ subscriber poll lifecycle:
drain commands → send command batch → long-poll → dispatch/failure.

Tests call `transport.poll_publisher()` — no direct calls to `handle_poll_response` or `handle_publisher_failure` from any test.

## Round 9 Re-Audit Findings — Resolution

### RAYLET-4

**Round 9 re-audit finding:**
1. No production transport loop that drains eviction subscriber commands and drives owner-poll delivery
2. Tests proved subscriber primitives, not full production-path integration

**Round 10 fix:**
- Added `SubscriberTransport` to `ray-pubsub/src/transport.rs` — production transport driver with `poll_publisher()` method
- Added `SubscriberClient` trait — pluggable transport interface (production: gRPC; tests: in-process)
- Added `InProcessSubscriberClient` — connects to a Publisher directly (test mock)
- Added `FailingSubscriberClient` — always fails (simulates unreachable owner)
- Added 2 tests:
  - `test_pin_object_ids_eviction_subscriber_is_driven_by_production_transport_loop` — pins object, sets up owner publisher, runs `transport.poll_publisher()`, owner publishes eviction, transport delivers, pin released. No direct `handle_poll_response` call.
  - `test_pin_object_ids_owner_failure_reaches_subscriber_without_manual_failure_call` — pins 2 objects, runs `transport.poll_publisher()` with FailingSubscriberClient, transport internally calls `handle_publisher_failure`, both pins released. No direct `handle_publisher_failure` call.

**Status: fixed**

### CORE-10

**Round 9 re-audit finding:**
1. No production path from owner failure to subscriber failure handling
2. Tests used direct `handle_publisher_failure` calls, not production transport

**Round 10 fix:**
- Used the same `SubscriberTransport` for CORE-10 tests
- Added 2 tests:
  - `test_object_location_subscription_owner_failure_is_detected_by_production_loop` — subscribes to WORKER_OBJECT_LOCATIONS_CHANNEL, runs `transport.poll_publisher()` with FailingSubscriberClient, failure callback fires. No direct `handle_publisher_failure` call.
  - `test_object_location_subscription_failure_callback_runs_without_manual_handle_publisher_failure` — subscribes, runs `transport.poll_publisher()` with InProcessSubscriberClient, owner publishes location update, callback fires. No direct `handle_poll_response` call.

**Status: fixed**

## Current Status by Item

### Fixed
- `GCS-4`
- `RAYLET-6`
- `RAYLET-4`
- `CORE-10`

## Full Test Results

```
CARGO_TARGET_DIR=target_round10 cargo test -p ray-raylet --lib       # 392 passed, 0 failed
CARGO_TARGET_DIR=target_round10 cargo test -p ray-core-worker --lib  # 481 passed, 0 failed
CARGO_TARGET_DIR=target_round10 cargo test -p ray-gcs --lib          # 443 passed, 0 failed
CARGO_TARGET_DIR=target_round10 cargo test -p ray-pubsub --lib       # 63 passed, 0 failed
Total: 1,379 tests, 0 failures
```
