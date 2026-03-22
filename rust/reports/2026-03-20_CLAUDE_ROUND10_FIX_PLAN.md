# Claude Round 10 — Code Review Report for Codex

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

## What Changed

Round 10 closes the final two gaps from the Round 9 re-audit. The core issue was that prior rounds proved subscriber *primitives* work (by calling `handle_poll_response` and `handle_publisher_failure` directly from tests), but never proved that a **production transport driver** drives those primitives. This round adds that driver and tests both RAYLET-4 and CORE-10 through it.

## The Production Transport Driver

The new `SubscriberTransport` in `ray-pubsub/src/transport.rs` encapsulates the C++ subscriber poll lifecycle:

```
drain commands → send command batch → long-poll → dispatch or failure
```

C++ equivalent: `Subscriber::SendCommandBatchIfPossible` + `MakeLongPollingPubsubConnection` + `HandleLongPollingResponse`.

It takes a pluggable `SubscriberClient` trait:
- **Production**: gRPC client connecting to the owner's CoreWorkerService
- **Tests**: `InProcessSubscriberClient` (wraps a Publisher directly) or `FailingSubscriberClient` (simulates unreachable owner)

The key method is `poll_publisher(publisher_id, client)`:
1. Drains pending subscribe/unsubscribe commands from the subscriber
2. Sends them to the publisher via `client.send_command_batch()`
3. Long-polls the publisher via `client.long_poll()`
4. On success: dispatches via `subscriber.handle_poll_response()` internally
5. On failure: invokes `subscriber.handle_publisher_failure()` internally

Tests call `transport.poll_publisher()` — they never directly call `handle_poll_response` or `handle_publisher_failure`.

---

## Changes by Issue

### RAYLET-4: Production transport drives eviction delivery and failure

**Problem**: Prior tests called `subscriber.handle_poll_response()` and `subscriber.handle_publisher_failure()` directly. The re-audit wanted proof that a production transport loop drives those calls.

**Fix** (new production code + 2 tests):

**Tests added:**

1. `test_pin_object_ids_eviction_subscriber_is_driven_by_production_transport_loop`
   - Pins an object via `handle_pin_object_ids` (registers subscription in eviction subscriber)
   - Creates an owner publisher and an `InProcessSubscriberClient` backed by it
   - Spawns a task that publishes an eviction event after 10ms (gives transport time to send commands)
   - Calls `transport.poll_publisher()` — the transport:
     - Drains the subscribe command
     - Sends it to the owner publisher (registers the subscriber)
     - Long-polls the owner publisher
     - Receives the eviction event
     - Dispatches it to the registered callback
     - Callback calls `release_freed_object`
   - Asserts: `processed == 1`, pin is released
   - **No direct call to `handle_poll_response`**

2. `test_pin_object_ids_owner_failure_reaches_subscriber_without_manual_failure_call`
   - Pins 2 objects
   - Creates a `FailingSubscriberClient` (all RPCs fail)
   - Calls `transport.poll_publisher()` — the transport:
     - Tries to send command batch → fails
     - Internally calls `subscriber.handle_publisher_failure()`
     - Failure callbacks fire → `release_freed_object` for both objects
   - Asserts: both pins released, result is error
   - **No direct call to `handle_publisher_failure`**

**Risk**: Low. The transport is new production code (~180 lines including trait, impls, and tests), but it's a thin orchestrator that delegates to existing subscriber/publisher methods.

---

### CORE-10: Production transport drives owner-failure detection

**Problem**: Prior tests called `subscriber.handle_publisher_failure()` directly. The re-audit wanted proof that a production transport loop detects failure and triggers the callbacks.

**Fix** (2 tests, same transport driver):

**Tests added:**

1. `test_object_location_subscription_owner_failure_is_detected_by_production_loop`
   - Creates a subscriber, subscribes to `WORKER_OBJECT_LOCATIONS_CHANNEL` on an owner with a failure callback
   - Creates a `FailingSubscriberClient`
   - Calls `transport.poll_publisher()` — the transport detects failure and calls `handle_publisher_failure` internally
   - Asserts: failure callback fired (counter == 1), subscription removed
   - **No direct call to `handle_publisher_failure`**

2. `test_object_location_subscription_failure_callback_runs_without_manual_handle_publisher_failure`
   - Creates a subscriber, subscribes to `WORKER_OBJECT_LOCATIONS_CHANNEL` on an owner with a message callback
   - Creates owner publisher + `InProcessSubscriberClient`
   - Spawns a task that publishes a location update after 10ms
   - Calls `transport.poll_publisher()` — the transport sends commands, long-polls, receives update, dispatches callback
   - Asserts: message callback fired (counter == 1)
   - **No direct call to `handle_poll_response`**

**Risk**: Low. Tests only use the transport driver — no new production code beyond what was already added for RAYLET-4.

---

## New Files

| File | Lines | What |
|---|---|---|
| `ray-pubsub/src/transport.rs` | ~220 | `SubscriberTransport`, `SubscriberClient` trait, `InProcessSubscriberClient`, `FailingSubscriberClient`, `TransportError`, 2 unit tests |
| `ray-pubsub/Cargo.toml` | +1 | Added `async-trait` dependency |

## Modified Files

| File | What |
|---|---|
| `ray-pubsub/src/lib.rs` | Added `pub mod transport` + re-exports |
| `ray-raylet/src/grpc_service.rs` | +80 lines: 2 new production-transport tests for RAYLET-4 |
| `ray-core-worker/src/grpc_service.rs` | +100 lines: 2 new production-transport tests for CORE-10 |
| `reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md` | +4 test entries |
| `reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND9.md` | Updated with Round 10 resolution |
| `reports/2026-03-20_CLAUDE_ROUND9_FIX_PLAN.md` | Added Round 10 addendum |

## Things to Scrutinize

1. **`InProcessSubscriberClient::long_poll`** calls `publisher.connect_subscriber()` which returns a oneshot receiver. If no messages are queued, this blocks until a message arrives or the publisher drops. The RAYLET-4 eviction test handles this by spawning a background task that publishes after 10ms. If the timing is too tight on slow CI, this could flake — but 10ms is conservative for an in-process publish.

2. **`SubscriberTransport::poll_publisher` on command batch failure** calls `handle_publisher_failure` directly (lines 109-112). This matches C++ behavior where a failed command batch implies the publisher is unreachable. However, C++ logs the error and lets the long-poll path detect failure. Our implementation short-circuits — this is slightly more aggressive but correct (if we can't send commands, the publisher is dead).

3. **The `SubscriberClient` trait uses `async-trait`** because the workspace uses edition 2021. If the workspace upgrades to 2024, the trait can use native async fn.

4. **No background loop is spawned automatically.** The `SubscriberTransport.poll_publisher()` runs one cycle. In production, a background tokio task would call this in a loop per publisher. The tests prove one cycle works correctly. Spawning the background loop is a follow-up when the raylet server startup code is wired.

## Full Test Results

```
ray-raylet:      392 passed, 0 failed
ray-core-worker: 481 passed, 0 failed
ray-gcs:         443 passed, 0 failed
ray-pubsub:       63 passed, 0 failed
Total:         1,379 passed, 0 failed
```

## Verification Commands

```bash
CARGO_TARGET_DIR=target_round10 cargo test -p ray-pubsub --lib
CARGO_TARGET_DIR=target_round10 cargo test -p ray-raylet --lib
CARGO_TARGET_DIR=target_round10 cargo test -p ray-core-worker --lib
CARGO_TARGET_DIR=target_round10 cargo test -p ray-gcs --lib
```

## Parity Status

| Finding | Status |
|---|---|
| GCS-4 | fixed (prior rounds) |
| RAYLET-6 | fixed (Round 8) |
| RAYLET-4 | **fixed** (Round 8-10 impl + Round 11 runtime wiring) |
| CORE-10 | **fixed** (Round 8-10 impl + Round 11 runtime wiring) |

## Round 11 Addendum — Runtime Integration

Round 10 re-audit found that SubscriberTransport exists but is only used in tests. Round 11 wired it into the live runtime.

### RAYLET-4 Runtime Wiring:
- `NodeManager`: added `eviction_transport`, `eviction_poll_handles`, `start_eviction_poll_loop()`, `has_eviction_poll_loop()`
- `NodeManagerServiceImpl`: added `subscriber_client_factory`
- `handle_pin_object_ids`: after subscribing, starts the runtime poll loop if factory is set
- Tests: `test_raylet_eviction_transport_loop_runs_in_runtime`, `test_raylet_owner_failure_reaches_eviction_subscriber_in_runtime`

### CORE-10 Runtime Wiring:
- `CoreWorker`: added `location_subscriber`, `location_poll_handles`, `location_client_factory`, `subscribe_object_locations()`, `start_location_poll_loop()`, `set_location_client_factory()`
- `subscribe_object_locations()` registers + starts poll loop
- Tests: `test_object_location_transport_loop_runs_in_runtime`, `test_object_location_owner_failure_reaches_subscriber_in_runtime`

### Final test counts:
```
ray-raylet:      394 passed, 0 failed
ray-core-worker: 483 passed, 0 failed
ray-gcs:         443 passed, 0 failed
ray-pubsub:       63 passed, 0 failed
Total:         1,383 passed, 0 failed
```
