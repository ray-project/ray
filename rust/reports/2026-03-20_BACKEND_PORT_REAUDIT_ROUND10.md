# Backend Port Re-Audit Round 10 → Round 11 Fix Status

Date: 2026-03-20
Branch: `cc-to-rust-experimental`

## Bottom Line — Updated After Round 11

Round 11 wired the `SubscriberTransport` into both live runtimes:

- **RAYLET-4**: `NodeManager.start_eviction_poll_loop()` spawns a background tokio task per owner. `handle_pin_object_ids` calls it automatically when a `subscriber_client_factory` is set. Tests prove the runtime drives delivery and failure — no test-created transport driver.
- **CORE-10**: `CoreWorker.subscribe_object_locations()` registers with the `location_subscriber` AND starts the runtime poll loop. Tests prove the runtime drives location-update delivery and owner-failure detection — no test-created transport driver.

## Round 10 Re-Audit Findings — Resolution

### RAYLET-4

**Round 10 finding:** Transport driver exists but is not wired into the live runtime.

**Round 11 fix:**
- Added `eviction_transport: SubscriberTransport` and `eviction_poll_handles: Mutex<HashMap<...>>` to `NodeManager`
- Added `start_eviction_poll_loop(publisher_id, client)` — spawns background tokio task that runs `transport.poll_publisher()` in a loop
- Added `has_eviction_poll_loop(publisher_id)` — checks if loop is running
- Added `subscriber_client_factory` to `NodeManagerServiceImpl` — configurable client factory
- Modified `handle_pin_object_ids` — after subscribing, if factory is set, starts the runtime poll loop
- Tests set the factory and verify the runtime drives delivery

**Tests added:**
- `test_raylet_eviction_transport_loop_runs_in_runtime` — pins object, runtime loop delivers eviction from owner publisher
- `test_raylet_owner_failure_reaches_eviction_subscriber_in_runtime` — pins objects, runtime loop detects failure with FailingSubscriberClient

**Status: fixed**

### CORE-10

**Round 10 finding:** Transport driver exists but is not wired into the live object-location subscription runtime.

**Round 11 fix:**
- Added `location_subscriber: Arc<Subscriber>` and `location_poll_handles: Mutex<HashMap<...>>` to `CoreWorker`
- Added `subscribe_object_locations(owner_id, object_id, item_cb, failure_cb)` — registers subscription AND starts runtime poll loop
- Added `start_location_poll_loop(publisher_id, client)` — spawns background tokio task
- Added `set_location_client_factory(factory)` — configurable client factory
- Tests set the factory, call `subscribe_object_locations`, and verify the runtime drives delivery/failure

**Tests added:**
- `test_object_location_transport_loop_runs_in_runtime` — subscribes, runtime loop delivers update from owner publisher
- `test_object_location_owner_failure_reaches_subscriber_in_runtime` — subscribes, runtime loop detects failure, callback fires

**Status: fixed**

## Current Status by Item

### Fixed
- `GCS-4`
- `RAYLET-6`
- `RAYLET-4`
- `CORE-10`

## Full Test Results

```
ray-raylet:      394 passed, 0 failed
ray-core-worker: 483 passed, 0 failed
ray-gcs:         443 passed, 0 failed
ray-pubsub:       63 passed, 0 failed
Total: 1,383 tests, 0 failures
```
