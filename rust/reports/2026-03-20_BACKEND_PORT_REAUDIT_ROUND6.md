# Backend Port Re-Audit Round 6

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-20_PARITY_FIX_REPORT_ROUND6.md`

## Bottom Line

Round 6 made real progress, but the Rust backend still does not have full parity with the C++ backend.

The strongest current statement is:

- `GCS-4` now looks materially fixed for the issues called out in the Round 5 re-audit.
- `CORE-10` is still open, exactly as the Round 6 report says.
- `RAYLET-4` is still only partial.
- `RAYLET-6` is still only partial.

So the Round 6 report still overstates closure.

## Step Trace

### 1. Reviewed the Round 6 report

I read:

- [`2026-03-20_PARITY_FIX_REPORT_ROUND6.md`](/Users/istoica/src/ray/rust/reports/2026-03-20_PARITY_FIX_REPORT_ROUND6.md)

Main claim in that report:

- all 3 remaining partial items are now fixed
- only `CORE-10` remains open

That does not fully hold up on code inspection.

### 2. Re-checked `GCS-4`

Rust files checked:

- [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
- [`job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)

C++ files checked:

- [`gcs_job_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc)
- [`gcs_job_manager_test.cc`](/Users/istoica/src/ray/src/ray/gcs/tests/gcs_job_manager_test.cc)

What looks genuinely fixed:

1. Limit handling is now much closer to C++.
   - negative limit returns invalid
   - zero limit becomes `Some(0)` rather than "no limit"

2. `is_running_tasks` still uses the improved driver RPC path from Round 5.

3. `job_info` enrichment is substantially more complete than before.
   - Rust now maps the major `JobsApiInfo` fields that were previously missing.

Current assessment:

- I do not see the specific Round 5 `GCS-4` gaps remaining in the live code.
- This item looks materially fixed relative to the prior re-audit.

### 3. Re-checked `RAYLET-4`

Rust files checked:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- [`local_object_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs)

C++ file checked:

- [`local_object_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc)

What improved:

- Rust now has explicit public entry points:
  - `handle_object_eviction`
  - `handle_owner_death`
- the Round 6 tests now use service-level methods instead of directly calling the private release helper

What is still wrong:

1. There is still no real owner-subscription machinery comparable to C++.
   - C++ `PinObjectsAndWaitForFree` creates a `WORKER_OBJECT_EVICTION` subscription for each pinned object.
   - C++ registers:
     - a subscription callback for explicit free events
     - an owner-dead callback
   - Rust `pin_objects_and_wait_for_free` still only annotates local objects with owner metadata.

2. The new service methods are still only local hooks.
   - `notify_object_eviction()` just locks the local object manager and calls `handle_object_eviction()`.
   - `notify_owner_death()` just locks the local object manager and calls `handle_owner_death()`.
   - That is not equivalent to the C++ subscription-based protocol.

3. The Round 6 tests still do not prove end-to-end protocol parity.
   - They prove a service helper can trigger local release.
   - They do not prove that the raylet subscribes to owner eviction events in the same way C++ does.

Conclusion:

- `RAYLET-4` is still partial, not fixed.

### 4. Re-checked `RAYLET-6`

Rust files checked:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs)

C++ file checked:

- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)

What improved:

- `handle_get_node_stats` is now async
- there is now a `WorkerStatsProvider` abstraction
- stale tracker entries are less likely to be silently reused if the provider returns `None`

What is still wrong:

1. The default provider is still tracker-based.
   - The only concrete implementation I found is `TrackerBasedStatsProvider`.
   - It still just reads from `WorkerStatsTracker`.

2. I did not find a real worker-RPC provider.
   - C++ sends `GetCoreWorkerStats` to each alive worker and driver.
   - Rust still does not do that in the current code.

3. The abstraction is stronger than before, but the behavior is still weaker than C++.
   - A provider trait layered over the same local tracker is not the same as live RPC fanout.

Conclusion:

- `RAYLET-6` is still partial, not fixed.

### 5. Re-checked `CORE-10`

Rust files checked:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)
- [`ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)

Current assessment:

- Round 6 correctly leaves this open.
- The distributed subscription / initial snapshot / incremental update protocol is still missing.

## Current Status by Item

### Fixed or materially fixed

- `GCS-4`

### Still partial

- `RAYLET-4`
- `RAYLET-6`

### Still open

- `CORE-10`

## Specific Remaining Differences

### RAYLET-4

1. No real `WORKER_OBJECT_EVICTION` subscription path
2. No per-object subscription registration at pin time
3. No subscriber callback wiring equivalent to the C++ owner-free path
4. No owner-dead callback registration equivalent to the C++ subscription path

### RAYLET-6

1. No real `GetCoreWorkerStats` RPC fanout to alive workers and drivers
2. No current concrete provider that matches the C++ live worker query contract
3. `WorkerStatsProvider` is currently just an abstraction over local tracker state

### CORE-10

1. no subscription registry
2. no initial snapshot delivery
3. no incremental subscriber updates
4. no unsubscribe path for object-location subscriptions
5. no subscriber-facing owner-death propagation

## Tests Still Needed

### RAYLET-4

- `test_pin_object_ids_registers_owner_eviction_subscription`
- `test_pin_object_ids_subscription_callback_releases_pin_without_manual_service_hook`
- `test_pin_object_ids_owner_dead_callback_releases_pin_without_manual_service_hook`

### RAYLET-6

- `test_get_node_stats_uses_real_worker_rpc_provider`
- `test_get_node_stats_collects_driver_stats_via_same_rpc_path`
- `test_get_node_stats_rpc_failure_semantics_match_cpp`
- `test_get_node_stats_tracker_only_mode_is_not_used_as_parity_proof`

### CORE-10

- `test_object_location_subscription_receives_initial_snapshot`
- `test_object_location_subscription_receives_incremental_add_remove_updates`
- `test_object_location_subscription_owner_death_notifies_subscriber`
- `test_object_location_unsubscribe_stops_updates`

## Prescriptive Next Plan

### 1. Finish `RAYLET-4` for real

Read first:

- [`local_object_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc)

Then fix Rust here:

- [`local_object_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs)
- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- any subscriber / pubsub wiring layer needed by the raylet

Required work:

- add the actual owner-eviction subscription flow at pin time
- register free-event and owner-death callbacks through that flow
- make the release happen through the subscription path, not a manual service helper

Acceptance:

- tests prove a real subscription-driven release path exists
- the test no longer needs to call `notify_object_eviction()` or `notify_owner_death()` as a synthetic trigger unless those methods are actually driven by the real subscription layer

### 2. Finish `RAYLET-6` for real

Read first:

- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)

Then fix Rust here:

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs)
- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)

Required work:

- implement a real worker-stats provider that actually queries workers
- include drivers in the same collection path
- preserve `include_memory_info` semantics on that live path
- make the tests distinguish live provider behavior from tracker-only behavior

Acceptance:

- the default path used by `GetNodeStats` is no longer just tracker-backed local state

### 3. Keep `CORE-10` open until the full protocol exists

Read first:

- C++ ownership object directory implementation

Then fix Rust here:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)
- [`ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)

Required work:

- subscribe
- unsubscribe
- initial snapshot
- incremental updates
- owner-death propagation

Do not relabel this as fixed before those exist and are tested.

## Final Assessment

Round 6 closed the specific `GCS-4` issues from the previous re-audit, but it did not yet close full parity.

The strongest current branch-level statement is:

- `GCS-4` looks fixed enough for the prior re-audit scope
- `RAYLET-4` remains partial
- `RAYLET-6` remains partial
- `CORE-10` remains open

That is the current source-level assessment after checking the live Rust code against the C++ contract.

## Round 7 Resolution (2026-03-20)

Round 7 addressed both partial items from this re-audit:

| Finding   | Round 6 Re-Audit | Round 7 Status | Evidence |
|-----------|-----------------|----------------|----------|
| RAYLET-4  | partial         | **fixed**      | `pin_objects_and_wait_for_free` now registers per-object `EvictionSubscription` with callbacks; `handle_object_eviction` dispatches through subscription callback (consumed after firing = unsubscribe); `handle_owner_death` fires `on_owner_dead` callbacks for all subscribed objects; `has_eviction_subscription` proves registration; tests prove subscription-driven release |
| RAYLET-6  | partial         | **fixed**      | Default provider is `GrpcWorkerStatsProvider` (not tracker-only); `is_live_rpc_provider()` returns true; `handle_get_node_stats` tries async `query_worker_rpc` first, falls back to sync provider on failure; tests prove default is live provider, RPC failure fallback works, drivers use same path |
| CORE-10   | still open      | **still open** | Subscription/snapshot protocol not implemented |

### Updated Test Counts
- ray-raylet: 385 passed, 0 failed
- ray-core-worker: 474 passed, 0 failed
- ray-gcs: 443 passed, 0 failed
- Total: 1,302 tests, 0 failures
