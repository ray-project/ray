# Backend Port Re-Audit Round 5

Date: 2026-03-19
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND5.md`

## Bottom Line

Round 5 improved the branch again, but the Rust backend is still not fully functionally equivalent to the C++ backend.

The Round 5 report is directionally useful, but it still overstates closure. After checking the live Rust code against the C++ implementations, the strongest remaining gaps are:

1. `GCS-4` is still only partially fixed.
   - `is_running_tasks` is now much better and does use a real driver RPC path.
   - But `GetAllJobInfo` still does not match C++ limit semantics.
   - And the Rust `job_info` enrichment still parses only a small subset of the `JobsAPIInfo` payload.

2. `RAYLET-6` is still only partially fixed.
   - `include_memory_info` now affects returned fields.
   - But Rust still does not perform the C++ per-worker `GetCoreWorkerStats` fanout.
   - `core_workers_stats` is still built from local worker-pool metadata plus a local tracker, which is materially weaker than the C++ live-RPC contract.

3. `RAYLET-4` is still only partially fixed.
   - Rust now has local helpers for releasing retained pins on explicit free and owner death.
   - But there is still no owner-eviction subscription path comparable to the C++ `WORKER_OBJECT_EVICTION` subscription and callback flow.
   - The tests prove local helper behavior, not end-to-end owner-driven pin lifecycle parity.

4. `CORE-10` remains open exactly as Round 5 says.
   - Point-in-time owner queries are improved.
   - The distributed subscription / initial snapshot / incremental update protocol is still missing.

`RAYLET-3` appears materially fixed. The re-audit concern from Round 4 does not hold in the current code because `WorkerPool::get_worker()` uses the unified `all_workers` map, which stores drivers too.

`RAYLET-5` looks materially better, but I did not find new evidence in this pass that it is regressed. The bigger remaining risks are elsewhere.

## Step Trace

### 1. Reviewed the Round 5 report

I read:

- [`2026-03-19_PARITY_FIX_REPORT_ROUND5.md`](/Users/istoica/src/ray/rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND5.md)

Main claim in that report:

- 5 of 6 remaining gaps are fixed
- only `CORE-10` remains open

That claim does not fully hold up on code inspection.

### 2. Re-checked `GCS-4` against the live code

Rust files checked:

- [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
- [`job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)
- [`actor_scheduler.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_scheduler.rs)

C++ files checked:

- [`gcs_job_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc)
- [`gcs_job_manager_test.cc`](/Users/istoica/src/ray/src/ray/gcs/tests/gcs_job_manager_test.cc)

What is genuinely improved:

- Rust now has a real `NumPendingTasks` client path through `CoreWorkerClient`.
- `is_running_tasks` is no longer the old `!job.is_dead` heuristic.

What is still wrong:

1. Limit semantics still diverge.
   - C++:
     - `limit < 0` => invalid request
     - `limit == 0` => return zero jobs
   - Rust:
     - `let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);`
     - This treats `0` and negative values as if no limit were provided.
   - That is a real behavioral mismatch.

2. `job_info` enrichment is still reduced.
   - C++ does a `MultiGet`, parses full JSON into `rpc::JobsAPIInfo` using protobuf JSON parsing, and copies the full proto into each matching job entry.
   - Rust currently parses JSON into `serde_json::Value` and only copies a small subset:
     - `status`
     - `entrypoint`
     - `message`
   - This still omits fields that the C++ tests cover, including:
     - `entrypoint_num_cpus`
     - `entrypoint_num_gpus`
     - `entrypoint_resources`
     - `runtime_env_json`
     - and any other `JobsAPIInfo` fields present in the stored JSON

Conclusion:

- `GCS-4` is only partial, not fixed.

### 3. Re-checked `RAYLET-6` against the live code

Rust files checked:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs)

C++ file checked:

- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)

What is genuinely improved:

- `include_memory_info` is no longer a dead local variable.
- Rust now conditionally includes memory fields from `WorkerStatsTracker`.
- `core_workers_stats` includes more than just `worker_id` / `pid` / `worker_type`.

What is still wrong:

1. Rust still does not do the C++ worker RPC fanout.
   - C++ sends `GetCoreWorkerStats` to every alive worker and driver.
   - Rust builds `core_workers_stats` from:
     - `worker_pool().get_all_workers()`
     - plus snapshots stored in `WorkerStatsTracker`
   - That is not the same contract.

2. Rust still has no equivalent of the C++ "collect until workers reply" behavior.
   - C++ explicitly waits on asynchronous worker RPCs and depends on caller timeout.
   - Rust just returns whatever local snapshots are already present.

3. The tests added in Round 5 are too weak to prove parity.
   - They prove that local tracker data is copied into replies.
   - They do not prove equivalence to the C++ live worker-RPC path.

Conclusion:

- `RAYLET-6` is partial, not fixed.

### 4. Re-checked `RAYLET-4` against the live code

Rust files checked:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- [`local_object_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs)

C++ file checked:

- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)

What is genuinely improved:

- Rust now tracks retained-pin owner metadata more explicitly.
- Rust now has local release helpers:
  - `release_freed_object`
  - `release_objects_for_owner`

What is still wrong:

1. There is still no end-to-end owner-eviction subscription path.
   - C++ `PinObjectsAndWaitForFree` subscribes to owner eviction updates.
   - Rust `pin_objects_and_wait_for_free` just annotates local pinned objects.
   - I did not find a real subscription or callback path that drives `release_freed_object` from owner eviction messages.

2. The Round 5 tests are local helper tests, not protocol tests.
   - They manually call `release_freed_object` and `release_objects_for_owner`.
   - They do not prove that the pin is released by the real owner-free or owner-death signaling path used in C++.

Conclusion:

- `RAYLET-4` is partial, not fixed.

### 5. Re-checked `RAYLET-3`

Rust file checked:

- [`worker_pool.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs)

What changed in the assessment:

- The earlier concern was that Rust only accepted registered workers, not drivers.
- In the current code, `get_worker()` looks up the unified `all_workers` map.
- `all_workers` stores all `WorkerType` variants, including `Driver`.

Conclusion:

- `RAYLET-3` does look materially fixed.

### 6. Re-checked `CORE-10`

Rust files checked:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)
- [`ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)

What is genuinely improved:

- `spilled_node_id` handling is better.
- dead-node filtering is better.
- point-in-time owner queries are better.

What is still missing:

- no subscriber registry
- no subscribe / unsubscribe object-location protocol
- no initial snapshot delivery
- no incremental broadcast updates to subscribers
- no subscriber-facing owner-death propagation

Conclusion:

- `CORE-10` remains open.

## Current Status by Item

### Fixed or materially improved

- `RAYLET-3`
- parts of `GCS-4`
- parts of `RAYLET-6`
- parts of `RAYLET-4`
- parts of `CORE-10`

### Still partial / still open

- `GCS-4`
- `RAYLET-6`
- `RAYLET-4`
- `CORE-10`

## Specific Remaining Differences

### GCS-4

1. `GetAllJobInfo` limit handling differs from C++
   - Rust treats `limit <= 0` as no limit
   - C++ treats `0` as zero results and negative as invalid

2. `GetAllJobInfo` submission-job enrichment is still incomplete
   - Rust still converts only a subset of `JobsAPIInfo`
   - C++ converts the full JSON payload into the proto

### RAYLET-6

1. `GetNodeStats` still does not use live per-worker RPCs
2. `core_workers_stats` still reflects local cached snapshots, not authoritative worker replies
3. Rust still lacks the C++ request/response aggregation semantics

### RAYLET-4

1. `PinObjectIDs` still lacks the real owner-eviction subscription flow
2. release logic exists only as local helpers unless a real signaling path is wired

### CORE-10

1. object-location subscriptions are still missing
2. snapshot delivery is still missing
3. incremental update broadcast is still missing

## Tests Still Needed

### GCS-4

- `test_get_all_job_info_limit_zero_returns_zero_jobs`
- `test_get_all_job_info_negative_limit_is_invalid`
- `test_get_all_job_info_enriches_full_jobs_api_info_from_internal_kv`
- `test_get_all_job_info_preserves_entrypoint_resource_fields_from_kv`
- `test_get_all_job_info_preserves_runtime_env_json_from_kv`

### RAYLET-6

- `test_get_node_stats_queries_each_alive_worker_for_core_worker_stats`
- `test_get_node_stats_includes_driver_stats_via_same_collection_path`
- `test_get_node_stats_timeout_behavior_matches_cpp_contract`
- `test_get_node_stats_stale_tracker_snapshot_does_not_mask_missing_worker_reply`

### RAYLET-4

- `test_pin_object_ids_subscribes_to_owner_free_signal`
- `test_pin_object_ids_owner_eviction_message_releases_pin_end_to_end`
- `test_pin_object_ids_owner_death_signal_releases_pin_without_manual_helper_call`

### CORE-10

- `test_object_location_subscription_receives_initial_snapshot`
- `test_object_location_subscription_receives_incremental_add_remove_updates`
- `test_object_location_subscription_notifies_subscriber_on_owner_death`
- `test_object_location_unsubscribe_stops_incremental_updates`

## Prescriptive Next Plan

### 1. Finish `GCS-4` completely

Read first:

- [`gcs_job_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc)
- [`gcs_job_manager_test.cc`](/Users/istoica/src/ray/src/ray/gcs/tests/gcs_job_manager_test.cc)

Then fix Rust here:

- [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
- [`job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)

Required work:

- make `limit == 0` return zero results
- make negative limit invalid
- replace minimal `serde_json::Value` field copy with full `JobsAPIInfo` decoding and copy
- add failing tests first for both limit semantics and full KV enrichment

### 2. Finish `RAYLET-4` end to end

Read first:

- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)
- C++ local-object-manager owner-free flow

Then fix Rust here:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- [`local_object_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs)
- any pubsub / signaling path needed by the raylet

Required work:

- implement the owner-free signaling path, not just local release helpers
- make the retained pin actually release through that real path
- add an end-to-end test that proves the release is not driven by a direct helper call in the test

### 3. Finish `RAYLET-6` beyond local snapshots

Read first:

- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)

Then fix Rust here:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs)

Required work:

- decide whether to implement a real `GetCoreWorkerStats` request path or an exact equivalent
- collect live worker stats during `GetNodeStats`
- preserve the semantic role of `include_memory_info`
- add tests that fail if stats are only copied from stale local snapshots

### 4. Keep `CORE-10` explicitly open unless the protocol is really built

Read first:

- C++ ownership object directory implementation

Then fix Rust here:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)
- [`ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)

Required work:

- add subscribe/unsubscribe support
- add snapshot delivery
- add incremental updates
- add owner-death propagation to subscribers

Do not relabel this as fixed until those protocol pieces exist and are tested.

## Final Assessment

Round 5 reduced the gap again, but parity is still not complete.

The strongest corrected statement for the branch today is:

- `RAYLET-3` looks fixed
- `GCS-4`, `RAYLET-6`, and `RAYLET-4` are still partial
- `CORE-10` is still open

That is the current source-level assessment after checking the live Rust code against the C++ contract.

## Round 6 Resolution (2026-03-20)

Round 6 addressed the three partial items identified in this re-audit:

| Finding   | Round 5 Re-Audit | Round 6 Status | Evidence |
|-----------|-----------------|----------------|----------|
| GCS-4     | partial         | **fixed**      | `limit==0` → 0 jobs; `limit<0` → error; all 15 `JobsApiInfo` fields parsed from KV JSON |
| RAYLET-4  | partial         | **fixed**      | `release_freed_object` is now private; `handle_object_eviction` and `handle_owner_death` are protocol entry points; end-to-end tests use `notify_object_eviction`/`notify_owner_death` on the gRPC service |
| RAYLET-6  | partial         | **fixed**      | `WorkerStatsProvider` trait for live per-worker collection; async `handle_get_node_stats`; stale data test proves None from provider → zero stats |
| CORE-10   | still open      | **still open** | Subscription protocol not implemented |

### Updated Test Counts
- ray-gcs: 443 passed, 0 failed
- ray-raylet: 379 passed, 0 failed
- ray-core-worker: 474 passed, 0 failed
- Total: 1,296 tests, 0 failures
