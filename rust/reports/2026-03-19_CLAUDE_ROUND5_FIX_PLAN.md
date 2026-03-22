# Claude Round 5 Fix Plan

Date: 2026-03-19
Reference: `rust/reports/2026-03-19_BACKEND_PORT_REAUDIT_ROUND5.md`

This plan is intentionally narrow. Do not reopen already-closed items unless new evidence shows a regression.

## Non-Negotiable Rules

1. Read the C++ implementation and tests first.
2. Add a failing Rust test first.
3. Confirm the failure mode before editing Rust.
4. Fix the Rust implementation.
5. Re-run the targeted tests.
6. Update the reports.

Do not mark an issue fixed because:

- the API looks closer
- a local helper exists
- a unit test manually simulates the missing protocol step
- a cached local snapshot resembles a live RPC result

## Work Order

1. Finish `GCS-4`
2. Finish `RAYLET-4`
3. Finish `RAYLET-6`
4. Keep `CORE-10` open unless the full protocol is actually implemented

## 1. GCS-4

### Read first

- `/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc`
- `/Users/istoica/src/ray/src/ray/gcs/tests/gcs_job_manager_test.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs`
- `/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs`

### What is still wrong

1. `limit == 0` is wrong
2. negative limit is wrong
3. `JobsAPIInfo` enrichment is still only partial

### Add these failing tests first

- `test_get_all_job_info_limit_zero_returns_zero_jobs`
- `test_get_all_job_info_negative_limit_is_invalid`
- `test_get_all_job_info_enriches_full_jobs_api_info_from_internal_kv`
- `test_get_all_job_info_preserves_entrypoint_resource_fields_from_kv`
- `test_get_all_job_info_preserves_runtime_env_json_from_kv`

### Implementation guidance

- preserve the distinction between:
  - no limit supplied
  - zero limit
  - negative limit
- do not parse JSON into a loose `serde_json::Value` and hand-copy a few fields
- parse and populate the full `rpc::JobsApiInfo` payload the same way C++ conceptually does
- keep `is_running_tasks` behavior as improved in Round 5

### Acceptance

- zero limit returns zero jobs
- negative limit returns invalid
- enriched jobs contain the same major `JobsAPIInfo` fields that C++ test coverage expects

## 2. RAYLET-4

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
- C++ local object manager owner-free flow tied to `PinObjectsAndWaitForFree`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs`
- any Rust signaling/pubsub path required to drive owner-free release

### What is still wrong

- the current Rust code has local release helpers
- but there is still no demonstrated end-to-end owner-free signaling path
- current tests directly call helper methods, which is weaker than C++

### Add these failing tests first

- `test_pin_object_ids_owner_eviction_signal_releases_pin_end_to_end`
- `test_pin_object_ids_owner_death_signal_releases_pin_end_to_end`
- `test_pin_object_ids_does_not_require_direct_helper_call_in_test`

### Implementation guidance

- wire a real signaling path that causes retained pins to be released
- do not rely on tests manually calling `release_freed_object`
- if the owner-free event source is not yet implemented elsewhere, build the minimum real path needed

### Acceptance

- the pin is released through the protocol path, not by direct helper invocation in the test
- explicit free and owner death both work end to end

## 3. RAYLET-6

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`

### What is still wrong

- current Rust `GetNodeStats` is still based on worker-pool data plus `WorkerStatsTracker`
- that is not the same as the C++ live per-worker RPC collection path

### Add these failing tests first

- `test_get_node_stats_queries_live_worker_stats_path`
- `test_get_node_stats_driver_stats_follow_same_collection_path`
- `test_get_node_stats_missing_worker_reply_does_not_silently_use_stale_snapshot`
- `test_get_node_stats_include_memory_info_affects_live_worker_response`

### Implementation guidance

- add a real live stats collection path or an exact equivalent
- keep `include_memory_info` as a real control over returned per-worker memory fields
- do not treat `WorkerStatsTracker` alone as sufficient evidence of parity unless you can prove it is fed by the same contract as the C++ RPC path

### Acceptance

- `GetNodeStats` is no longer just a local snapshot assembler
- the tests prove the live collection semantics

## 4. CORE-10

### Read first

- `/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc`
- `/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs`

### Status rule

If you do not implement:

- subscribe
- unsubscribe
- initial snapshot
- incremental broadcasts
- owner-death propagation to subscribers

then leave `CORE-10` open.

Do not rename "partial point-in-time support" into "fixed."

### Tests to add when you actually implement it

- `test_object_location_subscription_receives_initial_snapshot`
- `test_object_location_subscription_receives_incremental_add_remove_updates`
- `test_object_location_subscription_owner_death_notifies_subscriber`
- `test_object_location_unsubscribe_stops_updates`

## Validation

For each issue, report:

- exact C++ functions read
- exact Rust files changed
- tests added
- exact test commands run
- final status: `fixed`, `partial`, or `still open`

Update after each issue:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-19_BACKEND_PORT_REAUDIT_ROUND5.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND5.md`
