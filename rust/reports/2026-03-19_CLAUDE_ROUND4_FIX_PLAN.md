# Claude Fix Plan After Round 4 Re-Audit

Date: 2026-03-19
Scope: only the gaps still visible after the Round 4 pass

## Rules

For each issue below:
1. Read the C++ implementation first.
2. Read the C++ tests first.
3. Add a failing Rust test first.
4. Run the test and confirm the failure.
5. Fix the Rust implementation.
6. Re-run the targeted tests.
7. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-19_BACKEND_PORT_REAUDIT_ROUND4.md`
   - `rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND4.md`

Do not mark an issue fixed if:
- the implementation only matches the reply shape
- a request flag is read but has no semantic effect
- owner metadata is stored but no owner-driven lifecycle exists
- the report text itself still admits a heuristic or architectural gap

## Priority Order

1. RAYLET-3 remaining driver gap
2. GCS-4 remaining `is_running_tasks` gap
3. RAYLET-6 remaining worker-stats gap
4. RAYLET-4 remaining pin-lifetime gap
5. CORE-10 remaining subscription/snapshot gap
6. RAYLET-5 verification sweep

## 1. RAYLET-3 Remaining Driver Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandleReportWorkerBacklog`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs`

### Remaining Problem

Rust now verifies registered workers, but it still does not match the C++ driver case.

### Tests To Add First

- `test_report_worker_backlog_accepts_registered_driver`
- `test_report_worker_backlog_rejects_unknown_driver_id`

### Fix Guidance

- Add driver-aware lookup in the worker-pool layer or a dedicated driver query method.
- Match the C++ semantics exactly: registered worker OR registered driver.

### Acceptance Criteria

- Backlog from a registered driver is accepted.
- Backlog from an unknown driver-like ID is ignored.

## 2. GCS-4 Remaining is_running_tasks Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc`
    - `HandleGetAllJobInfo`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs`
  - `/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs`

### Remaining Problem

Rust still sets `is_running_tasks = !job.is_dead`, which is explicitly weaker than the C++ driver-RPC path.

### Tests To Add First

- `test_get_all_job_info_uses_driver_pending_tasks_rpc_for_is_running_tasks`
- `test_get_all_job_info_alive_job_with_no_pending_tasks_reports_false`

### Fix Guidance

- Introduce the minimum worker/driver RPC path needed to query pending tasks.
- Do not keep the heuristic and call the issue fixed.

### Acceptance Criteria

- `is_running_tasks` comes from a real task-count signal, not job liveness.

## 3. RAYLET-6 Remaining Worker-Stats Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandleGetNodeStats`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`

### Remaining Problems

1. `include_memory_info` is still a no-op.
2. `core_workers_stats` is still just worker-pool metadata, not real worker stats.

### Tests To Add First

- `test_get_node_stats_include_memory_info_changes_worker_stats_payload`
- `test_get_node_stats_core_worker_stats_include_nontrivial_fields`

### Fix Guidance

- Build the minimal worker-stats collection path needed to match the C++ observable contract.
- If full RPC fanout is not feasible in one pass, at least make `include_memory_info` change behavior and expose richer worker stats than worker-pool metadata.

### Acceptance Criteria

- `include_memory_info` is semantically meaningful.
- `core_workers_stats` contains fields that are not trivially copied from the worker pool.

## 4. RAYLET-4 Remaining Pin-Lifetime Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandlePinObjectIDs`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs`

### Remaining Problem

Rust now attaches owner metadata, but it still does not implement an owner-driven wait-for-free lifecycle.

### Tests To Add First

- `test_pin_object_ids_keeps_pin_until_owner_free_event`
- `test_pin_object_ids_owner_free_event_releases_retained_pin`

### Fix Guidance

- Add explicit owner/free signaling behavior.
- The retained pin must remain until the owner-side release condition occurs.

### Acceptance Criteria

- The pin lifetime is actually tied to an owner/free event, not just metadata attachment.

## 5. CORE-10 Remaining Subscription/Snapshot Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc`
  - `/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs`

### Remaining Problem

The distributed subscription/snapshot protocol is still not implemented end to end.

### Tests To Add First

- `test_object_location_subscription_receives_initial_snapshot`
- `test_object_location_subscription_receives_incremental_updates`
- `test_object_location_subscription_handles_owner_death`

### Fix Guidance

- If you implement this, do it end to end.
- If the protocol is too large for this pass, explicitly split it into a new tracked issue and stop calling `CORE-10` fixed in any report.

### Acceptance Criteria

- Either the protocol works end to end with tests, or the issue remains explicitly open.

## 6. RAYLET-5 Verification Sweep

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandleResizeLocalResourceInstances`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`

### Goal

Prove that the resize behavior is actually closed, not just improved.

### Tests To Add

- `test_resize_local_resource_instances_matches_cpp_for_custom_unit_resources`
- `test_resize_local_resource_instances_matches_cpp_delta_semantics_under_use`

### Acceptance Criteria

- The tests prove parity beyond just rejecting GPU by default.

## Final Validation

After all fixes:

1. Re-run targeted tests for each issue.
2. Re-run:
   - `cargo test -p ray-gcs --lib`
   - `cargo test -p ray-raylet --lib`
   - `cargo test -p ray-core-worker --lib`
3. Update the reports with exact status labels:
   - `fixed`
   - `partial`
   - `still open`

Do not collapse `partial` into `fixed`.
