# Blocker 2 Recheck: Task-Event Storage and Config Parity

Date: 2026-04-20
Repository: `/Users/istoica/src/cc-to-rust/ray`
Previous report: `ray/src/ray/rust/reports/2026-04-20-gcs-parity-review-recheck.md`
Scope: verify whether Finding 2 from the parity recheck report is now fully closed.

## Conclusion

Blocker 2 is only partially fixed. It is not fully closed.

The current Rust tree does appear to close the first two sub-findings:

1. per-task profile-event truncation is now implemented
2. `task_events_max_num_task_in_gcs` is now wired through `ray_config`

However, the third sub-finding is still open:

3. Rust still does not implement the C++ job-summary finalization / dropped-attempt tracking behavior

Because Blocker 2 in the original report was explicitly a three-part storage/config parity finding, I would not mark the blocker closed yet.

## What Is Fixed

### 2a. Per-task profile-event cap now exists

The earlier report said Rust did not enforce `task_events_max_num_profile_events_per_task`.

That part now looks fixed:

- Rust stores `max_profile_events_per_task` in `GcsTaskManager`:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:84-90`
- profile events are truncated on merge:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:196-208`
- profile events are also truncated on first insert:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:225-236`
- the truncation helper matches the C++ direction by dropping from the front:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:249-271`

This matches the C++ behavior at:

- `ray/src/ray/gcs/gcs_task_manager.cc:179-191`

I also ran the targeted Rust test:

- `profile_events_truncated_to_per_task_cap`
  - passed

### 2b. `task_events_max_num_task_in_gcs` is now wired through `RayConfig`

The earlier report said Rust still used `GcsServerConfig.max_task_events` without wiring the production path to `ray_config`.

That part also looks fixed:

- Rust `ray-config` now defines:
  - `task_events_max_num_task_in_gcs`
  - `task_events_max_num_profile_events_per_task`
  - `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:254-261`
- `GcsTaskManager::new_from_ray_config()` now reads both from one config snapshot:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:461-475`
- production server construction now uses `new_from_ray_config()` when the explicit test override is absent:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:307-320`

This is the right shape relative to C++:

- `ray/src/ray/gcs/gcs_task_manager.cc:42-45`

I also ran the targeted Rust test:

- `new_from_ray_config_uses_task_events_max_num_task_in_gcs`
  - passed

## What Is Still Not Fixed

### 2c remains open: Rust still misses the C++ dropped-attempt summary lifecycle

This is the remaining blocker.

The earlier report called out two related C++ behaviors:

1. `OnJobFinished()` finalizes and clears job-summary state
2. `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs` bounds the tracked dropped-attempt set

I do not see either of those behaviors in the current Rust code.

### Missing piece 1: `OnJobFinished()` does not finalize the job summary

C++ behavior:

- `GcsTaskManager::OnJobFinished()` marks tasks failed and then calls:
  - `task_event_storage_->UpdateJobSummaryOnJobDone(job_id);`
  - `ray/src/ray/gcs/gcs_task_manager.cc:768-769`

The corresponding C++ tests explicitly check that the dropped-task-attempt tracking set is empty after job completion:

- `ray/src/ray/gcs/tests/gcs_task_manager_test.cc:1687-1690`

Rust `on_job_finished()` only marks tasks failed:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:521-545`

There is no Rust equivalent of:

- `UpdateJobSummaryOnJobDone`
- clearing `dropped_task_attempts` once the job is finalized

That means Rust retains per-job dropped-attempt tracking state after job completion, while C++ explicitly finalizes and clears it.

This is still a real parity gap:

- memory/accounting lifetime differs from C++
- job-summary contents after job completion differ from C++
- long-lived workloads can accumulate different per-job state than the C++ GCS

### Missing piece 2: no dropped-attempt tracking cap

C++ behavior:

- `JobTaskSummary::GcOldDroppedTaskAttempts(...)` enforces:
  - `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
  - `ray/src/ray/gcs/gcs_task_manager.cc:773-808`

The matching C++ test verifies:

- size 10 before GC
- size 5 after GC
- total dropped count preserved through `num_dropped_task_attempts_evicted_`
- `ray/src/ray/gcs/tests/gcs_task_manager_test.cc:1694-1715`

Rust has no equivalent config entry in `ray-config` and no equivalent storage fields or GC routine.

Current Rust evidence:

- `JobTaskSummary` only stores:
  - `dropped_task_attempts`
  - `num_profile_events_dropped`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:36-44`
- there is no Rust hit for
  - `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
  - `GcOldDroppedTaskAttempts`
  - `UpdateJobSummaryOnJobDone`
- `get_task_events()` still reports per-job dropped status as:
  - `summary.dropped_task_attempts.len() as i32`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:670-673`

That is not equivalent to the C++ accounting model, where the tracked set may be GC’d while the total dropped count is still preserved.

### Why this still matters

Even though the cap-related config fixes are in place, the remaining summary-lifecycle gap is not cosmetic.

It changes the behavior of the task-event subsystem in ways the original blocker explicitly covered:

- per-job memory retention differs after a job finishes
- dropped-attempt bookkeeping semantics diverge from C++
- if the tracked-set cap is later exercised in C++, Rust has no equivalent bounded behavior
- Rust currently has no representation of the C++ “evicted but still counted” dropped-attempt state

So the blocker has narrowed, but it has not disappeared.

## Test Evidence

I ran these targeted Rust tests:

- `profile_events_truncated_to_per_task_cap`
  - passed
- `new_from_ray_config_uses_task_events_max_num_task_in_gcs`
  - passed
- `test_on_job_finished_marks_all_job_tasks_failed`
  - passed

These confirm the first two sub-fixes and basic `OnJobFinished()` task-failure marking.

They do not close the remaining parity gap, because I do not see a Rust test covering either:

- clearing dropped-attempt tracking on job completion
- bounding dropped-attempt tracking with a C++-equivalent cap while preserving total counts

## Required Work To Fully Close Blocker 2

At minimum, Rust still needs all of the following:

1. Add `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs` to Rust `ray-config`.
2. Extend `JobTaskSummary` to separate:
   - currently tracked dropped attempts
   - total dropped attempts
   - number of dropped attempts evicted from tracking
3. Implement the C++-equivalent dropped-attempt GC path.
4. Call the Rust equivalent of `UpdateJobSummaryOnJobDone(job_id)` from `on_job_finished()`.
5. Add tests matching the C++ coverage for:
   - dropped-attempt GC limit
   - post-job-finish summary finalization / clearing

Until that exists, Blocker 2 should remain open.
