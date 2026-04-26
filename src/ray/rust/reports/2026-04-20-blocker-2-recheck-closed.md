# Blocker 2 Recheck: Task-Event Storage and Config Parity

Date: 2026-04-20
Repository: `/Users/istoica/src/cc-to-rust/ray`
Previous report: `ray/src/ray/rust/reports/2026-04-20-gcs-parity-review-recheck.md`
Prior Blocker 2 recheck: `ray/src/ray/rust/reports/2026-04-20-blocker-2-recheck.md`
Scope: verify whether Finding 2 from the parity recheck report is now fully closed.

## Conclusion

Blocker 2 now appears closed.

The previous recheck found that profile-event truncation and `task_events_max_num_task_in_gcs` wiring had been fixed, but the C++ job-summary finalization and dropped-attempt tracking cap were still missing. In the current tree, those remaining pieces are now implemented and covered by focused tests.

I do not see a remaining Blocker 2 parity issue after re-reading the current Rust and C++ code paths.

## Verification Details

### 2a. Per-task profile-event cap remains fixed

Rust enforces `task_events_max_num_profile_events_per_task`:

- `GcsTaskManager` stores `max_profile_events_per_task`:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:171-180`
- merge path truncates profile events and records drops:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:287-300`
- insert path also truncates an over-large first batch:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:317-329`
- truncation drops from the front:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:342-365`

This matches C++:

- `ray/src/ray/gcs/gcs_task_manager.cc:179-191`

### 2b. `task_events_max_num_task_in_gcs` remains wired through `RayConfig`

Rust `ray-config` now defines the relevant task-event knobs:

- `task_events_max_num_task_in_gcs`
- `task_events_max_num_profile_events_per_task`
- `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:254-269`

`GcsTaskManager::new_from_ray_config()` reads the task-event storage caps:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:580-593`

The production server construction uses `new_from_ray_config()` unless a test override is supplied:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:307-320`

This matches the C++ construction path:

- `ray/src/ray/gcs/gcs_task_manager.cc:42-45`

### 2c. Job-summary finalization and dropped-attempt cap are now implemented

This was the part that remained open in the prior recheck. It now appears fixed.

Rust `JobTaskSummary` now separates:

- the currently tracked dropped-attempt set
- cumulative dropped-attempts evicted from the tracked set
- total dropped attempts as `tracked.len() + evicted`

Relevant Rust code:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:36-133`

This matches the C++ model:

- `JobTaskSummary::RecordTaskAttemptDropped`
- `JobTaskSummary::ShouldDropTaskAttempt`
- `JobTaskSummary::NumTaskAttemptsDropped`
- `JobTaskSummary::OnJobEnds`
- `JobTaskSummary::GcOldDroppedTaskAttempts`
- `ray/src/ray/gcs/gcs_task_manager.h:342-385`
- `ray/src/ray/gcs/gcs_task_manager.cc:773-808`

Rust `on_job_finished()` now calls `update_job_summary_on_job_done()` after marking job tasks failed:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:640-669`

That matches C++:

- `ray/src/ray/gcs/gcs_task_manager.cc:750-769`

Rust also exposes and runs the per-job dropped-attempt GC:

- storage-level GC:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:464-471`
- manager-level GC reading the current `ray_config` cap:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:673-684`
- server-side 5-second loop:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:217-223`
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:933-965`

That matches the C++ periodic runner:

- `ray/src/ray/gcs/gcs_task_manager.cc:50-52`

Finally, `GetTaskEvents` now reports per-job dropped status via `summary.num_task_attempts_dropped()`, not `summary.dropped_task_attempts.len()`:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:808-821`

This is important because it preserves the C++ behavior after dropped-attempt tracking has been GC'd:

- tracked set can shrink
- total dropped count still remains externally visible

## Test Evidence

I ran the focused tests for the fixed Blocker 2 paths. All passed:

- `profile_events_truncated_to_per_task_cap`
- `new_from_ray_config_uses_task_events_max_num_task_in_gcs`
- `on_job_finished_clears_dropped_attempts_tracking`
- `get_task_events_reports_post_gc_total_dropped`
- `gc_old_dropped_task_attempts_caps_and_preserves_total`
- `storage_gc_job_summary_applies_cap_per_job`
- `task_manager_gc_job_summary_observes_ray_config`

The test runs emitted existing warnings from `gcs-managers` such as unused imports and unexpected `cfg(test-support)` values. I did not see test failures.

## Residual Notes

I do not see a remaining issue in Blocker 2.

One implementation detail is intentionally equivalent rather than byte-for-byte identical: both C++ and Rust evict arbitrary hash-set entries when trimming old dropped attempts. C++ erases from `absl::flat_hash_set::begin()`, and Rust collects from `HashSet` iteration order. Neither is timestamp ordered, and that matches the C++ comment that eviction ignores timestamp.

This report only closes Finding 2 from `2026-04-20-gcs-parity-review-recheck.md`. Other findings in that report should still be evaluated independently.
