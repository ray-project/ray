# 2026-04-17 — Blocker 6 fix: task-manager lifecycle listeners

## Reviewer's finding (valid)

Three parity gaps remained around lifecycle callbacks that C++
`GcsServer::InstallEventListeners` wires up:

1. `gcs_worker_manager_->AddWorkerDeadListener` → `gcs_task_manager_->OnWorkerDead`
   (`gcs_server.cc:879-900`). Rust had no `on_worker_dead` on `GcsTaskManager`.
2. `gcs_job_manager_->AddJobFinishedListener` → `gcs_task_manager_->OnJobFinished`
   (`gcs_server.cc:902-907`). Rust had no `on_job_finished` on `GcsTaskManager`.
3. `gcs_job_manager_->OnNodeDead(node_id)` on node removal
   (`gcs_server.cc:865`). Rust had no `on_node_dead` on `GcsJobManager`.

All three are now implemented.

---

## Code changes

### `gcs-managers/src/task_manager.rs`

- **`is_task_finished` bugfix.** The helper was checking `state_ts_ns` for
  keys `6` and `8` — stale enum values from an older proto. The current
  proto uses `TaskStatus::Finished = 11` and `TaskStatus::Failed = 12`.
  `is_task_finished` now uses the enum constants directly, matching C++
  `IsTaskTerminated` (`protobuf_utils.cc:314-322`). One pre-existing test
  that relied on the stale value was updated to use the enum.
- **New `TaskEventStorage::mark_tasks_failed_on_worker_dead`.** Iterates
  every stored `TaskEvents`, and for any non-terminal one whose
  `state_updates.worker_id` matches the dead worker, sets
  `state_ts_ns[Failed] = failed_ts_ns` + `error_info` (type `WORKER_DIED`).
  Mirrors C++ `MarkTasksFailedOnWorkerDead` (`gcs_task_manager.cc:107-127`).
- **New `TaskEventStorage::mark_tasks_failed_on_job_ends`.** Same logic
  keyed on `job_id`. Mirrors C++ `MarkTasksFailedOnJobEnds`
  (`gcs_task_manager.cc:146-165`).
- **New `TaskEventStorage::mark_failed_if_needed`.** Single-event helper
  mirroring C++ `MarkTaskAttemptFailedIfNeeded`. Early-outs on terminated
  tasks — C++ parity.
- **New public `GcsTaskManager::on_worker_dead`.** Builds the WORKER_DIED
  error string exactly as C++ does (`"Worker running the task (<hex>) died
  with exit_type: <n> with error_message: <str>"`), defers the mark by
  `delay_ms` via `tokio::spawn + sleep` (C++ uses a boost ASIO deadline
  timer with `gcs_mark_task_failed_on_worker_dead_delay_ms`), then invokes
  `mark_tasks_failed_on_worker_dead`. Parity with C++
  `GcsTaskManager::OnWorkerDead` (`gcs_task_manager.cc:729-748`).
- **New public `GcsTaskManager::on_job_finished`.** Same pattern against
  `RayConfig::gcs_mark_task_failed_on_job_done_delay_ms`. Parity with
  C++ `GcsTaskManager::OnJobFinished` (`gcs_task_manager.cc:750-772`).
- **ns conversion.** C++ multiplies `end_time_ms` (and `job_finish_time_ms`)
  by `1_000_000` to get ns for `state_ts_ns`; Rust does the same.

### `gcs-managers/src/job_manager.rs`

- **New public `GcsJobManager::on_node_dead`.** Walks `jobs` DashMap,
  collects ids for every live (`!is_dead`) job whose
  `driver_address.node_id == node_id`, then calls `mark_job_finished` on
  each. This also cascades the existing job-finished listener pipeline,
  so `task_manager.on_job_finished` and the EXPORT_DRIVER_JOB write both
  fire naturally. Parity with C++ `GcsJobManager::OnNodeDead`
  (`gcs_job_manager.cc:488-512`).

### `gcs-server/src/lib.rs`

- **New `start_lifecycle_listeners()`.** Registers three wires, mirroring
  C++ `InstallEventListeners` exactly:
  1. `worker_manager.add_worker_dead_listener(|wd| task_manager.on_worker_dead(...))`
     — C++ `gcs_server.cc:879-900`.
  2. `job_manager.add_job_finished_listener(|jd| task_manager.on_job_finished(...))`
     — C++ `gcs_server.cc:902-907`. (The extra C++ cleanup
     `placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead` does
     not yet have a Rust counterpart. Flagged as follow-up; not part of this
     blocker. See "Known gaps" below.)
  3. `subscribe_node_removed` → `job_manager.on_node_dead(...)` — C++
     `gcs_server.cc:865`.
- **Delay constants honoured via env.** Rust reads
  `RAY_gcs_mark_task_failed_on_worker_dead_delay_ms` and
  `RAY_gcs_mark_task_failed_on_job_done_delay_ms` from the environment so
  operators can match C++ `RayConfig` values; default is `0` in both cases
  (matches the C++ tests' default).
- Registered into the startup chain right after the other listener
  installs (`start()` pre-initialize block), matching C++ call-order.

---

## Test coverage (all new tests passing)

### `task_manager::tests` (+4 tests, 11 total passing)
- `test_on_worker_dead_marks_matching_tasks_failed` — 3 tasks across 2
  workers; after `on_worker_dead(W1)`, tasks on W1 get `state_ts_ns[Failed]`
  and `error_info{type: WORKER_DIED, message: "Worker ..."}`, while task on
  W2 is untouched.
- `test_on_worker_dead_skips_terminated_tasks` — a task already at FINISHED
  is NOT re-marked FAILED (C++ `IsTaskTerminated` early-out parity).
- `test_on_job_finished_marks_all_job_tasks_failed` — two jobs; after
  `on_job_finished(jobA)`, both jobA tasks are FAILED and jobB tasks are
  untouched.
- `test_on_job_finished_failure_timestamp_is_ns` — asserts
  `state_ts_ns[Failed] == finish_ms * 1_000_000` (C++ ns conversion).

### `job_manager::tests` (+3 tests, 12 total passing)
- `test_on_node_dead_marks_jobs_with_drivers_on_node` — driver on dead node
  → `is_dead=true`; driver on live node untouched.
- `test_on_node_dead_skips_already_dead_jobs` — `end_time` of an
  already-dead job is not overwritten (`!is_dead` guard parity).
- `test_on_node_dead_no_match_is_noop` — no matching jobs → no-op, no
  crash.

### Regression check
- `cargo test -p gcs-managers` — **192 / 192 passing** (was 185; +4 task +
  3 job).
- `cargo test --workspace` — **270 / 270 passing** (was 263), 0 failures.

---

## C++ ↔ Rust parity matrix

| Callback | C++ site | Rust implementation | Rust wiring |
|---|---|---|---|
| worker_dead → mark worker's tasks FAILED | `gcs_task_manager.cc:729` | `GcsTaskManager::on_worker_dead` | `gcs-server/src/lib.rs start_lifecycle_listeners()` |
| job_finished → mark job's tasks FAILED | `gcs_task_manager.cc:750` | `GcsTaskManager::on_job_finished` | same |
| node_dead → mark jobs-with-driver-on-it finished | `gcs_job_manager.cc:488` | `GcsJobManager::on_node_dead` | same (via `subscribe_node_removed`) |
| Terminated-task early-out | `IsTaskTerminated` (`protobuf_utils.cc:314`) | `is_task_finished` (now uses real enum) | — |
| Error type on worker_dead | `ErrorType::WORKER_DIED` | `ErrorType::WorkerDied as i32` | — |
| ns conversion from end_time_ms | `end_time_ms * 1000 * 1000` | `end_time_ms * 1_000_000` | — |
| Delay before marking | `gcs_mark_task_failed_on_worker_dead_delay_ms` | `tokio::time::sleep(delay_ms)` from env | — |

---

## Known gaps (intentionally out of scope)

- C++ also calls `gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead`
  from the job-finished listener (`gcs_server.cc:906`). Rust has no
  equivalent yet. I did not invent a sibling because that method does not
  exist on the Rust `GcsPlacementGroupManager`, and adding it is a separate
  parity item orthogonal to Blocker 6 (which is scoped to task-manager
  lifecycle). Flagged here for visibility; the wiring code has a comment
  pointing at where to drop it in once the PG method exists.

---

## AWS testing

Not required. `on_worker_dead` / `on_job_finished` / `on_node_dead` run
entirely in-process on GCS state; the new tests drive them
deterministically with direct calls and verify the resulting in-memory
state via `get_task_events` / `get_job`. No external services are involved.

---

## Outcome

Blocker 6 (task-manager lifecycle listeners + job-manager on_node_dead) is
closed. The Rust GCS now reproduces the C++ task-state transitions that
fire on worker failure, job completion, and node removal, with the same
error types, error messages, and failure timestamps.
