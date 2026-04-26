# Blocker 1 Recheck: `GetTaskEvents` Parity

Date: 2026-04-20
Repository: `/Users/istoica/src/cc-to-rust/ray`
Previous report: `ray/src/ray/rust/reports/2026-04-20-gcs-parity-review-recheck.md`
Scope: verify whether Finding 1 from the parity recheck report is now closed, with the bar set to C++-identical externally visible `GetTaskEvents` behavior.

## Conclusion

Blocker 1 appears closed.

The Rust `GcsTaskManager::get_task_events()` implementation now matches the C++ `HandleGetTaskEvents` on the three parity gaps called out in the previous report:

1. `task_filters` are implemented in both the candidate-preselection path and the full predicate path.
2. events without `task_info` are now suppressed unconditionally.
3. malformed predicates now fail the RPC with `InvalidArgument` instead of being silently accepted.

I did not find a remaining discrepancy in this blocker after re-reading the current Rust and C++ code and running focused Rust tests covering each of the previously missing behaviors.

## What Changed

### 1. `task_filters` now exist in both places where C++ uses them

C++ behavior:

- `ray/src/ray/gcs/gcs_task_manager.cc:436-447` preselects by task ID when `task_filters` contains `EQUAL`.
- `ray/src/ray/gcs/gcs_task_manager.cc:501-512` also applies `task_filters` in the per-event predicate path.

Rust now mirrors both parts:

- candidate preselection:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:630-664`
- per-event predicate filtering:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:714-723`

Important parity details that are now present:

- Rust prefers `task_filters` `EQUAL` preselection ahead of `job_filters`, matching the C++ ordering.
- multiple distinct `EQUAL` task IDs short-circuit to an empty candidate set, matching the C++ behavior that no single task can satisfy both predicates.
- `NOT_EQUAL` task filters are handled in the full predicate path rather than incorrectly driving index preselection.

This closes the exact gap described in Finding 1a of the earlier report.

### 2. Rust now drops events without `task_info`, matching C++

C++ behavior:

- `ray/src/ray/gcs/gcs_task_manager.cc:491-495` unconditionally skips events that do not have `task_info()`.

Rust now has the same early gate:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:697-703`

That matters because lifecycle-only or profile-only task-event fragments can exist before the task-definition fragment is merged. The previous Rust behavior could leak those entries to clients; the current code no longer does.

This closes the exact gap described in Finding 1b of the earlier report.

### 3. Invalid predicates now return `InvalidArgument`

C++ behavior:

- `ray/src/ray/gcs/gcs_task_manager.cc:584-618` converts invalid predicates into an `InvalidArgument` failure.

Rust now validates predicates up front and returns the same class of failure:

- validation before filtering:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:610-618`
- validation helpers:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:831-859`

This is the right shape for parity:

- validation covers `task_filters`
- validation covers `job_filters`
- validation covers `actor_filters`
- validation covers `task_name_filters`
- validation covers `state_filters`

That closes the exact gap described in Finding 1c of the earlier report.

## Test Evidence

I ran focused Rust unit tests that directly cover the previously missing behaviors:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib task_filter_equal_preselects_by_task_id`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib events_without_task_info_are_skipped`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib invalid_predicate_returns_invalid_argument`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib combined_filters_apply_in_sequence`
  - passed

The source file also now contains additional parity-oriented tests for the `task_filters` cases:

- `task_filter_not_equal_drops_matching_events`
- `task_filter_multiple_equal_distinct_returns_empty`

Relevant test definitions:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:1429-1648`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:1723-1740` and following

## Residual Notes

I do not see a remaining Blocker 1 issue in the current tree.

Two boundaries are worth keeping clear:

- This recheck only addresses Finding 1 from the earlier report.
- The earlier report's other findings were separate:
  - task-event storage/config parity
  - usage-stats / metrics-exporter initialization

Those should still be evaluated on their own merits. But with respect to the `GetTaskEvents` parity issues that were explicitly listed as Blocker 1, the current implementation now looks materially aligned with C++.
