# 2026-04-20 — Blocker 2 recheck fix: job-summary finalization + dropped-attempt GC

## Problem

`2026-04-20-blocker-2-recheck.md` closed 2a (per-task profile-event cap)
and 2b (`task_events_max_num_task_in_gcs` through `ray_config`) but
flagged the third sub-finding as still open:

> Rust still does not implement the C++ job-summary finalization /
> dropped-attempt tracking behavior.

Specifically, four concrete gaps:

1. No Rust equivalent of C++ `JobTaskSummary::OnJobEnds()` — the Rust
   `on_job_finished` marked tasks failed but never cleared the per-job
   `dropped_task_attempts` tracking set. Tracked entries accumulated
   forever across job retirements.
2. No `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
   config knob and no GC routine to enforce it. C++
   `JobTaskSummary::GcOldDroppedTaskAttempts`
   (`gcs_task_manager.cc:773-809`) caps the tracked set while
   preserving the total via `num_dropped_task_attempts_evicted_`.
3. No `num_dropped_task_attempts_evicted` counter — so even if a GC
   path existed, Rust couldn't represent "evicted from tracking but
   still counted".
4. No periodic runner equivalent to C++'s 5-second
   `GcsTaskManager.GcJobSummary` periodical task
   (`gcs_task_manager.cc:50-52`) to drive the GC.

Client impact:

- Long-lived clusters accumulated unbounded per-job memory on the
  dropped-attempt tracking set.
- `GetTaskEvents` reported `summary.dropped_task_attempts.len()` as
  the per-job dropped count — after any (future) GC, this would
  under-report relative to C++'s `tracked + evicted` semantic.
- The documented configuration knob had no effect.

---

## Fix

### 1. Config knob

File: `crates/ray-config/src/lib.rs`.

New `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
(i64, default `1_000_000` — matches C++ `ray_config_def.h:506`).
Same precedence as every other knob: hard-coded default →
`RAY_{name}` env → `config_list` JSON.

### 2. `JobTaskSummary` rewrite

File: `crates/gcs-managers/src/task_manager.rs`.

New struct layout mirrors C++ `JobTaskSummary`
(`gcs_task_manager.h:342-385`):

```rust
struct JobTaskSummary {
    dropped_task_attempts: HashSet<(Vec<u8>, i32)>, // tracked set
    num_profile_events_dropped: i64,
    num_dropped_task_attempts_evicted: i64,
}
```

Methods (each with a comment pointing at its C++ counterpart):

- `record_task_attempt_dropped` — `RecordTaskAttemptDropped`.
- `should_drop_task_attempt` — `ShouldDropTaskAttempt`. Used by
  `add_or_replace` to skip re-ingestion of a previously-dropped
  attempt.
- `num_task_attempts_dropped()` — `NumTaskAttemptsDropped` =
  `tracked.len() + evicted`. This is the value the RPC reports; it is
  preserved across GC and across `on_job_ends`.
- `on_job_ends` — `OnJobEnds`: clears the tracked set; evicted counter
  stays so the RPC still reports the historical total.
- `gc_old_dropped_task_attempts(max_tracked)` —
  `GcOldDroppedTaskAttempts`: evicts `overflow + (overflow / 10)`
  entries when over the cap, matching C++'s 10% thrash-mitigation rule.
  `max_tracked == 0` is a no-op (same convention as every other
  `max_*` knob in the Rust GCS).

Widened `TaskEventStorage::total_attempts_dropped` and
`total_profile_events_dropped` from `i32` to `i64` so the cluster-wide
counters can survive long-running clusters without wrapping. Existing
`add_or_replace` / `evict_one` / `record_data_loss` now go through
`record_task_attempt_dropped` so the accounting stays consistent with
the new helper methods.

### 3. `UpdateJobSummaryOnJobDone`

Same file. New `TaskEventStorage::update_job_summary_on_job_done(job_id)`
(mirrors C++ `gcs_task_manager.h:255-261`). `GcsTaskManager::on_job_finished`
now calls it right after `mark_tasks_failed_on_job_ends` — closing
gap 1 from the recheck report.

### 4. `GcJobSummary` + 5-second periodic loop

- `TaskEventStorage::gc_job_summary(max_tracked)` iterates every job
  and delegates to `JobTaskSummary::gc_old_dropped_task_attempts`
  (mirrors C++ `gcs_task_manager.h:263-267`).
- `GcsTaskManager::gc_job_summary()` reads the cap from `ray_config`
  and runs one pass. One unified entry point for both the periodic
  loop and the tests.
- `GcsServer::start_task_gc_loop(period)` spawns a
  `tokio::time::interval` driven task that calls `gc_job_summary`
  every `period` (5 s by default, matching C++ at
  `gcs_task_manager.cc:50-52`). Wired into
  `install_listeners_and_initialize` so both `start()` and
  `start_with_listener()` cover it. `period = 0` disables the loop
  (same convention other `_period_*` knobs use). Previous loops are
  aborted before a new one is spawned, so repeat calls are safe and
  tests can install their own cadence deterministically.

### 5. `get_task_events` now reports the correct total

The per-job path previously read
`summary.dropped_task_attempts.len() as i32`; now it reads
`summary.num_task_attempts_dropped()`. Fix is regression-guarded by a
new test that first moves entries out of the tracked set via GC and
then verifies the RPC still reports the full 10.

The reply counters are i32 on the wire; internal accumulation is now
i64, and serialization goes through a `sat_i32` helper so a
long-running cluster saturates at INT_MAX rather than wrapping.

---

## Tests

### Unit — `task_manager::tests` (5 new)

- **`on_job_finished_clears_dropped_attempts_tracking`** — seeds two
  dropped attempts, runs `on_job_finished`, confirms the tracked set
  is empty after the delay-0 task completes. Parity guard for
  `gcs_task_manager_test.cc:1687-1690`.
- **`gc_old_dropped_task_attempts_caps_and_preserves_total`** —
  10 dropped attempts, cap=5, GC. Asserts `tracked == 5`,
  `evicted == 5`, `total == 10`. Matches the C++ test's
  `EXPECT_EQ 5 / EXPECT_EQ 5 / EXPECT_EQ 10` at
  `gcs_task_manager_test.cc:1709-1715`.
- **`storage_gc_job_summary_applies_cap_per_job`** — 2 jobs × 10
  attempts, `storage.gc_job_summary(3)`. Both jobs end with tracked
  set ≤ 3 and total = 10.
- **`get_task_events_reports_post_gc_total_dropped`** — regression
  guard for the RPC counter bug. Seed 10, GC to cap=3, query by
  job_id. Confirm `num_status_task_events_dropped == 10` (not 3).
- **`task_manager_gc_job_summary_observes_ray_config`** — sets the
  knob via `ray_config::initialize`, runs `mgr.gc_job_summary()`,
  confirms the cap landed. Closes the "no equivalent config entry"
  complaint.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    237 passed   (+5 vs previous)
gcs-server:       35 passed
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions.

---

## Before → after behaviour

| Scenario                                                        | Before                                       | After                                                            |
| --------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------------------- |
| Long-running job with 10M dropped attempts                      | All 10M retained in memory forever           | GC caps at 1M tracked; other 9M counted via `evicted`; still summable |
| `on_job_finished` on job with 100 dropped attempts              | Tracked set kept 100 entries indefinitely    | Tracked set cleared; historical count preserved through `evicted` |
| `config_list: {"task_events_max_dropped_…": 50}`                | No effect                                    | GC evicts from the tracking set down toward 50                    |
| `GetTaskEvents` per-job query after any tracked → evicted move  | Under-reported dropped count                 | Reports `tracked + evicted` — matches C++ `NumTaskAttemptsDropped` |
| 5 s periodic GC loop                                            | Not spawned                                  | Spawned; aborts cleanly on server restart                         |

---

## Scope and caveats

What's included:

- Complete parity with every bullet in the recheck report's
  "Required Work To Fully Close Blocker 2" list.
- Periodic GC loop wired into both entry points; `period == 0`
  disables for tests.

What's deliberately out of scope:

- **Mid-flight config changes.** Same as C++: the cap is read on each
  GC tick from `ray_config::instance()`, so a config reload does take
  effect at the next tick, but an already-pruned tracked set is not
  retroactively un-pruned. Matches C++ lifecycle.
- **AWS testing.** Not needed. Every parity guard here is a
  storage-side decision (set insertions, evictions, counter movements)
  that doesn't cross a network boundary. The existing integration
  tests in `gcs-server` already exercise the ingestion + RPC path
  end-to-end.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs` — one new knob.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs`:
  - `JobTaskSummary` rewritten with tracked / evicted / counter
    helpers.
  - `TaskEventStorage` now has `update_job_summary_on_job_done` and
    `gc_job_summary`; cluster counters widened to i64.
  - `GcsTaskManager::on_job_finished` calls
    `update_job_summary_on_job_done` after marking tasks failed.
  - New public `GcsTaskManager::gc_job_summary()` reads the cap from
    `ray_config`.
  - `get_task_events` reports `num_task_attempts_dropped()`; reply
    counters saturated at i32::MAX.
  - 5 new parity tests.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs`:
  - `GcsServer` gains `task_gc_handle`.
  - New `start_task_gc_loop(Duration)` spawns a `tokio::interval`
    task calling `gc_job_summary()`; `Duration::ZERO` disables.
  - Wired into `install_listeners_and_initialize` at 5 s.
