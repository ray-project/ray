# 2026-04-20 — Blocker 8 fix: `GetTaskEvents` + task-events config parity

## Problem

Five separate semantic gaps between Rust `GcsTaskManager` and C++
`GcsTaskManager`, all on the public `task_info.GetTaskEvents` RPC and its
supporting storage / config contract.

### RPC semantics (section 1)

- **1a — `task_filters` ignored.** C++ uses them both for index
  preselection (`gcs_task_manager.cc:436-447`) and in the full predicate
  path (`501-512`). Rust indexed only `job_filters` and never evaluated
  `task_filters` at all (`task_manager.rs:475-498`, `509-583`).
- **1b — Missing `has_task_info` gate.** C++ drops every event without
  task_info unconditionally (`491-495`). Rust only checked task_info
  inside the handful of filters that happen to read it, so a
  lifecycle-only or profile-only event that arrived before the task
  definition would leak through.
- **1c — No `InvalidArgument` on bad predicates.** C++ wraps filtering
  in `try/catch(std::invalid_argument)` and returns
  `Status::InvalidArgument` on an unknown predicate (`586-618`). Rust
  hard-coded predicate tests as `== 0` / `== 1`, so predicate 42 (or
  any other non-EQUAL/non-NOT_EQUAL value) was silently read as
  NOT_EQUAL — different result set, different status code.

### Storage / config (section 2)

- **2a — No per-task profile-event cap.** C++ trims profile events past
  `task_events_max_num_profile_events_per_task` on every merge
  (`179-191`). Rust merged profile events without any cap, so a
  chatty task could accumulate arbitrarily many entries and the
  dropped-profile-events counter could never diverge above zero.
- **2b — `task_events_max_num_task_in_gcs` not wired through
  `RayConfig`.** C++ constructs the manager with
  `RayConfig::instance().task_events_max_num_task_in_gcs()`
  (`42-45`). Rust used a hard-coded `GcsServerConfig::max_task_events
  = 100_000` that never consulted `ray_config`; `config_list` /
  `RAY_task_events_max_num_task_in_gcs` had no effect on eviction.

Severity: critical — these are RPC-visible differences on a public
surface and a broken configuration contract.

---

## Fix

### 1. Two new `RayConfig` knobs

File: `crates/ray-config/src/lib.rs`.

Added (with defaults matching C++ `ray_config_def.h:498` and `529`):

- `task_events_max_num_task_in_gcs` — `i64`, default `100_000`.
  Consumer: `GcsTaskManager::new_from_ray_config`.
- `task_events_max_num_profile_events_per_task` — `i64`, default `1000`.
  Consumer: `TaskEventStorage::add_or_replace` profile-truncation path.

Both knobs accept the same three override sources the rest of `RayConfig`
supports: hard-coded default → `RAY_{name}` env → `config_list` JSON.

### 2. Profile-event truncation on merge (2a)

File: `crates/gcs-managers/src/task_manager.rs`.

- `GcsTaskManager` gained `max_profile_events_per_task`, captured at
  construction from `ray_config` (or passed explicitly via
  `new_with_limits` for tests).
- `TaskEventStorage::add_or_replace` now takes the cap and runs
  `truncate_profile_events(&mut event, cap)` after each merge *and* on
  initial insert — the insert-side truncation prevents a single
  over-large batch from bypassing the cap.
- `truncate_profile_events` drops from the *front* (oldest first) to
  match C++ `mutable_events()->DeleteSubrange(0, to_drop)` at line 185,
  and records the overflow both per-job
  (`JobTaskSummary::num_profile_events_dropped`) and cluster-wide
  (`total_profile_events_dropped`) so `GetTaskEvents` surfaces it in
  `num_profile_task_events_dropped`.
- A cap of 0 disables truncation (same convention other Rust GCS
  config knobs use).

### 3. Storage-cap wiring (2b)

Files: `crates/gcs-managers/src/task_manager.rs`,
`crates/gcs-server/src/lib.rs`.

- `GcsTaskManager::new_from_ray_config()` pulls both caps from one
  `ray_config::snapshot()` — one snapshot so a burst of ingestion can't
  see half-and-half reconfiguration.
- `GcsServerConfig::max_task_events` changed from `usize` (default
  `100_000`) to `Option<usize>` (default `None`). `None` ⇒ the server
  constructs the task manager via `new_from_ray_config`; `Some(n)`
  pins it explicitly for tests.
- Server construction site (`lib.rs:307-321`) now branches on the
  option accordingly.

### 4. `get_task_events` rewrite (1a, 1b, 1c)

Same file. The handler is a step-for-step port of C++ `HandleGetTaskEvents`:

1. **Validate every predicate up front** via `validate_predicates` →
   `check_predicate`. Unknown predicates yield
   `Status::InvalidArgument` and the reply is never partially built
   (cleaner than mid-iteration error handling). Parity with C++ line
   586-618.
2. **Index preselection** in the same precedence as C++:
   `task_filters` first (lines 436-447), then `job_filters` (448-468),
   else scan everything. Multiple distinct EQUAL task/job IDs
   short-circuit to an empty result — matches C++ lines 446, 467.
   A single-ID preselect uses the new
   `TaskEventStorage::events_for_task_ids`, mirroring C++
   `GetTaskEvents(const flat_hash_set<TaskID>&)` at `81-93`.
3. **Per-event filter** applies the same order as C++: the `has_task_info`
   gate first (lines 491-495), then `exclude_driver` (496-499), then
   `task_filters` (501-512), `job_filters` (514-525), `actor_filters`
   (527-538), `task_name_filters` (540-550), and `state_filters`
   (552-579).
4. **Candidates iterated in reverse** so the newest events win under a
   truncating limit — matches C++ `| boost::adaptors::reversed` at
   line 587.
5. Per-event helpers `apply_predicate_bytes` and
   `apply_predicate_ignore_case` mirror the C++ `apply_predicate<T>`
   and `apply_predicate_ignore_case` at `394-422`. Case-insensitive
   compare via `eq_ignore_ascii_case`, which is the ASCII-scope
   equivalent of the absl function C++ uses (Ray state names and task
   names are ASCII).
6. Latest task state is computed the same way as C++: iterate
   `state_ts_ns` keys backward and take the largest enum value seen
   (`latest_task_state`, mirroring C++ lines 557-567).

### 5. Test fixture cleanup

The existing `make_task_events` helper in the task-manager tests
produced events with `task_info = None`. Before this fix those events
*were* returned by `get_task_events` (the bug in 1b). After the fix they
would be correctly filtered — making existing tests spuriously fail.
The fixture now populates `task_info` with a minimal
`TaskInfoEntry { job_id, .. }`, matching how real clients populate the
field. A sibling `make_task_events_no_info` fixture exists for the
one test that deliberately exercises the skip path.

Two `event_export_stub` tests that ingested a lifecycle-only or
profile-only event and expected it to show up in the reply were
updated to ingest a matching `TaskDefinitionEvent` first. That's the
real-world flow (the ingest layer merges the definition + the
lifecycle/profile payload under one `(task_id, attempt)` key) and
exercises the C++-parity path correctly.

---

## Tests

### Unit — `task_manager::tests` (8 new)

- **`task_filter_equal_preselects_by_task_id`** (1a) — a single EQUAL
  task-id filter selects only the matching event; the other event is
  absent from the reply.
- **`task_filter_not_equal_drops_matching_events`** (1a) — NOT_EQUAL
  falls through to the predicate path and drops the matching event.
- **`task_filter_multiple_equal_distinct_returns_empty`** (1a) — two
  distinct EQUAL task-ids short-circuit to empty.
- **`events_without_task_info_are_skipped`** (1b) — mixed ingest of
  one with / one without task_info. Reply has only the
  task_info-carrying event; `num_filtered_on_gcs == 1`,
  `num_total_stored == 2`.
- **`invalid_predicate_returns_invalid_argument`** (1c) — one bogus
  predicate per filter kind (task / job / actor / name / state); each
  surfaces as `InvalidArgument`.
- **`profile_events_truncated_to_per_task_cap`** (2a) — cap set to 3
  via `new_with_limits`; ingest 5 profile events, reply has only the
  3 newest (`e2..e4`) and `num_profile_task_events_dropped == 2`.
- **`new_from_ray_config_uses_task_events_max_num_task_in_gcs`** (2b) —
  sets both knobs through `ray_config::initialize`, confirms the
  constructed manager observes those values.
- **`combined_filters_apply_in_sequence`** (1a + full predicate path)
  — actor_filter + state_filter together resolve to the one event that
  satisfies both.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    232 passed   (+8 vs main)
gcs-server:       35 passed
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions. Two pre-existing `event_export_stub` tests were
updated to reflect the corrected skip-no-task_info semantics (they
were passing only because of the bug this commit fixes).

---

## Before → after behaviour

| Request                                                         | Before                                  | After                                                            |
| --------------------------------------------------------------- | --------------------------------------- | ---------------------------------------------------------------- |
| `task_filters: [{EQUAL, task_id=X}]`                            | `task_filters` silently ignored          | Only events for task X returned (indexed)                         |
| `task_filters: [{NOT_EQUAL, task_id=X}]`                        | `task_filters` silently ignored          | Matching event dropped; rest returned                             |
| `task_filters: [{EQUAL, X}, {EQUAL, Y}]` (distinct)             | Returned both                            | Empty result (C++ parity)                                         |
| `GetTaskEvents` on storage with lifecycle-only events           | Returned them                            | Dropped (no task_info); counted under `num_filtered_on_gcs`       |
| `state_filter` with `predicate = 42` (unknown value)            | Treated as NOT_EQUAL, silent              | `Status::InvalidArgument`; reply cleared                           |
| Ingest 5000 profile events for one task (default cap 1000)      | All 5000 stored and returned              | Oldest 4000 trimmed on merge; `num_profile_task_events_dropped += 4000` |
| `config_list: {"task_events_max_num_task_in_gcs": 50}`          | No effect on Rust; stayed at 100_000      | Observed; eviction kicks in at 50                                 |
| `RAY_task_events_max_num_profile_events_per_task=3`             | No effect on Rust                         | Per-task cap becomes 3                                            |

---

## Scope and caveats

What's included:

- Full behavioral parity for the five gaps called out in the blocker.
- Production wiring reads both caps from `ray_config` at construction,
  same single source of truth C++ uses.
- Unknown-predicate detection runs up-front — no partial reply on the
  error path.

What's deliberately out of scope:

- **Mid-flight config changes.** Both C++ and Rust read the cap at
  construction. A running GCS doesn't respond to in-flight config
  updates; restart is required for the cap to change.
- **`task_name` locale handling.** C++ uses `absl::EqualsIgnoreCase`
  which is ASCII-scope. Rust uses `eq_ignore_ascii_case` — same
  ASCII-only semantics. Both match historical Ray task names.
- **AWS testing.** Not needed. Every parity guard in this commit runs
  in-process against the real `GcsTaskManager` handler — the failure
  modes being exercised (wrong predicate handling, skip-no-task_info,
  profile truncation) are all server-side decisions that don't involve
  the transport layer. Running over AWS would test tonic, not the
  parity we're closing.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs` — two new knobs.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs` —
  `GcsTaskManager` gained `max_profile_events_per_task`,
  `new_with_limits`, `new_from_ray_config`; `add_or_replace` runs
  profile-event truncation; `events_for_task_ids` added to storage;
  `get_task_events` rewritten; new `latest_task_state`,
  `validate_predicates`, `check_predicate`,
  `apply_predicate_bytes`, `apply_predicate_ignore_case`. 8 new
  parity tests.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/event_export_stub.rs`
  — two tests updated to send a `TaskDefinitionEvent` before the
  lifecycle/profile event so the merged row has `task_info`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/Cargo.toml` — add
  `ray-config` dep.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs` —
  `GcsServerConfig::max_task_events` is now `Option<usize>`; server
  constructs the task manager via `new_from_ray_config` when `None`.
