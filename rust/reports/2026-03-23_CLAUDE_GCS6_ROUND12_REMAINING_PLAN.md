# Claude Remaining Plan — GCS-6 Round 12

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

Round 11 fixed the old "plain logs" problem, but the live Rust actor-event path still
does not fully match the C++ event model.

## Remaining Work

### 1. Fix registration event cardinality

C++ registration emits two events:

- one `ActorDefinitionEvent`
- one `ActorLifecycleEvent`

Rust currently appears to emit one `RayEvent` object that carries both.

That is not the same observable event stream.

You need to:

- add a failing test proving registration emits two events in the batch
- change the Rust conversion path to emit two `RayEvent` protos for registration

Suggested tests:

- `test_actor_registration_emits_definition_and_lifecycle_events`
- `test_actor_registration_event_types_match_cpp_cardinality`

### 2. Fill the missing actor-definition payload fields

The current Rust aggregator path still misses major `ActorDefinitionEvent` fields.

You need to add failing tests for:

- `required_resources`
- `placement_group_id`
- `label_selector`
- `call_site`
- `parent_id`
- `ref_ids`

Suggested tests:

- `test_actor_definition_event_includes_required_resources`
- `test_actor_definition_event_includes_placement_group_and_label_selector`
- `test_actor_definition_event_includes_call_site_parent_and_ref_ids`

Then:

- enrich the Rust actor-event builder in `actor_manager.rs`
- make sure the sink has the data needed to serialize these fields

### 3. Fill the missing lifecycle transition fields

The current Rust lifecycle event still does not clearly carry:

- `worker_id`
- `death_cause`
- `port`
- `restart_reason`

You need to add failing tests for each field in the relevant transitions:

- `test_actor_lifecycle_alive_event_includes_worker_id_and_port`
- `test_actor_lifecycle_dead_event_includes_death_cause`
- `test_actor_lifecycle_restarting_event_includes_restart_reason`

Then:

- enrich the live Rust event builder so those fields exist before the sink serializes them

### 4. Resolve fallback behavior honestly

Current Rust behavior in `server.rs` still falls back to `LoggingEventSink` if:

- `log_dir` is missing
- or `EventAggregatorSink::new(...)` fails

For FULL parity, you need to do one of two things:

1. Remove that fallback for the parity path and fail loudly / require the structured sink

or

2. Prove, with source-backed reasoning and tests, that this fallback is outside the
   parity contract being claimed

Do not silently keep the fallback and still claim FULL parity.

Suggested tests:

- `test_enable_ray_event_without_structured_sink_is_not_treated_as_full_parity_path`

## Required workflow

1. Read C++ first:
   - `src/ray/gcs/actor/gcs_actor.cc`
   - `src/ray/protobuf/public/events_actor_definition_event.proto`
   - `src/ray/protobuf/public/events_actor_lifecycle_event.proto`

2. Add failing tests first.

3. Fix the live Rust path in:
   - `rust/ray-gcs/src/actor_manager.rs`
   - `rust/ray-observability/src/export.rs`
   - `rust/ray-gcs/src/server.rs` if fallback behavior must change

4. Re-run:
   - targeted `ray-gcs` tests
   - targeted `ray-observability` tests
   - `cargo test -p ray-gcs --lib`
   - `cargo test -p ray-observability --lib`

5. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND12.md`
   - `rust/reports/2026-03-22_PARITY_FIX_REPORT_GCS6_ROUND11.md`

## Acceptance Criteria

Only call this fixed if all of the following are true:

1. registration emits the same number and types of events as C++
2. actor-definition payload is materially complete
3. lifecycle transition payload is materially complete
4. fallback behavior is either removed from the parity path or explicitly proven irrelevant
5. tests prove all of the above

If any of those is still missing, status remains:

- `still open`
