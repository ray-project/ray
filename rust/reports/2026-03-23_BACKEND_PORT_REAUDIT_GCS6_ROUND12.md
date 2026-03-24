# Backend Port Re-Audit — GCS-6 Round 12

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Round 12 fix author: Claude

## Scope

This re-audit checked the Round 11 claim against live Rust and C++ source.
All four gaps identified below have been fixed in Round 12.

## Gaps Found (all fixed)

### 1. Registration event cardinality — FIXED

**Problem**: Rust created one `RayEvent` proto with both `actor_definition_event` and
`actor_lifecycle_event` stuffed inside. C++ creates TWO separate `RayEvent` protos.

**Fix**: `actor_manager.rs` now emits two separate `RayEvent` objects on registration:
one with `event_kind="definition"` and one with `event_kind="lifecycle"`. The
`EventAggregatorSink` converts each into a separate proto: `ACTOR_DEFINITION_EVENT (9)`
and `ACTOR_LIFECYCLE_EVENT (10)`.

**Tests**: `test_actor_registration_emits_definition_and_lifecycle_events`,
`test_actor_registration_event_types_match_cpp_cardinality`

### 2. Actor definition payload — FIXED

**Problem**: Rust only populated 7 of 13 ActorDefinitionEvent fields
(`actor_id`, `job_id`, `is_detached`, `name`, `ray_namespace`, `serialized_runtime_env`,
`class_name`). Missing: `required_resources`, `placement_group_id`, `label_selector`,
`call_site`, `parent_id`, `ref_ids`.

**Fix**:
- `handle_register_actor()` now copies `required_resources`, `placement_group_id`,
  `call_site`, `parent_id`, `labels`, `serialized_runtime_env` from TaskSpec to
  ActorTableData at registration time.
- `write_actor_export_event()` now passes all these fields via custom_fields.
- `EventAggregatorSink::convert_to_proto_event()` now parses and populates all 13 fields.

**Tests**: `test_actor_definition_event_includes_required_resources`,
`test_actor_definition_event_includes_placement_group_and_label_selector`,
`test_actor_definition_event_includes_call_site_parent_and_ref_ids`

### 3. Lifecycle transition payload — FIXED

**Problem**: Rust StateTransition only filled `state`, `timestamp`, `node_id`,
`repr_name`, `pid`. Missing: `worker_id` (ALIVE), `port` (ALIVE), `death_cause` (DEAD),
`restart_reason` (RESTARTING).

**Fix**:
- `write_actor_export_event()` now passes state-specific fields:
  - ALIVE: `worker_id` (from `actor.address.worker_id`), `port` (from `actor.address.port`)
  - DEAD: `death_cause` (serialized JSON from `actor.death_cause`)
  - RESTARTING: `restart_reason` (2=NODE_PREEMPTION when `actor.preempted`, else 0)
- `convert_to_proto_event()` now parses and populates all transition fields.

**Tests**: `test_actor_lifecycle_alive_event_includes_worker_id_and_port`,
`test_actor_lifecycle_dead_event_includes_death_cause`,
`test_actor_lifecycle_restarting_event_includes_restart_reason`

### 4. Fallback behavior — FIXED

**Problem**: `server.rs` fell back to `LoggingEventSink` (plain text logs) when
`EventAggregatorSink` creation failed. That is not structured proto output.

**Fix**: Removed `LoggingEventSink` fallback from the `enable_ray_event` path.
When `EventAggregatorSink` cannot be created (missing log_dir or creation error),
no sink is attached — events are buffered but not delivered. This makes the
non-parity state observable (flush returns 0).

**Tests**: `test_enable_ray_event_without_structured_sink_is_not_treated_as_full_parity_path`

## Test Results

```
CARGO_TARGET_DIR=target_round12 cargo test -p ray-gcs --lib
test result: ok. 496 passed; 0 failed; 0 ignored

CARGO_TARGET_DIR=target_round12_obs cargo test -p ray-observability --lib
test result: ok. 45 passed; 0 failed; 0 ignored
```

496 = 487 (Round 11) + 9 (Round 12). Zero regressions.

## Files Changed

| File | Change |
|---|---|
| `ray-gcs/src/actor_manager.rs` | Enriched `write_actor_export_event()` to emit two events on registration and pass all fields. Enriched `handle_register_actor()` to copy definition fields to ActorTableData. Added 9 new tests. |
| `ray-observability/src/export.rs` | Rewrote `convert_to_proto_event()` to emit separate definition/lifecycle protos and populate all fields from custom_fields. |
| `ray-observability/Cargo.toml` | Added `base64` dependency. |
| `ray-gcs/src/server.rs` | Removed `LoggingEventSink` fallback from `enable_ray_event` path. |

## Status: FIXED

All four gaps are closed:
1. Registration emits 2 separate events (definition + lifecycle) — matches C++
2. ActorDefinitionEvent has all 13 fields — matches C++
3. ActorLifecycleEvent transitions have state-specific fields — matches C++
4. LoggingEventSink fallback removed from parity path — explicit failure mode
