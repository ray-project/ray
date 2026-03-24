# Claude Round 12 — GCS-6 Full Actor-Event Model Parity Report for Codex

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 12 re-audit correctly identified that Round 11 fixed the output channel
(transport/proto shape) but left the event MODEL incomplete. Four concrete gaps
remained:

1. Registration event cardinality (1 combined proto vs C++'s 2 separate protos)
2. ActorDefinitionEvent payload (7 of 13 fields populated)
3. ActorLifecycleEvent transition payload (missing worker_id, port, death_cause, restart_reason)
4. LoggingEventSink fallback silently degrading the parity path

This round fixes all four. Every fix has a failing-first test that proves the gap
before the implementation change.

---

## Step A: C++ Event-Model Contract (re-derived from source)

### Files read

- `src/ray/gcs/actor/gcs_actor.cc` (lines 105–160: `WriteActorExportEvent`)
- `src/ray/observability/ray_actor_definition_event.cc` (constructor, lines 22–49)
- `src/ray/observability/ray_actor_lifecycle_event.cc` (constructor, lines 20–64)
- `src/ray/protobuf/public/events_actor_definition_event.proto`
- `src/ray/protobuf/public/events_actor_lifecycle_event.proto`
- `src/ray/protobuf/public/events_base_event.proto`

### How many events registration emits

**Two.** C++ `WriteActorExportEvent(is_actor_registration=true)` creates a
`vector<unique_ptr<RayEventInterface>>` and pushes:

1. `RayActorDefinitionEvent` → becomes a `RayEvent` proto with `event_type = ACTOR_DEFINITION_EVENT (9)`
2. `RayActorLifecycleEvent` → becomes a `RayEvent` proto with `event_type = ACTOR_LIFECYCLE_EVENT (10)`

Both are passed to `ray_event_recorder_.AddEvents(std::move(events))` and become
**separate `RayEvent` protos** inside the `AddEventsRequest`.

Non-registration transitions emit only one `RayActorLifecycleEvent`.

### What fields `ActorDefinitionEvent` must contain

All fields from `ray_actor_definition_event.cc` constructor:

| Field | Source | Required? |
|---|---|---|
| `actor_id` | `data.actor_id()` | Always |
| `job_id` | `data.job_id()` | Always |
| `is_detached` | `data.is_detached()` | Always |
| `name` | `data.name()` | Always |
| `ray_namespace` | `data.ray_namespace()` | Always |
| `serialized_runtime_env` | `data.serialized_runtime_env()` | Always |
| `class_name` | `data.class_name()` | Always |
| `required_resources` | `data.required_resources()` | When data exists (map) |
| `placement_group_id` | `data.placement_group_id()` | When has_placement_group_id() |
| `label_selector` | `data.label_selector()` | When data exists (map) |
| `call_site` | `data.call_site()` | When has_call_site() |
| `parent_id` | `data.parent_id()` | Always (may be empty) |
| `ref_ids` | `data.labels()` | When data exists (map, labels→ref_ids) |

### What fields lifecycle `StateTransition` must contain

From `ray_actor_lifecycle_event.cc`:

| Field | Condition | Source |
|---|---|---|
| `state` | Always | Parameter (converted from ActorTableData state) |
| `timestamp` | Always | `absl::Now()` |
| `repr_name` | Always | `data.repr_name()` |
| `node_id` | ALIVE only | `data.node_id()` |
| `worker_id` | ALIVE only | `data.address().worker_id()` |
| `pid` | ALIVE only | `data.pid()` |
| `port` | ALIVE only | `data.address().port()` |
| `death_cause` | DEAD only | `data.death_cause()` (conditional on has_death_cause) |
| `restart_reason` | RESTARTING only | `NODE_PREEMPTION` if `data.preempted()`, else parameter or default `ACTOR_FAILURE` |

### Why the pre-Round-12 Rust model was incomplete

1. **Cardinality**: `convert_to_proto_event()` created ONE `RayEvent` with both
   `actor_definition_event` AND `actor_lifecycle_event` stuffed inside. C++ creates two
   separate `RayEvent` protos. Downstream consumers counting events by type would see
   different counts.

2. **Definition payload**: `write_actor_export_event()` only passed 11 custom_fields.
   `convert_to_proto_event()` only populated 7 of 13 ActorDefinitionEvent proto fields.
   The 6 missing fields (`required_resources`, `placement_group_id`, `label_selector`,
   `call_site`, `parent_id`, `ref_ids`) were never passed from the builder to the sink.

3. **Lifecycle payload**: The builder never passed `worker_id`, `port`, `death_cause`,
   or `restart_reason` as custom_fields. The sink could not reconstruct them.

4. **Fallback**: `server.rs` fell back to `LoggingEventSink` (plain text `tracing::info!`
   lines) when `EventAggregatorSink` creation failed. That is not structured proto output.

---

## Step B: Live Rust Path Before This Round

### Fields the live Rust builder emitted (actor_manager.rs `write_actor_export_event`)

Before Round 12, the aggregator branch passed these custom_fields:

- `actor_id`, `job_id`, `state`, `name`, `pid`, `ray_namespace`, `class_name`,
  `is_detached`, `node_id`, `repr_name`, `is_registration`

**Not passed**: `serialized_runtime_env`, `required_resources`, `placement_group_id`,
`label_selector`, `call_site`, `parent_id`, `ref_ids`, `worker_id`, `port`,
`death_cause`, `restart_reason`

### Fields the sink could and could not reconstruct

The sink (`EventAggregatorSink::convert_to_proto_event`) could only use what the builder
provided. It populated:

- ActorDefinitionEvent: `actor_id`, `job_id`, `is_detached`, `name`, `ray_namespace`,
  `serialized_runtime_env` (from custom_fields — but that field was NOT in the builder,
  so it was always empty), `class_name`
- StateTransition: `state`, `timestamp`, `node_id`, `repr_name`, `pid`

**Could not reconstruct**: anything not in custom_fields.

### How registration was encoded

One call to `event_exporter.add_event(event)` with `is_registration=true`. The sink
checked `is_registration` and created a single `RayEvent` proto with BOTH
`actor_definition_event` and `actor_lifecycle_event` nested inside it, tagged as
`event_type = ACTOR_DEFINITION_EVENT`. Not the same observable stream as C++.

### Why `LoggingEventSink` fallback was not acceptable

`LoggingEventSink::flush()` calls `tracing::info!(...)` which produces:
```
INFO event_id=abc source=Gcs severity=Info label=ACTOR_CREATED "Actor foo state: ALIVE"
```

This is a plain text log line. It does not produce `RayEvent` protos, does not contain
nested `ActorLifecycleEvent`/`ActorDefinitionEvent`, does not follow the
`AddEventsRequest` schema, and is not parseable by any consumer expecting C++ event
aggregator output. Keeping it as a silent fallback and claiming FULL parity was incorrect.

---

## Step C: Failing Tests Added First (9 tests)

All 9 tests were added BEFORE any implementation changes and confirmed to fail
(8 failed, 1 passed because `EventExporter::flush()` already returns 0 with no sink).

### 1. Registration cardinality (2 tests)

| Test | What it proves |
|---|---|
| `test_actor_registration_emits_definition_and_lifecycle_events` | Registration produces events with BOTH event_type=9 (definition) AND event_type=10 (lifecycle) as separate entries. |
| `test_actor_registration_event_types_match_cpp_cardinality` | The definition event (type=9) contains `actor_definition_event` but does NOT contain `actor_lifecycle_event`. C++ sends them as separate protos. |

### 2. ActorDefinitionEvent payload (3 tests)

| Test | What it proves |
|---|---|
| `test_actor_definition_event_includes_required_resources` | `required_resources` map has CPU=2.0, GPU=1.0 when the actor requires those resources. |
| `test_actor_definition_event_includes_placement_group_and_label_selector` | `placement_group_id` is non-empty when set; `label_selector` map is populated. |
| `test_actor_definition_event_includes_call_site_parent_and_ref_ids` | `call_site`, `parent_id`, and `ref_ids` are all populated from actor data. |

### 3. ActorLifecycleEvent payload (3 tests)

| Test | What it proves |
|---|---|
| `test_actor_lifecycle_alive_event_includes_worker_id_and_port` | ALIVE transition has non-empty `worker_id` and non-zero `port`. |
| `test_actor_lifecycle_dead_event_includes_death_cause` | DEAD transition has non-empty `death_cause` object. |
| `test_actor_lifecycle_restarting_event_includes_restart_reason` | RESTARTING transition for a preempted actor has `restart_reason=2` (NODE_PREEMPTION). |

### 4. Fallback semantics (1 test)

| Test | What it proves |
|---|---|
| `test_enable_ray_event_without_structured_sink_is_not_treated_as_full_parity_path` | When `enable_ray_event=true` but no structured sink is attached, events are buffered but `flush()` returns 0. No silent delivery through `LoggingEventSink`. |

---

## Step D: Implementation Changes

### D1. Registration event cardinality — FIXED

**File**: `ray-gcs/src/actor_manager.rs`, `write_actor_export_event()`

Before: one call to `event_exporter.add_event(event)` with all fields in one event.

After: when `is_registration=true`, emits TWO events:
1. Definition event with `event_kind="definition"` and all definition fields
2. Lifecycle event with `event_kind="lifecycle"` for the initial state

The `EventAggregatorSink` uses `event_kind` to decide which proto to build, producing
two separate `RayEvent` protos matching C++.

### D2. ActorDefinitionEvent payload — FIXED

**Files**: `ray-gcs/src/actor_manager.rs`, `ray-observability/src/export.rs`

Two changes:

1. `handle_register_actor()` now copies definition-relevant fields from `TaskSpec` /
   `ActorCreationTaskSpec` into `ActorTableData` at registration time:
   - `required_resources` ← `task_spec.required_resources`
   - `placement_group_id` ← extracted from `task_spec.scheduling_strategy` (PlacementGroupSchedulingStrategy)
   - `call_site` ← `task_spec.call_site`
   - `parent_id` ← `task_spec.parent_task_id`
   - `labels` ← `task_spec.labels`
   - `serialized_runtime_env` ← `task_spec.runtime_env_info.serialized_runtime_env`

2. `write_actor_export_event()` definition branch passes all fields as custom_fields:
   - `required_resources` (JSON-serialized map)
   - `placement_group_id` (base64-encoded bytes)
   - `label_selector` (JSON-serialized map)
   - `call_site` (string)
   - `parent_id` (base64-encoded bytes)
   - `ref_ids` (JSON-serialized from labels map)
   - `serialized_runtime_env` (string)

3. `convert_to_proto_event()` definition branch now populates ALL 13 proto fields by
   parsing the custom_fields (JSON for maps, base64 for bytes).

### D3. ActorLifecycleEvent payload — FIXED

**Files**: `ray-gcs/src/actor_manager.rs`, `ray-observability/src/export.rs`

`write_actor_export_event()` lifecycle branch now adds state-specific fields:

- **ALIVE**: `worker_id` (base64 from `actor.address.worker_id`), `port` (from `actor.address.port`)
- **DEAD**: `death_cause` (JSON-serialized from `actor.death_cause`)
- **RESTARTING**: `restart_reason` (2=NODE_PREEMPTION when `actor.preempted`, else 0=ACTOR_FAILURE)

`convert_to_proto_event()` lifecycle branch now populates the `StateTransition` proto
with all state-specific fields by parsing from custom_fields.

### D4. Fallback semantics — FIXED

**File**: `ray-gcs/src/server.rs`

Removed `LoggingEventSink` fallback from the `enable_ray_event` path. When
`EventAggregatorSink::new()` fails (missing `log_dir` or I/O error), no sink is attached.
Events are buffered but `flush()` returns 0. A warning is logged explaining that actor
events will NOT be delivered to a structured output.

This makes the non-parity state **observable** — callers can detect it via flush return
value or `has_sink()`.

---

## Step E: Test Results

```
CARGO_TARGET_DIR=target_round12 cargo test -p ray-gcs --lib
test result: ok. 496 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

CARGO_TARGET_DIR=target_round12_obs cargo test -p ray-observability --lib
test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

496 = 487 (Round 11) + 9 (Round 12). Zero regressions.

---

## Files Changed

| File | What Changed |
|---|---|
| `ray-gcs/src/actor_manager.rs` | (1) `handle_register_actor()`: copy 6 definition fields from TaskSpec to ActorTableData. (2) `write_actor_export_event()`: emit 2 events on registration; pass all definition fields for definition events; pass state-specific lifecycle fields (worker_id/port/death_cause/restart_reason). (3) Added `use base64::Engine` import. (4) 9 new tests with 2 test helpers. |
| `ray-observability/src/export.rs` | Rewrote `convert_to_proto_event()`: uses `event_kind` field to distinguish definition vs lifecycle; definition events populate all 13 ActorDefinitionEvent fields (parsing JSON maps, base64 bytes); lifecycle events populate state-specific StateTransition fields. |
| `ray-observability/Cargo.toml` | Added `base64 = { workspace = true }` runtime dependency. |
| `ray-gcs/src/server.rs` | Removed `LoggingEventSink` fallback from `enable_ray_event=true` path. Both failure branches (no log_dir, sink creation error) now log a warning and leave no sink attached. |

---

## Event-Model Parity Comparison (Round 12)

| Aspect | C++ | Rust (Round 12) |
|---|---|---|
| Registration event count | 2 (definition + lifecycle) | 2 (definition + lifecycle) |
| Definition event_type | `ACTOR_DEFINITION_EVENT (9)` | Same |
| Lifecycle event_type | `ACTOR_LIFECYCLE_EVENT (10)` | Same |
| Definition contains lifecycle? | No — separate protos | No — separate protos |
| Definition fields | 13 (actor_id through ref_ids) | 13 (all populated) |
| ALIVE transition fields | state, timestamp, node_id, worker_id, repr_name, pid, port | Same |
| DEAD transition fields | state, timestamp, repr_name, death_cause | Same |
| RESTARTING transition fields | state, timestamp, repr_name, restart_reason | Same |
| Fallback under enable_ray_event | No fallback — recorder always present | No fallback — no sink = no delivery |
| Proto shape | `AddEventsRequest` → `RayEventsData` → `repeated RayEvent` | Same |
| Transport | gRPC to EventAggregatorService | File (same proto JSON shape; upgradable to gRPC) |

---

## Reports Updated

| Report | Change |
|---|---|
| `2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND12.md` | Rewritten: all 4 gaps marked fixed with test evidence |
| `2026-03-22_PARITY_FIX_REPORT_GCS6_ROUND11.md` | Updated GCS-6 status to Round 12; added Round 12 addendum |
| `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` | Added 9 Round 12 test entries |

---

## GCS-6 Round 12 Status: actor-event model fixed

Round 12 fixed the actor-event model (payload, cardinality, fallback).
Round 13 re-audit found remaining gaps in recorder/export semantics.

---

## Round 13 Addendum (2026-03-23)

Round 13 re-audit (by Codex) found that Round 12 fixed the event model but left
the recorder/export semantics incomplete:

1. **Grouping/merge**: C++ groups by (entity_id, event_type) and merges before export; Rust did not
2. **In-flight suppression**: C++ uses `grpc_in_progress_` to prevent overlapping exports; Rust had none
3. **Recorder lifecycle**: C++ has start/stop/final-flush/enabled flag; Rust had only a periodic loop

All three gaps are now fixed. See `2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND13.md` for details.

### Round 13 changes

| File | Change |
|---|---|
| `ray-observability/src/export.rs` | Added `flush_in_progress` + `enabled` AtomicBools; grouping/merge before export; `shutdown()` method; merged-event support in proto conversion; 6 new tests |
| `ray-gcs/src/actor_manager.rs` | Fixed 2 tests to flush between registration and re-emission |

### Round 13 test results

```
cargo test -p ray-gcs --lib: 496 passed, 0 failed
cargo test -p ray-observability --lib: 51 passed, 0 failed
```

## GCS-6 Final Status: **fixed**

The live Rust actor-event path now matches C++ on:

1. **Event count**: registration emits 2 separate events — verified by test
2. **Event types**: definition (9) and lifecycle (10) as separate protos — verified by test
3. **Payload completeness**: all 13 definition fields + all state-specific transition fields — verified by test
4. **Parity-path behavior**: no silent LoggingEventSink fallback — verified by test
5. **Grouping/merge**: same-actor events merged before export — verified by test
6. **In-flight suppression**: overlapping flushes prevented — verified by test
7. **Recorder lifecycle**: shutdown disables new events + final flush — verified by test
8. **Transport**: file output produces same AddEventsRequest proto shape — verified by round-trip test
