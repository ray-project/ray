# Backend Port Re-Audit After GCS-6 Round 9

Date: 2026-03-22
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-22_PARITY_FIX_REPORT_GCS6_ROUND8.md`

## Bottom Line

Round 9 closes all remaining gaps from the Round 8 re-audit.

The Rust actor export/event feature now matches the full C++ configurable contract:
1. Config-model parity: `enable_export_api_write`, `enable_export_api_write_config`, `enable_ray_event`
2. Output-channel parity: `FileExportEventSink` writes JSON to `{log_dir}/export_events/event_EXPORT_ACTOR.log`
3. Payload parity: all 16 `ExportActorData` proto fields populated
4. Aggregator-path parity: `enable_ray_event=true` buffers `RayEvent` and bypasses file export (matching C++ branch logic)

## Round 8 Gaps (from re-audit) → Round 9 Status

| Gap | Round 8 Status | Round 9 Status |
|---|---|---|
| Config model: single boolean vs 3 C++ flags | **open** | **fixed**: `ExportApiConfig` with `enable_export_api_write`, `enable_export_api_write_config`, `enable_ray_event` |
| Output: in-memory buffer only, no file sink | **open** | **fixed**: `FileExportEventSink` writes JSON-per-line to `export_events/event_EXPORT_ACTOR.log` |
| Payload: 11 string fields vs 16 proto fields | **open** | **fixed**: `write_actor_export_event_to_file` builds `ExportActorData` proto with all 16 fields |
| Aggregator path: no `enable_ray_event` equivalent | **open** | **fixed**: `enable_ray_event=true` → aggregator path → bypasses file export |

## C++ Feature Contract (extracted from source)

### Config flags (`ray_config_def.h`)

| Flag | Type | Default | Semantics |
|---|---|---|---|
| `enable_export_api_write` | bool | false | Global enable for ALL export types to file |
| `enable_export_api_write_config` | Vec<String> | {} | Selective per-type enable; ignored when global is true |
| `enable_ray_event` | bool | false | Enables aggregator-path events; bypasses file export |

### Two output paths (mutually exclusive)

1. **Export API path** (`gcs_actor.cc` lines 130-160): `ExportActorData` proto → `RayExportEvent::SendEvent()` → `EventManager::PublishExportEvent()` → `LogEventReporter` → `{log_dir}/export_events/event_EXPORT_ACTOR.log`
2. **Aggregator path** (`gcs_actor.cc` lines 109-128): `RayActorLifecycleEvent` → `ray_event_recorder_.AddEvents()` → gRPC to dashboard

### ExportActorData fields (16)

actor_id, job_id, state, is_detached, name, pid, ray_namespace, serialized_runtime_env, class_name, death_cause, required_resources, node_id, placement_group_id, repr_name, labels, label_selector

### 6 emission points (`gcs_actor_manager.cc`)

| Line | Transition | is_registration |
|---|---|---|
| 761 | Registration | true |
| 867 | PENDING_CREATION | false |
| 1145 | Death/cancellation | false |
| 1523 | RESTARTING | false |
| 1572 | Death in restart | false |
| 1686 | ALIVE (creation success) | false |

## Rust Implementation (Round 9)

### Config model (`ActorExportConfig` in `actor_manager.rs`)

```rust
pub struct ActorExportConfig {
    pub export_api_config: ExportApiConfig,
}
```

Where `ExportApiConfig` (in `export.rs`) contains:
```rust
pub struct ExportApiConfig {
    pub enable_export_api_write: bool,           // C++ parity
    pub enable_export_api_write_config: Vec<String>,  // C++ parity
    pub enable_ray_event: bool,                  // C++ parity
    pub log_dir: Option<PathBuf>,                // for file sink
}
```

Methods:
- `is_export_enabled_for(source_type)` — mirrors `IsExportAPIEnabledSourceType()`
- `is_actor_export_enabled()` — checks global OR selective config
- `is_ray_event_enabled()` — checks aggregator path flag

### Output channels

1. **File export** (`FileExportEventSink` in `export.rs`): writes `ExportEvent` proto as JSON-per-line to `{log_dir}/export_events/event_EXPORT_ACTOR.log`
2. **Aggregator path** (`EventExporter`): buffers `RayEvent` objects for periodic gRPC flush

### Payload (`write_actor_export_event_to_file` in `actor_manager.rs`)

Builds `ExportActorData` proto with ALL 16 fields:
1. `actor_id` — from `ActorTableData`
2. `job_id` — from `ActorTableData`
3. `state` — converted via `ExportActorState` enum
4. `is_detached` — from `ActorTableData`
5. `name` — from `ActorTableData`
6. `pid` — from `ActorTableData`
7. `ray_namespace` — from `ActorTableData`
8. `serialized_runtime_env` — from `ActorTableData`
9. `class_name` — from `ActorTableData`
10. `death_cause` — from `ActorTableData`
11. `required_resources` — from `ActorTableData`
12. `node_id` — from `ActorTableData`
13. `placement_group_id` — from `ActorTableData`
14. `repr_name` — from `ActorTableData`
15. `labels` — from `actor_task_specs` (C++ uses `task_spec_->labels()`)
16. `label_selector` — from `ActorTableData`

### Branch logic in `write_actor_export_event`

```
if enable_ray_event:
    → aggregator path (RayEvent → event_exporter)
    → return (bypass file export — matches C++)
elif is_actor_export_enabled:
    → build ExportActorData proto with 16 fields
    → wrap in ExportEvent proto
    → write JSON to file via FileExportEventSink
    → also buffer in event_exporter for programmatic access
```

## Tests Added (Round 9)

### ray-observability (6 new tests)

| Test | Proves |
|---|---|
| `test_export_api_config_default_all_disabled` | Default config: all flags false |
| `test_export_api_config_global_enables_all_types` | `enable_export_api_write=true` enables all types |
| `test_export_api_config_selective_enables_actor_only` | `enable_export_api_write_config=["EXPORT_ACTOR"]` enables only actor |
| `test_export_api_config_ray_event_path` | `enable_ray_event=true` is independent of export API |
| `test_file_export_event_sink_creates_directory_and_writes` | File created, JSON valid, payload correct |
| `test_file_export_event_sink_appends_multiple_events` | Multiple events append as separate lines |

### ray-gcs (4 new tests — matching required test names)

| Test | Proves |
|---|---|
| `test_actor_export_event_emitted_to_file_when_export_api_enabled` | File output at `export_events/event_EXPORT_ACTOR.log` with valid JSON |
| `test_actor_export_event_honors_export_api_write_config_actor_only` | Selective config enables actor events; non-actor config does not |
| `test_actor_export_event_payload_matches_expected_fields` | All 16 ExportActorData fields present in file output |
| `test_actor_ray_event_path_emits_when_enable_ray_event_is_enabled` | Aggregator path buffers events; file export bypassed |

## Files Changed

| File | What Changed |
|---|---|
| `ray-observability/src/export.rs` | Added `ExportApiConfig`, `FileExportEventSink`, 6 tests |
| `ray-observability/Cargo.toml` | Added `hex` dev-dependency |
| `ray-gcs/src/actor_manager.rs` | Rewrote `ActorExportConfig` (3 flags), `write_actor_export_event` (two-path branch), `write_actor_export_event_to_file` (16-field proto), `file_export_sink` field, 4 tests |
| `ray-gcs/Cargo.toml` | Added `uuid` dependency |

## Test Results

```
CARGO_TARGET_DIR=target_round9 cargo test -p ray-gcs --lib
test result: ok. 479 passed; 0 failed; 0 ignored

CARGO_TARGET_DIR=target_round9 cargo test -p ray-observability --lib
test result: ok. 45 passed; 0 failed; 0 ignored
```

## Final Status: **fixed**

All 4 Round 8 re-audit gaps are closed:
1. Config-model parity ✓ (3 flags matching C++)
2. Output-channel parity ✓ (file sink writes JSON to `event_EXPORT_ACTOR.log`)
3. Payload parity ✓ (all 16 `ExportActorData` proto fields)
4. Aggregator-path parity ✓ (`enable_ray_event` branch bypasses file export)
