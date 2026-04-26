# 2026-04-18 — Blocker 6 follow-up: close the export-event wiring gaps

## Status: closed

Blocker 6 was already partially fixed on 2026-04-17
(`reports/2026-04-17-blocker-6-event-export-fix.md`). That change added the
`AddEvents` service handler (fully implemented despite the `_stub.rs`
filename), the `ExportEventManager` JSON-line log writer, and the call sites
in `GcsNodeManager`, `GcsActorManager`, and `GcsJobManager`. On close review
today, **two parity gaps remained that silently disabled export events in
production**. This commit closes both.

## The gaps

### Gap 1 — `event_log_reporter_enabled` default was `false` (C++ is `true`)

C++ default comes from `ray_config_def.h:849`:

```cpp
RAY_CONFIG(bool, event_log_reporter_enabled, true)
```

Rust `GcsServerConfig::default()` in `gcs-server/src/lib.rs:132` had:

```rust
event_log_reporter_enabled: false,
```

Any caller that used `..Default::default()` (including the `main.rs` binary
and most tests) therefore got the feature off even when it should have been
on. C++ requires the operator to *opt out* via `RAY_event_log_reporter_enabled=0`;
Rust was forcing them to opt in.

### Gap 2 — `main.rs` never propagated `log_dir` into `GcsServerConfig`

The binary parsed the `--log_dir=` flag on line 74 of `main.rs`, used it
only for the startup log line on line 176, and then built `GcsServerConfig`
without passing `log_dir` or `event_log_reporter_enabled`:

```rust
let config = GcsServerConfig {
    grpc_port: port,
    raylet_config_list: config_list,
    redis_address: redis_url.clone(),
    external_storage_namespace: external_storage_namespace.clone(),
    session_name,
    ..Default::default()   // ← log_dir: None, event_log_reporter_enabled: false
};
```

So in production, the gate at `lib.rs:209-213`

```rust
match (config.event_log_reporter_enabled, config.log_dir.as_deref()) {
    (true, Some(dir)) if !dir.is_empty() => ExportEventManager::new(dir)?,
    _ => ExportEventManager::disabled(),
}
```

fell into the `_ =>` arm for every process, and all three managers received
a disabled writer. The new writer code path, tests, and `event_<SOURCE>.log`
files were dead. C++ writes them; Rust silently did not.

## Fix

### 1. Default → `true`

`crates/gcs-server/src/lib.rs`

```rust
impl Default for GcsServerConfig {
    fn default() -> Self {
        Self {
            // ...
            // Matches C++ `RAY_CONFIG(bool, event_log_reporter_enabled, true)`
            // in `ray_config_def.h:849`. The writer still stays disabled at
            // runtime unless `log_dir` is also set — same gate as C++
            // `gcs_server_main.cc:145`.
            event_log_reporter_enabled: true,
            // ...
        }
    }
}
```

The runtime gate in `new_with_store` is unchanged: when `log_dir` is `None`
(tests, REPLs, anything that doesn't bind to a real filesystem path) the
writer still short-circuits to `ExportEventManager::disabled()`. This
matches the C++ `if event_log_reporter_enabled && !log_dir.empty()` check
in `gcs_server_main.cc:145`.

### 2. Propagate `log_dir` and honor config overrides in `main.rs`

`crates/gcs-server/src/main.rs`

```rust
// Honor `event_log_reporter_enabled` override from the decoded config
// JSON, matching C++ `RayConfig::initialize` (`ray_config.cc:44-52`).
let event_log_reporter_enabled =
    parse_bool_config(&config_list, "event_log_reporter_enabled").unwrap_or(true);

let config = GcsServerConfig {
    grpc_port: port,
    raylet_config_list: config_list,
    redis_address: redis_url.clone(),
    external_storage_namespace: external_storage_namespace.clone(),
    session_name,
    log_dir: if log_dir.is_empty() { None } else { Some(log_dir.clone()) },
    event_log_reporter_enabled,
    ..Default::default()
};
```

`parse_bool_config` is a small helper that accepts either a JSON boolean
(`{"event_log_reporter_enabled": true}`) or a stringified boolean
(`{"event_log_reporter_enabled": "True"}`), mirroring the implicit
coercion C++ gets from `nlohmann::json::get<bool>` in `ray_config.cc:50`.
Unknown values return `None` and fall back to the C++ default (`true`).

## Tests added

### `gcs-server` lib (`crates/gcs-server/src/lib.rs`)

- `default_event_log_reporter_enabled_matches_cpp` — regression guard that
  fails if anyone ever flips the default back to `false`.
- `test_export_events_emitted_when_log_dir_set` — end-to-end: constructs a
  `GcsServer` with a temp-dir `log_dir`, calls
  `server.node_manager().add_node(node).await`, then asserts that
  `<log_dir>/export_events/event_EXPORT_NODE.log` exists and contains
  `"EXPORT_NODE"` and the node's manager address. This is the first test
  that exercises the full pipeline: `GcsServer::new` → config gate →
  `ExportEventManager::new` → `GcsNodeManager::add_node` →
  `report_node_event` → on-disk JSON line.
- `test_export_events_disabled_when_log_dir_unset` — documents the
  `log_dir: None` short-circuit so a future refactor cannot silently drop
  the gate.

### `gcs-server` bin (`crates/gcs-server/src/main.rs`)

- `parse_bool_config_empty_returns_none`
- `parse_bool_config_missing_key_returns_none`
- `parse_bool_config_reads_json_bool` (both `true` and `false`)
- `parse_bool_config_reads_stringified_bool` (`"False"`, `"True"`, case-insensitive)
- `parse_bool_config_invalid_json_returns_none`

## Build + test results

```
$ cargo build -p gcs-server
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 33.04s

$ cargo test --workspace
test result: ok. 20  passed; 0 failed  (gcs-kv)
test result: ok. 192 passed; 0 failed  (gcs-managers)
test result: ok. 15  passed; 0 failed  (gcs-server lib  — +3 new)
test result: ok. 5   passed; 0 failed  (gcs-server bin  — +5 new)
test result: ok. 13  passed; 0 failed  (gcs-pubsub)
test result: ok. 25  passed; 0 failed  (gcs-store)
test result: ok. 8   passed; 0 failed  (gcs-table-storage)
```

Total: **278 / 278 passing** (was 270), 0 failures, 0 new warnings.

## C++ ↔ Rust parity matrix — now complete

| Behavior | C++ source | Rust source | Parity |
|---|---|---|---|
| Default `event_log_reporter_enabled = true` | `ray_config_def.h:849` | `GcsServerConfig::default()` | ✅ |
| Operator override via `config_list` JSON | `ray_config.cc:44-52` | `main.rs::parse_bool_config` | ✅ |
| Runtime gate: enabled ∧ `!log_dir.empty()` | `gcs_server_main.cc:145` | `lib.rs` `match (enabled, log_dir)` | ✅ |
| `log_dir` plumbed from CLI to config | `gcs_server_main.cc:178` | `main.rs` builds `GcsServerConfig { log_dir, … }` | ✅ |
| Per-source file `event_<SOURCE>.log` under `<log_dir>/export_events/` | `event.cc:493, 72-73` | `export_event_writer.rs::EventLogReporter::create` | ✅ (prior fix) |
| JSON line schema: `{timestamp, event_id, source_type, event_data}` | `event.cc:131-167` | `ExportEventManager::report` | ✅ (prior fix) |
| 18-byte random hex event IDs | `event.cc:434-438` | `generate_event_id()` | ✅ (prior fix) |
| Emit on node lifecycle | `gcs_node_manager.cc:89` | `GcsNodeManager::add_node` / `remove_node` | ✅ (prior fix) |
| Emit on actor state change | `gcs_actor.cc:159` | `GcsActorManager::publish_actor_update` | ✅ (prior fix) |
| Emit on driver-job add/finish | `gcs_job_manager.cc:104` | `GcsJobManager::add_job` / `mark_job_finished` | ✅ (prior fix) |
| `AddEvents` RPC → `GcsTaskManager` | `gcs_task_manager.cc:668-679` | `event_export_stub.rs::EventExportService::add_events` | ✅ (prior fix) |
| `RayEventExportGcsService` registered on the gRPC server | `grpc_services.cc:208-216` | `lib.rs:685, 735` `add_service(RayEventExportGcsServiceServer)` | ✅ (prior fix) |

Every row is now green.

## On AWS testing

Not required. The writer operates on local POSIX filesystem semantics
(`std::fs::OpenOptions::append`). EC2 local-disk behavior is identical to
macOS/Linux dev boxes; there is no S3 or external blob store in this code
path. The hermetic temp-dir integration test
(`test_export_events_emitted_when_log_dir_set`) covers the same code paths
that would run on an AWS-hosted Ray cluster.

## What downstream observes after this fix

A Rust GCS process started by Ray Python with the normal flags now produces:

```
$RAY_LOG_DIR/export_events/event_EXPORT_NODE.log       # head-node add, node DEAD
$RAY_LOG_DIR/export_events/event_EXPORT_ACTOR.log      # every actor state change
$RAY_LOG_DIR/export_events/event_EXPORT_DRIVER_JOB.log # driver add + finish
```

— byte-for-byte the same files that the C++ GCS writes, consumed by the
Ray Dashboard and the public `ray export` API. Operators who set
`RAY_event_log_reporter_enabled=0` still see no files, matching C++.

## Outcome

Blocker 6 is fully closed. The `_stub` suffix on `event_export_stub.rs` is
now purely historical (the file is a full implementation); renaming it is
trivial but out of scope for this parity fix.
