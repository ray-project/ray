# Closure Response to Codex Raylet Parity Verification Report

**Date:** 2026-03-28
**Subject:** Point-by-point response to `2026-03-28_CODEX_RAYLET_PARITY_VERIFICATION_REPORT.md`
**Branch:** `cc-to-rust-experimental`

---

## Executive Summary

All 7 findings from the Codex verification report have been addressed. The 3 HIGH merge-blocking issues (session_dir naming, runtime-env protocol, worker command parsing) are fixed. The 4 MEDIUM issues (metrics port propagation, CLI defaults, command-line parsing, metrics collection config) are also fixed.

**Test results:** 443 unit tests + 24 integration tests = **467 passed, 0 failed, 0 ignored**.

---

## Finding 1 (HIGH): session_dir port-file naming -- FIXED

**Problem:** Rust read `{session_dir}/ports/{port_name}` but C++ uses `{session_dir}/{port_name}_{node_id_hex}`.

**Fix in `agent_manager.rs`:**
- Changed path from `session_dir.join("ports").join(port_name)` to `session_dir.join(format!("{}_{}", port_name, node_id))`
- Renamed `_node_id` parameter to `node_id` (now used in filename)
- Changed poll interval from 100ms to 50ms (C++ default)
- Changed default timeout to 15s (C++ `kDefaultPortWaitTimeoutMs`)

**Fix in `node_manager.rs` `resolve_all_ports()`:**
- Updated port names to match C++ constants with `_port` suffix:
  - `"metrics_agent"` -> `"metrics_agent_port"`
  - `"metrics_export"` -> `"metrics_export_port"`
  - `"dashboard_agent_listen"` -> `"dashboard_agent_listen_port"`
  - `"runtime_env_agent"` -> `"runtime_env_agent_port"`
- Timeout changed to 15s matching C++

**Tests rewritten:** All tests now use the C++ naming convention (`{port_name}_{node_id_hex}` in session_dir root, no `ports/` subdirectory). Added `test_cpp_port_file_naming_compatibility` explicitly documenting the C++ contract.

---

## Finding 2 (HIGH): Runtime-env agent protocol -- FIXED

**Problem:** Rust sent JSON; C++ and the Python agent use serialized protobuf with `application/octet-stream`.

**Fix in `runtime_env_agent_client.rs` (complete rewrite):**
- **Protobuf serialization:** Requests now build `proto::GetOrCreateRuntimeEnvRequest` / `proto::DeleteRuntimeEnvIfPossibleRequest` and serialize via `prost::Message::encode_to_vec()`
- **Protobuf response parsing:** Responses decoded via `prost::Message::decode()`, status checked against `AgentRpcStatus::Ok`
- **Content-Type:** Changed from `application/json` to `application/octet-stream`
- **Auth header:** Added `auth_token: Option<String>` to config; when set, sends `Authorization: Bearer {token}` header
- **`source_process` field:** Both request types set `source_process: "raylet"` matching C++
- **`job_id` as hex bytes:** Set to `job_id.hex().into_bytes()` matching C++ `request.set_job_id(job_id.Hex())`
- **`RuntimeEnvConfig` parsing:** Added helper that deserializes the config JSON into the proto message
- **Binary HTTP transport:** Replaced `http_post(&str) -> String` with `http_post_bytes(&[u8]) -> Vec<u8>`

**Trait preserved:** `RuntimeEnvAgentClientTrait` signature unchanged; all callers and mock implementations work without changes.

**Tests:** 11 unit tests covering protobuf round-trip, reply deserialization, auth token, config parsing.

---

## Finding 3 (HIGH): python_worker_command parsing -- FIXED

**Problem:** Rust treated the command as a single program path and hardcoded `-m ray.worker`. C++ parses it via `ParseCommandLine()` into a token vector.

**Fix in `worker_spawner.rs`:**
- Imported `parse_command_line` from `agent_manager` (shared POSIX parser)
- Default changed from `"python"` to `"python -m ray.worker"` (full command)
- Command is now parsed into argv tokens: `parsed[0]` = program, `parsed[1..]` = base args
- Removed hardcoded `-m ray.worker` -- the parsed command already contains it
- Worker-specific args (`--node-ip-address`, `--node-manager-port`, etc.) still appended after parsed base args

**Tests added:**
- `test_parse_simple_python_command`: `"python -m ray.worker"` parses correctly
- `test_parse_command_with_flags`: `/usr/bin/python -u -W ignore -m ray.worker` preserves all tokens
- `test_parse_command_with_quoted_path`: `"/path/to/my python" -m ray.worker` handles quoted paths
- `test_default_python_worker_command`: `None` defaults to `"python -m ray.worker"`

---

## Finding 4 (MEDIUM): Unresolved metrics_export_port -- FIXED

**Problem:** `WorkerSpawnerConfig` was built before `resolve_all_ports()`, so workers got the pre-resolution value.

**Fix in `node_manager.rs`:**
- Moved entire `WorkerSpawnerConfig` construction, `set_start_worker_callback()`, cgroup setup, and prestart workers to AFTER `resolve_all_ports().await`
- Changed `metrics_export_port` to read from `self.resolved_metrics_export_port.load(Ordering::Acquire) as u16` instead of `self.config.metrics_export_port`

---

## Finding 5 (MEDIUM): CLI default alignment -- FIXED

**Problem:** `metrics_export_port` defaults to 0 (C++: 1).

**Fix in `main.rs`:**
- Changed `metrics_export_port` default from `0` to `1` (matching C++ gflags default)
- Other signed defaults (`metrics_agent_port`, `object_manager_port`): C++ uses -1 as "not set" but `ray start` always provides explicit values, so 0 is acceptable for u16. Documented.

---

## Finding 6 (MEDIUM): Command-line parsing robustness -- FIXED

**Problem:** Homegrown quote-toggle parser in `agent_manager.rs` lacked C++ POSIX shell semantics.

**Fix in `agent_manager.rs` `parse_command_line()`:**
- Rewrote using `Peekable<Chars>` iterator for proper lookahead
- Added **backslash escaping** outside quotes: `\` escapes the next character
- Added **single-quote support**: everything inside is literal, no escape processing
- Added **double-quote backslash handling**: `\` only escapes `"` and `\` inside double quotes; other `\X` sequences preserved literally

**Tests added:**
- `test_parse_command_line_backslash_escaping_space`: `hello\ world` -> `["hello world"]`
- `test_parse_command_line_mixed_quotes`: `"hello 'world'"` -> `["hello 'world'"]`
- `test_parse_command_line_backslash_in_double_quotes_escapes`: proper escape handling

---

## Finding 7 (MEDIUM): enable_metrics_collection -- FIXED

**Problem:** Rust hardcoded `enable_metrics = true`; C++ reads from `RayConfig` and conditionally appends `--disable-metrics-collection`.

**Fix in `config.rs`:**
- Added `enable_metrics_collection: bool` to `RayConfig` (default `true`)
- Added JSON parsing and environment variable override support

**Fix in `node_manager.rs`:**
- Replaced hardcoded `let enable_metrics = true;` with `let enable_metrics = config.ray_config.enable_metrics_collection;`
- `create_dashboard_agent_manager()` already conditionally appends `--disable-metrics-collection` when `enable_metrics` is false

---

## Updated Status Table

| Area | Previous Status | New Status |
|------|-----------------|------------|
| GCS `enable_ray_event` | Closed | **Closed** |
| Agent subprocess monitoring | Improved | **Closed** |
| `session_dir` persisted-port resolution | Open | **Closed** (C++ naming contract) |
| Runtime-env agent compatibility | Open | **Closed** (protobuf + auth) |
| Runtime-env startup sequencing | Partially improved | **Closed** |
| Python worker command parity | Open | **Closed** (parsed argv) |
| Metrics port propagation | Open | **Closed** (post-resolution config) |
| CLI defaults | Mismatched | **Closed** (metrics_export_port=1) |
| Command-line parsing | Weak | **Closed** (POSIX semantics) |
| Metrics collection config | Missing | **Closed** (enable_metrics_collection) |
| Object-store live runtime | Partial | **Partial** (unchanged) |
| Metrics exporter equivalence | Architecturally different | **Architecturally different** (unchanged) |

---

## Verification

```
$ cargo check -p ray-raylet
    Finished `dev` profile [unoptimized + debuginfo] target(s)

$ cargo test -p ray-raylet --lib
test result: ok. 443 passed; 0 failed; 0 ignored

$ cargo test -p ray-raylet --test integration_test
test result: ok. 24 passed; 0 failed; 0 ignored
```

All 467 tests pass, 0 failures, 0 ignored.
