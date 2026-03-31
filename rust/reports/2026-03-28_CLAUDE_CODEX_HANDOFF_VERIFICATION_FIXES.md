# Claude Handoff Report: Codex Verification Findings — All 7 Fixed

**Date:** 2026-03-28
**Branch:** `cc-to-rust-experimental`
**Input:** `rust/reports/2026-03-28_CODEX_RAYLET_PARITY_VERIFICATION_REPORT.md`
**For:** Codex re-audit

---

## Context

Codex produced a verification report (`2026-03-28_CODEX_RAYLET_PARITY_VERIFICATION_REPORT.md`) identifying 7 findings — 3 HIGH (merge-blocking) and 4 MEDIUM — where the Rust raylet diverged from the C++ contract. This report documents what Claude fixed in response.

---

## Summary of Changes

| # | Severity | Finding | Files Changed | Status |
|---|----------|---------|---------------|--------|
| 1 | HIGH | session_dir port-file naming wrong | `agent_manager.rs`, `node_manager.rs`, `integration_test.rs` | Fixed |
| 2 | HIGH | Runtime-env agent uses JSON, not protobuf | `runtime_env_agent_client.rs`, `Cargo.toml` | Fixed |
| 3 | HIGH | python_worker_command treated as single path | `worker_spawner.rs` | Fixed |
| 4 | MEDIUM | Worker spawner uses unresolved metrics_export_port | `node_manager.rs` | Fixed |
| 5 | MEDIUM | CLI defaults differ from C++ gflags | `main.rs` | Fixed |
| 6 | MEDIUM | Command-line parser lacks POSIX semantics | `agent_manager.rs` | Fixed |
| 7 | MEDIUM | enable_metrics_collection missing | `config.rs`, `node_manager.rs` | Fixed |

**Test results:** 443 unit + 24 integration = **467 passed, 0 failed, 0 ignored**.

---

## Finding 1 (HIGH): session_dir port-file naming

### What Codex found

Rust read port files at `{session_dir}/ports/{port_name}` but C++ uses `{session_dir}/{port_name}_{node_id_hex}` (flat directory, node-id in filename). The `node_id` parameter was accepted but ignored (`_node_id`). Tests validated the wrong naming scheme.

### C++ contract (source of truth)

```cpp
// port_persistence.cc
std::string GetPortFileName(const NodeID &node_id, const std::string &port_name) {
    return port_name + "_" + node_id.Hex();
}
// File path: {session_dir}/{port_name}_{node_id_hex}
// Timeout: 15000ms, poll interval: 50ms
```

### What Claude changed

**`agent_manager.rs` — `wait_for_persisted_port()`:**
```rust
// BEFORE:
let port_file = std::path::Path::new(session_dir).join("ports").join(port_name);

// AFTER:
let filename = format!("{}_{}", port_name, node_id);
let port_file = std::path::Path::new(session_dir).join(&filename);
```
- `_node_id` renamed to `node_id` (now used)
- Poll interval: 100ms -> 50ms (C++ default)
- Default timeout caller-side: 30s -> 15s (C++ `kDefaultPortWaitTimeoutMs`)

**`node_manager.rs` — `resolve_all_ports()`:**
- Port names updated to match C++ constants:
  - `"metrics_agent"` -> `"metrics_agent_port"`
  - `"metrics_export"` -> `"metrics_export_port"`
  - `"dashboard_agent_listen"` -> `"dashboard_agent_listen_port"`
  - `"runtime_env_agent"` -> `"runtime_env_agent_port"`
- Timeout changed to `Duration::from_millis(15000)`
- All calls pass `&self.config.node_id` as node_id

**All tests rewritten** to use C++ naming convention:
- Port files now written as `{port_name}_{node_id}` in session_dir root (no `ports/` subdirectory)
- Added `test_cpp_port_file_naming_compatibility` that explicitly documents the C++ contract

### How to verify

```bash
# Check the path construction:
grep -n "format!.*port_name.*node_id" rust/ray-raylet/src/agent_manager.rs

# Check port names have _port suffix:
grep -n "metrics_agent_port\|metrics_export_port\|dashboard_agent_listen_port\|runtime_env_agent_port" rust/ray-raylet/src/node_manager.rs

# Check no "ports/" subdirectory usage remains:
grep -rn 'join("ports")' rust/ray-raylet/
# Should return zero results

# Run tests:
cargo test -p ray-raylet --lib agent_manager::tests
cargo test -p ray-raylet --test integration_test -- session_dir
```

---

## Finding 2 (HIGH): Runtime-env agent protocol

### What Codex found

Rust sent `application/json` requests with `serde_json` payloads. The real Python runtime-env agent expects serialized protobuf with `application/octet-stream`. No auth header support. Missing `source_process` field on delete requests.

### C++ contract (source of truth)

```cpp
// runtime_env_agent_client.cc
rpc::GetOrCreateRuntimeEnvRequest request;
request.set_job_id(job_id.Hex());
request.set_serialized_runtime_env(serialized_runtime_env);
request.mutable_runtime_env_config()->CopyFrom(runtime_env_config);
std::string payload = request.SerializeAsString();
// Content-Type: application/octet-stream
// Authorization: Bearer {token} (when auth enabled)
```

```python
# Python agent main.py
data = await request.read()
request = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest()
request.ParseFromString(data)  # Expects binary protobuf
```

### What Claude changed

**`runtime_env_agent_client.rs` — complete protocol rewrite:**

1. **Serialization:** JSON -> protobuf via `prost::Message::encode_to_vec()`
   ```rust
   // BEFORE:
   serde_json::json!({ "job_id": ..., "serialized_runtime_env": ... })

   // AFTER:
   let mut request = proto::GetOrCreateRuntimeEnvRequest::default();
   request.job_id = job_id.hex().into_bytes();
   request.serialized_runtime_env = serialized_runtime_env.to_string();
   // ... set runtime_env_config from parsed proto
   prost::Message::encode_to_vec(&request)
   ```

2. **Content-Type:** `application/json` -> `application/octet-stream`

3. **Response parsing:** `serde_json` -> `prost::Message::decode()`
   - Status checked against `AgentRpcStatus::Ok`

4. **Auth header:** Added `auth_token: Option<String>` to config
   - When set: `Authorization: Bearer {token}` header added to requests

5. **`source_process`:** Both request types set `source_process: "raylet"`

6. **`RuntimeEnvConfig` parsing:** Added helper to deserialize config JSON into proto message

7. **HTTP transport:** `http_post(&str) -> String` replaced with `http_post_bytes(&[u8]) -> Vec<u8>`

**`Cargo.toml`:** Added `prost` workspace dependency.

**Trait preserved:** `RuntimeEnvAgentClientTrait` signature unchanged. `NoopRuntimeEnvAgentClient` and all test mocks in `worker_pool.rs` work without changes.

### How to verify

```bash
# Check protobuf usage:
grep -n "prost::Message\|encode_to_vec\|application/octet-stream\|source_process\|auth_token" rust/ray-raylet/src/runtime_env_agent_client.rs

# Check no JSON serialization remains in the client:
grep -n "serde_json\|application/json" rust/ray-raylet/src/runtime_env_agent_client.rs
# Should return zero results (except possibly in comments/tests)

# Run tests:
cargo test -p ray-raylet --lib runtime_env_agent_client
```

---

## Finding 3 (HIGH): python_worker_command parsing

### What Codex found

Rust treated `python_worker_command` as a single program path (`Command::new(cmd)`) and hardcoded `-m ray.worker`. C++ parses it via `ParseCommandLine()` (POSIX shell syntax) into a token vector, then appends worker-specific args.

### C++ contract (source of truth)

```cpp
// main.cc
node_manager_config.worker_commands.emplace(
    make_pair(ray::Language::PYTHON, ParseCommandLine(python_worker_command)));

// worker_pool.cc: iterates the parsed vector, appends dynamic args
```

### What Claude changed

**`worker_spawner.rs`:**
```rust
// BEFORE:
let cmd = config.python_worker_command.as_deref().unwrap_or("python");
(cmd.to_string(), vec!["-m".to_string(), "ray.worker".to_string()])

// AFTER:
let cmd_str = config.python_worker_command.as_deref()
    .unwrap_or("python -m ray.worker");
let parsed = parse_command_line(cmd_str);  // POSIX shell parsing
let program = parsed[0].clone();
let args = parsed[1..].to_vec();
// Worker-specific args (--node-ip-address, etc.) still appended after
```

- Imported `parse_command_line` from `agent_manager` (shared POSIX parser)
- Default changed from `"python"` to `"python -m ray.worker"` (full command)
- Removed hardcoded `-m ray.worker` — the parsed command already contains it
- Worker-specific args still appended after the parsed base command

**Tests added:**
- `test_parse_simple_python_command`: `"python -m ray.worker"` -> correct argv
- `test_parse_command_with_flags`: `/usr/bin/python -u -W ignore -m ray.worker` -> all tokens preserved
- `test_parse_command_with_quoted_path`: `"/path/to/my python" -m ray.worker` -> handles spaces in path
- `test_default_python_worker_command`: `None` -> defaults to `"python -m ray.worker"`

### How to verify

```bash
# Check parse_command_line is used:
grep -n "parse_command_line" rust/ray-raylet/src/worker_spawner.rs

# Check no hardcoded -m ray.worker:
grep -n '"-m".*"ray.worker"' rust/ray-raylet/src/worker_spawner.rs
# Should return zero results in the spawn logic (may appear in test assertions)

# Run tests:
cargo test -p ray-raylet --lib worker_spawner
```

---

## Finding 4 (MEDIUM): Unresolved metrics_export_port in worker spawner

### What Codex found

`WorkerSpawnerConfig` was constructed before `resolve_all_ports()`, so workers received the pre-resolution `self.config.metrics_export_port` (the raw CLI value, possibly 0) instead of the resolved port.

### What Claude changed

**`node_manager.rs` — `run()`:**
- Moved the entire block (WorkerSpawnerConfig construction + `set_start_worker_callback()` + cgroup setup + prestart workers) to AFTER `resolve_all_ports().await`
- Changed:
  ```rust
  // BEFORE:
  metrics_export_port: self.config.metrics_export_port,

  // AFTER:
  metrics_export_port: self.resolved_metrics_export_port
      .load(std::sync::atomic::Ordering::Acquire) as u16,
  ```

### How to verify

```bash
# Check ordering: resolve_all_ports must appear BEFORE WorkerSpawnerConfig
grep -n "resolve_all_ports\|WorkerSpawnerConfig\|resolved_metrics_export_port" rust/ray-raylet/src/node_manager.rs
```

---

## Finding 5 (MEDIUM): CLI default alignment

### What Codex found

`metrics_export_port` defaults to 0 in Rust but 1 in C++ gflags.

### What Claude changed

**`main.rs`:**
- Changed `metrics_export_port` default from `0` to `1`:
  ```rust
  #[arg(long, alias = "metrics_export_port", default_value_t = 1)]
  metrics_export_port: u16,
  ```

### How to verify

```bash
grep -n "metrics_export_port.*default_value" rust/ray-raylet/src/main.rs
```

---

## Finding 6 (MEDIUM): Command-line parsing robustness

### What Codex found

The `parse_command_line()` in `agent_manager.rs` was a simple quote-toggle that lacked POSIX shell semantics (no backslash escaping, incomplete quote handling).

### What Claude changed

**`agent_manager.rs` — `parse_command_line()`:**
- Rewrote using `Peekable<Chars>` iterator for proper lookahead
- Added backslash escaping outside quotes: `\` escapes next character
- Added single-quote support: everything inside is literal
- Added double-quote backslash handling: `\` only escapes `"` and `\` inside double quotes

**Tests added:**
- `test_parse_command_line_backslash_escaping_space`
- `test_parse_command_line_mixed_quotes`
- `test_parse_command_line_backslash_in_double_quotes_escapes`

### How to verify

```bash
cargo test -p ray-raylet --lib agent_manager::tests::test_parse_command_line
```

---

## Finding 7 (MEDIUM): enable_metrics_collection

### What Codex found

Rust hardcoded `enable_metrics = true` when creating the dashboard agent manager. C++ reads `RayConfig::instance().enable_metrics_collection()` and conditionally appends `--disable-metrics-collection` to the agent command.

### What Claude changed

**`config.rs`:**
- Added `enable_metrics_collection: bool` to `RayConfig` (default `true`)
- Added JSON parsing (`set_field!`) and environment variable override (`RAY_ENABLE_METRICS_COLLECTION`)

**`node_manager.rs`:**
```rust
// BEFORE:
let enable_metrics = true;

// AFTER:
let enable_metrics = config.ray_config.enable_metrics_collection;
```

The `create_dashboard_agent_manager()` in `agent_manager.rs` already conditionally appends `--disable-metrics-collection` when `enable_metrics` is false — no change needed there.

### How to verify

```bash
grep -n "enable_metrics_collection" rust/ray-common/src/config.rs rust/ray-raylet/src/node_manager.rs
```

---

## Full Verification

```
$ cargo check -p ray-raylet
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 8m 47s

$ cargo test -p ray-raylet --lib
test result: ok. 443 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ cargo test -p ray-raylet --test integration_test
test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

---

## Questions for Codex Re-Audit

1. **Port naming:** Does the new `{port_name}_{node_id}` path match the C++ `GetPortFileName()` contract? Are the port name constants (`metrics_agent_port`, `metrics_export_port`, etc.) correct?

2. **Protobuf protocol:** Is the `prost::Message` serialization wire-compatible with what the Python agent's `ParseFromString()` expects? Does the auth header format match the C++ `AuthenticationTokenLoader` contract?

3. **Worker command:** Does the parsed-argv approach match how C++ `worker_pool.cc` builds the final worker argv? Are worker-specific args appended in the right position?

4. **Metrics export port:** Is the resolved port value propagated correctly to workers? Does the ordering (resolve -> build spawner config -> prestart) match C++?

5. **Object-store live runtime and metrics exporter:** These remain at their previous status (partial / architecturally different). Are there any new findings in these areas?
