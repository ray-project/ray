# Codex Verification Report: Remaining C++ vs Rust Raylet Parity Gaps

**Date:** March 28, 2026  
**Scope:** Verification of `/Users/istoica/src/ray/rust/reports/2026-03-28_CODEX_COLLABORATION_REPORT.md` against current Rust and C++ sources  
**Verdict:** The collaboration report overstates closure. Several material parity gaps remain. At least three are merge-blocking.

## Executive Verdict

Do **not** treat raylet parity as closed.

The Rust raylet has real improvements, but the current code still diverges from C++ in production-significant ways:

1. **`session_dir` port-file parity is not closed.**
2. **Runtime-env agent transport is not compatible with the real Python runtime-env agent.**
3. **Python worker command handling is not compatible with the C++ worker command contract.**

Those three items are sufficient by themselves to invalidate the report's current "fully matched" claims for `session_dir`, runtime-env sequencing, and practical worker startup parity.

## Findings

### 1. HIGH: `session_dir` / persisted-port resolution does not match the C++ contract

**Why this matters**

The report claims session-dir resolution is fully closed. It is not. Rust reads the wrong file path and ignores the node-id-based filename that C++ uses.

**Rust implementation**

- [`agent_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L449) says persisted ports live at `<session_dir>/ports/<port_name>`.
- [`agent_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L453) implements exactly that path and explicitly ignores `node_id`.
- Rust tests encode the same contract:
  - [`integration_test.rs`](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L661)
  - [`integration_test.rs`](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L1096)

**C++ implementation**

- [`port_persistence.h`](/Users/istoica/src/ray/src/ray/util/port_persistence.h#L35) defines the standard filename as `"{port_name}_{node_id_hex}"`.
- [`port_persistence.cc`](/Users/istoica/src/ray/src/ray/util/port_persistence.cc#L23) implements `GetPortFileName(node_id, port_name)`.
- [`port_persistence.cc`](/Users/istoica/src/ray/src/ray/util/port_persistence.cc#L35) waits on `dir / "{port_name}_{node_id_hex}"`.
- [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3339) and [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3400) call `WaitForPersistedPort(config.session_dir, self_node_id, ...)`.

**Consequence**

If the Python-side agent persists ports using the C++ contract, the Rust raylet can fail to resolve them even though the files exist. The existing Rust tests are false reassurance because they validate the Rust-only filename scheme, not the C++ one.

**Recommendation**

This is a **must-fix before merge**. Replace the Rust port lookup with the exact C++ file naming contract and rewrite tests to use node-id-suffixed files.

### 2. HIGH: Rust runtime-env agent client does not speak the real runtime-env agent protocol

**Why this matters**

The report presents runtime-env sequencing as fully matched. That is not supportable because the Rust client is not wire-compatible with the actual Python runtime-env agent.

**Rust implementation**

- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L117) sends raw HTTP over `TcpStream`.
- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L135) uses `Content-Type: application/json`.
- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L172) sends JSON fields, not protobuf.
- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L200) expects a JSON response.
- There is no auth-header injection anywhere in this file.

**C++ / Python contract**

- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L365) documents `POST /get_or_create_runtime_env` with a **serialized protobuf** body.
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L451) does the same for delete.
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L424) serializes `rpc::GetOrCreateRuntimeEnvRequest`.
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L492) serializes `rpc::DeleteRuntimeEnvIfPossibleRequest`.
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L135) adds the authorization header when auth is enabled.
- [`main.py`](/Users/istoica/src/ray/python/ray/_private/runtime_env/agent/main.py#L182) parses a serialized protobuf request body.
- [`main.py`](/Users/istoica/src/ray/python/ray/_private/runtime_env/agent/main.py#L197) parses a serialized protobuf delete body.
- [`test_runtime_env_agent_auth.py`](/Users/istoica/src/ray/python/ray/tests/test_runtime_env_agent_auth.py#L24) shows token-authenticated requests against the Python agent.

**Consequence**

Even if Rust correctly delays worker startup until "runtime env creation succeeds", the current Rust implementation is talking the wrong protocol. Against the real runtime-env agent, this path is not parity-closed.

**Recommendation**

This is a **must-fix before merge**. Reimplement the Rust runtime-env client to use:

- serialized protobuf request/response payloads
- `application/octet-stream`
- auth-header injection
- the C++ timeout / retry / fatal-on-unreachable semantics

Do not paper over this with more unit tests using a Rust-shaped mock server.

### 3. HIGH: `python_worker_command` handling is not C++-compatible

**Why this matters**

The Rust CLI claims `python_worker_command` is a full shell command line, but the worker spawner does not honor that contract.

**Rust implementation**

- [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L67) documents `python_worker_command` as a "full shell command line".
- [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L92) treats `python_worker_command` as a single program path.
- [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L97) hardcodes `-m ray.worker`.

**C++ implementation**

- [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L611) parses `python_worker_command` via `ParseCommandLine(...)`.
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L323) builds the actual worker command from the parsed token vector and appends worker-specific arguments.

**Consequence**

A real `ray start` command that passes a composite Python worker command can fail under Rust because Rust may try to execute the entire command line as a single binary name. Even when it does start, it is not preserving the C++ worker-command token stream.

**Recommendation**

This is a **must-fix before merge**. Worker commands must be stored and executed as parsed argv vectors, not as a single opaque string. The Rust spawner needs the same model as C++.

### 4. MEDIUM: Rust worker-spawner uses unresolved `metrics_export_port`

**Why this matters**

Even if the port-file lookup were corrected, the resolved port is not what the worker spawner receives today.

**Rust implementation**

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L982) constructs `WorkerSpawnerConfig` before `resolve_all_ports()`.
- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L993) passes `self.config.metrics_export_port`, not the resolved value.
- [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L85) propagates that value to `RAY_METRICS_EXPORT_PORT`.

**Consequence**

If `metrics_export_port` is supplied only via persisted port files, the raylet may resolve it for its own use later but workers still receive the pre-resolution value.

**Recommendation**

Fix this in the same patch as finding 1. The worker-spawner config must be built after resolved ports are known, or it must be able to consult an updated shared source of truth.

### 5. MEDIUM: Rust defaults diverge from the C++ entrypoint defaults

**Evidence**

- Rust defaults `metrics_export_port` to `0` in [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L120); C++ defaults it to `1` in [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L73).
- Rust defaults `metrics_agent_port` to `0` in [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L110); C++ defaults it to `-1` in [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L72).
- Rust defaults `object_store_memory` to `0` in [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L138); C++ defaults it to `-1` in [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L111).

**Consequence**

These are not all equally severe, but they are real contract differences at the binary boundary. They can alter fallback behavior when the launcher omits a flag.

**Recommendation**

Normalize Rust CLI defaults to the C++ values unless there is a documented, intentional deviation.

### 6. MEDIUM: Rust command-line parsing is much weaker than C++ parsing

**Evidence**

- Rust parser is a homegrown quote toggle in [`agent_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L367).
- C++ uses the shared command-line parser in [`cmd_line_utils.cc`](/Users/istoica/src/ray/src/ray/util/cmd_line_utils.cc#L186), with extensive tests for escaping and quoting semantics.

**Consequence**

This is a latent compatibility bug for dashboard-agent commands, runtime-env-agent commands, and any future worker-command parsing Rust adopts.

**Recommendation**

Do not extend the Rust mini-parser. Replace it with behaviorally equivalent parsing or a shared implementation strategy.

### 7. MEDIUM: `enable_metrics_collection` parity is still incomplete

**Evidence**

- C++ conditionally appends `--disable-metrics-collection` in [`node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3316).
- Rust hardcodes `enable_metrics = true` in [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L481).
- Rust `RayConfig` currently does not include that field in [`config.rs`](/Users/istoica/src/ray/rust/ray-common/src/config.rs#L48).

**Consequence**

Rust cannot currently honor the C++ metrics-disable control path.

**Recommendation**

Add the missing config field and honor it when constructing the dashboard agent command.

## Areas That Are Improved But Still Not Fully Closed

### Object-store live runtime

The collaboration report is correct to downgrade this to partial. The code itself says so:

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L688)
- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L439)

This remains open and should continue to be labeled partial until pin/free/spill lifetimes are backed by the real Plasma store.

## Prescriptive Closure Plan

### Phase 1: Fix the hard contract mismatches

These are merge-blocking.

1. **Port persistence**
   - Change Rust persisted-port lookup to `session_dir / "{port_name}_{node_id_hex}"`.
   - Remove the Rust-only `session_dir/ports/<name>` contract.
   - Rewrite all affected tests to use the real filename scheme.
   - Add one test that proves Rust can read a file written by the exact C++ naming convention.

2. **Runtime-env agent transport**
   - Replace JSON request/response handling with protobuf request/response handling.
   - Use `application/octet-stream`.
   - Inject the auth header when cluster auth is enabled.
   - Include `source_process` on delete requests, matching C++.
   - Match the retry semantics from C++, including the fatal behavior when the runtime-env agent cannot be contacted within the configured deadline.
   - Add an integration test against the real Python runtime-env agent endpoint contract, not a Rust mock.

3. **Worker command execution**
   - Represent worker commands as tokenized argv, not as a single string.
   - Parse `python_worker_command` once at startup and preserve all arguments.
   - Stop hardcoding `-m ray.worker` when an explicit worker command is provided.
   - Add a regression test with a multi-token worker command containing quotes or spaces.

### Phase 2: Fix the secondary runtime mismatches

1. **Resolved metrics export port propagation**
   - Build the worker spawner after port resolution, or update the spawner config once ports are resolved.
   - Add a test where `metrics_export_port` comes only from the persisted port file and confirm workers see the resolved value.

2. **CLI default alignment**
   - Audit every clap default against the C++ gflags default.
   - Correct the ones that differ unless explicitly documented as intentional.

3. **Metrics-disable parity**
   - Add `enable_metrics_collection` to Rust `RayConfig`.
   - Honor it in dashboard-agent command construction.
   - Add a test proving `--disable-metrics-collection` is appended when the config disables metrics.

### Phase 3: Tighten verification so this does not regress again

1. **Stop relying on Rust-only tests for C++ parity claims**
   - Any parity claim involving a binary/file/network boundary must be validated against the C++ wire/file contract, not a Rust-only reimplementation of it.

2. **Introduce source-of-truth parity tests**
   - Port-file naming compatibility test.
   - Runtime-env agent wire-compatibility test.
   - Worker-command tokenization/execution compatibility test.

3. **Downgrade status documents until phase 1 lands**
   - Remove "fully matched" for `session_dir`.
   - Remove "fully matched" for runtime-env parity in practical production terms.
   - Remove any suggestion that worker startup parity is closed while `python_worker_command` remains incompatible.

## Recommended Status Table

| Area | Actual Status |
|------|---------------|
| GCS `enable_ray_event` | Closed |
| Agent subprocess monitoring | Improved, likely close |
| `session_dir` persisted-port resolution | Open |
| Runtime-env agent compatibility | Open |
| Runtime-env startup sequencing | Partially improved, not parity-closed |
| Python worker command parity | Open |
| Object-store live runtime | Partial |
| Metrics exporter equivalence | Architecturally different |

## Bottom Line

The right stance is not "mostly done with two honest downgrades." The right stance is:

- **GCS event parity appears closed.**
- **Raylet parity is improved but still materially incomplete.**
- **Do not merge under a claim of raylet parity closure until the three HIGH findings above are fixed and re-verified.**
