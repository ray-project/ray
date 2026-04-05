# Claude Code Prompt: Finish Raylet Parity Without Another Overclaim

Stop overclaiming. You are still not done.

The latest raylet parity classification is better than the earlier reports, but it still marks several items as "Fully matched" when the live Rust runtime does not actually support that claim. This needs to end now.

Do not come back with another parity-closure report unless the code and tests support it line by line. A helper module existing is not parity. A config field being stored is not parity. A value being published to `GcsNodeInfo` is not parity. A constructor test is not parity. If the live runtime path is not there, the item is not closed.

Codex re-audited the actual Rust and C++ sources and found the following remaining gaps.

## What Is Still Wrong

### 1. `object_manager_port` is still not fully matched

Rust today:
- parses/stores the flag in `rust/ray-raylet/src/main.rs`
- publishes it in `GcsNodeInfo` in `rust/ray-raylet/src/node_manager.rs`
- does not appear to run a distinct object-manager server/client path on that port

C++:
- carries it into `ObjectManagerConfig`
- uses it for the object-manager server/client path

Source references:
- Rust: `rust/ray-raylet/src/main.rs:314`, `rust/ray-raylet/src/node_manager.rs:800`
- C++: `src/ray/raylet/main.cc:653`, `src/ray/raylet/main.cc:866`

Consequence:
- the current “Fully matched” classification is false

### 2. `runtime_env_agent_port` is only partially matched

Rust now:
- resolves the port
- creates a `RuntimeEnvAgentClient`
- installs it into `WorkerPool`

But:
- `WorkerPool` only stores the client
- there is still no source-backed worker lifecycle path that actually calls:
  - `get_or_create_runtime_env(...)`
  - `delete_runtime_env_if_possible(...)`

Source references:
- Rust install path: `rust/ray-raylet/src/node_manager.rs:948-1008`
- Rust storage-only path: `rust/ray-raylet/src/worker_pool.rs:93-150`

Consequence:
- this is not parity with C++ worker-pool behavior yet

### 3. `dashboard_agent_command` and `runtime_env_agent_command` are still only partially matched

Rust now has an `AgentManager`, which is real progress.

But the implementation still only:
- spawns once
- tracks PID
- can be stopped

It does NOT:
- monitor the child continuously
- respawn on exit
- use `respawn_on_exit`
- use `fate_shares`

Source references:
- Rust: `rust/ray-raylet/src/agent_manager.rs:40-168`
- startup calls: `rust/ray-raylet/src/node_manager.rs:913-924`

Consequence:
- your report’s “launched and monitored” claim is not supported by the code

### 4. `session_dir` parity is still incomplete

C++ resolves three dashboard-side ports via `WaitForDashboardAgentPorts(...)`:
- `metrics_agent_port`
- `metrics_export_port`
- `dashboard_agent_listen_port`

Rust only resolves:
- `metrics_agent`
- `runtime_env_agent`

There is still no Rust `dashboard_agent_listen_port` CLI/config/runtime path.
Rust also does not resolve `metrics_export_port` from `session_dir`; it still uses the CLI value directly.

Source references:
- Rust: `rust/ray-raylet/src/node_manager.rs:930-964`
- C++: `src/ray/raylet/node_manager.cc:3341-3360`

Consequence:
- this row is not parity-closed

### 5. The new object-store construction path is not wired into the live raylet data path

Rust now constructs an `ObjectManager` from raylet config, which is real progress.

But the live raylet RPC/object-management paths still use `local_object_manager()`, not this newly constructed `object_manager`.

Source references:
- constructor path: `rust/ray-raylet/src/node_manager.rs:617-682`
- live object-manager usage still goes through `local_object_manager()` in `rust/ray-raylet/src/grpc_service.rs`

Consequence:
- the current test only proves constructor-time wiring, not live runtime parity

### 6. `metrics_agent_port` is improved but still not fully matched

Rust now:
- resolves the port
- creates a `MetricsAgentClient`
- waits for readiness

But after readiness it only logs.

C++ actually calls:
- `ConnectOpenCensusExporter(...)`
- `InitOpenTelemetryExporter(...)`

Source references:
- Rust: `rust/ray-raylet/src/node_manager.rs:966-989`
- C++: `src/ray/raylet/main.cc:1071-1075`

Consequence:
- this is still not full parity

## Your Assignment

You must do one of two things for each open item:

1. implement the missing runtime behavior
2. or explicitly downgrade the classification and prove why parity is not yet claimed

You are NOT to do any of the following:
- do not mark something “Fully matched” because a helper module exists
- do not mark something “Fully matched” because a config field is stored or published
- do not mark something “Fully matched” because a constructor test passes
- do not say a feature is “monitored” when the code only spawns and stops a process
- do not say a client is integrated when it is only installed and never used

If you do that again, your report will be rejected again.

## Required Work

### PHASE 1: Fix the classification table first

Before anything else, update the classification table so it is technically accurate.

At minimum, downgrade these rows immediately unless you finish the runtime work in the same pass:
- `object_manager_port`
- `runtime_env_agent_port`
- `dashboard_agent_command`
- `runtime_env_agent_command`
- `session_dir`
- `metrics_agent_port`
- `object_store_memory`
- `plasma_directory`
- `fallback_directory`
- `huge_pages`

If you re-upgrade any of them to “Fully matched”, your final report must include the exact runtime code path and test that proves it.

### PHASE 2: Close the real runtime gaps

#### A. `object_manager_port`

You need to decide explicitly:
- either implement the real separate object-manager port/server/client path
- or admit Rust does not match C++ here and classify it as open/intentionally different

Do not leave this in an ambiguous “published therefore matched” state.

#### B. `runtime_env_agent_port`

You need actual worker lifecycle integration.

Add the missing call paths so that the worker pool or startup flow actually invokes:
- `get_or_create_runtime_env(...)`
- `delete_runtime_env_if_possible(...)`

where C++ does.

If the Rust architecture intentionally avoids this, then prove the replacement path end-to-end and downgrade the classification accordingly.

#### C. `AgentManager`

If you want to claim parity:
- implement actual process monitoring
- implement respawn behavior when `respawn_on_exit` is true
- define and implement `fate_shares` semantics
- verify shutdown semantics

If you are not going to implement these, then remove or downgrade the claim. Right now the code and the report are inconsistent.

#### D. `session_dir`

Add the missing dashboard-side parity:
- support `dashboard_agent_listen_port`
- support `metrics_export_port` fallback resolution from `session_dir`
- publish resolved values, not just raw CLI values
- ensure CLI values still take precedence over port files

This must match the C++ `WaitForDashboardAgentPorts(...)` contract.

#### E. `metrics_agent_port`

If Rust has an intended equivalent to:
- `ConnectOpenCensusExporter(...)`
- `InitOpenTelemetryExporter(...)`

then wire it in after readiness succeeds.

If not, explicitly document the semantic difference and downgrade the classification.

#### F. object-store flags and runtime wiring

You have added constructor-side wiring, but that is not enough.

You must either:
- wire the new object manager/plasma stack into the live raylet object-management path
- or stop claiming the object-store flags are fully matched

The current path looks like a side object used only for tests. That is not runtime parity.

## PHASE 3: Replace weak tests with decisive runtime tests

Your current new tests are too shallow. They mostly prove:
- modules exist
- constructors run
- objects are stored
- a PID can be read

That is not enough.

You must add tests that prove the actual runtime behavior.

### Required tests

#### 1. Real runtime-env agent usage test
- start a mock runtime-env HTTP server
- run the real raylet startup path
- trigger the worker path that should require runtime-env creation
- assert that `get_or_create_runtime_env(...)` was actually called
- then trigger cleanup and assert `delete_runtime_env_if_possible(...)` was actually called

#### 2. Real agent monitoring/respawn test
- configure an agent command that exits quickly
- start the real raylet path
- verify whether the agent is monitored
- if parity target is respawn, verify respawn actually occurs
- if parity target is failure propagation, verify that instead

This test must fail on the current implementation.

#### 3. Full `session_dir` dashboard-port resolution test
- write all relevant port files:
  - `metrics_agent`
  - `metrics_export`
  - `dashboard_agent_listen`
  - `runtime_env_agent`
- start raylet with zero CLI ports
- verify all resolved values are actually consumed and published

#### 4. Metrics exporter-init test
- run the real readiness path with a mock metrics agent
- verify the intended exporter hookup occurs after readiness
- not just a connectivity check and log line

#### 5. Live object-store wiring test
- start raylet with non-default:
  - `object_store_memory`
  - `plasma_directory`
  - `fallback_directory`
  - `huge_pages`
- exercise a raylet path that actually uses the runtime object store
- verify the live runtime, not just a side object, is using those settings

#### 6. `object_manager_port` test
- if you still claim parity, prove a real server/client path uses that port
- otherwise downgrade the classification and remove the false claim

## Required Final Report Format

Your final response must include a table with these columns:
- item
- previous claim
- actual Rust behavior before this pass
- actual Rust behavior after this pass
- C++ behavior
- final status: fully matched / partially matched / published only / unsupported / intentionally different
- exact source references
- exact tests proving the claim

You must explicitly answer these questions:

1. Is `object_manager_port` now truly matched, or merely published?
2. Is `RuntimeEnvAgentClient` now actually used in worker lifecycle behavior, or still only stored?
3. Does Rust now monitor/respawn agent subprocesses, or not?
4. Does Rust now match C++ `WaitForDashboardAgentPorts(...)`, including `metrics_export_port` and `dashboard_agent_listen_port`?
5. Is the new object-manager/plasma stack part of the live raylet runtime path, or still a side object used only for tests?
6. Does Rust now actually perform the metrics exporter hookup after readiness, or only log readiness?

## Completion Standard

Do not say “fully matched” for any row unless:
- the runtime code path exists
- the behavior is exercised in a decisive test
- the test would have failed before your fix

Another overclaim is worse than a precise downgrade. If a gap still exists, say so explicitly. If you close it, prove it with source and runtime tests.

Do not come back with another softened report. Finish the runtime work or downgrade the claims honestly.
