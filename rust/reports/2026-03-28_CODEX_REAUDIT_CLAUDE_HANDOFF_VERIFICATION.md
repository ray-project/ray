# Codex Re-Audit: Remaining Gaps After Claude Handoff Verification Fixes

**Date:** March 28, 2026  
**Input reviewed:** [2026-03-28_CLAUDE_CODEX_HANDOFF_VERIFICATION_FIXES.md](/Users/istoica/src/ray/rust/reports/2026-03-28_CLAUDE_CODEX_HANDOFF_VERIFICATION_FIXES.md)  
**Verdict:** Several prior gaps were genuinely improved, but parity is still **not** closed. The handoff report overstates completion.

## Executive Verdict

The following items appear genuinely fixed:

- C++-style persisted port-file naming is now implemented.
- `metrics_export_port` is now resolved before building the worker spawner.
- `enable_metrics_collection` is now represented in Rust config and consulted.
- `python_worker_command` is no longer treated as a single opaque executable path.

However, there are still **material remaining gaps**, including at least **two HIGH** issues in the runtime-env path and **one MEDIUM** issue in worker launch parity. These are enough to keep raylet parity **open**.

## Findings

### 1. HIGH: Runtime-env auth support exists in the client type but is not wired on the production path

**Rust now supports auth in principle**

- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L84) adds `auth_token`.
- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L156) emits `Authorization: Bearer ...` if configured.

**But production `NodeManager::run()` does not pass the token**

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1155) constructs `RuntimeEnvAgentClient::new(...)`.
- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1158) passes `RuntimeEnvAgentClientConfig::default()`, which means `auth_token: None`.

**Why this is still a real gap**

The C++ path uses the configured auth token when talking to the runtime-env agent:

- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L135)

The Rust implementation now has the feature, but the production path still does not use it. In an authenticated cluster, this remains a wire-compatibility gap.

**Recommendation**

This is a **must-fix before merge**. Thread the raylet auth token into `RuntimeEnvAgentClientConfig` from the real runtime path and add an integration test that proves the header is present on actual requests.

### 2. HIGH: Runtime-env retry / timeout / failure semantics still do not match C++

**Rust behavior**

- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L250) retries in a loop for get-or-create, but on exhaustion only calls the callback with failure.
- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L326) delete retries at most one extra time on 404, then returns failure.
- There is no Rust equivalent of the C++ `ExitImmediately()` path.

**C++ behavior**

- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L295) defines `ExitImmediately()`.
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L334) `RetryInvokeOnNotFoundWithDeadline(...)` exits the raylet when the deadline is exceeded.
- C++ applies the same deadline-driven retry framework to both get/create and delete paths.

**Why this matters**

This is not cosmetic. C++ treats runtime-env agent reachability as a critical subsystem with explicit shutdown behavior after deadline expiry. Rust still degrades to an ordinary callback failure. That is different operational behavior.

**Recommendation**

This is a **must-fix before merge** if parity is the claim. Port the C++ deadline/retry/fatal behavior directly. Do not classify runtime-env parity as closed without this.

### 3. HIGH: Runtime-env config propagation is still missing from the actual Rust call sites

**Rust client can now serialize `RuntimeEnvConfig`, but the production call sites still pass `"{}"`**

- [`worker_pool.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L359) passes `"{}"` to `get_or_create_runtime_env(...)`.
- [`worker_pool.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L437) passes `"{}"` again on eager install.
- [`worker_pool.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L685) passes `"{}"` again on the callback-gated production path.
- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L326) extracts only `serialized_runtime_env` and drops the rest of `runtime_env_info`.

**C++ uses the real config**

- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1379) passes `runtime_env_info_.runtime_env_config()`.
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L748) uses job config runtime-env settings.
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1549) does the same in prestart flow.

**Why this matters**

The wire format is better now, but the actual semantic payload is still incomplete. Fields like eager-install and setup timeout live in `RuntimeEnvConfig`; Rust is still dropping them on the main paths.

**Recommendation**

This is a **must-fix before merge**. Introduce a Rust-side representation of the runtime-env info/config at the worker-pool boundary and propagate the actual config through:

- prestart path
- eager-install path
- callback-gated worker start path

Then add tests that fail if `RuntimeEnvConfig` is dropped.

### 4. MEDIUM: Worker command parsing improved, but full C++ worker argv parity is still not implemented

**What improved**

- Rust now parses `python_worker_command` into argv:
  - [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L96)

**What still differs**

- Rust immediately uses the parsed base args unchanged:
  - [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L113)
- The code itself says worker-specific args are only something it “may add in the future”.

**C++ worker launch path is richer**

- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L323) starts from the parsed worker command.
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L350)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L364)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L377)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L380)

C++ appends runtime worker arguments such as worker type, worker ID, node ID, runtime-env hash, language, and serialized runtime-env context, with additional setup-worker handling.

**Why this matters**

The narrow parsing bug is fixed, but the report’s broader implication that worker-command parity is closed is still too optimistic. The Rust worker launch contract remains architecturally thinner than C++.

**Recommendation**

Do not call worker-command parity closed. Either:

1. port the C++ argv construction contract, or  
2. explicitly downgrade this area as intentionally different and document which worker arguments are now carried only via environment instead of argv.

### 5. MEDIUM: CLI default alignment is only partially fixed

Claude fixed `metrics_export_port`, but other defaults still differ from the C++ entrypoint:

- Rust `metrics_agent_port` default is `0`:
  - [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L115)
- C++ `metrics_agent_port` default is `-1`:
  - [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L72)

- Rust `object_store_memory` default is `0`:
  - [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L139)
- C++ `object_store_memory` default is `-1`:
  - [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L111)

- Rust `object_manager_port` default is `0`:
  - [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L176)
- C++ `object_manager_port` default is `-1`:
  - [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L70)

**Recommendation**

This is not the highest-risk issue, but the claim “CLI defaults differ from C++ gflags: fixed” is inaccurate. Finish the audit and align the remaining defaults.

## Areas That Now Look Correct

These changes do look materially improved and source-backed:

- Persisted port-file naming:
  - [`agent_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L492)
  - [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L623)
- Resolved `metrics_export_port` propagation:
  - [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1018)
  - [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1034)
- `enable_metrics_collection` plumbing:
  - [`config.rs`](/Users/istoica/src/ray/rust/ray-common/src/config.rs#L95)
  - [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L483)

## Prescriptive Plan

### Phase 1: Close the remaining runtime-env parity blockers

1. **Wire auth token into the production runtime-env client**
   - Pass the actual raylet auth token into `RuntimeEnvAgentClientConfig` in `NodeManager::run()`.
   - Add an integration test that inspects a real request and proves the authorization header is present when cluster auth is enabled.

2. **Port C++ retry/deadline semantics**
   - Implement a shared retry helper equivalent to C++ `RetryInvokeOnNotFoundWithDeadline(...)`.
   - Apply it to both get/create and delete paths.
   - Implement the C++ fatal behavior on deadline expiry instead of ordinary callback failure.

3. **Propagate real `RuntimeEnvConfig`**
   - Stop passing `"{}"` from worker-pool call sites.
   - Thread actual runtime-env config through request handling and worker-pool APIs.
   - Add regression tests for:
     - `setup_timeout_seconds`
     - `eager_install`
     - delete path preserving relevant metadata expectations

### Phase 2: Resolve worker launch parity honestly

1. **Decide whether full argv parity is required**
   - If yes: port the C++ worker-argv augmentation logic.
   - If no: explicitly document the intentional divergence and stop claiming full worker-command parity.

2. **If keeping the current env-var-based launch model**
   - Add a document listing which C++ worker arguments are replaced by env vars.
   - Add coverage proving the Rust worker still receives equivalent information.

### Phase 3: Finish the defaults audit

1. Align remaining defaults with C++ or document each deliberate deviation.
2. Update the status documents so they stop saying “all 7 fixed”.

## Bottom Line

Claude’s handoff made real progress, but the right final classification is:

- **Port-file naming:** fixed
- **Runtime-env wire format:** improved but **not fully closed**
- **Runtime-env production wiring:** **still open**
- **Worker command handling:** improved but **not fully closed**
- **CLI default alignment:** **partially fixed**

Do **not** mark raylet parity closed yet.
