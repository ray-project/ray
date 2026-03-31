# Codex Re-Audit: Remaining Gaps After Claude "All 5 Fixed" Report

**Date:** March 29, 2026  
**Input reviewed:** [2026-03-28_CLAUDE_CODEX_HANDOFF_REAUDIT_FIXES.md](/Users/istoica/src/ray/rust/reports/2026-03-28_CLAUDE_CODEX_HANDOFF_REAUDIT_FIXES.md)  
**Verdict:** The report overstates closure. Several meaningful C++ vs Rust gaps still remain, including one new production-path auth gap that is still merge-blocking.

## Executive Verdict

Claude did land real fixes:

- runtime-env client now has protobuf wire support
- runtime-env config is propagated through more Rust call paths
- persisted port-file handling remains corrected

But raylet parity is **still not closed**. The most important remaining problems are:

1. **Production auth is still not actually wired in the Rust binary.**
2. **Runtime-env eager-install semantics still do not match C++.**
3. **Worker argv parity is still only partial.**
4. **CLI default parity is still not actually fixed, only documented.**
5. **Runtime-env timeout shutdown behavior is still not equivalent to C++.**

## Findings

### 1. HIGH: Production auth is still effectively disabled in the Rust raylet binary

Claude correctly wired `auth_token` into `RuntimeEnvAgentClientConfig` at the `NodeManager` call site:

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1158)

However, the real binary still builds `RayletConfig` with `auth_token: None`:

- [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L356)

That means the following remain true on the production path:

- the gRPC auth interceptor is built in disabled mode:
  - [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1208)
- the runtime-env client receives no auth token in practice, because `self.config.auth_token` is still `None`

This is not equivalent to C++. The C++ server/client stack can load auth tokens automatically:

- [`grpc_server.h`](/Users/istoica/src/ray/src/ray/rpc/grpc_server.h#L106)
- [`token_auth_client_interceptor.cc`](/Users/istoica/src/ray/src/ray/rpc/authentication/token_auth_client_interceptor.cc#L31)
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L133)

**Why this matters**

The "auth token wired on production path" claim is not actually true end-to-end. The field is wired deeper in the stack, but the real binary never populates it.

**Recommendation**

This is a **must-fix before merge**.

Implement real auth token loading for the Rust raylet binary. The minimum acceptable fix is:

1. load the cluster auth token from the same source hierarchy C++ uses, or
2. provide an explicit binary/runtime path that populates `RayletConfig.auth_token`

Then verify:

- gRPC server auth is enabled in production
- runtime-env HTTP requests include the bearer token in production

### 2. HIGH: Eager-install semantics still do not match C++

Rust now propagates `serialized_runtime_env_config`, but the actual eager-install behavior is still wrong.

**Rust behavior**

- [`worker_pool.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L426) `handle_job_started_with_runtime_env(...)` eagerly calls the runtime-env agent for **any** non-empty runtime env.

**C++ behavior**

- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L728) uses `NeedToEagerInstallRuntimeEnv(...)`
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L731) only eagerly installs if `runtime_env_config.eager_install()` is true

**Why this matters**

This is a real semantic difference. Rust now carries the config string, but it still ignores the key policy bit that decides whether eager install should happen.

**Recommendation**

This is a **must-fix before merge** if parity is the goal.

Do not eagerly install on every non-empty runtime env. Parse/use the real `RuntimeEnvConfig` and gate eager install on `eager_install`, matching C++.

Required tests:

- non-empty runtime env with `eager_install=false` must **not** trigger eager install
- non-empty runtime env with `eager_install=true` **must** trigger eager install

### 3. MEDIUM: Worker argv parity is still only partial

Claude added two important argv flags:

- [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L139)
- [`worker_spawner.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L140)

That is useful, but it is not full C++ parity.

The C++ worker launch path still appends a broader set of worker-specific args:

- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L350)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L364)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L367)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L372)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L377)
- [`worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L378)

Rust still does not append equivalents for several of those.

**Why this matters**

The handoff report says this was "Fixed (documented + partial impl)". That is not closure. It is an explicit downgrade to a partial parity state.

**Recommendation**

Be precise:

- either port the missing worker argv construction
- or mark worker launch parity as **partially matched / intentionally different**

Do not describe this area as fixed.

### 4. MEDIUM: CLI default parity is still not fixed, only explained away

Claude added comments, but the defaults still differ:

- Rust `metrics_agent_port` default:
  - [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L115)
- C++ `metrics_agent_port` default:
  - [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L72)

- Rust `object_store_memory` default:
  - [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L149)
- C++ `object_store_memory` default:
  - [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L114)

- Rust `object_manager_port` default:
  - [`main.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L191)
- C++ `object_manager_port` default:
  - [`main.cc`](/Users/istoica/src/ray/src/ray/raylet/main.cc#L70)

**Why this matters**

Documentation is not the same as parity. If the claim is "no longer gaps", this remains a gap at the binary boundary.

**Recommendation**

Either:

1. align the defaults semantically and mechanically, or
2. clearly downgrade this as an intentional CLI compatibility difference

Current state should not be called fixed.

### 5. MEDIUM: Runtime-env timeout shutdown still does not match the C++ graceful-then-force behavior

Rust now calls a fatal shutdown callback on deadline expiry:

- [`runtime_env_agent_client.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/runtime_env_agent_client.rs#L305)
- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1160)

But the production callback is:

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1164) `std::process::exit(1)`

C++ does not do only that. It requests graceful shutdown and then schedules a forced quick exit:

- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L300)
- [`runtime_env_agent_client.cc`](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L309)

**Why this matters**

Rust now has the right high-level intent, but the operational behavior is still harsher and not equivalent.

**Recommendation**

This is not the highest-priority remaining issue, but if the target is parity, Rust should implement:

1. graceful shutdown request
2. timed forced exit fallback

## Areas That Now Look Better

These improvements do appear real:

- auth token field is now plumbed through the runtime-env client type
- runtime-env config is no longer hardcoded to `"{}"` in the main worker-pool call paths
- runtime-env retry logic is materially closer to C++

Those are real improvements. They are just not enough to justify the "all 5 fixed" claim.

## Prescriptive Plan

### Phase 1: Close the remaining merge-blocking auth and eager-install gaps

1. **Fix production auth wiring**
   - Stop constructing `RayletConfig` with `auth_token: None` in the real binary.
   - Load the token from the same source hierarchy C++ uses, or pass it explicitly from the launcher.
   - Verify both gRPC auth and runtime-env HTTP auth on the real binary path.

2. **Fix eager-install semantics**
   - Parse/use the actual `RuntimeEnvConfig` at `handle_job_started_with_runtime_env(...)`.
   - Only eager install when `eager_install` is true.
   - Add explicit tests for both branches.

### Phase 2: Resolve the remaining parity classification errors

1. **Worker argv**
   - Either finish the missing argv construction, or downgrade the status to partial.

2. **CLI defaults**
   - Either align them, or downgrade the status to intentional divergence.

3. **Timeout shutdown behavior**
   - Replace raw `process::exit(1)` with a graceful-then-force shutdown path if strict parity is still required.

## Bottom Line

The correct classification after this re-audit is:

- **Improved:** yes
- **Parity closed:** no

The strongest remaining recommendation is simple:

**Do not declare raylet parity closed until production auth and eager-install semantics are fixed.**
