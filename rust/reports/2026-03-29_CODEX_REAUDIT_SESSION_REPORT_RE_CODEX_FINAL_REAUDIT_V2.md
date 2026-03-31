# Codex Re-Audit of Session Report Re Final Re-Audit V2

**Date:** 2026-03-29
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-29_SESSION_REPORT_RE_CODEX_FINAL_REAUDIT_V1.md`
**Scope:** Source audit of the latest Claude session report against current Rust and C++ raylet code.

## Executive Verdict

The latest Claude report is still too optimistic.

It correctly reflects that several earlier gaps are now fixed, especially auth token loading and eager-install gating. But the report incorrectly claims closure in areas that still have material parity gaps.

## Findings

### 1. Runtime-env timeout shutdown is still not C++-equivalent end to end

**Severity:** HIGH

Claude now routes the runtime-env timeout path through a oneshot shutdown signal, and that is a real improvement. But it is still not full C++ parity.

#### What Rust now does

On timeout, Rust sends a oneshot signal that causes `serve_with_incoming_shutdown(...)` to stop the gRPC server, then later unregisters from GCS and stops agent subprocesses:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1155)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1243)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1268)

However, Rust unregisters with a default `UnregisterNodeRequest` and does not populate any death metadata:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L948)

#### What C++ does

C++ constructs `NodeDeathInfo` with `UNEXPECTED_TERMINATION`, sets a reason message, and passes that through `UnregisterSelf(...)`:

- [runtime_env_agent_client.cc](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L309)
- [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L495)

That is not just an implementation detail. GCS and downstream node-lifecycle handling consume that death reason and message.

#### Why the current Rust behavior is still a gap

Rust now shuts down more gracefully than before, but it still does not preserve the C++ node-death contract for this fatal path.

The current report says this area is closed. That is not technically defensible.

#### Required fix

Implement shutdown-state propagation so the runtime-env timeout path unregisters with explicit death info equivalent to:

1. `reason = UNEXPECTED_TERMINATION`
2. `reason_message = "Raylet could not connect to Runtime Env Agent"`

Do not mark this area closed until that metadata flows into `UnregisterNodeRequest`.

### 2. Worker argv parity is still overstated

**Severity:** HIGH for strict parity claims

Claude’s report says Rust now passes 10/12 C++ flags via argv. That is not what the current source actually guarantees.

#### Placeholder values are still hardcoded

The worker spawner accepts `runtime_env_hash` and `serialized_runtime_env_context`, but the production `NodeManager::run()` path still hardcodes them to `0` and empty string:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1024)

The spawner itself still documents this as TODO work:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L160)

So the flags may exist syntactically, but they are not yet semantically wired.

#### Rust still misses worker-type-specific argv

C++ still appends worker-type-specific arguments such as:

- `--worker-type=...`
- `--object-spilling-config=...`

See:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L346)

Rust does not emit those arguments in the current spawner path.

#### Language handling is still wrong for non-Python workers

Rust currently pushes `--language=PYTHON` unconditionally:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L157)

That is not equivalent to C++, which uses the actual worker language and also uses different argv forms for C++ workers:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L363)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L370)

For C++ workers, C++ appends:

- `--ray_worker_id=...`
- `--ray_runtime_env_hash=...`

Rust does not.

#### Required fix

Do not describe worker argv as “mostly matched” yet.

The next patch must:

1. Thread real `runtime_env_hash` and `serialized_runtime_env_context` from the worker-pool/task path into `WorkerSpawnerConfig`
2. Emit the correct `--language=<actual>` value
3. Add missing IO-worker arguments
4. Add the proper C++ worker argv for `Language::Cpp`

Only after those changes is a “mostly matched” label credible.

### 3. CLI defaults remain a divergence, not a closure

**Severity:** MEDIUM

This is the least serious of the remaining items, but it still should not be represented as fixed parity.

Rust defaults remain:

- `metrics_agent_port = 0` in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L119)
- `object_store_memory = 0` in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L147)
- `object_manager_port = 0` in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L188)

C++ defaults remain:

- `metrics_agent_port = -1` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L72)
- `object_store_memory = -1` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L114)
- `object_manager_port = -1` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L70)

This is acceptable only if it is explicitly classified as an intentional non-parity divergence. It is not acceptable to count it toward parity closure.

## Confirmed closures

The following items do appear genuinely fixed in the current source:

### Production auth token loading

Rust now loads auth token from env/file sources and passes it into `RayletConfig`:

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L342)
- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L363)

### Eager-install semantics

Rust now gates eager install on `runtime_env_config.eager_install()` semantics:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L480)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

## Prescriptive plan

### Priority 1: fix node-death-info propagation on fatal runtime-env timeout

Implement all of the following:

1. Extend the Rust shutdown path so it records shutdown cause, not just a generic shutdown signal.
2. Carry `NodeDeathInfo` equivalent data through the runtime-env timeout path.
3. Populate `UnregisterNodeRequest.node_death_info` on unregister.
4. Add a focused test that verifies the unregister request includes `UNEXPECTED_TERMINATION` and the expected reason message.

This should be treated as merge-blocking if the claim is C++ parity.

### Priority 2: complete real worker argv wiring

Implement all of the following:

1. Stop hardcoding `runtime_env_hash = 0`
2. Stop hardcoding `serialized_runtime_env_context = ""`
3. Use the actual worker language when building `--language=...`
4. Add IO-worker-only flags where applicable
5. Add C++-specific argv forms for C++ workers

Add tests that assert the actual argv produced for:

1. Python worker
2. IO worker
3. C++ worker

Do not rely on doc comments as evidence of parity.

### Priority 3: keep CLI defaults out of closure claims unless aligned

Pick one of two positions and enforce it consistently:

1. Align to C++ sentinel defaults
2. Keep the divergence, but exclude it from parity closure language everywhere

The current ambiguous framing should be removed.

## Merge Recommendation

Do **not** declare parity closed yet.

The technically accurate status is:

- Auth token loading: closed
- Eager-install gating: closed
- Runtime-env timeout shutdown metadata: still open
- Worker argv construction: still open
- CLI defaults: still divergent by design

That is the defensible conclusion from the current Rust and C++ source.
