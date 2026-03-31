# Codex Re-Audit of Session Report Re Codex Re-Audit V5

**Date:** 2026-03-30
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-30_SESSION_REPORT_RE_CODEX_REAUDIT_V4.md`
**Scope:** Source-level audit of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

Claude's latest round improved the implementation again, but the report is still too optimistic.

Two gaps remain:

1. Runtime-env hash parity is still not exact C++ parity
2. IO-worker argv support is not integrated into the production start path

That means the answer to "are there no longer gaps?" is still **no**.

## Findings

### 1. Runtime-env hash parity is still not fully closed

**Severity:** MEDIUM to HIGH depending on how strict the parity claim is

Rust now has a dedicated helper and uses `i32` with empty-env `0`, which is an improvement:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L86)

But the helper still uses Rust `DefaultHasher`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L90)

while C++ uses:

- `std::hash<std::string>()`
- cast to `int`

See:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

#### Why this is still open

Claude’s report argues that exact cross-language equivalence is unnecessary because the hash is only used self-consistently within one raylet process. That may be operationally sufficient for the current Rust implementation, but it is **not the same thing as C++ parity**.

If the claim is strict parity, the helper still diverges from the C++ contract at the hash-function level.

#### Firm recommendation

Do not label runtime-env hash parity as closed unless one of these is true:

1. Rust reproduces the exact C++ hash behavior, or
2. The project explicitly narrows the claim from "C++ parity" to "Rust-internal self-consistency"

Right now the code and the report are mixing those two standards. That should be corrected.

### 2. IO-worker argv support is not actually wired through production

**Severity:** HIGH for the specific claim that IO-worker argv was fixed

Claude added `worker_type` support to `SpawnWorkerContext` and `build_worker_argv()`, and the helper can emit `--worker-type=...`:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L204)

But in the actual production start paths I checked, the contexts are still built with:

- `..SpawnWorkerContext::default()`

which means `worker_type` remains `WorkerType::Worker`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L425)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L788)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L113)

So the new IO-worker flag support is currently helper-level and test-level, not a real production-path closure.

#### Additional remaining gap

`--object-spilling-config` is still not implemented:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L207)

C++ still appends both worker-type and object-spilling config for IO workers:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L346)

#### Firm recommendation

Do **not** say IO-worker argv is fixed. It is not.

At best, the code now has a partial scaffold for IO-worker argv formatting.

### 3. CLI defaults and broader exclusions remain non-parity areas

**Severity:** LOW to MEDIUM

These remain as before:

1. CLI sentinel defaults are intentionally divergent
2. Object-store live runtime remains partial
3. Metrics exporter protocol remains architecturally different

These are not new findings, but they still prevent any broad "no remaining gaps" conclusion.

## Confirmed Closures

The following earlier issues remain genuinely fixed:

### Shutdown death-info propagation

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1302)

### Production auth loading

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)

### Eager-install gating

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

### Runtime-env context threading into worker start

Rust now does thread `serialized_runtime_env_context` through the production runtime-env success path:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L425)

That part is real progress.

## Prescriptive Plan

### Priority 1: decide and enforce the runtime-env-hash standard

Pick one standard and encode it explicitly:

1. Exact C++ parity:
   implement the same hash contract as C++
2. Rust-local operational equivalence:
   stop claiming parity for the hash itself

Do not keep the code on one standard and the report on another.

### Priority 2: wire IO-worker type through the real start path

Implement all of the following:

1. Identify the production call sites that start spill/restore/delete workers
2. Pass the real `WorkerType` into `SpawnWorkerContext`
3. Verify that the emitted argv on that path contains the correct `--worker-type=...`

Acceptance criteria:

1. Production code, not just helper tests, constructs non-default worker types
2. Integration or focused unit tests verify the real path

### Priority 3: implement `--object-spilling-config`

This remains open and should be finished if IO-worker parity is in scope.

Implement:

1. RayConfig access in the spawn path
2. Base64-escaped object-spilling config emission matching C++

### Priority 4: keep the status claim narrow

The report should say something like:

`Most runtime-env and regular worker-start gaps are closed, but exact runtime-env-hash parity and IO-worker argv remain open.`

Do not say there are no remaining gaps.

## Merge Recommendation

My recommendation is still firm:

- Do **not** declare parity closed
- Do **not** say IO-worker argv is fixed
- Do **not** say there are no remaining gaps

The correct status is:

- Shutdown death-info: closed
- Auth loading: closed
- Eager-install semantics: closed
- Runtime-env context threading: closed
- Runtime-env hash exact parity: still open
- IO-worker argv integration: still open
- `--object-spilling-config`: still open

That is the technically defensible reading of the current Rust and C++ code.
