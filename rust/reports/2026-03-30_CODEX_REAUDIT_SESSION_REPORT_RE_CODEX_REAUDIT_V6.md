# Codex Re-Audit of Session Report Re Codex Re-Audit V6

**Date:** 2026-03-30
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-30_SESSION_REPORT_RE_CODEX_REAUDIT_V4.md`
**Scope:** Source-level verification of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

The latest Claude report is still too optimistic.

There are still remaining gaps. Two matter most:

1. IO-worker argv support is not actually wired through the production start path
2. Runtime-env hash handling is improved, but the report still presents a narrower operational argument as if it were full C++ parity

## Findings

### 1. IO-worker argv is not closed

**Severity:** HIGH

Claude’s report says IO-worker argv is fixed because `--worker-type` can now be emitted. That is not sufficient, because the production worker-start path still does not pass non-default worker types into `SpawnWorkerContext`.

The helper-level support is real:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L204)

But the production contexts I checked still use default worker context:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L406)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L428)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L655)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L733)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L791)

And the default context still sets:

- `worker_type = WorkerType::Worker`

See:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L113)

That means the new `--worker-type=...` support is currently helper-level and unit-test-level, not production-path parity.

`--object-spilling-config` also remains unimplemented:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L207)

C++ still appends both flags for IO workers:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L346)

#### Recommendation

Do **not** describe IO-worker argv as fixed or mostly closed. It remains open until real production call sites propagate non-default worker types and object-spilling config.

### 2. Runtime-env hash parity is still not full C++ parity

**Severity:** MEDIUM

Rust now has a dedicated helper and uses `i32` with empty-env `0`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L86)

That is better than the previous state. But the helper still uses Rust `DefaultHasher`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L91)

while C++ uses:

- `std::hash<std::string>()`
- cast to `int`

See:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

Claude’s report changes the standard from “C++ parity” to “self-consistent within one Rust process” and then calls the item fixed. That is not the same claim.

#### Recommendation

Pick one standard and stick to it:

1. Exact C++ parity: reproduce the C++ hash contract
2. Rust-local operational equivalence: stop calling this parity closure

Until that choice is explicit, this area should not be described as closed parity.

### 3. Other excluded gaps remain excluded, not resolved

These are unchanged:

1. CLI defaults remain intentionally divergent
2. Object-store live runtime remains partial
3. Metrics exporter protocol remains architecturally different

These exclusions are acceptable only if the report stays narrow and precise.

## Confirmed Closures

The following earlier issues still appear genuinely fixed:

### Shutdown death-info propagation

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1302)

### Production auth loading

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)

### Eager-install gating

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

### Runtime-env context threading

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L425)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L788)

## Prescriptive Plan

### Priority 1: wire IO-worker type through real production paths

Implement all of the following:

1. Identify the actual Rust call paths for spill, restore, and delete worker startup
2. Pass real `WorkerType` values into `SpawnWorkerContext`
3. Add focused tests that exercise those production paths, not just `build_worker_argv()`

Acceptance criteria:

1. Production code constructs `SpawnWorkerContext { worker_type: SpillWorker | RestoreWorker | DeleteWorker, ... }`
2. Resulting argv contains the correct `--worker-type=...`

### Priority 2: implement `--object-spilling-config`

Implement:

1. access to the relevant RayConfig/object-spilling config in the spawn path
2. base64-escaped emission matching C++

This is required for honest IO-worker parity.

### Priority 3: decide the runtime-env-hash standard

Choose one:

1. match C++ exactly
2. explicitly downgrade the claim from parity to Rust-local equivalence

Do not mix those standards in the same report.

## Merge Recommendation

Do **not** declare “no remaining gaps.”

The accurate status is:

- Shutdown death-info: closed
- Auth loading: closed
- Eager-install semantics: closed
- Runtime-env context threading: closed
- IO-worker argv integration: still open
- `--object-spilling-config`: still open
- Runtime-env hash exact parity: still open unless the claim is explicitly narrowed

That is the defensible reading of the current Rust and C++ source.
