# Codex Re-Audit of Session Report Re Codex Re-Audit V11

**Date:** 2026-03-31
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-31_SESSION_REPORT_RE_CODEX_REAUDIT_V10.md`
**Scope:** Source-level verification of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

Claude's latest report is coherent and technically defensible in its narrowed framing.

It does not overclaim full parity. It explicitly limits the closure claim to the implemented Rust subset and keeps several items as exclusions or accepted exceptions.

However, the answer to your direct question is still **no**: there are still remaining gaps if the reference standard is the full C++ raylet.

## Findings

### 1. IO-worker subsystem remains unimplemented

**Severity:** HIGH

This remains the largest substantive parity gap.

C++ still has:

- `PopSpillWorker`, `PopRestoreWorker`, `PopDeleteWorker` in [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L999)
- `TryStartIOWorkers` in [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1739)
- LocalObjectManager integration in [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327)

Rust still explicitly documents these as not implemented:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L68)

This is still a real remaining gap.

### 2. Runtime-env hash remains an accepted exception, not exact C++ parity

**Severity:** MEDIUM

Rust explicitly documents this as `RUST-LOCAL OPERATIONAL EQUIVALENCE`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L77)

Rust still uses `DefaultHasher`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L98)

C++ still uses `std::hash<std::string>()`:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

This is acceptable only under the narrowed operational-equivalence scope. It is not full C++ parity.

### 3. Other exclusions remain non-closures

The latest Claude report still relies on explicit exclusions for:

1. IO workers
2. object-store pin/eviction
3. metrics exporter protocol
4. CLI sentinel defaults
5. exact cross-language hash reproduction

That makes the narrowed claim defensible, but it also means there are still remaining gaps relative to broad parity.

## Confirmed Closures

The following still appear genuinely closed in the current source:

### Runtime-env protocol/auth/eager-install/shutdown

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)

### Runtime-env context threading into worker start

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L439)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L802)

### Regular worker-start path within the implemented subset

This remains fairly described as closed only for the implemented regular-worker path.

## Prescriptive Plan

### Priority 1: keep the scope claim narrow and final

Use one stable statement:

`The implemented audited subset is closed, with explicit exclusions.`

Do not use:

`There are no remaining gaps.`

### Priority 2: if broad parity is still the real target, implement IO workers next

The next concrete milestone remains:

1. IO-worker pool APIs
2. `TryStartIOWorkers`
3. LocalObjectManager integration
4. production-path worker-type propagation
5. `--object-spilling-config`

### Priority 3: keep runtime-env hash classified as an exception unless exact parity is implemented

Do not drift back into parity-closed language for this item unless the implementation changes.

## Merge Recommendation

My firm recommendation is unchanged:

- Do **not** say there are no remaining gaps
- Do **not** say full C++ raylet parity is closed
- It is acceptable to say the implemented audited subset is closed, with explicit exclusions

That is the strongest technically defensible conclusion from the current Rust and C++ code.
