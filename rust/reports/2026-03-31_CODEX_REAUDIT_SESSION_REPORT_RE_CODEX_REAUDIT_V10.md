# Codex Re-Audit of Session Report Re Codex Re-Audit V10

**Date:** 2026-03-31
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-31_SESSION_REPORT_RE_CODEX_REAUDIT_V8.md`
**Scope:** Source-level verification of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

Claude's latest report is technically defensible in its narrowed framing.

It no longer overclaims full parity. Instead, it claims operational equivalence for the implemented Rust subset and explicitly excludes several remaining areas. That scope statement is coherent with the current source.

However, under the direct question "are there no longer gaps?", the answer is still **no**. There are still remaining gaps. They are now documented as exclusions instead of being mislabeled as closures.

## Findings

### 1. IO-worker subsystem remains a real gap

**Severity:** HIGH

Claude is correct that this is not just a small argv issue. The Rust raylet still lacks the corresponding C++ IO-worker subsystem.

C++ still has:

- dedicated IO-worker pool operations in [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L999)
- `TryStartIOWorkers` in [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1739)
- LocalObjectManager integration in [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327)

Rust still explicitly documents those pieces as not implemented:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L58)

This remains a genuine parity gap relative to the C++ raylet.

### 2. Runtime-env hash remains an accepted exception, not exact parity

**Severity:** MEDIUM

Rust now documents this honestly as `RUST-LOCAL OPERATIONAL EQUIVALENCE`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L77)

Rust still uses `DefaultHasher`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L98)

C++ still uses `std::hash<std::string>()`:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

That means the item is no longer being overclaimed, but it is still not strict C++ parity.

### 3. Other exclusions remain real non-closures

The latest Claude report explicitly excludes:

1. IO-worker subsystem
2. object-store live runtime
3. metrics exporter protocol
4. CLI sentinel defaults
5. exact cross-language runtime-env hash reproduction

That is a coherent scope boundary, but those are still remaining gaps relative to broad C++ raylet parity.

## Confirmed Closures

The following core areas still appear genuinely closed:

### Runtime-env protocol/auth/eager-install/shutdown

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)

### Runtime-env context threading into worker start

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L439)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L802)

### Regular worker-start path within the implemented subset

This is now fairly described as closed only for the implemented regular-worker path, not for the full C++ worker model.

## Prescriptive Plan

### Priority 1: stop using "no remaining gaps" language

This should now be a hard rule.

Allowed claim:

`The implemented Rust subset is closed under the agreed operational-equivalence scope, with explicit exclusions.`

Disallowed claim:

`There are no remaining gaps.`

### Priority 2: if full parity remains the real objective, make IO workers the next milestone

Implement:

1. IO-worker pool APIs
2. `TryStartIOWorkers`
3. LocalObjectManager integration
4. production-path worker-type propagation
5. `--object-spilling-config`

This is the largest remaining subsystem gap.

### Priority 3: keep runtime-env hash classified as an exception unless exact parity is implemented

If the project later wants strict parity, replace the exception with an exact-compatible implementation. Until then, keep it explicitly categorized as an accepted operational-equivalence exception.

## Merge Recommendation

My recommendation is firm:

- Do **not** declare full C++ raylet parity closed
- Do **not** say there are no remaining gaps
- It is acceptable to say the implemented audited subset is closed, with explicit exclusions

That is the strongest technically defensible conclusion from the current Rust and C++ code.
