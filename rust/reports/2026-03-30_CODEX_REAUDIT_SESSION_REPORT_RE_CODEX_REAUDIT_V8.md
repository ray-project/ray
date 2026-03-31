# Codex Re-Audit of Session Report Re Codex Re-Audit V8

**Date:** 2026-03-30
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-30_SESSION_REPORT_RE_CODEX_REAUDIT_V6.md`
**Scope:** Source-level verification of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

The latest Claude report is more accurate than the previous ones, but there are still remaining gaps.

The answer to “are there no longer gaps?” is still **no**.

## Findings

### 1. IO-worker subsystem remains unimplemented in Rust

**Severity:** HIGH

Claude’s latest report is correct to downgrade this from an argv-only issue to a subsystem gap.

C++ has dedicated IO-worker pool operations:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L999)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1008)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1072)

and LocalObjectManager integration:

- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327)
- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L477)
- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L581)

Rust still has no equivalent production subsystem. The Rust code itself now says so:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L68)

This remains a real parity gap. It is not closed.

### 2. Runtime-env hash remains a narrowed-equivalence item, not exact C++ parity

**Severity:** MEDIUM

Rust now explicitly documents that this is Rust-local operational equivalence, not exact C++ parity:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L79)

That is a more honest framing. But it also means this item is still not exact parity.

Rust uses:

- `DefaultHasher`
- truncated to `i32`

See:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L98)

C++ uses:

- `std::hash<std::string>()`
- cast to `int`

See:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

That difference is acceptable only if the project explicitly accepts operational equivalence instead of strict parity for this item.

### 3. Excluded gaps remain excluded

These remain open or intentionally divergent:

1. CLI sentinel defaults
2. Object-store live runtime
3. Metrics exporter protocol

So a broad parity-closed statement is still not defensible.

## Confirmed Closures

The following earlier items still appear genuinely closed:

### Shutdown death-info propagation

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1302)

### Production auth loading

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)

### Eager-install gating

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

### Runtime-env context threading into worker start

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L439)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L802)

## Prescriptive Plan

### Priority 1: decide whether the standard is strict parity or narrowed equivalence

This is now the gating reporting decision.

Choose one:

1. Strict C++ parity
2. Operational equivalence for the implemented Rust subset

If the project chooses strict parity, runtime-env hash remains open and IO workers remain open.

If the project chooses subset equivalence, that must be stated clearly and consistently in every report.

### Priority 2: implement the IO-worker subsystem if full parity is still the goal

This is the next major code milestone.

Implement:

1. Worker-pool APIs equivalent to `PopSpillWorker`, `PopRestoreWorker`, `PopDeleteWorker`
2. LocalObjectManager integration
3. Production-path worker-type propagation
4. `--object-spilling-config`

Do not frame this as minor cleanup. It is substantive missing functionality.

### Priority 3: formalize the runtime-env-hash exception if you are keeping it

If the project will keep Rust-local hash equivalence instead of exact C++ parity, make that a written project decision and keep it out of parity-closed language.

## Merge Recommendation

Do **not** declare that there are no remaining gaps.

The defensible status is:

- Core runtime-env/auth/eager-install/shutdown work: closed
- Runtime-env hash: narrowed operational equivalence, not exact C++ parity
- IO-worker subsystem: not implemented
- Broader object-store and metrics differences: still open/excluded

That is the correct reading of the current Rust and C++ code.
