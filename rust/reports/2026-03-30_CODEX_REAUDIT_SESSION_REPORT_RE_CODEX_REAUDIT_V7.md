# Codex Re-Audit of Session Report Re Codex Re-Audit V7

**Date:** 2026-03-30
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-30_SESSION_REPORT_RE_CODEX_REAUDIT_V6.md`
**Scope:** Source-level audit of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

Claude’s latest report is materially more accurate than the prior one.

It now correctly stops claiming that IO-worker argv is fixed and instead classifies the entire IO-worker subsystem as unimplemented in Rust. That is the right framing.

However, there are still remaining gaps. The answer to “are there no longer gaps?” is still **no**.

## Remaining Gaps

### 1. IO-worker subsystem is not implemented in Rust

**Severity:** HIGH

This is no longer just an argv issue. The missing functionality is broader:

C++ has:

- `PopSpillWorker`
- `PopRestoreWorker`
- `PopDeleteWorker`
- LocalObjectManager integration that actually requests these workers

See:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L995)
- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327)

Rust still does not implement the corresponding subsystem. Claude’s latest report is correct on that point.

This remains a real parity gap.

### 2. Runtime-env hash is still not exact C++ parity

**Severity:** MEDIUM

Rust now explicitly documents this as “Rust-local operational equivalence,” which is more honest:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L98)

But the implementation still uses `DefaultHasher`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L103)

while C++ still uses:

- `std::hash<std::string>()`
- cast to `int`

See:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

That means the latest report is acceptable only if the project intentionally narrows the claim away from exact C++ parity for this item.

If the standard remains “match C++ behavior,” this item is still open.

### 3. Other declared exclusions remain exclusions, not closures

These remain outside closure:

1. CLI sentinel defaults are intentionally divergent
2. Object-store live runtime remains partial
3. Metrics exporter protocol remains architecturally different

A broad “raylet parity closed” claim is still not supportable.

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

### Runtime-env context threading

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L439)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L802)

## Prescriptive Plan

### Priority 1: make an explicit decision on parity scope

This has to be resolved before any final parity statement.

Choose one:

1. **Strict C++ parity**
2. **Operational equivalence for the implemented Rust subset**

Do not mix these standards in the same report.

If the project chooses strict C++ parity, runtime-env hash remains open and IO workers remain open.

If the project chooses subset equivalence, say so explicitly and exclude those items from closure claims.

### Priority 2: implement the IO-worker subsystem if full parity is required

If full parity is the goal, the next work item is not a small cleanup. It is a subsystem implementation.

Implement:

1. IO-worker pool APIs equivalent to C++ `PopSpillWorker`, `PopRestoreWorker`, `PopDeleteWorker`
2. Integration from Rust local object manager into that pool
3. Production-path worker-type propagation
4. `--object-spilling-config` emission

Treat this as a separate parity milestone, not a small tail task.

### Priority 3: either accept or close the runtime-env-hash difference

If strict parity is required:

1. Reproduce the C++ hash contract more exactly, or
2. Prove that exact C++ hash equality is unnecessary and formally downgrade the claim

Right now the latest report does the second informally. That should be made explicit in project-level parity criteria.

## Merge Recommendation

Do **not** state that there are no remaining gaps.

The technically defensible status is:

- Core runtime-env protocol/auth/eager-install/shutdown fixes: closed
- Regular worker-start path: largely closed
- Runtime-env hash: narrowed to Rust-local equivalence, not exact C++ parity
- IO-worker subsystem: not implemented
- Broader object-store and metrics differences: still open/excluded

That is the correct reading of the current Rust and C++ source.
