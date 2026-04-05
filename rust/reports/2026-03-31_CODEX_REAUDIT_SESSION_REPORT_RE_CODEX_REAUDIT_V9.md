# Codex Re-Audit of Session Report Re Codex Re-Audit V9

**Date:** 2026-03-31
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-31_SESSION_REPORT_RE_CODEX_REAUDIT_V8.md`
**Scope:** Source-level verification of the latest Claude report against current Rust and C++ raylet code.

## Executive Verdict

Claude’s latest report is the most accurate one so far.

It no longer overclaims closure and instead makes a narrow, explicit scope claim: operational equivalence for the implemented Rust subset, with formal exclusions.

That narrower claim is technically defensible from the current source.

However, under the direct question “are there no longer gaps?”, the answer is still **no**. There are still remaining gaps. They are now explicitly classified rather than mislabeled.

## Findings

### 1. IO-worker subsystem remains unimplemented

**Severity:** HIGH

Claude is correct to classify this as out of current scope rather than accidentally “closed.” But it is still a remaining gap in broader raylet parity.

C++ still has:

- IO-worker pool APIs:
  [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L999)
- `TryStartIOWorkers`:
  [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1739)
- LocalObjectManager integration:
  [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327)

Rust still explicitly documents that these are not implemented:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L58)

This is still a real gap. It is simply now being described honestly.

### 2. Runtime-env hash remains an accepted exception, not exact parity

**Severity:** MEDIUM

Claude now states this as a formal operational-equivalence exception. That is much better than claiming exact parity.

Rust documents:

- `RUST-LOCAL OPERATIONAL EQUIVALENCE`

See:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L77)

Rust still uses `DefaultHasher`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L98)

while C++ still uses `std::hash<std::string>()`:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)

So this is no longer a report overclaim, but it is still a real non-parity item if the standard is strict C++ equivalence.

### 3. Excluded items are still not closed

Claude’s latest report formally excludes:

1. IO-worker subsystem
2. Object-store live runtime
3. Metrics exporter protocol
4. CLI sentinel defaults
5. Exact cross-language hash reproduction

That is a coherent and defensible scope statement.

But those are still remaining gaps relative to broad C++ raylet parity.

## Confirmed Closures

The following core areas still appear genuinely closed:

### Runtime-env protocol/auth/eager-install/death-info

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)

### Runtime-env context threading into worker start

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L439)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L802)

### Regular worker-start path within the implemented subset

This is now fairly described as closed for the implemented regular-worker path, not for the full C++ worker ecosystem.

## Prescriptive Plan

### Priority 1: lock the scope statement in writing

This should now be a project-level decision, not a report-by-report interpretation.

Use one canonical statement:

`The Rust raylet targets operational equivalence for the implemented subset. IO workers, object-store pin/eviction, metrics exporter protocol, CLI sentinels, and exact cross-language hash reproduction are excluded from the current parity scope.`

Do not reopen this wording every audit round.

### Priority 2: if full parity is still desired, make IO workers the next milestone

If the actual long-term target is still full raylet parity, the next milestone is clear:

1. Implement IO-worker pool APIs
2. Add LocalObjectManager integration
3. Propagate worker type on production paths
4. Implement `--object-spilling-config`

This is the largest remaining missing subsystem.

### Priority 3: keep reports honest about exceptions

For runtime-env hash and other accepted differences:

1. stop using “closed parity” language
2. use “accepted exception” or “operational-equivalence exception”
3. keep those exceptions listed in one place

## Merge Recommendation

My firm recommendation is:

- Do **not** say there are no remaining gaps
- Do **not** say full C++ raylet parity is closed
- It is acceptable to say the audited implemented subset is closed, with explicit exclusions

That is the strongest technically defensible conclusion from the current Rust and C++ code.
