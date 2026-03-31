# Codex Re-Audit of Session Report Re Codex Re-Audit V4

**Date:** 2026-03-30
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-29_SESSION_REPORT_RE_CODEX_REAUDIT_V3.md`
**Scope:** Source-level audit of the latest Claude report against current Rust and C++ raylet implementations.

## Executive Verdict

Claude's latest round made real progress. The production worker-start path now carries runtime-env context instead of hardcoding placeholder values, and the previously disputed shutdown death-info gap remains fixed.

However, parity is **still not fully closed**.

The main remaining issue is that Rust now computes and emits a runtime-env hash, but it still does not match the C++ runtime-env-hash contract closely enough to justify a closure claim for regular workers.

## Findings

### 1. Runtime-env hash parity is still open

**Severity:** HIGH for any claim that regular-worker argv parity is closed

Claude’s report says Rust now matches C++ by computing `runtime_env_hash` from the serialized runtime-env string. The production threading is real, but the implementation still diverges from C++ in a way that matters.

#### What Rust now does

Rust computes the hash with `DefaultHasher` and stores/passes it as `u64`:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L399)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L768)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L81)
- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L122)

#### What C++ does

C++ computes runtime-env hash via `CalculateRuntimeEnvHash(...)`, which:

1. returns `0` for empty runtime env
2. otherwise computes `std::hash<std::string>()(serialized_runtime_env)`
3. casts that result to `int`

See:

- [scheduling_class_util.cc](/Users/istoica/src/ray/src/ray/common/scheduling/scheduling_class_util.cc#L196)
- [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L1867)

#### Why this is still a gap

There are two concrete mismatches:

1. Rust uses a different hash implementation surface: `DefaultHasher` rather than C++ `std::hash<std::string>`
2. Rust uses `u64`, while C++ truncates to `int`

That means Claude’s statement that this "matches C++" is not established by the source and is very likely false.

This is an inference from the code, but it is a strong one: exact cross-language hash equivalence is not something you get for free just because both sides hash the same string.

#### Recommendation

Do **not** mark regular-worker argv parity closed until Rust either:

1. reproduces the C++ runtime-env hash contract exactly, including output type and empty-env behavior, or
2. proves that the Rust worker path does not depend on exact C++ hash values and formally narrows the parity claim

### 2. IO-worker argv is still open

**Severity:** MEDIUM

Claude’s report is accurate here: IO-worker-specific argv remains unimplemented in Rust.

C++ still appends:

- `--worker-type=...`
- `--object-spilling-config=...`

See:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L346)

Rust still does not implement that path. This remains an open gap, not a documentation issue.

### 3. CLI defaults remain an intentional divergence

**Severity:** LOW to MEDIUM

This remains unchanged:

- Rust uses `0`
- C++ uses `-1`

for several sentinel defaults.

That is acceptable only as an explicit exclusion from parity closure, not as parity itself.

## Confirmed Closures

These earlier issues remain closed in the current source:

### Runtime-env timeout death-info propagation

Rust now carries shutdown reason and populates `node_death_info` for runtime-env timeout unregister:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1302)

### Production auth loading

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)

### Eager-install gating

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

## Prescriptive Plan

### Priority 1: fix runtime-env hash parity

This is now the main blocking gap for any claim that regular-worker startup parity is closed.

Implement all of the following:

1. Introduce a dedicated Rust helper for runtime-env hash computation instead of ad hoc `DefaultHasher` use
2. Match the C++ contract exactly:
   `""` and `"{}"` must map to `0`
3. Decide whether Rust should store/pass the value as signed 32-bit parity-compatible data rather than `u64`
4. Add a cross-language verification test using fixed runtime-env strings and expected values derived from the C++ helper

If exact C++ reproduction is not feasible, say so explicitly and stop calling this area closed.

### Priority 2: finish IO-worker argv parity

Implement:

1. `--worker-type`
2. `--object-spilling-config`

and add tests for the IO-worker spawn path.

### Priority 3: keep the parity claim narrow and accurate

Use one of these statements and nothing broader:

1. `Most runtime-env and regular worker-start gaps are closed, but runtime-env-hash parity and IO-worker argv remain open`
2. `Full raylet worker-start parity remains open`

Do not claim “no remaining gaps.”

## Merge Recommendation

Do **not** declare the worker-start parity work complete yet.

The current technically defensible position is:

- Shutdown death-info: closed
- Auth loading: closed
- Eager-install semantics: closed
- Runtime-env context threading: improved
- Runtime-env hash parity: still open
- IO-worker argv: still open
- CLI defaults: intentionally divergent

That is the correct reading of the current Rust and C++ source.
