# Codex Re-Audit of Session Report Re Codex Re-Audit V3

**Date:** 2026-03-29
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-03-29_SESSION_REPORT_RE_CODEX_REAUDIT_V2.md`
**Scope:** Source-level audit of current Rust raylet against the corresponding C++ raylet behavior.

## Executive Verdict

Claude's latest round is materially better and substantially more accurate than the prior report.

The previously disputed high-severity shutdown gap is now genuinely fixed: Rust now propagates runtime-env timeout shutdown cause into `UnregisterNodeRequest.node_death_info`, which closes the specific C++ parity issue I raised.

However, parity is **still not fully closed**. The remaining gaps are narrower, but they are real:

1. Worker argv parity is still only partial
2. CLI defaults remain an intentional divergence
3. Broader non-parity areas remain explicitly open: object-store live runtime and metrics exporter protocol

## Confirmed Closures

### 1. Runtime-env timeout death-info propagation is now fixed

Rust now carries shutdown cause through a typed shutdown path and includes `UNEXPECTED_TERMINATION` death info for runtime-env timeout unregisters:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L961)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1186)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1302)

This now matches the relevant C++ contract closely enough to close the specific gap:

- [runtime_env_agent_client.cc](/Users/istoica/src/ray/src/ray/raylet/runtime_env_agent_client.cc#L309)
- [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L495)

### 2. Production auth loading remains closed

Rust still loads auth token from env/file sources and passes it into production config:

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L346)

### 3. Eager-install gating remains closed

Rust still gates eager install on `runtime_env_config.eager_install()` semantics:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L427)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L480)
- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L730)

## Remaining Gaps

### 1. Worker argv parity is still incomplete

**Severity:** MEDIUM

This remains the main open parity item in the area Claude was addressing.

#### What is fixed

Rust now correctly avoids the earlier hardcoded `--language=PYTHON` bug and has language-specific handling:

- Python and Java use hyphen-delimited flags
- C++ uses `--ray_worker_id` and `--ray_runtime_env_hash`

See:

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L159)

#### What is still not closed

The production spawner config still hardcodes:

- `runtime_env_hash = 0`
- `serialized_runtime_env_context = ""`

See:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1056)

That means the argv surface exists, but the production path still does not carry real per-task runtime-env state.

Rust also still does not implement the C++ IO-worker-specific flags:

- `--worker-type=...`
- `--object-spilling-config=...`

See C++ reference:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L346)

#### Assessment

Claude’s current classification of this area as `Partially matched` is fair. It should stay open.

It is not acceptable to roll this into a “no longer gaps” conclusion.

### 2. CLI defaults remain intentionally divergent

**Severity:** LOW to MEDIUM

Rust still uses `0` where C++ uses `-1` for some sentinel defaults:

- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L119)
- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L147)
- [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L188)

vs.

- [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L70)
- [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L72)
- [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L114)

This is acceptable only as an explicit non-parity carveout. It is still a divergence.

### 3. Broader non-parity areas remain open by Claude’s own status table

**Severity:** depends on scope

These were not newly introduced, but they are still open if the claim is broad raylet parity:

1. Object-store live runtime is still partial
2. Metrics exporter protocol is still architecturally different

If the project wants to claim only parity for the audited runtime-env and worker-spawn fixes, that narrower claim is defensible. A broad “raylet parity closed” claim is still not.

## Prescriptive Plan

### Priority 1: finish worker argv parity

This is the next concrete parity item to close.

Implement all of the following:

1. Thread real `runtime_env_hash` through the Rust worker startup path
2. Thread real `serialized_runtime_env_context` through the Rust worker startup path
3. Add IO-worker-specific argv handling equivalent to C++
4. Add tests that assert actual production-path argv, not only helper-level formatting

Acceptance criteria:

1. Production path no longer hardcodes `runtime_env_hash = 0`
2. Production path no longer hardcodes empty runtime-env context
3. IO-worker argv matches the C++ cases that matter

### Priority 2: either align CLI sentinels or keep them permanently out of parity claims

Pick one and enforce it consistently:

1. Match C++ default sentinels exactly
2. Keep Rust semantics as-is and permanently classify them as intentional divergence

Do not let this remain half-in, half-out of parity language.

### Priority 3: decide the scope of the parity claim

This is a reporting discipline issue, not just a code issue.

Use one of these two phrasings:

1. `Runtime-env and core worker-spawn parity gaps are mostly closed, with remaining worker argv work`
2. `Full raylet parity is still open due to worker argv, object-store runtime, and metrics exporter differences`

Do not use a generic “parity closed” statement.

## Merge Recommendation

My recommendation is:

- Do **not** declare full C++ raylet parity closed
- It is reasonable to declare the earlier shutdown/death-info issue closed
- It is reasonable to declare auth and eager-install closed
- It is **not** reasonable to say there are no remaining gaps

The remaining gaps are narrower now, but worker argv parity is still open, and the broader non-parity areas are still explicitly open.
