# Codex Re-Audit of Claude Final Raylet Parity Fixes

Date: 2026-03-27
Branch: `cc-to-rust-experimental`
Subject: Re-audit of `2026-03-27_CLAUDE_FINAL_RAYLET_PARITY_FIXES.md`

## Executive Summary

The latest Claude pass fixed one real bug and improved several proof gaps. But raylet parity is still **not** fully closed.

The most important remaining issue is severe: the corrected `start_worker_with_runtime_env()` sequencing appears to be **test-only**. I found no production call site using it. That means the worker-start/runtime-env parity claim is still not closed in the live raylet path.

Two more important issues remain:

1. the new object-manager/plasma path is still mostly a sidecar for stats, not the main live object-management runtime path
2. the metrics path is now honestly downgraded as architecturally different, which is better reporting, but it is still a real C++ vs Rust difference rather than closure

Bottom line: substantial progress, but there are still remaining gaps.

---

## Findings

### 1. High: the runtime-env sequencing fix is not wired into the live production path

Claude fixed `start_worker_with_runtime_env()` itself. The new implementation now waits for runtime-env creation success before starting the worker:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L343)

That part is real and improved.

However, I searched for production call sites and found `start_worker_with_runtime_env(...)` referenced only inside `worker_pool.rs` tests:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1524)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1602)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1640)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1650)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1711)

I did **not** find any production raylet/scheduler path calling it.

That means the fixed sequencing exists in code, but there is no source-backed proof that the live worker-start path actually uses it.

C++ clearly uses the runtime-env-gated path in production worker startup:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1377)

This is the most important remaining gap. A parity fix that is only exercised by unit tests is not closure.

Firm conclusion: runtime-env worker-start parity is **still open**.

### 2. High: runtime-env failure reporting is still not clearly matched to C++

C++ does more than just “not start the worker.” It also reports a specific failure status:

- `PopWorkerStatus::RuntimeEnvCreationFailed`

See:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1387)

Rust now avoids starting the worker in the helper, but the current fix still returns `None` immediately from `start_worker_with_runtime_env()` in [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L388), and I did not find a production path propagating a `RuntimeEnvCreationFailed` equivalent back through the scheduler-facing lease/startup flow.

The type `PopWorkerStatus::RuntimeEnvCreationFailed` exists in Rust, but the current audit did not find a live path that uses it for this worker-start failure scenario.

So even beyond the missing production call site, the observable failure contract is still underimplemented.

Firm conclusion: runtime-env failure semantics are **still not fully matched**.

### 3. High: the object-manager/plasma path is still not the main live object-management runtime

Rust now constructs an `ObjectManager` from raylet config:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L699)

This is real progress.

But the actual object-management RPC handling still overwhelmingly routes through `local_object_manager()`:

- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L419)
- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L460)
- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L746)
- many more later call sites in the same file

The new `object_manager()` is now used for stats-side reads in `GetNodeStats`:

- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L752)

Claude’s new “live allocation” test proves that:

- objects created directly through the side `ObjectManager` affect `GetNodeStats`

See:

- [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L752)

That is useful, but it still does **not** prove that the running raylet object-management behavior uses this path for its real object lifecycle. It proves stats visibility, not full live-path parity.

Firm conclusion: object-store/runtime parity is **still not fully closed**.

### 4. Medium: metrics/exporter behavior remains a real C++ vs Rust difference

Claude improved the reporting here by explicitly downgrading the claim.

Rust now documents the difference clearly in:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1055)

That is better.

But the difference still exists:

- C++ connects OpenCensus/OpenTelemetry exporters to the metrics agent in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L1074)
- Rust starts a standalone `MetricsExporter` and Prometheus HTTP server in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1085)

This is no longer a reporting error, but it is still a real implementation difference. It should not be mistaken for closure.

Firm conclusion: metrics/exporter parity is **intentionally different, not closed**.

### 5. Medium: the stronger `session_dir` proof looks materially better

This is one area where the latest pass does look much stronger.

Rust now has:

- `resolve_all_ports()` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L624)
- end-to-end tests verifying four-port resolution and CLI override precedence in:
  - [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L1075)
  - [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L1145)

I do not currently see a new source-backed reason to keep the `session_dir` helper path open as a primary finding.

Firm conclusion: `session_dir` coverage now appears **substantially addressed**.

### 6. Medium: agent-monitoring proof is stronger, but the core remaining blockers are elsewhere

The monitoring implementation is materially better than earlier rounds, and the tests are stronger than before:

- respawn loop in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L180)
- stronger tests in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L899)

I would not keep agent monitoring as a top blocker now. The more important remaining gaps are the missing production runtime-env hookup and the still-sidecar object-manager path.

---

## Recommended Status

The correct status after this pass is:

- runtime-env helper sequencing bug: fixed
- runtime-env live production hookup: still open
- runtime-env failure-status parity: still open
- object-store live runtime wiring: still open
- metrics/exporter parity: architecturally different
- `session_dir` end-to-end resolution: likely closed
- agent monitoring: much improved, likely no longer a top blocker

That is the source-backed assessment.

---

## Prescriptive Closure Plan

### Phase 1: Wire the runtime-env-gated worker start into the actual live worker-start path

This is the top priority.

You already fixed the helper. Now use it in production.

Required work:

1. identify the real scheduler / worker-pool path that starts workers today
2. replace the direct start path with `start_worker_with_runtime_env(...)` where runtime env is non-trivial
3. make sure the caller receives the correct deferred/success/failure behavior

This must not remain a test-only utility.

### Phase 2: Propagate `RuntimeEnvCreationFailed` semantics through the real worker-start flow

Matching C++ requires more than “worker did not start.”

Required work:

1. define the real Rust call path that should observe worker-start failure
2. propagate a `RuntimeEnvCreationFailed`-equivalent status through that path
3. add a decisive test proving a failed runtime-env setup produces the expected externally visible failure

Without this, the helper behavior still falls short of the C++ contract.

### Phase 3: Either wire the new ObjectManager into the live raylet object path or downgrade the object-store claims

Right now the object manager is still mostly a stats-visible sidecar.

You must do one of:

1. integrate it into the actual live object-management/runtime path
2. or explicitly downgrade the object-store rows to partial/incomplete

Required decisive test if you keep parity claims:

- exercise a real raylet object-management path
- prove the configured object-store runtime is the one actually serving that path

Stats-only evidence is no longer enough.

### Phase 4: Keep the metrics row downgraded unless you truly implement the C++ exporter contract

Do not re-upgrade the metrics row unless you actually close the protocol/semantics gap.

If you keep it architecturally different, that is acceptable. What is not acceptable is calling it equivalent without the actual behavior.

---

## Verification Notes

I directly inspected the latest Rust and C++ sources cited above.

I also attempted targeted `cargo test` runs from `/Users/istoica/src/ray/rust`, but cargo artifact locking prevented immediate verification completion in this turn. The conclusions above are therefore based primarily on direct source comparison plus inspection of the newly added tests in source.

---

## Final Recommendation

Do not ask Claude for another broad parity summary.

Ask for one narrow, final corrective pass:

1. wire the runtime-env-gated worker-start helper into the real production path
2. propagate runtime-env creation failure through the actual worker-start status path
3. either wire the new object manager into the live object path or downgrade the object-store claims

That is the shortest path to ending the back-and-forth without another false closure claim.
