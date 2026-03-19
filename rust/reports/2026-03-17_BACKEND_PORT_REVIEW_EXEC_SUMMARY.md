# PR-Style Summary: Ray C++ to Rust Backend Port Review

## What Was Reviewed

This review compared the `cc-to-rust-experimental` branch against Ray’s
original C++ backend across five areas:

- GCS
- Raylet
- Core Worker
- Object Manager
- Python/native (`_raylet` / PyO3 / RDT) boundary

The goal was not to compare coding style. The goal was to determine whether the
Rust backend preserves the same externally visible behavior as the C++ backend.

## Bottom Line

The Rust port is not functionally equivalent to the original C++ backend.

This is not just a matter of internal refactoring. The review found multiple
confirmed differences in observable behavior, including:

- control-plane RPCs that are stubbed or materially reduced,
- distributed ownership and reference-counting semantics that are weaker,
- worker lease and shutdown behavior that diverges,
- object transfer and spill/restore behavior that is not equivalent,
- a Python/native API surface that is materially smaller than the Cython
  `_raylet` contract.

As a result, this branch should be treated as experimental, not as a drop-in
replacement for the C++ backend.

## Highest-Risk Gaps

### 1. GCS is incomplete in important control-plane paths

The Rust GCS still diverges from C++ in several core behaviors:

- internal KV uses in-memory storage where C++ uses the configured GCS backend,
- some job/worker/actor RPCs are stubbed or reduced,
- placement-group lifecycle and autoscaler/drain behavior are weaker than C++.

These are not cosmetic differences. They affect persistence, recovery, job
metadata, actor control, and cluster management behavior.

### 2. Raylet behavior is not a drop-in match

The Rust raylet does not yet preserve several important C++ semantics:

- worker lease return and request lifecycle,
- dependency management and `ray.wait` behavior,
- worker-pool matching and reuse,
- drain/shutdown and cleanup control flows.

This creates a high risk of behavioral drift under real workload pressure,
especially around scheduling, worker reuse, and node lifecycle transitions.

### 3. Core Worker semantics differ in fundamental ways

The Core Worker review found major non-equivalences in:

- ownership and reference counting,
- normal-task and actor-task submission,
- task execution ordering and cancellation,
- dependency and future resolution,
- object recovery,
- actor transport and actor lifecycle management.

This is one of the strongest reasons the Rust backend cannot currently be
treated as equivalent to the C++ implementation.

### 4. Object Manager semantics are weaker than C++

The Rust object-manager path diverges in several important areas:

- distributed object-location tracking,
- pull prioritization and failure behavior,
- push serving and resend behavior,
- inbound chunk validation/sealing,
- spill/restore integration.

In particular, the current receive path does not preserve the same chunk
validation and sealing guarantees as the C++ object manager.

### 5. The Python/native contract is materially different

The Rust Python boundary is not a thin reimplementation of Cython `_raylet`.
It exposes a smaller and different contract:

- `_raylet` coverage is much smaller,
- `CoreWorker` initialization/lifecycle differs,
- `ObjectRef` behavior is weaker,
- `get` and error behavior differ,
- `GcsClient` is reduced and includes stubs,
- RDT remains a hybrid Python/Rust subsystem.

This means Python-visible compatibility is currently not preserved.

## What Needs To Happen Before Claiming Equivalence

The must-fix order should be:

1. Restore correctness of distributed ownership, object fetch/transfer, and
   worker lease behavior.
2. Remove or replace explicit control-plane stubs across GCS, Raylet, and Core
   Worker.
3. Restore the minimum Python/native contract required by the existing Python
   stack.
4. Validate the result with conformance tests against the C++ backend before
   making broader equivalence claims.

## Recommended Test Strategy

The next step should not be “assume redesign is okay.” It should be test-first
validation against the C++ baseline.

Highest-priority tests:

- object fetch / wait / timeout / spill-restore conformance,
- actor task ordering / restart / cancellation,
- worker lease acquire / return / drain,
- placement-group lifecycle,
- Python `_raylet` / `ObjectRef` / `CoreWorker.get` compatibility,
- owner-death and node-death failure scenarios,
- duplicate and out-of-order object chunk delivery.

## Deliverables

The full detailed audit is here:

- [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)

This summary is intended to be the short human-readable version for PR or
leadership discussion.
