# Backend Port Status Report

Date: 2026-03-18

Reference documents:

- [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)
- [`2026-03-17_BACKEND_PORT_REVIEW_EXEC_SUMMARY.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW_EXEC_SUMMARY.md)
- [`2026-03-17_BACKEND_PORT_TEST_MATRIX.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md)
- [`2026-03-17_BACKEND_PORT_BUG_TRACKER_SEED.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_BUG_TRACKER_SEED.md)

## Purpose

This report answers a narrower question than the original audit:

- what has actually been implemented since the audit,
- what has actually been tested,
- and what still remains before the Rust backend could be treated as functionally
  equivalent to the C++ backend.

The baseline conclusion from the original audit still stands:

The Rust backend is still not functionally equivalent to the original C++
backend.

What has changed is that several previously confirmed gaps have now been either:

- fixed outright,
- partially narrowed,
- or converted from report-only findings into executable tests.

## Original Remediation Strategy

The remediation strategy implied by the audit and test matrix was:

1. Preserve a clear subsystem map and behavioral contract.
2. Fix the highest-value externally visible discrepancies first.
3. Add executable tests for each confirmed bug or behavioral difference.
4. Prefer conformance-style validation over hand-waving redesign arguments.
5. Defer broad equivalence claims until distributed control-plane, ownership,
   object transfer, and Python/native contract gaps are covered.

That strategy is still the correct one.

## Current Status Summary

### Overall status

- The review and bug inventory are complete.
- The test matrix exists and covers every confirmed audit finding.
- Python compatibility and Tier 1 cluster scaffold tests are green.
- A subset of high-value GCS and Core Worker gaps has now been fixed and
  validated with targeted Rust tests.
- Large parts of Raylet, distributed ownership/reference semantics, Object
  Manager parity, and remaining GCS control-plane behavior are still open.

### What is now materially better than at audit time

- Rust `_raylet` / Python boundary compatibility is much stronger.
- Tier 1 cluster-backed smoke and conformance tests are passing.
- `ReportJobError` is no longer stubbed.
- `ReportActorOutOfScope` is no longer stubbed.
- worker lookup/update GCS RPCs are no longer stubbed or reduced in the same way
  they were at audit time.
- actor lineage-reconstruction restart RPC is no longer stubbed.
- owner-side Core Worker object-location update handling now covers more of the
  intended recipient and spill-update semantics.

## What Has Been Implemented

### Python / native compatibility work

Implemented areas:

- [`rust/ray/__init__.py`](/Users/istoica/src/ray/rust/ray/__init__.py)
  - per-actor serial dispatch to preserve actor call ordering
  - richer `ObjectRef` compatibility surface
- [`rust/ray/_raylet.py`](/Users/istoica/src/ray/rust/ray/_raylet.py)
  - expanded shim exports
- [`rust/ray/util/__init__.py`](/Users/istoica/src/ray/rust/ray/util/__init__.py)
  - minimal placement-group helper surface
- [`rust/ray-core-worker-pylib/src/gcs_client.rs`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/gcs_client.rs)
  - `drain_nodes`
  - async/multi-get KV compatibility helpers
- [`rust/ray/experimental/rdt/rdt_store.py`](/Users/istoica/src/ray/rust/ray/experimental/rdt/rdt_store.py)
  - removed explicit Python fallback mode

Status:

- Implemented
- Tested
- Green

### Tier 1 cluster-backed scaffold

Implemented tests in:

- [`rust/tests/test_tier1_cluster_scaffold.py`](/Users/istoica/src/ray/rust/tests/test_tier1_cluster_scaffold.py)

Covered scenarios:

- startup
- put/get roundtrip
- wait timeout/fetch baseline
- actor ordering
- placement-group lifecycle baseline
- worker lease/drain baseline

Status:

- Implemented
- Tested
- Green

### GCS fixes landed

Implemented runtime fixes:

- `GCS-2`
  - `ReportJobError` now publishes to `RAY_ERROR_INFO_CHANNEL`
  - files:
    - [`job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)
    - [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

- `GCS-10`
  - `ReportActorOutOfScope` now transitions actors to `DEAD`, removes named
    actor state, and suppresses stale reports
  - files:
    - [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)
    - [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

- `GCS-11`
  - `GetWorkerInfo`, `GetAllWorkerInfo`, `UpdateWorkerDebuggerPort`,
    `UpdateWorkerNumPausedThreads` are now implemented with filtering and
    count semantics
  - files:
    - [`worker_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/worker_manager.rs)
    - [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
    - [`server.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/server.rs)

- `GCS-9`
  - `RestartActorForLineageReconstruction` now has a real actor-manager and
    gRPC path, including stale-request suppression and restart scheduling
  - files:
    - [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)
    - [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

Supporting test-only work:

- [`pubsub_handler.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/pubsub_handler.rs)
  - added a test helper to inspect pending messages without reaching into
    private fields

Status:

- Implemented for the above findings
- Tested with targeted Rust tests
- Passing in isolated cargo runs

### Core Worker fixes landed

Implemented runtime fix:

- `CORE-10` partial
  - `UpdateObjectLocationBatch` now:
    - validates intended recipient worker ID
    - applies spilled-location updates to the reference counter
  - file:
    - [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)

Status:

- Partially implemented
- Targeted Rust tests pass
- Does not close the full distributed ownership gap from the original audit

## What Has Been Tested

### Python and cluster-backed tests

Verified passing:

- `rust/.venv/bin/python -m pytest rust/tests/test_backend_port_parity.py -q -rA`
  - result: `10 passed`
- `rust/.venv/bin/python -m pytest rust/tests/test_tier1_cluster_scaffold.py -q -rA`
  - result: `6 passed`

Interpretation:

- the Python/native surface is in much better shape than at audit time
- the Tier 1 cluster scaffold no longer shows the earlier obvious failures in
  actor ordering, placement-group surface, and drain-node baseline behavior

### Targeted Rust tests

Verified passing with isolated cargo target directories:

- `CARGO_TARGET_DIR=/tmp/ray-cargo-ray-gcs cargo test -p ray-gcs test_worker_grpc_get_all_filters_and_counts -- --nocapture`
  - result: passed
- `CARGO_TARGET_DIR=/tmp/ray-cargo-ray-gcs cargo test -p ray-gcs test_actor_grpc_restart_lineage_reconstruction -- --nocapture`
  - result: passed
- `CARGO_TARGET_DIR=/tmp/ray-cargo-core-worker cargo test -p ray-core-worker test_update_object_location_batch_sets_spilled_url -- --nocapture`
  - result: passed
- `CARGO_TARGET_DIR=/tmp/ray-cargo-core-worker cargo test -p ray-core-worker test_update_object_location_batch_rejects_wrong_recipient -- --nocapture`
  - result: passed

Important note:

- isolated `CARGO_TARGET_DIR`s were needed because the shared workspace target
  directory was frequently blocked by cargo artifact locking

### Test conversion progress versus the matrix

Findings with `added` status in the test matrix now include:

- Python/native compatibility tests
- Tier 1 cluster scaffold tests
- multiple Object Manager parity-gap tests
- `GCS-2`
- `GCS-9`
- `GCS-10`
- `GCS-11`
- `CORE-10` partial owner-update coverage

This is real progress, but most of the matrix is still in `spec`.

## What Has Been Completed Relative to the Original Proposal

### Completed

- Full audit and subsystem mapping
- Detailed finding inventory
- Executive summary
- Bug tracker seed list
- Per-finding test matrix
- Python/native compatibility remediation
- Tier 1 scaffold creation and execution
- Conversion of several explicit GCS stubs into real implementations
- Partial Core Worker ownership-path improvement
- Targeted executable verification for the newly implemented GCS/Core Worker work

### Partially completed

- “add tests for each bug and behavioral difference”
  - complete as a matrix/specification
  - only partially complete as runnable code
- “fix discrepancies between C++ and Rust”
  - complete only for a subset of high-value gaps
  - not complete overall
- distributed ownership remediation
  - partially improved in one owner-update path
  - not complete overall

### Not completed

- broad Raylet parity work
- broad Object Manager parity work
- remaining Core Worker parity work beyond the narrow `CORE-10` slice
- remaining GCS findings beyond the implemented subset
- full conformance harness against the C++ implementation
- proof that the Rust backend is a drop-in replacement

## What Remains To Be Done

### Highest-priority open runtime gaps

These remain the highest-value unfinished items from the audit:

1. Distributed ownership / reference-count / borrower protocol
   - audit refs: `CORE-2`, `CORE-10`, `OBJECT-1`
   - status: still open overall

2. Raylet worker lease / request lifecycle / drain / shutdown parity
   - audit refs: `RAYLET-1`, `RAYLET-2`, `RAYLET-7`, `RAYLET-8`
   - status: still open

3. Object transfer, fetch, resend, spill/restore parity
   - audit refs: `OBJECT-2`, `OBJECT-3`, `OBJECT-4`, `OBJECT-5`
   - status: only partial parity-gap tests exist; runtime parity still open

4. Remaining Core Worker semantic gaps
   - audit refs: `CORE-2` through `CORE-9`, `CORE-11`, `CORE-12`
   - status: open

5. Remaining GCS control-plane and persistence gaps
   - audit refs: `GCS-1`, `GCS-3` through `GCS-8`, `GCS-12` through `GCS-18`
   - status: open

### Highest-priority test work still missing

1. Redis-backed GCS persistence/restart conformance
   - especially `GCS-1`

2. Real distributed ownership tests
   - owner death
   - borrower cleanup
   - object location add/remove/spill updates

3. Raylet lease lifecycle conformance
   - request
   - return
   - retry
   - drain/reject semantics

4. Object Manager end-to-end transfer validation
   - exact bytes
   - metadata
   - duplicate/out-of-order chunk handling
   - spilled-object retrieval

5. Core Worker task and actor semantic conformance
   - cancellation
   - restart
   - dependency resolution
   - future resolution via owner

## Recommended Next Steps

Recommended next implementation order:

1. Finish distributed ownership and object-location protocol work.
   - This is still one of the largest blockers to parity.

2. Take the Raylet lease/drain/shutdown path next.
   - This is still one of the largest user-visible operational risks.

3. Finish the Object Manager parity path.
   - Especially remote push/readback and spill/restore semantics.

4. Return to the remaining GCS persistence and placement-group/autoscaler gaps.

5. After that, run broader conformance suites rather than only narrow targeted
   tests.

## Bottom Line

Compared with the original proposal, the work is now in a much better state:

- the audit is complete,
- the remediation is now tracked,
- several important explicit stubs have been replaced with real behavior,
- Python/Tier 1 tests are green,
- and targeted GCS/Core Worker Rust tests now pass.

But the core conclusion has not changed:

The Rust backend is still experimental and still not functionally equivalent to
the C++ backend.

The remaining work is not polish. It is still primarily about distributed
correctness and semantic parity.
