# Claude Execution Plan: Closing C++ vs Rust Backend Parity Gaps

Date: 2026-03-18

Primary references:

- [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)
- [`2026-03-17_BACKEND_PORT_TEST_MATRIX.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md)
- [`2026-03-18_BACKEND_PORT_STATUS_REPORT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md)

## Goal

Close the highest-value behavioral gaps between Ray’s original C++ backend and
the Rust backend in the `cc-to-rust-experimental` branch.

This plan assumes the current status is:

- Python/native compatibility work is largely stabilized.
- Tier 1 cluster scaffold is green.
- Some GCS gaps have already been fixed.
- The largest remaining risks are distributed ownership/reference semantics,
  Raylet lease and drain behavior, and Object Manager transfer/spill parity.

This plan is designed so Claude can execute it step by step without needing to
re-decide priorities.

## Non-Negotiable Rules

Claude should follow these rules throughout:

1. Do not treat a redesign as acceptable unless the observable behavior matches
   the C++ contract.
2. Do not close findings based on “it compiles” or “tests pass locally” unless
   those tests actually encode the C++ behavior.
3. For every runtime fix, add or upgrade a test that would have failed before
   the fix.
4. When a difference is found, classify it as:
   - intentional equivalent redesign,
   - intentional behavior change,
   - likely Rust bug,
   - inherited C++ bug,
   - still unresolved.
5. Do not remove or weaken existing parity-gap tests unless the runtime behavior
   has genuinely been fixed.
6. Keep the reports updated as work lands.
7. Prefer small, verifiable batches over huge refactors with no conformance
   checkpoints.

## Deliverables

Claude should produce all of the following:

1. Runtime fixes in Rust code.
2. Executable tests for every fix.
3. Updates to:
   - [`2026-03-17_BACKEND_PORT_TEST_MATRIX.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md)
   - [`2026-03-17_BACKEND_PORT_BUG_TRACKER_SEED.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_BUG_TRACKER_SEED.md)
   - [`2026-03-18_BACKEND_PORT_STATUS_REPORT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md)
4. A short execution log at the end of each tranche:
   - what was fixed,
   - what tests were added,
   - what passed,
   - what remains.

## Execution Order

Claude should work in this exact order unless blocked:

1. Distributed ownership and object-location protocol
2. Raylet lease / drain / shutdown parity
3. Object Manager transfer / spill / chunk validation parity
4. Remaining Core Worker semantic parity
5. Remaining GCS persistence / placement-group / autoscaler parity
6. Broader conformance and regression test sweep

This order is important. It prioritizes distributed correctness and
cluster-critical behavior ahead of surface completeness.

---

## Tranche 1: Distributed Ownership and Object-Location Protocol

### Why this is first

This is still one of the strongest reasons the Rust backend is not a drop-in
replacement. Several other subsystems depend on it behaving correctly.

Relevant findings:

- `CORE-2`
- `CORE-10`
- `OBJECT-1`
- parts of `CORE-8`
- parts of `CORE-9`

### Step 1.1: Reconstruct the exact C++ behavioral contract

Claude should first read and summarize the C++ owner-driven protocol in:

- [`src/ray/object_manager/ownership_object_directory.cc`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc)
- [`src/ray/core_worker/core_worker.cc`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc)
- [`src/ray/core_worker/reference_counter.cc`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc)
- [`src/ray/protobuf/core_worker.proto`](/Users/istoica/src/ray/src/ray/protobuf/core_worker.proto)

Claude should extract:

- who owns location truth,
- how add/remove/spill updates are propagated,
- batching semantics,
- owner-death behavior,
- borrower registration and cleanup behavior,
- what happens for dynamically created return IDs,
- what subscribers should observe when locations change.

Acceptance criterion:

- Claude writes a short contract summary into the status report before making
  implementation claims.

### Step 1.2: Build the missing tests first

Claude should add tests before major runtime changes.

Required tests:

1. Owner location add/remove propagation
   - object starts with no locations
   - add location update
   - verify owner reports location
   - remove location update
   - verify owner no longer reports location

2. Spilled-object owner update
   - send spilled location update
   - verify spill URL is tracked
   - verify local-vs-remote spill semantics match C++

3. Wrong recipient rejection
   - already partially added
   - keep and extend if needed

4. Owner-death behavior
   - borrower/subscriber should observe failure or cleanup behavior equivalent
     to C++

5. Borrower cleanup
   - add borrower
   - remove borrower
   - verify owner-side state converges correctly

6. Batched mixed updates
   - send add/remove/spill updates in one batch
   - verify final owner state matches C++

7. Dynamic return / generator edge case
   - only if Rust currently exposes the necessary path
   - otherwise document as blocked but keep a concrete spec in the test matrix

Files likely involved:

- [`rust/ray-core-worker/src/grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)
- [`rust/ray-core-worker/src/reference_counter.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs)
- [`rust/ray-core-worker/src/ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)
- possibly new integration tests under [`rust/ray-core-worker/tests`](/Users/istoica/src/ray/rust/ray-core-worker/tests)

Acceptance criterion:

- at least 4 ownership/location tests are runnable and failing for the right
  reason before the major implementation pass

### Step 1.3: Implement the missing runtime behavior

Claude should then implement the smallest correct runtime changes to satisfy the
tests.

Likely work items:

- extend owner-side object-location update handling beyond the currently fixed
  subset
- preserve batching semantics
- propagate spill URL and node semantics correctly
- register/remove borrower information in the right places
- surface owner-death consequences instead of keeping stale local state only
- avoid silently dropping updates for unknown but still valid objects when C++
  would track them

Acceptance criterion:

- the new ownership tests pass
- existing Core Worker tests still pass
- the status report explicitly says which parts of `CORE-10` remain open

### Step 1.4: Update tracking

Claude must then:

- update `CORE-10` notes in the matrix
- split `CORE-10` into sub-findings in the bug tracker if needed
- record what was fixed vs what remains

---

## Tranche 2: Raylet Lease / Drain / Shutdown Parity

### Why this is second

This is still one of the biggest operational correctness risks in cluster mode.

Relevant findings:

- `RAYLET-1`
- `RAYLET-2`
- `RAYLET-7`
- `RAYLET-8`
- likely parts of `RAYLET-9`

### Step 2.1: Extract the C++ contract

Claude should compare:

- worker lease request lifecycle
- return lease behavior
- spillback/retry behavior
- drain acceptance/rejection conditions
- shutdown semantics and ordering

Primary references:

- [`src/ray/raylet/node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)
- [`src/ray/raylet/worker_pool.cc`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc)
- [`src/ray/raylet/scheduling/*`](/Users/istoica/src/ray/src/ray/raylet/scheduling)

Acceptance criterion:

- Claude documents the C++ lease/drain contract before changing Rust behavior

### Step 2.2: Add conformance tests first

Required tests:

1. Request worker lease lifecycle
   - request lease
   - grant lease
   - return lease
   - verify resources are restored

2. Retry/spillback path
   - request cannot be served locally
   - verify retry/spillback behavior matches expected contract

3. Drain acceptance vs rejection
   - not every drain request should be blindly accepted
   - verify Rust matches the intended semantics

4. Shutdown ordering
   - verify shutdown does not silently skip cleanup steps expected by C++

5. Cleanup RPC effect test
   - actor/task cleanup requests must change state, not just return success

Files likely involved:

- [`rust/ray-raylet/src/node_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs)
- [`rust/ray-raylet/src/worker_pool.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs)
- existing Tier 1 scaffold
- possibly new raylet integration tests

Acceptance criterion:

- at least 3 new lease/drain tests exist and fail before fixes

### Step 2.3: Implement runtime fixes

Likely work items:

- correct resource accounting on lease return
- preserve lease request state transitions
- tighten drain semantics
- ensure shutdown path performs the expected cleanup/state changes

Acceptance criterion:

- new lease/drain tests pass
- Tier 1 cluster scaffold remains green

---

## Tranche 3: Object Manager Transfer / Spill / Chunk Validation Parity

### Why this is third

The audit found major data-plane mismatches that can produce silent corruption
or incorrect fetch behavior.

Relevant findings:

- `OBJECT-2`
- `OBJECT-3`
- `OBJECT-4`
- `OBJECT-5`

### Step 3.1: Use the existing parity-gap tests as the starting point

Claude should first inspect the already-added parity-gap tests and determine
which ones can be promoted from ignored/spec to active tests after fixes.

Existing test locations:

- [`rust/ray-object-manager/src/pull_manager.rs`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs)
- [`rust/ray-object-manager/src/object_buffer_pool.rs`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_buffer_pool.rs)
- [`rust/ray-object-manager/tests/integration_test.rs`](/Users/istoica/src/ray/rust/ray-object-manager/tests/integration_test.rs)

### Step 3.2: Add missing end-to-end tests

Required tests:

1. Remote push/readback exact bytes and metadata
2. Duplicate chunk does not complete object
3. Out-of-order chunk does not complete object
4. Undersized chunk does not complete object
5. Pull for nonlocal object is deferred then served correctly
6. Spilled-object fetch through the normal path
7. Duplicate pull request resend/dedup semantics

Acceptance criterion:

- at least 3 currently ignored/spec Object Manager tests are either promoted or
  replaced by runnable active tests

### Step 3.3: Implement runtime fixes

Likely work items:

- enforce chunk validation/sealing semantics
- preserve correct deferred push behavior
- implement missing resend/dedup behavior
- integrate spill/restore path into the normal object-manager path

Acceptance criterion:

- active Object Manager parity tests pass
- failures are no longer hidden behind `#[ignore]` where behavior is now fixed

---

## Tranche 4: Remaining Core Worker Semantic Parity

### Why this is fourth

This area is still broad, but it should be tackled after the ownership and
data-plane foundations are stronger.

Relevant findings:

- `CORE-2`
- `CORE-3`
- `CORE-4`
- `CORE-5`
- `CORE-6`
- `CORE-7`
- `CORE-8`
- `CORE-9`
- `CORE-11`
- `CORE-12`

### Step 4.1: Prioritize by user-visible semantics

Claude should do these in order:

1. task submission and cancellation
2. dependency resolution
3. future resolution via owner
4. object recovery / lineage
5. actor transport / actor lifecycle

### Step 4.2: Add targeted conformance tests

Required categories:

- actor task ordering and cancellation under failure
- dependency resolution for nonlocal and actor-aware paths
- future resolution via owner RPC
- recovery after object loss using lineage
- named actor state updates and restart handling

Acceptance criterion:

- each fixed Core Worker behavior has at least one failing-then-passing test

---

## Tranche 5: Remaining GCS Persistence / Placement-Group / Autoscaler Gaps

### Why this is fifth

These are still important, but they should come after the deeper distributed
correctness work above.

Relevant findings:

- `GCS-1`
- `GCS-3`
- `GCS-4`
- `GCS-5`
- `GCS-6`
- `GCS-7`
- `GCS-8`
- `GCS-12`
- `GCS-13`
- `GCS-14`
- `GCS-15`
- `GCS-16`
- `GCS-17`
- `GCS-18`

### Step 5.1: Start with persistence and placement groups

Order:

1. internal KV persistence across restart
2. placement-group lifecycle
3. placement-group removal persistence
4. wait-until-ready semantics
5. autoscaler state/version correctness
6. node drain acceptance correctness

### Step 5.2: Add executable tests

Required tests:

- Redis-backed GCS restart persistence
- full placement-group lifecycle
- removed PG remains queryable in the correct state
- async wait semantics and timeout behavior
- stale autoscaler version rejection
- drain-node acceptance tied to actual backend state

Acceptance criterion:

- no remaining explicit GCS control-plane stubs
- persistence and PG behaviors have runnable tests

---

## Tranche 6: Broader Conformance and Regression Sweep

### Step 6.1: Re-run all existing targeted suites

Claude should re-run at minimum:

- Python parity tests
- Tier 1 cluster scaffold
- targeted `ray-gcs` tests
- targeted `ray-core-worker` tests
- targeted Object Manager tests
- targeted Raylet tests added in earlier tranches

If cargo locking remains a problem, use isolated `CARGO_TARGET_DIR`s.

### Step 6.2: Promote tests from “documentation of known gap” to “active parity test”

Claude should review the current `added` tests that are still ignored or were
originally documenting gaps, and promote them if the runtime has been fixed.

### Step 6.3: Update the reports

Claude must finish by updating:

- status report
- test matrix
- bug tracker seed
- executive summary, if the high-level risk picture has changed

Acceptance criterion:

- the reports clearly distinguish:
  - fixed findings
  - partially fixed findings
  - findings still open

---

## How Claude Should Work Within Each Tranche

For each tranche, Claude should use the following mini-loop:

1. Read the C++ implementation and write down the exact behavioral contract.
2. Compare the Rust code against that contract.
3. Add or activate failing tests that encode the contract.
4. Fix the Rust runtime behavior.
5. Re-run the targeted tests.
6. Update the tracking documents.
7. Write a short tranche summary.

Claude should not skip step 1 or step 3.

## What Counts as “Done”

Claude should only mark a finding as done when:

1. the runtime behavior is implemented,
2. there is an executable test covering it,
3. the test passes,
4. the test matrix is updated,
5. the status report says whether the entire finding is closed or only narrowed.

## What Claude Should Not Do

Claude should not:

- declare parity based on green smoke tests alone
- paper over distributed behavior with Python shims
- collapse multiple unresolved gaps into one vague “improved” note
- weaken or delete tests that expose real discrepancies
- stop after surface-level API matching when the underlying state machine still
  differs

## Immediate Next Action

Claude should start with Tranche 1, Step 1.1:

Read the C++ ownership/location protocol, summarize the contract in the status
report, and then add the missing owner-location tests before implementing more
runtime changes.
