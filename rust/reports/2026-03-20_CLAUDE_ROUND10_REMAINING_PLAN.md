# Claude Round 10 Remaining Plan

Date: 2026-03-20
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND10.md`

## Non-Negotiable Rules

1. Read the C++ code first.
2. Add a failing runtime-integration test first.
3. Confirm the failure before changing implementation.
4. Fix Rust.
5. Re-run the targeted tests.
6. Update the reports.

Do not mark anything fixed because:

- the transport driver exists
- the transport driver works in isolated tests
- the transport driver is called from test code

Full parity requires runtime integration, not just test-driven transport simulation.

## Work Order

1. Finish `RAYLET-4`
2. Finish `CORE-10`

## 1. RAYLET-4

### Read first

- the C++ owner-eviction subscriber polling lifecycle

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-pubsub/src/transport.rs`

### What is still wrong

- `SubscriberTransport` exists, but it is not wired into the live raylet runtime

### Add these failing tests first

- `test_raylet_eviction_transport_loop_runs_in_runtime`
- `test_raylet_owner_failure_reaches_eviction_subscriber_in_runtime`

### Implementation guidance

- add the runtime task or loop that polls publishers for the eviction subscriber
- ensure pending commands are actually sent and long-polls are actually driven in production code

### Acceptance

- no test-created manual transport driver is required to prove the path works

## 2. CORE-10

### Read first

- the C++ object-location subscription polling/failure lifecycle

### Rust files

- `/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-pubsub/src/transport.rs`
- any runtime-side owner/subscriber orchestration code used for object locations

### What is still wrong

- `SubscriberTransport` exists, but it is not wired into the live object-location subscription runtime

### Add these failing tests first

- `test_object_location_transport_loop_runs_in_runtime`
- `test_object_location_owner_failure_reaches_subscriber_in_runtime`

### Implementation guidance

- add the runtime task or loop that polls publishers for object-location subscriptions
- ensure failure detection and callback delivery happen in runtime

### Acceptance

- no test-created manual transport driver is required to prove the path works

## Validation

For each issue, report:

- exact C++ functions read
- exact Rust files changed
- exact tests added
- exact test commands run
- final status: `fixed`, `partial`, or `still open`

Update:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND10.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_CLAUDE_ROUND10_FIX_PLAN.md`
