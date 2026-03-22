# Claude Round 9 Remaining Plan

Date: 2026-03-20
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND9.md`

## Non-Negotiable Rules

1. Read the C++ code first.
2. Add a failing Rust test first.
3. Confirm the failure before changing implementation.
4. Fix Rust.
5. Re-run the targeted tests.
6. Update the reports.

Do not mark anything fixed because:

- the subscriber primitive works in isolation
- the test directly calls `handle_poll_response(...)`
- the test directly calls `handle_publisher_failure(...)`

Those prove primitives. They do not automatically prove production-path integration.

## Work Order

1. Finish `RAYLET-4`
2. Finish `CORE-10`

## 1. RAYLET-4

### Read first

- the C++ owner-eviction subscriber transport path

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-pubsub/src/subscriber.rs`

### What is still wrong

- current tests prove subscriber primitives
- they do not yet prove the raylet production transport loop actually drives those primitives

### Add these failing tests first

- `test_pin_object_ids_eviction_subscriber_is_driven_by_production_transport_loop`
- `test_pin_object_ids_owner_failure_reaches_subscriber_without_manual_failure_call`

### Implementation guidance

- if the production loop already exists, test it directly
- if it does not, add the missing integration

### Acceptance

- no direct call to `handle_poll_response(...)` or `handle_publisher_failure(...)` from the test is needed to prove the production path

## 2. CORE-10

### Read first

- the C++ object-location subscription failure path

### Rust files

- `/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-pubsub/src/subscriber.rs`

### What is still wrong

- current tests prove subscriber failure primitives
- they do not yet prove the live object-location subscription path actually triggers that failure path

### Add these failing tests first

- `test_object_location_subscription_owner_failure_is_detected_by_production_loop`
- `test_object_location_subscription_failure_callback_runs_without_manual_handle_publisher_failure`

### Implementation guidance

- if the production failure path already exists, test it directly
- if it does not, add the missing integration

### Acceptance

- owner death is proven through the production subscriber/publisher failure path

## Validation

For each issue, report:

- exact C++ functions read
- exact Rust files changed
- exact tests added
- exact test commands run
- final status: `fixed`, `partial`, or `still open`

Update:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND9.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_CLAUDE_ROUND9_FIX_PLAN.md`
