# Claude Round 8 Remaining Plan

Date: 2026-03-20
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND8.md`

## Non-Negotiable Rules

1. Read the C++ code first.
2. Add a failing Rust test first.
3. Confirm the failure before changing implementation.
4. Fix Rust.
5. Re-run the targeted tests.
6. Update the reports.

Do not mark anything fixed because:

- the subscriber object exists
- a helper injects messages into the subscriber
- a test removes a subscriber and calls that owner death

## Work Order

1. Finish `RAYLET-4`
2. Finish `CORE-10`

## 1. RAYLET-4

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-pubsub/src/subscriber.rs`

### What is still wrong

- the raylet path still appears to rely on helper-injected subscriber delivery
- there is not yet proof of a fully integrated transport loop for eviction delivery

### Add these failing tests first

- `test_pin_object_ids_subscriber_commands_are_drained_for_owner`
- `test_pin_object_ids_long_poll_delivery_releases_pin_without_notify_helper`
- `test_pin_object_ids_owner_death_via_real_subscriber_failure_path`

### Implementation guidance

- wire the raylet subscriber into the actual command/drain/poll lifecycle or an exact equivalent
- do not leave helper injection as the only demonstrated path

### Acceptance

- pin release is proven through the intended subscriber transport path

## 2. CORE-10

### Read first

- the C++ object-location subscription failure behavior

### Rust files

- `/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs`
- `/Users/istoica/src/ray/rust/ray-pubsub/src/subscriber.rs`

### What is still wrong

- the owner-death test currently simulates subscriber removal, not publisher failure
- that is not good enough to call full parity

### Add these failing tests first

- `test_object_location_subscription_owner_death_via_publisher_failure`
- `test_object_location_subscription_failure_path_uses_subscriber_failure_mechanism`

### Implementation guidance

- exercise and, if needed, implement the real publisher-failure path for object-location subscriptions
- do not use subscriber deletion as a stand-in for owner death

### Acceptance

- owner death is proven through the real subscriber/publisher failure mechanism

## Validation

For each issue, report:

- exact C++ functions read
- exact Rust files changed
- exact tests added
- exact test commands run
- final status: `fixed`, `partial`, or `still open`

Update:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND8.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_CLAUDE_ROUND8_FIX_PLAN.md`
