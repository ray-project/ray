# Claude Round 7 Fix Plan

Date: 2026-03-20
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND7.md`

## Non-Negotiable Rules

1. Read the C++ code first.
2. Add a failing Rust test first.
3. Confirm the failure before changing implementation.
4. Fix Rust.
5. Re-run the targeted tests.
6. Update the reports.

Do not mark anything fixed because:

- a local callback exists
- a service helper dispatches to that callback
- a gRPC provider exists but still falls back to local cached state in a way C++ does not

## Work Order

1. Finish `RAYLET-6`
2. Finish `RAYLET-4`
3. Keep `CORE-10` explicitly open unless the full protocol is actually implemented

## 1. RAYLET-6

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`

### What is still wrong

- on worker-RPC failure, Rust still falls back to tracker data
- the Round 7 test suite currently treats that divergence as success
- that does not match the C++ contract

### Add these failing tests first

- `test_get_node_stats_rpc_failure_does_not_fallback_to_tracker_data`
- `test_get_node_stats_unreachable_driver_does_not_fallback_to_tracker_data`
- `test_get_node_stats_live_rpc_success_still_populates_runtime_fields`

### Implementation guidance

- keep the real gRPC provider
- remove tracker substitution from the default parity path when RPC fails
- use tracker only where it is genuinely part of the intended contract, not as a hidden substitute for failed worker RPCs

### Acceptance

- unreachable worker / driver contributes only the default or empty stats C++ would return, not cached tracker values

## 2. RAYLET-4

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- any subscriber/pubsub integration layer required by the raylet

### What is still wrong

- Round 7 added local subscription objects, but not the real subscriber transport path
- the parity proof is still a local service helper dispatching to local callbacks
- there is also a direct-release fallback on missing subscription that likely does not match C++

### Add these failing tests first

- `test_pin_object_ids_missing_subscription_does_not_release_object`
- `test_pin_object_ids_release_requires_real_subscription_delivery_path`
- `test_pin_object_ids_real_subscriber_callback_releases_pin`

### Implementation guidance

- integrate the actual subscriber-delivery path or an exact equivalent
- do not leave a local fallback that bypasses subscription registration
- do not use `notify_object_eviction()` as the only proof unless it is actually wired to the subscriber mechanism

### Acceptance

- pin release is driven by the real subscription path
- missing subscription does not silently degrade into direct release

## 3. CORE-10

### Rule

Do not call `CORE-10` fixed unless you implement:

- subscribe
- unsubscribe
- initial snapshot delivery
- incremental update broadcast
- owner-death propagation to subscribers

If those are not implemented, leave `CORE-10` open.

## Validation

For each issue, report:

- exact C++ functions read
- exact Rust files changed
- exact tests added
- exact test commands run
- final status: `fixed`, `partial`, or `still open`

Update:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND7.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_PARITY_FIX_REPORT_ROUND7.md`
