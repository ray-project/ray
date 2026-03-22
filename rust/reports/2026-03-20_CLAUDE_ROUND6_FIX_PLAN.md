# Claude Round 6 Fix Plan

Date: 2026-03-20
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND6.md`

## Non-Negotiable Rules

1. Read the C++ code first.
2. Add a failing Rust test first.
3. Confirm the failure before changing implementation.
4. Fix Rust.
5. Re-run the targeted tests.
6. Update the reports.

Do not mark anything fixed because a service helper exists.
Do not mark anything fixed because a trait abstraction exists.
Do not rename "closer architecture" into "matching behavior."

## Work Order

1. Finish `RAYLET-4`
2. Finish `RAYLET-6`
3. Keep `CORE-10` explicitly open unless the full protocol is implemented

## 1. RAYLET-4

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
- any Rust subscriber/pubsub layer needed for owner eviction

### What is still wrong

- `notify_object_eviction()` and `notify_owner_death()` are still local service hooks
- `pin_objects_and_wait_for_free()` still does not register the C++-style owner-eviction subscription path

### Add these failing tests first

- `test_pin_object_ids_registers_owner_eviction_subscription`
- `test_pin_object_ids_subscription_callback_releases_pin`
- `test_pin_object_ids_owner_dead_callback_releases_pin`

### Implementation guidance

- replicate the C++ flow, not just the release effect
- pinning must install the subscription/callback relationship
- explicit free and owner death must arrive through that subscribed path

### Acceptance

- the release no longer depends on directly calling a synthetic service helper in the test

## 2. RAYLET-6

### Read first

- `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`

### Rust files

- `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`

### What is still wrong

- the only concrete provider in the current code is tracker-based
- there is still no real live worker query path equivalent to the C++ worker RPC fanout

### Add these failing tests first

- `test_get_node_stats_uses_real_worker_stats_provider_by_default`
- `test_get_node_stats_collects_driver_stats_through_same_live_path`
- `test_get_node_stats_worker_rpc_failure_semantics_match_cpp`

### Implementation guidance

- add a real live stats provider
- use it in the default `GetNodeStats` path
- preserve `include_memory_info`
- make tests fail if the code silently falls back to stale tracker-only state

### Acceptance

- the default runtime path is no longer just tracker-backed

## 3. CORE-10

### Rule

Do not call `CORE-10` fixed unless you implement:

- subscribe
- unsubscribe
- initial snapshot
- incremental updates
- owner-death propagation

If you do not implement those, leave it open.

## Validation

For each issue, report:

- exact C++ functions read
- exact Rust files changed
- exact tests added
- exact test commands run
- final status: `fixed`, `partial`, or `still open`

Update:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND6.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_PARITY_FIX_REPORT_ROUND6.md`
