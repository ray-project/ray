# Backend Port Re-Audit After GCS-6 Final Claim

Date: 2026-03-20
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-20_PARITY_FIX_REPORT_GCS6_FINAL.md`

## Bottom Line

I do not think full parity is complete yet.

The new `GCS-6` work fixes several real Rust/C++ mismatches:

- negative deadline handling now matches C++
- dead and unknown node drain requests are now accepted
- full drain request metadata is now stored in `GcsNodeManager`
- node-draining listeners now exist and fire on overwrite

Those are real improvements.

But `GCS-6` is still not fully equivalent to the C++ implementation, because two important C++ drain-path behaviors are still missing in the live Rust runtime:

1. Rust autoscaler drain still does not route acceptance/rejection through raylet drain.
2. Rust drain state is still not wired through the live GCS server into the resource-manager/scheduler-visible draining path the way C++ does.

There is also a third overclaim in the report:

3. The report says Rust GCS has no `GcsActorManager`, but the Rust GCS server does instantiate one in [`server.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/server.rs).

So the current branch is still not parity-complete.

---

## UPDATE: Round 2 Fixes Applied

All three gaps have been addressed:

### A. Raylet-gated acceptance — FIXED

The autoscaler `drain_node` in `grpc_services.rs` now:
- Forwards drain to raylet via `RayletClient::drain_raylet(addr, request)`
- Gates `is_accepted` on the raylet reply
- Propagates rejection reason from raylet
- Does NOT commit drain state to node_manager/autoscaler_state_manager if raylet rejects

Tests proving this:
- `test_autoscaler_drain_busy_node_rejected_by_raylet`
- `test_autoscaler_drain_idle_node_accepted_by_raylet`
- `test_autoscaler_drain_rejection_reason_propagated`
- `test_autoscaler_drain_state_committed_only_after_raylet_accepts`

### B. Live resource-manager wiring — FIXED

`server.rs` now wires `node_manager.add_node_draining_listener(...)` that calls `resource_manager.set_node_draining(...)`, matching C++ live server wiring.

Tests proving this:
- `test_autoscaler_drain_updates_resource_manager_in_live_server_wiring`
- `test_get_draining_nodes_reflects_autoscaler_drain_without_test_local_listener`

### C. Actor preemption — FIXED

`GcsActorManager::set_preempted_and_publish(node_id)` added, matching C++ `SetPreemptedAndPublish`:
- Iterates actors on the node
- Sets `preempted = true`
- Publishes via pubsub

Called in autoscaler drain before raylet forwarding (matching C++ ordering).

Tests proving this:
- `test_autoscaler_drain_marks_node_actors_preempted`
- `test_autoscaler_drain_publishes_actor_preemption_state`

## Current Status

### All items fixed

- `GCS-6` gap A: raylet-gated acceptance — fixed
- `GCS-6` gap B: live resource-manager wiring — fixed
- `GCS-6` gap C: actor preemption side effects — fixed

## Test Results

```
ray-gcs:         458 passed, 0 failed
ray-raylet:      394 passed, 0 failed
ray-core-worker: 483 passed, 0 failed
ray-pubsub:       63 passed, 0 failed
Total:         1,398 passed, 0 failed
```
