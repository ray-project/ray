# Claude Remaining Plan After GCS-6 Final Claim

Date: 2026-03-20
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim full backend parity yet.

`GCS-6` is still open in the live runtime for three concrete reasons:

1. autoscaler drain still does not go through raylet acceptance/rejection
2. node-draining listeners are still not wired into the live resource-manager path
3. autoscaler drain still does not perform actor preemption publishing

## Non-Negotiable Rule

Do not call `GCS-6` fixed unless all three of these are true:

1. an alive-node autoscaler drain request can be rejected if the raylet rejects it
2. accepted drain becomes scheduler-visible through the live server’s resource-manager path
3. actor preemption side effects are either implemented or conclusively shown to be non-observable in the Rust architecture

## Required Work Order

### 1. Re-derive the exact C++ drain contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc`
- `/Users/istoica/src/ray/src/ray/gcs/gcs_node_manager.cc`
- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/Users/istoica/src/ray/src/ray/gcs/tests/gcs_autoscaler_state_manager_test.cc`
- `/Users/istoica/src/ray/src/ray/gcs/tests/gcs_resource_manager_test.cc`

Write down explicitly:

- when `reply.is_accepted` becomes true
- what happens if raylet rejects
- what state changes happen only after raylet accepts
- how drain state reaches the scheduler-visible resource manager
- what actor preemption side effect is observable

### 2. Fix live resource-manager wiring first

Read Rust files:

- `/Users/istoica/src/ray/rust/ray-gcs/src/server.rs`
- `/Users/istoica/src/ray/rust/ray-gcs/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-gcs/src/resource_manager.rs`

Required change:

- wire `node_manager.add_node_draining_listener(...)` in the live server
- have that listener call `resource_manager.set_node_draining(node_id, is_draining, deadline)`

Add failing tests first:

- `test_autoscaler_drain_updates_resource_manager_in_live_server_wiring`
- `test_get_draining_nodes_reflects_autoscaler_drain_without_test_local_listener`

Acceptance:

- no ad hoc listener registration inside the parity proof
- the live server wiring itself drives the resource-manager draining state

### 3. Fix raylet acceptance/rejection gating

Read Rust files:

- `/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs`
- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_scheduler.rs`
- any existing Rust raylet client abstraction already available in GCS
- `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`

Required behavior:

- alive-node autoscaler drain must not blindly return `is_accepted: true`
- it must go through the Rust raylet drain path
- if raylet rejects, GCS reply must reject and carry the rejection reason
- only if raylet accepts should node-draining state be committed

Add failing tests first:

- `test_autoscaler_drain_busy_node_rejected_by_raylet`
- `test_autoscaler_drain_idle_node_accepted_by_raylet`
- `test_autoscaler_drain_rejection_reason_propagated`
- `test_autoscaler_drain_state_committed_only_after_raylet_accepts`

Acceptance:

- the autoscaler drain reply is gated by the raylet reply
- there is a real rejection path
- node-manager/resource-manager drain state is not committed before acceptance

### 4. Revisit actor preemption side effects honestly

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

Do not assume this is “architectural” without proof.

You must do one of two things:

1. implement a Rust equivalent of marking affected actors preempted and publishing that state
2. or prove, with code references and tests, that this state is not externally observable in the Rust backend architecture and therefore not a parity requirement

Add tests first if you implement it:

- `test_autoscaler_drain_marks_node_actors_preempted`
- `test_autoscaler_drain_publishes_actor_preemption_state`

Acceptance:

- either the behavior exists
- or the report makes a defensible, source-backed argument for why the behavior is truly not part of the Rust externally visible contract

## Final Validation

After all fixes:

- rerun targeted `ray-gcs` tests for drain behavior
- `cargo test -p ray-gcs --lib`

If shared GCS/raylet wiring changes:

- rerun targeted `ray-raylet` tests for `DrainRaylet`

## Reporting Requirements

When you report back, include:

- exact C++ contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status for `GCS-6`: `fixed` or `still open`

Do not use “architectural difference” as a substitute for proof.
If any of the three remaining items is not fully closed, say `still open`.
