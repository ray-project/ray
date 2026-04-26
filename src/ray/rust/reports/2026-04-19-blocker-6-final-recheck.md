## Blocker 6 Recheck: DrainNode parity with C++ GCS

Date: 2026-04-19
Reviewer: Codex
Status: Closed

### Scope of this recheck

This recheck focused on the original Blocker 6 from the Rust-vs-C++ GCS parity review:

- `DrainNode` on an alive node must match the C++ control flow.
- Rust GCS must mark actors on the target node as preempted and publish those updates.
- Rust GCS must forward the drain request to the target raylet.
- Rust GCS must only record the node as draining if the raylet accepts.
- Rust GCS must propagate raylet rejection and transport errors instead of silently accepting.

The earlier Rust behavior diverged from C++ because it accepted alive-node drain requests without consulting the raylet and did not perform the actor preemption publish step.

### Conclusion

I re-reviewed the implementation and reran targeted tests. The blocker now appears fixed.

The Rust implementation now matches the C++ `HandleDrainNode` semantics on the critical parity points:

- actor preemption is performed before the raylet drain RPC
- the drain request is forwarded to the target raylet with the original reason, message, and deadline
- the node is marked draining only when the raylet accepts
- raylet rejection is propagated without setting draining state
- raylet transport failure is surfaced as an RPC error instead of being converted into acceptance
- dead or unknown nodes still follow the C++ fast path and are accepted without contacting a raylet

I do not see a remaining parity gap for Blocker 6 based on the current code and tests.

### Code review findings

No new blocker-level findings.

The key parity gaps called out in the previous review are addressed by the following changes.

#### 1. `DrainNode` now forwards to the raylet and gates drain state on the reply

File:
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs`

Relevant locations:
- trait method added at lines 84-99
- autoscaler dependencies added at lines 170-195
- main drain logic at lines 804-914

What changed:

- `RayletClientPool` now exposes `drain_raylet(...)`, which gives the autoscaler the same missing capability that the C++ path already had.
- `GcsAutoscalerStateManager` now holds both:
  - an actor manager reference
  - a raylet client pool reference suitable for production and test injection
- In `drain_node(...)`, Rust now:
  - validates deadline
  - accepts dead or unknown nodes immediately, matching the C++ fast path
  - calls `set_preempted_and_publish(node_id)` before the raylet RPC
  - constructs and forwards `NodeDrainRayletRequest`
  - returns `Err(status)` on raylet transport failure
  - records drain state only if `raylet_reply.is_accepted`
  - returns a rejection reply without recording drain state when the raylet rejects

This is the central parity fix. It removes the earlier semantic mismatch where Rust accepted alive-node drains unconditionally.

#### 2. Actor preemption publish is now implemented

File:
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`

Relevant location:
- `set_preempted_and_publish(...)` at lines 248-294

What changed:

- The actor manager now looks up actors created on the target node.
- Each actor is marked `preempted = true` in memory.
- The updated row is persisted to the actor table.
- The updated actor state is then published.

Why this matters:

- This is the missing parity hook for the C++ `SetPreemptedAndPublish(node_id)` step.
- The persist-then-publish ordering is correct and matches the C++ intent: subscribers that observe the publication can also observe the persisted state.

#### 3. Server wiring now supplies the actor manager to the autoscaler

File:
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs`

Relevant location:
- lines 325-341

What changed:

- `GcsServer::new(...)` now explicitly calls `autoscaler_manager.set_actor_manager(actor_manager.clone())`.

Why this matters:

- Without this, the autoscaler-side `set_preempted_and_publish(...)` implementation would exist but not actually be wired in production.
- The server wiring closes that gap.

### Test evidence

I reran the targeted tests that cover the parity points above.

#### Passed: autoscaler acceptance path

Command:

```bash
cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib drain_node_alive_forwards_to_raylet_and_accepts
```

Result:
- passed

Coverage:
- verifies alive-node drain forwards exactly one raylet request
- verifies forwarded address, port, reason, reason message, and deadline
- verifies accepted reply
- verifies node manager records draining state

Test source:
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:2051`

#### Passed: raylet transport error propagation

Command:

```bash
cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib drain_node_raylet_rpc_error_is_propagated
```

Result:
- passed

Coverage:
- verifies raylet gRPC failure is returned as an error
- verifies the node is not marked draining on transport failure

Test source:
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:2125`

#### Passed: actor preemption persistence

Command:

```bash
cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib set_preempted_and_publish_flips_bit_and_persists
```

Result:
- passed

Coverage:
- verifies `preempted` is flipped
- verifies the updated actor row is persisted

Test source:
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:2297`

#### Passed: end-to-end server wiring

Command:

```bash
cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib drain_node_forwards_to_raylet_and_marks_actors_preempted_end_to_end
```

Result:
- passed

Coverage:
- verifies the server wiring supplies the actor manager into the autoscaler path
- verifies `DrainNode` forwards to the scripted raylet client
- verifies actor state becomes `preempted = true`
- verifies request fields are preserved end-to-end

Test source:
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:2562`

### Residual risk assessment

I do not see a functional parity blocker remaining for this item.

Residual risk is low and mostly non-functional:

- there are still unrelated compiler warnings in the workspace
- the end-to-end test uses a scripted raylet client rather than a live raylet service

Those do not undermine the parity conclusion for this blocker because the critical control-flow and state-transition behavior are now directly covered.

### Final assessment

Blocker 6 is closed.

The Rust GCS `DrainNode` path now implements the missing C++ semantics that previously prevented drop-in replacement on this behavior.
