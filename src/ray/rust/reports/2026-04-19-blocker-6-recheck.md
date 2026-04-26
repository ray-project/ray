# 2026-04-19 — Blocker 6 recheck

## Verdict

Blocker 6 is **not fixed** if Blocker 6 refers to the remaining
drop-in-parity issue from the main review:

- Rust `DrainNode` semantics are still weaker than C++.

There is no evidence in the current tree that Rust now:

1. marks actors on the target node as preempted and republishes them
2. forwards the drain request to the target raylet
3. accepts or rejects the drain based on the raylet's reply

So under the parity bar used in the main review, Blocker 6 remains open.

---

## What I Checked

### 1. Current Rust `DrainNode` path

Current Rust implementation:

- [autoscaler_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:692)

The code still does the following:

- validates deadline
- treats unknown/dead nodes as accepted
- if the node is alive, records drain state locally via
  `node_manager.set_node_draining(...)`
- ensures the autoscaler cache contains the node
- marks the autoscaler resource cache as draining
- returns `DrainNodeReply { is_accepted: true, rejection_reason_message: "" }`

It does **not**:

- call any actor-manager preemption method
- call any raylet `DrainRaylet` RPC
- return a rejection coming from the raylet

### 2. Current C++ `DrainNode` path

C++ implementation:

- `ray/src/ray/gcs/gcs_autoscaler_state_manager.cc:457-523`

That code does all of the following:

- `gcs_actor_manager_.SetPreemptedAndPublish(node_id);`
- resolves the target raylet address
- calls `raylet_client->DrainRaylet(...)`
- only records node draining if `raylet_reply.is_accepted()`
- propagates `rejection_reason_message` if the raylet rejects

This is the key semantic difference that remains.

### 3. No actor-manager reference exists on the Rust autoscaler manager

The Rust autoscaler manager still holds:

- `node_manager`
- `placement_group_load_source`
- `raylet_client_pool`
- `kv`
- `session_name`

Definition:

- [autoscaler_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:131)

It does **not** hold an actor manager, so there is still no path here to
implement the C++:

- `gcs_actor_manager_.SetPreemptedAndPublish(node_id);`

That is a structural sign the fix has not landed.

### 4. The Rust raylet client abstraction still lacks any drain RPC

The current Rust `RayletClientPool` trait only includes:

- `resize_local_resource_instances(...)`

Definition:

- [autoscaler_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:68)

There is no `drain_raylet(...)` or equivalent method on the trait.

That means the Rust autoscaler manager currently has no abstraction
available to perform the C++ drain forwarding step at all.

### 5. The existing Rust tests still only validate local-acceptance semantics

Current Rust drain-node tests cover:

- unknown node accepted
- dead node accepted
- alive node accepted and shows draining
- negative deadline rejected

Tests:

- [autoscaler_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:1615)

There are still no tests for:

- actor preemption publication on drain
- forwarding to the raylet
- raylet rejection surfacing back to the caller
- race conditions around reconnecting or delayed raylet replies

Those are exactly the areas where C++ is stronger.

---

## What This Means

The event-export side of the older Blocker 6 reports appears to have
been fixed earlier. But the remaining blocker from the main parity
review was specifically the weaker `DrainNode` semantics.

That remains unresolved in the current code.

So if the user means:

- "Blocker 6 from the old event-export/listener notes"

then that may already have been closed previously.

But if the user means:

- "Blocker 6 from the main parity review/update"

then it is still open, and the current code does not support a closure
claim.

---

## What Needs To Change To Close It

At minimum, Rust needs all of the following:

1. Add actor-manager integration so draining a node marks affected
   actors as preempted and republishes them, matching C++
   `SetPreemptedAndPublish(node_id)`.

2. Extend the Rust raylet client abstraction with a drain RPC, then
   forward `DrainNode` requests to the target raylet.

3. Only record drain state locally when the raylet accepts the request,
   matching the C++ accepted/rejected flow.

4. Propagate raylet rejection back to the caller via
   `is_accepted=false` and `rejection_reason_message`.

5. Add tests covering:
   - accepted drain
   - rejected drain
   - actor preemption side effects
   - race behavior comparable to the C++ drain-node tests

Until that work lands, I would keep Blocker 6 open.
