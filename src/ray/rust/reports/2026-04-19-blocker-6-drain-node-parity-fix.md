# 2026-04-19 — Blocker 6 fix: `DrainNode` semantic parity with C++

## Problem

C++ `GcsAutoscalerStateManager::HandleDrainNode`
(`gcs/gcs_autoscaler_state_manager.cc:463-523`) does four things in order
after validating the deadline and dispatching the dead/unknown-node fast
path:

1. `gcs_actor_manager_.SetPreemptedAndPublish(node_id)` — flip the
   `preempted` bit on every alive actor on the node and publish the
   updated row on the GCS actor channel
   (`actor/gcs_actor_manager.cc:1417-1444`).
2. `raylet_client->DrainRaylet(...)` — forward the drain request to the
   target raylet.
3. Only on `is_accepted == true`, call `gcs_node_manager_.SetNodeDraining`.
4. On `is_accepted == false`, propagate
   `raylet_reply.rejection_reason_message` back verbatim.

The Rust port was doing none of that:

- `autoscaler_stub.rs:681-752` marked the node draining immediately on
  every alive drain request, without ever contacting the raylet.
- No `SetPreemptedAndPublish` analogue existed. The `preempted` bit on
  `ActorTableData` was only ever set back to `false` on creation success
  (`actor_stub.rs:1059`).
- The autoscaler didn't even hold a reference to the actor manager —
  unlike C++ which holds `GcsActorManager &gcs_actor_manager_` at
  `gcs_autoscaler_state_manager.h:189-190`.
- The `RayletClientPool` trait had no `drain_raylet` method.

Client-visible consequences:

- A raylet that should have rejected a drain (e.g. non-preemptible work
  on the node) was silently accepted by Rust.
- Actors on a draining node never got their `preempted` bit flipped or
  republished, so downstream consumers that distinguish preemption from
  application faults (Ray's own restart / death-cause logic at
  `actor_manager.cc:1505-1506`) couldn't tell the difference.
- Transport errors contacting the raylet (unreachable, timeout) were
  impossible — the code never tried.

---

## Fix

### 1. `GcsActorManager::set_preempted_and_publish`

File: `crates/gcs-managers/src/actor_stub.rs`.

New `async fn set_preempted_and_publish(&self, node_id: &[u8])` that
mirrors C++ `SetPreemptedAndPublish` (`gcs_actor_manager.cc:1417-1444`):

- Looks up `node_id` in `created_actors` (NodeID → WorkerID → ActorID).
  Missing entry ⇒ no-op, same early-return as C++ line 1421-1423.
- Snapshots the list of actor ids under the short-held lock, releases it
  before any async work (avoids holding the lock across awaits).
- For each actor, flips `preempted=true` in the registered-actors
  DashMap, persists the updated row via `ActorTable::put`, then
  publishes on the `GcsActorChannel`. Persist-before-publish ordering
  matches C++ (`put` callback publishes).

Also adds three small test-only helpers:
`insert_registered_actor`, `record_created_actor`,
`get_registered_actor` — the cross-crate integration test in
`gcs-server` needs to seed actor state without running the full
creation pipeline.

### 2. `RayletClientPool::drain_raylet`

File: `crates/gcs-managers/src/autoscaler_stub.rs`.

Extended the trait with `async fn drain_raylet(address, port, request)
-> Result<DrainRayletReply, Status>`. Production
`TonicRayletClientPool::drain_raylet` issues
`NodeManagerServiceClient::drain_raylet` over tonic (same lazy-connect
pattern as `resize_local_resource_instances`).

The test fake `FakeRayletClientPool` got a default "accept" impl so
every other autoscaler test kept working.

### 3. Autoscaler now holds references to actor manager + a mutable raylet pool

Same file:

- New field `actor_manager: RwLock<Option<Arc<GcsActorManager>>>` plus
  setter `set_actor_manager`. Wrapped in `RwLock<Option<…>>` rather
  than a constructor argument because the actor manager depends on the
  node manager (which the autoscaler already holds) — a direct
  constructor arg would create a layering problem in the server's
  setup function.
- The raylet pool field moved from `Arc<dyn RayletClientPool>` to
  `RwLock<Arc<dyn RayletClientPool>>`, with setter
  `set_raylet_client_pool`. This lets the server-level integration
  tests swap in a scripted pool after the autoscaler is
  `Arc`-shared, the same fixture C++ tests use. Every call site
  now snapshots the Arc under the read lock and drops the guard before
  any `.await`.

### 4. Faithful `drain_node` port

Same file, `autoscaler_stub.rs:758-900`.

The new implementation follows C++ step-for-step:

1. Validate `deadline_timestamp_ms >= 0`.
2. Dead/unknown-node fast path: accept immediately, do NOT contact
   any raylet (C++ lines 482-496).
3. `actor_manager.set_preempted_and_publish(&node_id)` before the
   raylet RPC (C++ line 498). Done early so subscribers see the
   preemption signal even if the raylet hangs.
4. Forward `DrainRayletRequest { reason, reason_message, deadline }` to
   the raylet. Transport errors surface as `Err(Status)` — matches C++
   `send_reply_callback(status, nullptr, nullptr)` at line 521.
5. On `is_accepted=true`, call `node_manager.set_node_draining` and
   update the autoscaler's own cache so `get_cluster_resource_state`
   reports `DRAINING`.
6. On `is_accepted=false`, do NOT record draining; return the raylet's
   `rejection_reason_message` unchanged.

### 5. Server wiring

File: `crates/gcs-server/src/lib.rs`.

After constructing both `actor_manager` and `autoscaler_manager`,
the server calls `autoscaler_manager.set_actor_manager(actor_manager.clone())`.
This closes the wiring gap called out in the blocker evidence
(`gcs-server/src/lib.rs:282-288` had no actor-manager arg on the
autoscaler). Layering is safe because both managers are already
`Arc`-shared by this point.

### 6. `GcsNodeManager::is_node_draining`

Small accessor added to `crates/gcs-managers/src/node_manager.rs` so
tests can assert the drain-on-acceptance-only contract. Mirrors C++
`draining_nodes_.contains(node_id)`.

---

## Tests

### Unit — `autoscaler_stub::tests` (4 new)

Scripted `ScriptedDrainPool` lets each test program the raylet's
reply and record the forwarded arguments.

- **`drain_node_alive_forwards_to_raylet_and_accepts`** — assert the
  autoscaler forwards `(address, port, reason, reason_message,
  deadline)` exactly and, on accept, records the drain on the node
  manager. This is the load-bearing parity test for C++ lines 498-514.
- **`drain_node_alive_rejection_is_propagated_and_no_state_set`** —
  raylet returns `is_accepted=false` with a rejection reason. The GCS
  reply must carry `is_accepted=false`, the reason verbatim, and the
  node manager must NOT record a drain.
- **`drain_node_raylet_rpc_error_is_propagated`** — raylet returns
  `Status::unavailable`. GCS returns the same `Status`; no drain recorded.
- **`drain_node_unknown_does_not_call_raylet`** — unknown node short-circuits
  with `is_accepted=true` and zero raylet calls.

### Unit — `actor_stub::tests` (2 new)

- **`set_preempted_and_publish_flips_bit_and_persists`** — three actors
  spread across two nodes, drain one. Asserts:
  - `preempted` flipped only on actors on the drained node
  - drained-node actors persisted to the ActorTable with
    `preempted=true`
  - non-drained actor has not been persisted by this call (verifies
    the scoping doesn't over-reach)
- **`set_preempted_and_publish_on_unknown_node_is_noop`** — drain a
  node id that has no entry in `created_actors`; nothing changes,
  no panic.

### Integration — `gcs_server::tests` (1 new)

- **`drain_node_forwards_to_raylet_and_marks_actors_preempted_end_to_end`**
  — spin up the full `GcsServer`, register an alive node, seed an
  alive actor on that node via the new public helpers, build an
  autoscaler with a scripted `RayletClientPool` that records the
  `DrainRaylet` call, wire in the real `actor_manager`, and call
  `drain_node`. Asserts all four observable effects:
    - RPC reply `is_accepted=true`
    - raylet received exactly one `DrainRaylet` with matching
      `(address, port, reason, reason_message, deadline)`
    - the seeded actor now has `preempted=true`
    - node manager has the drain recorded (`is_node_draining`)

The existing `test_drain_node_propagates_to_resource_manager` was
passing only because the previous Rust drain_node didn't talk to the
raylet. It now correctly would try; the test was updated to inject a
mock accepting pool, which is the same fixture C++ tests use.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    220 passed   (+6 vs main)
gcs-server:       33 passed   (+1 vs main)
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions.

---

## Before → after behaviour

| Scenario                                                 | Before                                   | After                                             |
| -------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------- |
| Drain a dead/unknown node                                | `is_accepted=true`, no raylet call        | `is_accepted=true`, no raylet call (unchanged)    |
| Drain an alive node, raylet accepts                      | `is_accepted=true`, state recorded       | `is_accepted=true`, state recorded + actors preempted + raylet received `DrainRaylet` |
| Drain an alive node, raylet rejects                      | `is_accepted=true` (wrong), state recorded (wrong) | `is_accepted=false`, rejection reason propagated, NO state recorded |
| Drain an alive node, raylet unreachable                  | `is_accepted=true` (wrong), state recorded (wrong) | `Err(Status::Unavailable)` propagated, NO state recorded |
| Alive actor on a drained node                            | `preempted` remains `false`; no publish  | `preempted=true`; row persisted; pubsub update     |
| Autoscaler's actor-manager reference                     | absent                                   | wired via `set_actor_manager` in server setup      |

---

## Scope and caveats

What's included:

- Full semantic parity with the C++ `HandleDrainNode` flow for every
  case the blocker called out: raylet forward, acceptance gating, state
  publication, rejection propagation.
- Test-friendly lifecycle: the autoscaler's raylet pool and actor
  manager are both swap-in-able on an already-`Arc`-shared instance.

What's deliberately out of scope:

- **Connection pooling**. Same rationale as the blocker-5 fix: the
  production `TonicRayletClientPool` opens a channel per call, matching
  the `pg_scheduler.rs` / autoscaler pattern already in this crate. A
  pooled variant is a drop-in if the per-call connect cost becomes a
  bottleneck.
- **Parallel drain**. C++ issues one `DrainRaylet` per `HandleDrainNode`
  call, sequentially awaited. Rust matches that.
- **AWS testing**. Not needed. The integration test uses a scripted
  in-process `RayletClientPool`, which exercises the exact code path a
  real raylet would drive (the wire format is tonic's responsibility, not
  ours). Running this against AWS would test the transport, not the
  parity we're closing.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs` —
  new `set_preempted_and_publish`, three test-only accessors
  (`insert_registered_actor`, `record_created_actor`,
  `get_registered_actor`), two new unit tests.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs` —
  trait gained `drain_raylet`; production impl added; autoscaler gained
  `actor_manager` (+ `set_actor_manager`) and made `raylet_client_pool`
  swappable (`set_raylet_client_pool`); `drain_node` rewritten to
  mirror C++; four new unit tests + updated `FakeRayletClientPool`
  to implement the new trait method.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs` —
  new `is_node_draining` read accessor.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/raylet_load.rs` —
  `NullPool` test fake gets a `drain_raylet` stub so it still
  satisfies the trait.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs` — server
  wires `autoscaler_manager.set_actor_manager(actor_manager)`; one new
  integration test; the existing `test_drain_node_propagates_to_resource_manager`
  now injects a mock accepting pool.
