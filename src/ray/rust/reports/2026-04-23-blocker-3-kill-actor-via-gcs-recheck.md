# 2026-04-23 KillActorViaGcs Parity Recheck

## Scope

This report re-checks the Rust GCS implementation of `KillActorViaGcs` against the C++
GCS implementation, with the bar that Rust must be a drop-in replacement for the C++
server. The review is source-based and focuses on public RPC behavior plus the
observable state-machine side effects of the RPC.

Files reviewed:

- `ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`

## Executive Summary

The original Blocker 3 issue is fixed:

- Rust now returns Ray `NotFound` for unknown actor ids, matching C++
  `HandleKillActorViaGcs`.

However, the overall `KillActorViaGcs` behavior is still **not** a drop-in replacement
for C++. I found two remaining parity mismatches in the same RPC path, plus one
additional missing input semantic:

1. Rust ignores `force_kill`.
2. Rust implements `no_restart=false` by directly calling `destroy_actor(..., OUT_OF_SCOPE, ...)`,
   which does not match the C++ `KillActor(...)` path.
3. As a result, Rust mishandles at least the `DEPENDENCIES_UNREADY` case and also
   publishes/persists different actor-state transitions for live/pending actors.

Conclusion: Blocker 3 should be considered fixed narrowly for the unknown-actor status
code, but `KillActorViaGcs` as a whole is still not at drop-in parity with C++.

## What Is Fixed

### Unknown actor now returns NotFound

C++:

- `HandleKillActorViaGcs` checks only `registered_actors_`.
- If the actor is absent, it replies with
  `Status::NotFound("Could not find actor with ID %s.")`.
- Source: `ray/src/ray/gcs/actor/gcs_actor_manager.cc:636-660`

Rust:

- `kill_actor_via_gcs` now checks only `self.actors`.
- If the actor is absent, it returns `not_found_status(...)` with the matching message.
- Source: `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1115-1158`

This closes the specific mismatch called out in the 2026-04-21 parity review.

## Findings

### Finding 1: `force_kill` is ignored in Rust

Severity: High

C++ behavior:

- `HandleKillActorViaGcs` reads `request.force_kill()` and threads it into
  `DestroyActor(...)` or `KillActor(...)`.
- `DestroyActor(...)` uses `force_kill` to cancel graceful-shutdown timers and to decide
  whether the raylet kill request should be forceful.
- `NotifyRayletToKillActor(...)` includes `request.set_force_kill(force_kill)`.
- Sources:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:642-647`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:985-1075`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1843-1850`

Rust behavior:

- `kill_actor_via_gcs` reads the request but never uses `inner.force_kill`.
- The field has no effect on any branch.
- Source: `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1115-1158`

Impact:

- Callers sending graceful vs forceful kills do not get C++-matching behavior.
- This is not just an internal implementation difference. `force_kill` is part of the
  public RPC input contract, and Rust currently collapses two distinct C++ behaviors
  into one.

Required fix:

- Rust must model the `force_kill` input semantics instead of ignoring the field.
- If Rust intentionally cannot reproduce the raylet-side distinction yet, that should be
  documented as a known non-parity gap rather than described as matching C++.

### Finding 2: `no_restart=false` uses the wrong Rust path and wrong death cause

Severity: High

C++ behavior:

- `no_restart=true` calls `DestroyActor(actor_id, GenKilledByApplicationCause(...))`.
- `no_restart=false` calls `KillActor(actor_id, force_kill)`.
- `KillActor(...)` does **not** map this to `OUT_OF_SCOPE`.
- For created actors it sends a kill request with `RAY_KILL`.
- For not-yet-created actors it cancels scheduling and calls
  `RestartActor(actor_id, /*need_reschedule=*/true, GenKilledByApplicationCause(...))`.
- Sources:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:642-647`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1877-1914`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1446-1545`

Rust behavior:

- `no_restart=false` calls:
  `self.destroy_actor(&actor_id, DeathReason::OutOfScope, "Killed via ray.kill() with no_restart=false")`
- This directly marks the actor `DEAD`, persists that state, and publishes it.
- Sources:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1115-1158`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:605-680`

Why this is not equivalent:

- C++ uses the `ray.kill`/application-killed path, not `OUT_OF_SCOPE`.
- C++ restart behavior for `no_restart=false` is driven by `KillActor` plus
  `RestartActor`, not by marking the actor dead with an out-of-scope cause.
- C++ may transition the actor to `RESTARTING` and reschedule it immediately.
- Rust always transitions first to `DEAD` and relies on its `OUT_OF_SCOPE` restartability
  heuristic, which is a different state machine and different death-cause model.

Impact:

- Subscribers observing actor updates can see different states and different death causes.
- Storage rows can differ from C++.
- Follow-on logic that keys off the death cause or restart state can diverge.

Required fix:

- Rust should not implement `no_restart=false` by calling `destroy_actor(..., OUT_OF_SCOPE, ...)`.
- It needs a separate path that matches the C++ `KillActor(...)` contract and resulting
  state transitions.

### Finding 3: `no_restart=false` on `DEPENDENCIES_UNREADY` is wrong in Rust

Severity: Medium

C++ behavior:

- `KillActor(...)` returns immediately if the actor state is `DEAD` or
  `DEPENDENCIES_UNREADY`.
- So `KillActorViaGcs(no_restart=false)` for a registered-but-unresolved actor replies
  `OK` and does not destroy the actor.
- Source: `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1886-1889`

Rust behavior:

- Registered actors start in `DEPENDENCIES_UNREADY`.
- `kill_actor_via_gcs(no_restart=false)` always calls `destroy_actor(..., OUT_OF_SCOPE, ...)`.
- `destroy_actor(...)` transitions the actor to `DEAD`, persists it, and may clean it up.
- Sources:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:699-723`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1115-1158`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:605-680`

Impact:

- A `KillActorViaGcs(no_restart=false)` request sent before dependencies resolve changes
  actor state in Rust but not in C++.
- This is externally visible through `GetActorInfo`, `GetAllActorInfo`, and actor pubsub.

Required fix:

- The Rust `no_restart=false` path must special-case the unresolved state the same way
  C++ does, or more directly, implement the full C++ `KillActor(...)` behavior rather
  than approximating it with `destroy_actor`.

## Additional Notes

I did not find a remaining mismatch in the specific unknown-actor status branch:

- Rust now correctly returns Ray code 17 (`NotFound`) for unknown ids.
- Rust also correctly returns `NotFound` for actor ids that have already been evicted
  from the registered table into the destroyed cache, which matches the C++
  `registered_actors_` lookup contract.

That narrow fix is real. The remaining gaps are adjacent behavioral mismatches in the
same RPC.

## Recommended Tests To Add Before Calling This Parity-Closed

1. `KillActorViaGcs(no_restart=false)` on a `DEPENDENCIES_UNREADY` actor:
   assert `OK` and no actor-state transition.
2. `KillActorViaGcs(no_restart=false)` on an `ALIVE` actor:
   assert Rust does not immediately persist/publish a `DEAD` row if C++ would still be in
   the kill/restart flow.
3. `KillActorViaGcs(no_restart=false)` on a `PENDING_CREATION` actor:
   assert scheduling cancellation and restart semantics match C++.
4. `KillActorViaGcs(force_kill=false)` vs `force_kill=true`:
   assert the input changes behavior in Rust the same way it changes behavior in C++.

## Bottom Line

The reported Blocker 3 bug is fixed narrowly: unknown actor kill now returns `NotFound`
instead of `OK`.

But the full `KillActorViaGcs` RPC is still not a drop-in replacement for C++ because
Rust:

- ignores `force_kill`,
- uses the wrong semantic path for `no_restart=false`,
- and mishandles at least the unresolved-actor case as a result.

I would not mark `KillActorViaGcs` parity complete yet.
