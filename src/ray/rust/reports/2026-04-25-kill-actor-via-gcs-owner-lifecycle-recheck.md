# 2026-04-25 KillActorViaGcs Owner Lifecycle Recheck

## Scope

This is a fresh follow-up parity review of the current Rust GCS actor-lifecycle code
against the C++ GCS implementation, with the drop-in replacement bar: public RPC status,
actor retirement semantics, owner-driven cleanup, and death-cause transitions must match
C++.

Files reviewed:

- `ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `ray/src/ray/gcs/actor/gcs_actor_manager.h`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`

## Executive Summary

The previously reported `KillActorViaGcs` mismatches around:

- unknown-actor `NotFound`,
- `force_kill` propagation,
- `no_restart=false` kill-vs-destroy behavior,
- and graceful-shutdown timeout escalation

are fixed in the current Rust code.

However, Rust is still **not** a drop-in replacement for the C++ GCS actor lifecycle. I
found one remaining parity blocker:

1. Rust still does not implement C++ non-detached actor owner tracking and
   `WaitForActorRefDeleted` cleanup.

This is not a narrow internal detail. It changes when actors are destroyed, which death
cause is persisted, and whether owner death recursively destroys child actors.

## What Matches Now

I re-checked the previously-fixed `KillActorViaGcs` behavior:

- unknown actors return `NotFound`
- destroyed-cache hits return `NotFound`
- dead-but-still-registered restartable actors still return `OK`
- `no_restart=false` uses the `KillActor` path rather than incorrectly forcing
  `OUT_OF_SCOPE`
- `force_kill` is forwarded correctly
- graceful `DestroyActor(force_kill=false)` now has timer-based escalation to a second
  force-kill RPC

The current Rust source and tests look consistent on those points.

## Finding

### Finding 1: Rust still lacks C++ owner-tracking / `WaitForActorRefDeleted` lifecycle

Severity: High

C++ behavior:

- During `RegisterActor`, non-detached actors are registered under the owner's
  `(owner_node_id, owner_worker_id)` entry and GCS starts a long-poll
  `WaitForActorRefDeleted` request to the owner.
- When that wait returns, C++ calls `DestroyActor(..., GenActorRefDeletedCause(...),
  force_kill=false, timeout_ms)` so the actor retires with `REF_DELETED`.
- C++ also uses the same `owners_` structure on owner-worker death to destroy all child
  actors with `OWNER_DIED`.
- C++ removes actors from the owner registry when they retire.

Relevant C++ sources:

- non-detached actor owner polling in `RegisterActor`:
  `ray/src/ray/gcs/actor/gcs_actor_manager.cc:738-741`
- owner tracking + `WaitForActorRefDeleted`:
  `ray/src/ray/gcs/actor/gcs_actor_manager.cc:939-979`
- child-actor destruction when the owner worker dies:
  `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1219-1248`
- owner-registry cleanup on final destroy:
  `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1117-1118`
  and `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1823-1838`
- `REF_DELETED` death cause definition:
  `ray/src/ray/gcs/actor/gcs_actor_manager.cc:126-129`
- `DestroyActor` upgrading an already-dead actor from `OUT_OF_SCOPE` to `REF_DELETED`:
  `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1097-1106`

Rust behavior:

- `register_actor` handles detached runtime-env pinning, but there is no corresponding
  non-detached owner-poll path at all.
- There is no owner registry analogous to C++ `owners_`.
- There is no `WaitForActorRefDeleted` request or callback flow.
- `on_worker_dead` only handles:
  - a creating-phase actor from scheduler cancellation, or
  - the actor currently running on that worker.
  It does **not** destroy child actors owned by that worker.
- `destroy_actor` has no equivalent to `RemoveActorFromOwner`.
- Rust has no implemented `REF_DELETED` path in this manager.

Relevant Rust sources:

- `register_actor` has no owner-poll branch for non-detached actors:
  `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1318-1398`
- `on_worker_dead` only handles the worker's own actor / scheduler lease:
  `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:2180-2215`
- `destroy_actor` contains no owner-registry cleanup:
  `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:800-969`

Why this is a real parity blocker:

In C++, for non-detached actors:

1. the owner can tell GCS that all references are gone,
2. GCS retires the actor with `REF_DELETED`,
3. owner worker death recursively destroys all child actors with `OWNER_DIED`.

Rust currently has none of that behavior. As a result:

- non-detached actors can outlive the point where C++ would destroy them,
- `REF_DELETED` is never produced by this manager,
- owner worker death does not destroy child actors the way C++ does,
- actor death-cause history diverges from C++,
- actor retention / cleanup timing diverges from C++.

This is directly user-visible through actor liveness, `GetActorInfo` / `GetAllActorInfo`,
and stored actor death causes.

## Why Earlier Rechecks Missed This

The recent parity rechecks were focused on the `KillActorViaGcs` RPC path itself:

- `NotFound` vs `OK`
- `force_kill`
- `KillActor` vs `DestroyActor`
- graceful timeout escalation

Those are now much closer to C++. But the surrounding actor-lifecycle contract in C++
still depends on owner tracking, and Rust is missing that whole subsystem in this file.

So the current implementation is still not a drop-in replacement for C++ actor lifecycle,
even though the narrow kill RPC semantics are largely corrected.

## Recommended Fix

Rust needs to implement the owner lifecycle that C++ already has:

1. Add an owner registry equivalent to C++ `owners_`, keyed by owner node / worker.
2. During non-detached `register_actor`, start the owner ref-deletion wait flow
   equivalent to `PollOwnerForActorRefDeleted`.
3. On owner ref deletion, destroy the actor with `REF_DELETED` and graceful shutdown.
4. On owner worker death, destroy all child actors with `OWNER_DIED`.
5. On final non-detached actor retirement, remove the actor from the owner registry.
6. Preserve the C++ `DestroyActor` special case that upgrades `OUT_OF_SCOPE` to
   `REF_DELETED` when appropriate.

## Bottom Line

Current status:

- The previously reported `KillActorViaGcs` branch mismatches are fixed.
- But Rust still lacks the owner-driven actor cleanup lifecycle that C++ relies on for
  non-detached actors.

Conclusion:

- The current Rust GCS actor manager is still **not** a full drop-in replacement for the
  C++ GCS.
