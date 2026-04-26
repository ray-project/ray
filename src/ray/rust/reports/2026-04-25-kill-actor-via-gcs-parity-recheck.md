# 2026-04-25 KillActorViaGcs Parity Recheck

## Scope

This is a fresh source-level re-review of the current Rust GCS implementation of
`KillActorViaGcs` against the C++ GCS implementation, with the drop-in replacement bar:
public RPC status, actor-state transitions, death-cause selection, and raylet-kill side
effects must match C++.

Files reviewed:

- `ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`

## Executive Summary

The previously reported parity gaps around:

- unknown-actor `NotFound` handling,
- `force_kill` propagation,
- and the `no_restart=false` `KillActor` vs `DestroyActor` split

are now fixed.

However, the Rust implementation is still **not fully identical** to C++ and is therefore
still **not yet a true drop-in replacement** for this RPC path.

I found one remaining parity issue:

1. Rust does not implement C++'s graceful-shutdown timeout escalation for
   `force_kill=false`.

Everything else I checked in the current `KillActorViaGcs` path matches the C++ control
flow closely enough that I do not have additional findings at this time.

## Verified Parity

### 1. Unknown actor returns `NotFound`

C++:

- `HandleKillActorViaGcs` returns
  `Status::NotFound("Could not find actor with ID %s.")`
  when `registered_actors_` has no entry.
- Source: `ray/src/ray/gcs/actor/gcs_actor_manager.cc:636-660`

Rust:

- `kill_actor_via_gcs` now checks only `self.actors` and returns the matching
  `not_found_status(...)`.
- Source: `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1448-1496`

This matches the original Blocker 3 requirement.

### 2. `force_kill` is now propagated correctly

C++:

- `KillActorViaGcs(no_restart=false)` forwards `request.force_kill()` into `KillActor(...)`,
  which forwards it into `NotifyRayletToKillActor(...)`.
- `DestroyActor(...)` also forwards `force_kill` to the raylet and uses it to manage
  graceful-shutdown timers.
- Sources:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:642-647`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:985-1082`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1843-1918`

Rust:

- `kill_actor_via_gcs(no_restart=true)` correctly ignores the request bit and uses
  `force_kill=true`, matching C++ default `DestroyActor(...)`.
- `kill_actor_via_gcs(no_restart=false)` forwards `inner.force_kill` through
  `kill_actor(...)` and then into `notify_raylet_to_kill_actor(...)`.
- Sources:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:644-839`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:897-993`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1448-1496`

### 3. `no_restart=false` now uses the correct `KillActor` semantics

C++:

- `no_restart=false` calls `KillActor(actor_id, force_kill)`, not `DestroyActor(...)`.
- For already-created actors, C++ only notifies the raylet and leaves state transition to
  later `OnWorkerDead`.
- For not-yet-created actors, C++ cancels scheduling and calls `RestartActor(...)` with a
  killed-by-application death cause.
- Sources:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:642-647`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1877-1918`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1446-1545`

Rust:

- `no_restart=false` now dispatches to a dedicated `kill_actor(...)` helper.
- Created actors stay in their current state until `on_worker_dead(...)`.
- Not-yet-created actors cancel scheduling and flow through `restart_actor_internal(...)`
  with `RAY_KILL`.
- Sources:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:897-993`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1448-1496`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1837-2061`

### 4. Edge-case behavior now matches

The current Rust tests cover and pass the key edge cases that previously diverged:

- unknown actor returns `NotFound`
- destroyed-cache hit returns `NotFound`
- dead-but-still-registered restartable actor returns `OK`
- `DEPENDENCIES_UNREADY` is a no-op on `no_restart=false`
- pending-creation actor restarts with budget
- pending-creation actor dies with `RAY_KILL` without budget
- alive actor stays `ALIVE` until simulated `OnWorkerDead`

## Finding

### Finding 1: Rust still lacks C++ graceful-shutdown timeout escalation

Severity: Medium

C++ behavior:

- `DestroyActor(..., force_kill=false, graceful_shutdown_timeout_ms>0)` installs a
  per-worker timer.
- If the worker does not exit before the timeout, C++ falls back to
  `NotifyRayletToKillActor(..., force_kill=true)`.
- Sources:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1009-1015`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1039-1075`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1468-1472` (timer cancellation during restart)

Rust behavior:

- `destroy_actor(..., force_kill=false)` forwards the graceful kill to the raylet, but it
  does not keep any timer and never escalates to a force kill if the graceful exit hangs.
- The code explicitly documents this as a non-parity gap.
- Source:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:653-658`

Impact:

- For `force_kill=false` flows such as `ReportActorOutOfScope` and any future graceful
  `DestroyActor` callers, Rust and C++ can diverge if the worker does not exit promptly.
- In C++, GCS eventually sends a second kill with `force_kill=true`.
- In Rust, the actor can remain dependent on external cleanup paths instead of receiving
  the C++ timeout-driven escalation.

Why this matters for drop-in replacement:

- This is not a purely internal refactor difference. It changes observable runtime
  behavior under a hung graceful shutdown.
- C++ promises escalation-by-timeout; Rust currently does not.

Required fix:

- Implement the equivalent of C++ `graceful_shutdown_timers_` in Rust for the
  `destroy_actor(force_kill=false)` path.
- The timer should:
  - be armed only once per worker,
  - be canceled on restart / force-kill upgrade,
  - and send a second `KillLocalActor` RPC with `force_kill=true` when the timeout fires.

## Test Evidence

I ran:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib force_kill_parity`

Result:

- 12 passed, 0 failed

These passing tests confirm the currently-fixed behavior for:

- `NotFound` on unknown actor
- `force_kill` forwarding
- `RAY_KILL` death cause on `no_restart=false`
- `ALIVE` → `OnWorkerDead` → `RESTARTING`
- pending-creation restart / no-op edge cases

The remaining graceful-timeout gap is not covered by those tests because Rust does not
yet implement the timeout mechanism.

## Bottom Line

Current status:

- `KillActorViaGcs` parity is much closer and the previously reported major mismatches are
  fixed.
- But Rust is still **not fully identical** to C++ on this path because the
  graceful-shutdown timeout escalation logic is missing.

Conclusion:

- The current Rust GCS implementation for this RPC should **not** yet be described as a
  full drop-in replacement for the C++ GCS.
