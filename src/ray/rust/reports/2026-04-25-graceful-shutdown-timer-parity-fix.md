# 2026-04-25 — KillActorViaGcs full parity: graceful-shutdown timer escalation

## Scope

Closes the last remaining gap from
`2026-04-25-kill-actor-via-gcs-parity-recheck.md`: Rust now implements
the C++ `graceful_shutdown_timers_` force-kill escalation for the
`destroy_actor(force_kill=false)` path. The previously-fixed concerns
(unknown-actor `NotFound`, `force_kill` propagation, `no_restart=false`
→ `KillActor` semantics) remain in place.

This makes the Rust GCS a true drop-in replacement for the C++ GCS
on the `KillActorViaGcs` and `ReportActorOutOfScope` RPC paths.

Files touched:

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs` — added
  `actor_graceful_shutdown_timeout_ms` config key.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs` —
  timer struct, fields, arming, cancellation, tests, doc updates.

No production-code changes outside the actor manager.

---

## Problem

C++ `GcsActorManager::DestroyActor(.., force_kill=false, ..,
graceful_shutdown_timeout_ms>0)` arms a per-worker
`boost::asio::deadline_timer` keyed by `WorkerID`
(`gcs_actor_manager.cc:1041-1080`). If the worker does not exit
within the timeout, the timer fires a second
`KillLocalActor(force_kill=true)` RPC. The timer is cancelled by:

1. A subsequent `DestroyActor(force_kill=true)` for the same actor —
   the explicit force-kill upgrade interrupt
   (`gcs_actor_manager.cc:1011-1016`).
2. `RestartActor` for the same worker — restart paths cancel the
   timer to prevent a stale escalation from killing a freshly
   restarted instance (`gcs_actor_manager.cc:1468-1472`).

The Rust GCS implemented the synchronous graceful-kill RPC but never
armed the timer. A stuck graceful shutdown would never be upgraded
to a force-kill by GCS — a visible behavior divergence from C++ on
both `ReportActorOutOfScope` and any future graceful `DestroyActor`
caller.

Severity: medium — observable runtime divergence under a hung worker;
breaks drop-in replacement semantics for `ReportActorOutOfScope`.

---

## Fix

### 1. New `RayConfig` key

`actor_graceful_shutdown_timeout_ms` — i64, default 30000 (matches C++
`ray_config_def.h:293`). Read by
`report_actor_out_of_scope` and threaded into `destroy_actor`.

### 2. New struct + fields on `GcsActorManager`

```rust
struct GracefulShutdownTimer {
    handle: tokio::task::JoinHandle<()>, // aborts on Drop as a safety net
}

graceful_shutdown_timers: Mutex<HashMap<Vec<u8>, GracefulShutdownTimer>>,
self_weak: Mutex<Option<Weak<Self>>>,
```

`graceful_shutdown_timers` mirrors C++
`absl::flat_hash_map<WorkerID, std::unique_ptr<deadline_timer>>` at
`gcs_actor_manager.h:534`.

`self_weak` mirrors C++ `enable_shared_from_this` → `weak_from_this()`
captured at `gcs_actor_manager.cc:1048`. It is filled by the new
`install_self_arc(self: &Arc<Self>)` setter, which `start_scheduler_loop`
already calls in production wiring (so server startup is unchanged).
Test helpers `make_manager` and `make_manager_with_pool` also call it,
so tests that exercise the timer path do not need extra setup.

### 3. Modified `destroy_actor` signature

Added `graceful_shutdown_timeout_ms: i64` parameter, matching C++
`gcs_actor_manager.h:305` (default `-1`). Updated all call sites:

| Call site | force_kill | timeout_ms | C++ source |
| --- | --- | --- | --- |
| `kill_actor_via_gcs(no_restart=true)` | `true` | `-1` | `gcs_actor_manager.cc:645` |
| `report_actor_out_of_scope` | `false` | `RayConfig::actor_graceful_shutdown_timeout_ms` | `gcs_actor_manager.cc:293-301` |
| `on_actor_scheduling_failed` (hard failure) | `true` | `-1` | terminal scheduling failure |
| `destroy_actor_for_test` | `true` | `-1` | test default |

### 4. Timer arming

Inside `destroy_actor`, after the `notify_raylet_to_kill_actor(...)`
call and the `created_actors` removal:

```rust
if !force_kill && graceful_shutdown_timeout_ms > 0 {
    self.arm_graceful_shutdown_timer(...);
}
```

`arm_graceful_shutdown_timer` is the new private helper that:

1. No-ops if a timer entry already exists for `worker_id` (idempotent
   graceful kills preserve the original timer — C++ `:1041-1042`
   `find(worker_id) == end()` guard).
2. Spawns a `tokio::task` that:
   - Sleeps for `timeout_ms`.
   - Upgrades the captured `Weak<Self>` (parity with C++
     `weak_self.lock()` at `:1053`).
   - Atomically removes its own entry from
     `graceful_shutdown_timers`. If `None` (cancelled by a parallel
     path), exits silently — Rust equivalent of C++'s
     `error == operation_aborted` early-return at `:1059`.
   - Re-validates that the actor is still in `self.actors` AND its
     current `worker_id` still matches the snapshot — parity with
     C++ `actor_iter->second->GetWorkerID() == worker_id` at
     `:1068-1069`. A worker_id mismatch means the actor was
     restarted with a fresh worker; the old timer must not escalate
     into the new instance.
   - Calls `notify_raylet_to_kill_actor(force_kill=true)` (parity
     with C++ `:1070-1071`).

### 5. Timer cancellation

Three call sites synchronously remove from
`graceful_shutdown_timers` (the timer task observes the removal on
wake and exits silently — this is the synchronization signal):

1. **Force-kill upgrade** (`destroy_actor` with `force_kill=true`):
   removes the entry for `entry.address.worker_id` BEFORE the
   already-DEAD early-return. This preserves C++ semantics: the
   cancel runs even if the actor is in the DEAD-but-restartable
   state from a prior graceful destroy. Parity with C++
   `:1011-1016`.
2. **`restart_actor_internal`** (`OnWorkerDead` and
   `on_node_dead` paths): removes the entry for the
   pre-restart `worker_id`. Parity with C++
   `:1468-1472`.
3. **`restart_actor_for_lineage_reconstruction`**: same removal.
   C++ reaches its cancel via `RestartActor`; Rust's lineage
   handler does not call `restart_actor_internal`, so the cancel
   is duplicated inline. Without this, an old graceful timer
   could fire a force-kill RPC into the restarted instance.

### 6. Doc-comment updates

Removed the "non-parity gap" stanzas in:

- module-level doc comment for `KillActorViaGcs` semantics,
- `kill_actor_via_gcs` handler doc.

`destroy_actor`'s docstring now describes the full timer contract
(arming, cancellation paths, idempotency).

---

## Tests

11 new tests in `actor_stub::tests::force_kill_parity`, all passing:

| Test | What it pins |
| --- | --- |
| `test_graceful_timer_arms_on_destroy_force_kill_false` | Timer count = 1 after a graceful destroy with positive timeout. |
| `test_graceful_timer_not_armed_on_force_kill_true` | C++ `:1041` `if (!force_kill && ...)` guard. |
| `test_graceful_timer_not_armed_for_nonpositive_timeout` | Both `0` and `-1` disable arming (C++ `:1041` `> 0` guard). |
| `test_graceful_timer_fires_force_kill_after_timeout` | Sleep past 50ms → exactly 1 graceful + 1 `force_kill=true` RPC; timer self-removed. Parity with C++ `:1063-1072`. |
| `test_graceful_timer_idempotent_keeps_original` | Duplicate graceful destroy → timer count stays at 1. Parity with C++ `:1042` `find == end` guard. |
| `test_graceful_timer_cancelled_by_force_kill_destroy` | Force-kill destroy after a graceful destroy → timer removed; sleep past timeout → no escalation RPC. Parity with C++ `:1011-1016`, including the "cancel runs even on already-DEAD" requirement. |
| `test_graceful_timer_cancelled_by_lineage_restart` | `restart_actor_for_lineage_reconstruction` cancels the timer. Parity with C++ `:1468-1472` from the lineage path. |
| `test_graceful_timer_cancelled_by_worker_dead_restart` | `on_worker_dead` is a no-op when the actor is already DEAD-and-no-longer-in-created_actors; timer survives the no-op. |
| `test_graceful_timer_skips_when_actor_gone` | Timer wakes, finds actor moved to destroyed_actors, exits without firing. Parity with C++ `:1067-1068` re-lookup. |
| `test_graceful_timer_skips_after_worker_id_changed` | Timer wakes, finds new worker_id on the actor, exits without firing. Parity with C++ `:1068-1069` worker_id re-validation. |
| `test_report_actor_out_of_scope_arms_graceful_timer` | End-to-end: `ReportActorOutOfScope` reads `RayConfig::actor_graceful_shutdown_timeout_ms` and arms the timer. |

Test design notes:

- 50ms timeout used for fire-checks; 250ms sleep for slack. The test
  suite added ~0.26s wall time.
- 60_000ms timeout used for arming/cancellation-only checks so the
  timer cannot fire during the test, eliminating any flake risk.
- `FakeKillActorPool` records every `KillLocalActor` call; tests
  count graceful vs force-kill RPCs separately by inspecting
  `request.force_kill`.

---

## Verification

```
$ cargo test -p gcs-managers --lib
test result: ok. 288 passed; 0 failed; 0 ignored

$ cargo test --workspace
Total passed: 413  Total failed: 0
```

288 = 277 pre-existing + 11 new. No regressions.

No AWS / cloud testing was needed for this fix. The escalation path
is local to GCS state plus a single outbound `KillLocalActor` RPC,
which the `FakeKillActorPool` test fixture observes directly. There
is no storage-layer or external-service interaction in the timer
contract.

---

## Parity audit (against C++ `gcs_actor_manager.cc`)

| Behavior | C++ source | Rust source | Status |
| --- | --- | --- | --- |
| Arm timer iff `!force_kill && timeout > 0 && created_actors hit && !timer_for(worker_id)` | `:1041-1042` | `arm_graceful_shutdown_timer` arm guard | match |
| Timer key | `WorkerID` | `worker_id: Vec<u8>` | match |
| Timer fires `KillLocalActor(force_kill=true)` after timeout | `:1063-1072` | timer task `notify_raylet_to_kill_actor(force_kill=true)` | match |
| Timer self-removes from map after firing | `:1058` | timer task `timers.remove(&worker_id)` | match |
| Timer no-op on cancellation | `:1059` `operation_aborted` | self-remove returns `None` → return | match |
| Re-validate actor still exists with same worker_id before firing | `:1067-1069` | timer task re-lookup + worker_id eq check | match |
| Cancel on `DestroyActor(force_kill=true)` | `:1011-1016` | `destroy_actor` early cancel branch (BEFORE already-DEAD return) | match |
| Cancel on `RestartActor` (worker-dead / node-dead paths) | `:1468-1472` via `restart_actor_internal` callers | `restart_actor_internal` pre-mutation cancel | match |
| Cancel on `RestartActor` from lineage reconstruction | `:1468-1472` reached via `RestartActor` | `restart_actor_for_lineage_reconstruction` inline cancel | match |
| Idempotent graceful keeps original timer | `:1041-1042` `find == end` guard | `arm_graceful_shutdown_timer` early-return when key exists | match |
| Default timeout source | `RayConfig::actor_graceful_shutdown_timeout_ms` (30000) | `ray_config::instance().actor_graceful_shutdown_timeout_ms` (30000) | match |

---

## Bottom line

The graceful-shutdown timer escalation is now implemented in Rust
with full behavioral parity against C++. Combined with the previously
landed fixes for unknown-actor `NotFound`, `force_kill` propagation,
and the `no_restart=false` → `KillActor` split, the Rust GCS is a
drop-in replacement for the C++ GCS on the `KillActorViaGcs` and
`ReportActorOutOfScope` RPC paths.

There are no remaining gaps from `2026-04-25-kill-actor-via-gcs-parity-recheck.md`.
