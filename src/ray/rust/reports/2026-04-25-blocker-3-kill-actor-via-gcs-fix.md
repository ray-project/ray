# 2026-04-25 — Blocker 3 fix: `KillActorViaGcs` unknown-actor `NotFound`

## Problem

`HandleKillActorViaGcs` in C++ replies with
`Status::NotFound("Could not find actor with ID %s.")` whenever the
actor id is absent from `registered_actors_`
(`ray/src/ray/gcs/actor/gcs_actor_manager.cc:636-663`).

The original Rust implementation in
`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs` returned
`OK` for that same case. That broke the public RPC contract: a client
could not distinguish "actor killed" from "actor never existed", which
violates the drop-in replacement requirement against the C++ GCS.

Severity: medium — it does not deadlock or corrupt state, but it is a
visible API compatibility break on a stable RPC, and any client
asserting on `NotFound` against the C++ server will silently get OK
against the Rust server.

The blocker also called out two adjacent concerns that needed to be
verified rather than just hidden under the unknown-id branch:

1. The `destroyed_actors` cache: C++ does not consult it on this path,
   so a kill issued for an actor that was already permanently destroyed
   must still return `NotFound`, not OK.
2. The "already dead but restartable" case: C++ does not early-return
   on DEAD entries — it calls `DestroyActor` / `KillActor`
   unconditionally and lets those helpers no-op internally. So an
   already-dead-but-restartable actor (still tracked in
   `registered_actors_` / `self.actors`) must reply OK, not NotFound.

---

## Fix

File: `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`.

The `kill_actor_via_gcs` handler now mirrors C++ exactly on the lookup
contract:

```rust
// Unknown actor → NotFound, matching C++ gcs_actor_manager.cc:655-660.
// The `self.actors` table (== C++ `registered_actors_`) is the only
// thing checked; an actor evicted to `destroyed_actors` is also
// unknown to C++ at this point.
if !self.actors.contains_key(&actor_id) {
    let msg = format!("Could not find actor with ID {}.", hex(&actor_id));
    debug!(actor_id = hex(&actor_id), "{}", msg);
    return Ok(Response::new(KillActorViaGcsReply {
        status: Some(not_found_status(&msg)),
    }));
}
```

Key properties of the fix:

- The check looks at `self.actors` only — no consultation of
  `destroyed_actors`. This matches C++'s `registered_actors_.find(...)`
  exactly. An actor that has been moved to the destroyed cache is
  therefore reported as `NotFound`, which is the same answer C++
  would give in the same state.
- The status is constructed via `not_found_status(...)` (Ray code 17,
  i.e. `StatusCode::NotFound`).
- The message is `"Could not find actor with ID {hex}."`, including
  the trailing period and lowercase hex actor id, matching the C++
  format string.
- The early-return is taken **before** any state mutation, so the
  destroy/kill side effects do not run for an unknown id.
- For ids that *are* registered, the handler proceeds into the
  C++-matching branches:
  - `no_restart=true`  → `destroy_actor(.., RayKill, .., force_kill=true)`
    matching `DestroyActor(.., GenKilledByApplicationCause(...))`
    where C++ uses the default `force_kill=true` and ignores the
    request's `force_kill` field on this branch.
  - `no_restart=false` → `kill_actor(actor_id, force_kill)`, the
    private method that mirrors `GcsActorManager::KillActor`'s
    state machine: notify-only for created actors (state stays as
    is until the worker dies and `OnWorkerDead` runs), and a full
    cancel-scheduling + `restart_actor_internal(RayKill, ...)`
    flow for not-yet-created actors.

The `destroy_actor` and `kill_actor` helpers both no-op on
`DEAD`/`DEPENDENCIES_UNREADY` entries internally, which is what
allows a kill on an already-dead-but-restartable actor (still in
`self.actors`) to return OK without changing state — the same
externally-visible answer C++ produces.

---

## Tests

Three unit tests pin the contract
(`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`):

1. `test_kill_actor_unknown_returns_not_found` — asserts an
   un-registered id returns Ray code 17 with a message containing
   "Could not find actor with ID" and the hex id.
2. `test_kill_actor_after_permanent_destroy_returns_not_found` —
   registers and creates an actor, kills it with `no_restart=true`
   (RAY_KILL is non-restartable, so the actor is evicted from
   `self.actors` into `destroyed_actors`), then issues a second kill
   for the same id. The second call must return code 17, confirming
   that the `destroyed_actors` cache is **not** consulted as a
   fallback (matching C++'s `registered_actors_`-only lookup).
3. `test_kill_actor_already_dead_restartable_returns_ok` — drives an
   actor with restart budget into DEAD via out-of-scope (it stays in
   `self.actors`), then kills it via GCS. Must return OK (code 0),
   not NotFound — because the entry is still in the registered table
   and C++ would call `DestroyActor`/`KillActor` unconditionally.

The original `test_kill_actor` integration check is preserved, so the
happy-path RAY_KILL eviction still has coverage.

---

## Verification

Tests run from `ray/src/ray/rust/gcs`:

- `cargo test -p gcs-managers --lib actor_stub::tests::test_kill_actor`
  → 4 passed (covers the three NotFound/OK cases above plus the
  baseline kill).
- `cargo test -p gcs-managers --lib actor_stub::` → 50 passed.
- `cargo test -p gcs-managers --lib` → 277 passed, 0 failed.

No AWS / cloud testing was needed for this blocker. The bug is in the
RPC reply construction at the RPC handler boundary — the contract is
fully covered by the in-process unit tests, which exercise the same
code paths that any networked client would hit. There is no
storage-layer or external-service interaction in the divergence.

---

## Parity audit (against C++ `gcs_actor_manager.cc:636-663`)

| Case | C++ reply | Rust reply | Status |
| --- | --- | --- | --- |
| Unknown actor id | `NotFound("Could not find actor with ID %s.")` | `not_found_status("Could not find actor with ID {hex}.")` | match |
| Actor evicted to destroyed cache | `NotFound` (only `registered_actors_` is checked) | `NotFound` (only `self.actors` is checked) | match |
| Registered, ALIVE, `no_restart=true` | `DestroyActor(.., GenKilledByApplicationCause(..))`, `force_kill` request field ignored on this branch (default `force_kill=true`) | `destroy_actor(.., RayKill, .., force_kill=true)` | match |
| Registered, ALIVE, `no_restart=false` | `KillActor(actor_id, request.force_kill)` — notify raylet only, state unchanged until `OnWorkerDead` | `kill_actor(actor_id, force_kill)` — same notify-only behavior, state unchanged | match |
| Registered, PENDING_CREATION, `no_restart=false` | `KillActor`: cancel scheduling + `RestartActor(need_reschedule=true, GenKilledByApplicationCause)` | `kill_actor`: `cancel_actor_scheduling` + `restart_actor_internal(RayKill, ..)` | match |
| Registered, DEPENDENCIES_UNREADY | `KillActor` early-returns (no-op) | `kill_actor` early-returns; `destroy_actor` also early-returns | match |
| Registered, DEAD-but-restartable | C++ enters `DestroyActor`/`KillActor` then no-ops internally; reply is OK | Rust enters `destroy_actor`/`kill_actor` then no-ops on DEAD; reply is OK | match |

---

## Non-parity gap (documented, tracked as separate follow-up)

C++ additionally arms a per-worker `graceful_shutdown_timers_` map
(`gcs_actor_manager.cc:1041-1080`) that escalates a graceful kill to a
force-kill after `RayConfig::actor_graceful_shutdown_timeout_ms` if
the worker does not exit. Rust does not yet implement this fallback.
A stuck graceful shutdown will not be auto-upgraded to force-kill by
the GCS; the worker must exit on its own or be killed via
node-drain / worker-dead handling. This is documented in the
docstring on `kill_actor_via_gcs` and is out of scope for the
unknown-actor `NotFound` blocker.

---

## Bottom line

The blocker is fully resolved:

- Unknown actor id → Ray `NotFound` with the C++ message format.
- Destroyed-cache id → still `NotFound` (C++ only consults
  `registered_actors_`; Rust only consults `self.actors`).
- Already-dead-but-restartable id → still OK (entry remains in the
  registered table; both languages call into helpers that no-op on
  DEAD).
- The `force_kill` and `no_restart` input semantics, which were
  flagged as adjacent concerns in the 2026-04-23 recheck, are now
  threaded through the correct paths (`destroy_actor` vs the new
  `kill_actor` private method that mirrors C++ `KillActor`).

Tests added are durable parity pins, not just regression checks for
the unknown-id branch.
