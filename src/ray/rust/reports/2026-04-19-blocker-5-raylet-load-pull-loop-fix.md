# 2026-04-19 — Blocker 5 fix: periodic raylet `GetResourceLoad` pull loop

## Problem

C++ `GcsServer::InitGcsResourceManager` installs a periodic task
(`gcs/gcs_server.cc:415-446`) that, every
`gcs_pull_resource_loads_period_milliseconds` (default 1000 ms,
`ray_config_def.h:62`), iterates every alive raylet, issues
`NodeManagerService::GetResourceLoad`, and feeds the reply into *both*:

- `GcsResourceManager::UpdateResourceLoads(...)` — copies
  `resource_load` and `resource_load_by_shape` into the per-node usage
  row (`gcs_resource_manager.cc:147-156`).
- `GcsAutoscalerStateManager::UpdateResourceLoadAndUsage(...)` —
  replaces the autoscaler's cached `ResourcesData` for the node
  (`gcs_autoscaler_state_manager.cc:290-304`).

The Rust GCS had `update_resource_load_and_usage()` on the autoscaler
but it had **no callers** (the code said so:
`autoscaler_stub.rs:213-229`), and it had **no** equivalent for the
resource manager at all, plus **no** periodic loop. So:

- The autoscaler's `pending_resource_requests`,
  `resource_load_by_shape`, and per-node running/idle state were seeded
  from node-add totals and drain-state deltas and never refreshed.
- `GetAllResourceUsage` returned rows with empty
  `resource_load` / `resource_load_by_shape`.
- Any logic (in Ray itself or downstream consumers) relying on live
  raylet-reported resource pressure got stale data.

---

## Fix

### 1. New `ray_config` knob

Added `gcs_pull_resource_loads_period_milliseconds` with the same
default as C++ (1000 ms, `ray_config_def.h:62`) and a comment pointing
at its consumer.

File: `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs`.

### 2. `GcsResourceManager::update_resource_loads`

Ported the C++ method verbatim (field copy + early-return on unknown
node) — `crates/gcs-managers/src/resource_manager.rs:45-58`. Matches
`gcs_resource_manager.cc:147-156`.

### 3. New module — `gcs_managers::raylet_load`

File: `ray/src/ray/rust/gcs/crates/gcs-managers/src/raylet_load.rs` (new, ~520 lines).

Contents:

- `RayletLoadFetcher` trait — abstracts the raylet RPC for testability.
- `TonicRayletLoadFetcher` — production gRPC implementation. Opens a
  channel per call (lazy-connect, same pattern as
  `pg_scheduler.rs`/`autoscaler_stub.rs`) and issues
  `NodeManagerServiceClient::get_resource_load`.
- `pull_once(fetcher, node_manager, resource_manager, autoscaler)` —
  executes one polling pass:
  1. `node_manager.get_all_alive_nodes()`.
  2. For every alive node, spawn a concurrent `fetch(address, port)` in
     a `tokio::task::JoinSet` — matches the C++ callback-based fan-out
     (`gcs_server.cc:425-442`) so a single slow raylet never blocks the
     pass. Implemented with `JoinSet` rather than `futures_util` to
     avoid pulling in an extra dep for one combinator.
  3. For every successful reply, fill in the `node_id` from the GCS
     side if the raylet returned empty (older-raylet compat), then call
     `resource_manager.update_resource_loads(&data)` *and*
     `autoscaler.update_resource_load_and_usage(data)`. Two consumers,
     one source of truth — exactly the two-call shape C++ uses at
     `gcs_server.cc:435-437`.
  4. For every error, log a sampled WARN (every 10th failure) —
     parity with C++ `RAY_LOG_EVERY_N(WARNING, 10)` at
     `gcs_server.cc:439-441`.
- `spawn_load_pull_loop(..., period_ms)` — spawns a
  `tokio::time::interval`-driven loop with
  `MissedTickBehavior::Skip` (so a slow pass doesn't queue up a backlog
  that hammers the raylets after recovery — same semantics as the C++
  `PeriodicalRunner`). `period_ms == 0` disables the loop (returns
  `None` for the join handle), matching the convention every other
  `_period_*` knob in the Rust GCS uses.

### 4. `GcsAutoscalerStateManager::get_node_resource_info`

Added a public read accessor on the autoscaler's per-node cache so
tests (and debug-dump paths) can observe what the autoscaler currently
knows. Production code already had write paths (`on_node_add`,
`update_resource_load_and_usage`); the missing read side was blocking
end-to-end tests. File:
`crates/gcs-managers/src/autoscaler_stub.rs:245-254`.

### 5. Server wiring

File: `crates/gcs-server/src/lib.rs`.

- `GcsServer` gained two new fields:
  - `raylet_load_fetcher: Mutex<Arc<dyn RayletLoadFetcher>>` — defaults
    to `TonicRayletLoadFetcher::new()`. Tests swap in a mock via
    `set_raylet_load_fetcher`.
  - `raylet_load_pull_handle: Mutex<Option<JoinHandle<()>>>` — the
    loop's handle so we can abort an earlier loop before spawning a
    new one (idempotent `start_raylet_load_pull_loop` calls).
- New method `start_raylet_load_pull_loop(&self)` reads the period from
  `ray_config::instance().gcs_pull_resource_loads_period_milliseconds`
  and calls `spawn_load_pull_loop`.
- `install_listeners_and_initialize()` now calls
  `start_raylet_load_pull_loop()` as its last step, after
  `start_debug_dump_loop()`. Both `start()` and `start_with_listener()`
  route through this function so the two entry points cannot drift —
  same bug-shape guard as the previous lifecycle-listener fix.

### 6. Cargo wiring

- `gcs-server/Cargo.toml` gains `parking_lot` (needed to hold the
  fetcher and join handle).

---

## Tests

### Unit (`gcs_managers::raylet_load::tests`, 5 new)

- `pull_once_updates_both_downstreams` — one alive node, fake fetcher
  returning a RESOURCE_LOAD with `resource_load = {CPU: 3.5}` and
  `resources_available = {CPU: 0.5}`. Asserts the resource manager's
  row picked up `resource_load`, and the autoscaler's per-node cache
  picked up `resources_available`. This is the load-bearing parity
  guard for `gcs_server.cc:435-437`.
- `pull_once_swallows_per_node_errors` — two nodes, one fetch succeeds,
  one fails. Returned `PullPassStats { ok: 1, err: 1 }`. A failing
  raylet must not poison the pass for healthy ones.
- `pull_once_fills_missing_node_id_from_gcs_side` — older raylets may
  reply with an empty `node_id`; verify the downstream `update_*` calls
  still land on the GCS-side id (parity with C++ which uses
  `NodeID::FromBinary(data.node_id())` but effectively falls back to
  the requesting raylet when empty).
- `spawn_load_pull_loop_period_zero_is_noop` — period 0 returns
  `None` for the handle.
- `spawn_load_pull_loop_actually_ticks` — period 30ms with a single
  fake-node wait 120 ms, at least one call must have happened.

### Integration (`gcs_server::tests`, 2 new)

- `raylet_load_pull_loop_updates_resource_manager_and_autoscaler` —
  spins up the full `GcsServer` (in-memory store), registers one alive
  node, injects a counting mock fetcher returning a rich reply, sets
  the period to 30ms via `ray_config::initialize`, calls
  `start_raylet_load_pull_loop`, waits 150ms, then asserts:
    - the fetcher was called at least once
    - `GetAllResourceUsage` returns a batch row with
      `resource_load["CPU"] == 3.25` and a non-empty
      `resource_load_by_shape`
    - the autoscaler's `get_node_resource_info(node_id)` reflects
      `resources_available["CPU"] == 0.75`

  Before this commit, both downstream observations would have stayed
  at their node-registration values (empty / 4.0) because no loop
  existed — this test would have hung on the first assertion.
- `raylet_load_pull_loop_period_zero_disables` — period 0 → the
  fetcher is never called even after 120ms of wall time, and the
  stored join handle is `None`.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    214 passed   (+5 vs main)
gcs-server:       32 passed   (+2 vs main)
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions.

---

## Before → after behaviour

| Observation at steady state                                | Before                              | After                                                                                    |
| ---------------------------------------------------------- | ----------------------------------- | ---------------------------------------------------------------------------------------- |
| `GetAllResourceUsage`.`resource_load_by_shape` for a node  | empty/default (never populated)     | populated from the raylet's most recent reply (≤1s stale by default)                    |
| Autoscaler's per-node `ResourcesData` cache                | seeded at `OnNodeAdd`, then frozen  | replaced on every tick with the raylet's live totals, available, idle, is_draining, etc. |
| `pending_resource_requests`                                | can diverge arbitrarily from C++    | driven by the same raylet data path C++ uses                                             |
| Per-node running/idle determination (`NodeState::status`)  | derived from stale totals           | derived from fresh `idle_duration_ms` / `resources_available`                            |
| Behaviour when every raylet times out for 10s              | silent                              | one WARN per 10 failures (sampled; mirrors C++ `RAY_LOG_EVERY_N(WARNING, 10)`)            |
| Period 0 (operator disables)                               | no code path                        | cleanly skips spawning the loop; handle stays `None`                                    |

---

## Scope and caveats

What's included:

- The periodic loop is wired into both `start()` and
  `start_with_listener()` via the shared
  `install_listeners_and_initialize()` entry point, so the production
  binary (which uses `start_with_listener`) is covered.
- The fetcher is trait-based, so any `RayletLoadFetcher` implementation
  can replace the default in tests or in follow-up work (e.g. a
  connection-pooled variant). Production currently constructs a fresh
  channel per call; if raylet fan-out becomes a bottleneck, swapping in
  a pooled fetcher is a drop-in.

What's deliberately out of scope for this commit:

- **Connection pooling**. C++ uses `RayletClientPool::GetOrConnectByAddress`.
  The Rust port matches the existing `pg_scheduler.rs` /
  `autoscaler_stub.rs` pattern of lazy per-call connection. For
  typical cluster sizes (tens of nodes, 1-second cadence) this is
  cheap. A pooled variant is a trivial drop-in if needed.
- **Back-pressure between the loop and the raylets**. C++ has no
  explicit back-pressure either — each callback-based RPC is
  independent. `MissedTickBehavior::Skip` handles the client-side
  side of the same concern.
- **AWS testing**. Not needed — the integration test injects an
  in-process mock fetcher that returns scripted `ResourcesData`,
  which exercises the same code path that a real raylet would drive.
  Running against an AWS raylet would test the transport layer, not
  the parity we're closing.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs` — new knob
  `gcs_pull_resource_loads_period_milliseconds`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/raylet_load.rs` — new
  module: fetcher trait, production tonic impl, `pull_once`,
  `spawn_load_pull_loop`, 5 unit tests.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/lib.rs` — register
  `pub mod raylet_load`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs` —
  new `update_resource_loads(&ResourcesData)`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs` —
  new `get_node_resource_info(&[u8]) -> Option<ResourcesData>`.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs` — two new
  `GcsServer` fields, `start_raylet_load_pull_loop`,
  `set_raylet_load_fetcher`, and wiring into
  `install_listeners_and_initialize`. Two new integration tests.
- `ray/src/ray/rust/gcs/crates/gcs-server/Cargo.toml` — add
  `parking_lot`.
