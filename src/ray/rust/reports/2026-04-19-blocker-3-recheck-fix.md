# 2026-04-19 — Blocker 3 recheck fix: thread-num default + orphan settings + stale doc

## Reviewer's remaining items

From `2026-04-19-blocker-3-recheck.md`:

1. **High** — Rust default for `gcs_server_rpc_server_thread_num` is
   `1`; C++ default is `std::max(1U, std::thread::hardware_concurrency() / 4U)`.
2. **Medium** — Four ported `RayConfig` entries have no Rust runtime
   consumer: `emit_main_service_metrics`,
   `gcs_pull_resource_loads_period_milliseconds`,
   `debug_dump_period_milliseconds`, `event_stats_print_interval_ms`.
3. **Low** — `main.rs` top-of-file flag table still says
   `gcs_server_port` default is `6379`; the runtime default is now `0`.

All three are fixed in this commit.

---

## 1. `gcs_server_rpc_server_thread_num` default now matches C++

`crates/ray-config/src/lib.rs`

```rust
/// Rust port of C++'s compile-time default expression
/// `std::max(1U, std::thread::hardware_concurrency() / 4U)` from
/// `ray_config_def.h:382-384`. Rust's `std::thread::available_parallelism`
/// is the idiomatic equivalent of C++'s `hardware_concurrency`.
/// Fallback of 1 matches C++'s `std::max(1U, ...)` floor.
pub fn cpu_quarter_default() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
        .saturating_div(4)
        .max(1)
}

ray_configs! {
    // …
    // C++ default: `std::max(1U, std::thread::hardware_concurrency() / 4U)`.
    (num(u32), gcs_server_rpc_server_thread_num, cpu_quarter_default()),
    // …
}
```

The macro already accepted `$default:expr`, so passing a function call
works with no framework changes — the expression evaluates at
`Default::default()` call time, exactly when C++'s class-initializer
evaluates its expression.

### Regression test

`gcs_server_rpc_server_thread_num_default_matches_cpp_formula` in
`ray-config` derives the expected value from the test machine's CPU
count the same way C++ does:

```rust
let hardware_concurrency: u32 = std::thread::available_parallelism()
    .map(|n| n.get() as u32)
    .unwrap_or(1);
let expected = (hardware_concurrency / 4).max(1);
assert_eq!(RayConfig::default().gcs_server_rpc_server_thread_num, expected);
```

No hardcoded number to drift between CI runners. On a 12-CPU box:
expected = 3. On a 1-CPU box: expected = 1 (the `std::max(1U, ...)`
floor).

---

## 2. Orphan settings: wire one, remove three

The "scope policy" doc at the top of `ray-config/src/lib.rs` now
states the rule explicitly:

> Only settings with a real Rust runtime consumer live here. Shipping
> an entry without a consumer is anti-parity — it implies
> configurability the runtime doesn't provide.

Applied to the four orphan settings the reviewer flagged:

| Setting | Before | Now |
|---|---|---|
| `debug_dump_period_milliseconds` | defined, no consumer | **wired** — drives `GcsServer::start_debug_dump_loop`, called from `install_listeners_and_initialize`; 0 disables (parity with C++ `gcs_server.cc:314-318`) |
| `emit_main_service_metrics` | defined, no consumer | **removed** — Rust uses tokio, which doesn't expose C++'s io_context-metrics hook; no meaningful consumer today |
| `gcs_pull_resource_loads_period_milliseconds` | defined, no consumer | **removed** — Rust has no resource-load pull loop today |
| `event_stats_print_interval_ms` | defined, no consumer | **removed** — Rust has no event-stats reporter today |

### The new `start_debug_dump_loop` consumer

```rust
fn start_debug_dump_loop(&self) {
    let period_ms = ray_config::instance().debug_dump_period_milliseconds;
    if period_ms == 0 { return; }
    let node_manager = self.node_manager.clone();
    let job_manager = self.job_manager.clone();
    let actor_manager = self.actor_manager.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(
            std::time::Duration::from_millis(period_ms),
        );
        ticker.tick().await; // skip the immediate first tick
        loop {
            ticker.tick().await;
            info!(
                target: "ray.gcs.debug_state",
                alive_nodes = node_manager.get_all_alive_nodes().len(),
                dead_nodes  = node_manager.get_all_dead_nodes().len(),
                actor_cache_cap = actor_manager.max_destroyed_actors_cached(),
                job_manager_present = …,
                "GCS debug state"
            );
        }
    });
}
```

Mirrors C++ `GcsServer::PrintDebugState` at `gcs_server.cc:917` —
periodically emits cluster-state counts. Zero-period short-circuits to
parity with C++ `gcs_server.cc:314-318` which skips the periodical
registration when the period is 0.

### Regression tests (gcs-server)

- `debug_dump_loop_respects_disable` — with `period=0`, calling
  `start_debug_dump_loop` must return immediately without spawning.
- `debug_dump_loop_spawns_when_enabled` — with `period=50ms`, the
  loop spawns, runs for >2 ticks, and the config value round-trips.

Both use the existing `RAY_CONFIG_TEST_LOCK` for serialization.

### Why remove rather than wire the other three

- `emit_main_service_metrics`: C++ attaches it to an io_context
  constructor. Tokio has no equivalent hook. Shipping the knob would
  imply operator control that doesn't exist.
- `gcs_pull_resource_loads_period_milliseconds`: C++ drives a periodic
  pull from the autoscaler. Rust has no such pull loop today.
  Introducing one just to consume the knob would be feature work
  disguised as config work.
- `event_stats_print_interval_ms`: C++ drives a stats reporter. No
  Rust equivalent. Same rationale.

The scope-policy comment at the top of `ray-config/src/lib.rs` now
states this explicitly, so future reviewers can compare the Rust
surface to the C++ surface without reverse-engineering which knobs
have consumers.

---

## 3. Stale main.rs flag-table doc

`crates/gcs-server/src/main.rs`

```diff
-//! | `gcs_server_port` | int | 6379 | gRPC listen port |
+//! | `gcs_server_port` | int | 0 | gRPC listen port (0 = OS-assigned, matches C++ `gcs_server_main.cc:48`) |
…
-//! | `external_storage_namespace` | string | "" | Redis key namespace |
+//! | `external_storage_namespace` | string | "" | Redis key namespace (falls back to `RayConfig::external_storage_namespace` = `"default"`) |
```

Doc now matches the runtime behavior. The reviewer's complaint
(6379 vs 0) is addressed.

---

## Every entry in `ray-config` now has a documented consumer

Each entry in the `ray_configs!` macro invocation now has a
`// Consumer:` line pointing at the call site that uses it:

```rust
// ray_config_def.h:932 — raylet health-check cadence.
// Consumer: `GcsNodeManager::start_health_check_loop`.
(num(i64), health_check_period_ms, 3000),
```

If an entry has no consumer, it shouldn't be in the crate — the policy
is now self-enforcing on review.

---

## Build + test results

```
$ cargo test --workspace
test result: ok. 20  passed; 0 failed  (gcs-kv)
test result: ok. 201 passed; 0 failed  (gcs-managers)
test result: ok. 13  passed; 0 failed  (gcs-pubsub)
test result: ok. 28  passed; 0 failed  (gcs-server lib   — +2 new)
test result: ok. 4   passed; 0 failed  (gcs-server bin)
test result: ok. 25  passed; 0 failed  (gcs-store)
test result: ok. 8   passed; 0 failed  (gcs-table-storage)
test result: ok. 14  passed; 0 failed  (ray-config lib   — +1 new)
```

Total: **313 / 313 passing** (was 310, +3 new), 0 failures, 0 new
warnings.

---

## Coverage against the reviewer's close-criteria

1. ✅ `gcs_server_rpc_server_thread_num` default now computed the same
   way as C++.
2. ✅ Regression test derives the expected value from the current
   machine's CPU count.
3. ✅ Each remaining entry in `ray-config` has a real runtime consumer,
   documented inline. Entries without a Rust consumer were removed;
   the scope-policy doc makes the rule explicit.

---

## On AWS testing

Not required. Every fix is in-process state plumbing:

- `cpu_quarter_default()` is a pure function over
  `std::thread::available_parallelism` — deterministic per CPU count,
  hermetic across host environments.
- Removing orphan settings is a code change with no runtime effect.
- `start_debug_dump_loop` exercises tokio primitives; the regression
  test runs the loop for >100ms and observes its round-trip behavior.

The 313 tests run locally exercise the same code paths that would run
on EC2. There's no network, storage, or AWS-specific resource involved.

---

## Outcome

Blocker 3 is fully closed under the drop-in-replacement bar.

- Every C++ default the reviewer flagged matches Rust's default.
- Every Rust `RayConfig` entry has a real consumer.
- Every stale doc the reviewer flagged is corrected.
- Regression tests guard each fix.
