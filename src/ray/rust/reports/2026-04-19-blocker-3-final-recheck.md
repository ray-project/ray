# 2026-04-19 — Blocker 3 final recheck

## Verdict

Blocker 3 is now **closed**.

The last remaining parity issue from my previous review was the default
for `gcs_server_rpc_server_thread_num`. That is now fixed: Rust computes
the same default formula as C++ and uses the value to size the runtime
worker pool before starting the server.

I also rechecked the other follow-up claims from the latest close-out:

- the previously orphaned `debug_dump_period_milliseconds` setting is
  now wired into a real runtime loop
- the other previously orphaned settings I flagged
  (`emit_main_service_metrics`,
  `gcs_pull_resource_loads_period_milliseconds`,
  `event_stats_print_interval_ms`) have been removed from the Rust
  `ray-config` surface instead of being left as dead config knobs
- the stale `main.rs` flag-table documentation is fixed

Under the standard used throughout this review, I would now mark
Blocker 3 as closed.

---

## What Changed Since The Last Recheck

### 1. `gcs_server_rpc_server_thread_num` default now matches C++

Rust now defines the default with a helper that mirrors the C++ formula:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:68)

```rust
pub fn cpu_quarter_default() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
        .saturating_div(4)
        .max(1)
}
```

and uses that as the `RayConfig` default:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:241)

This matches C++:

- `ray/src/ray/common/ray_config_def.h:382-384`

```cpp
std::max(1U, std::thread::hardware_concurrency() / 4U)
```

Rust also still uses the value to size the runtime worker pool before
launch:

- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:93)

### 2. The default-formula regression test exists and passes

There is now a dedicated test:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:348)

I ran:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p ray-config --lib gcs_server_rpc_server_thread_num_default_matches_cpp_formula`

Result: passed.

### 3. `debug_dump_period_milliseconds` now has a real runtime consumer

Rust now has:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:417)

`start_debug_dump_loop()` reads
`ray_config::instance().debug_dump_period_milliseconds`, treats `0` as
disabled, and is called during server startup:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:820)

I ran both new targeted tests:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib debug_dump_loop_respects_disable`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib debug_dump_loop_spawns_when_enabled`
  - passed

### 4. Previously orphaned settings were removed from the Rust config surface

I rechecked `ray-config/src/lib.rs`. The following entries I had
previously called out as dead knobs are no longer present:

- `emit_main_service_metrics`
- `gcs_pull_resource_loads_period_milliseconds`
- `event_stats_print_interval_ms`

The remaining entries now have inline `Consumer:` comments pointing to
their runtime use sites:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:218)

That resolves the earlier problem where Rust exposed config entries that
did not change behavior.

### 5. The stale `main.rs` flag table doc is corrected

The top-of-file doc table now correctly says:

- `gcs_server_port` default `0`

at:

- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:10)

This was previously stale and is now aligned with the runtime.

---

## Revalidated Previously Fixed Areas

I also rechecked the fixes from the immediately prior round and they are
still in place:

- Redis heartbeat cadence reads
  `gcs_redis_heartbeat_interval_milliseconds`
- actor-manager destroyed-actor cache cap comes from
  `maximum_gcs_destroyed_actor_cached_count`
- listener-delay `config_list` overrides still bridge correctly through
  `RayConfig::initialize` → `export_to_env`
- `event_level`, `emit_event_to_log_file`, and
  `external_storage_namespace` are still wired into runtime consumers

I did not rerun every older targeted test in this turn, but the relevant
code paths remain present and the newly added changes do not regress
them.

---

## Tests Run In This Recheck

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p ray-config --lib gcs_server_rpc_server_thread_num_default_matches_cpp_formula`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib debug_dump_loop_respects_disable`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib debug_dump_loop_spawns_when_enabled`
  - passed

---

## Conclusion

The specific reasons I had for keeping Blocker 3 open are now addressed:

1. Rust computes the same default thread-count formula as C++.
2. That value is actually consumed in runtime construction.
3. The remaining Rust `RayConfig` surface no longer contains the dead
   knobs I previously flagged.
4. The last newly added config consumer (`debug_dump_period_milliseconds`)
   is implemented and test-backed.

On that basis, I would now consider Blocker 3 fully closed.
