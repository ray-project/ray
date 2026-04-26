# 2026-04-19 — Blocker 3 recheck

## Verdict

Blocker 3 is **not fully closed yet**.

The latest changes do fix two of the concrete runtime gaps from the
previous review:

- Redis heartbeat cadence now reads the proper `RayConfig` field.
- The actor-manager destroyed-actor cache cap now comes from
  `maximum_gcs_destroyed_actor_cached_count`.

I verified both code paths and ran the new targeted tests for them.

However, under the stated bar that the Rust GCS should be a **drop-in
replacement** for the C++ GCS, there is still at least one material
config-parity gap:

- `gcs_server_rpc_server_thread_num` still does not have the same
  default as C++.

There are also still several `RayConfig` entries in Rust that are
defined but have no runtime consumer.

That means Blocker 3 has narrowed again, but I would still keep it open.

---

## What Is Fixed

### 1. Redis heartbeat now uses the correct RayConfig knob

Rust now reads:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:365)

```rust
let interval_ms: u64 =
    ray_config::instance().gcs_redis_heartbeat_interval_milliseconds;
```

This replaces the previous wrong-named env path and wrong default.

That is a real parity improvement against C++:

- `ray/src/ray/gcs/gcs_server.cc:182-183`
- `ray/src/ray/common/ray_config_def.h:395`

I also ran the new regression test:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib redis_heartbeat_default_matches_cpp`

Result: passed.

### 2. Destroyed-actor cache cap is now driven by RayConfig

The actor manager is no longer hard-coded to `100_000` in production.

The server now captures the configured value at construction:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:276)

and the actor manager stores and uses that per-instance cap:

- [actor_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:136)
- [actor_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:440)

I ran the new end-to-end regression test:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib actor_manager_honors_max_destroyed_actors_cached_from_ray_config`

Result: passed.

### 3. `gcs_server_rpc_server_thread_num` now has a real runtime consumer

Rust `main.rs` no longer relies on `#[tokio::main]`. It now initializes
`RayConfig` first, then builds the tokio runtime explicitly with:

- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:93)

```rust
let worker_threads = ray_config::instance().gcs_server_rpc_server_thread_num as usize;
rt_builder.worker_threads(worker_threads);
```

That is a real runtime effect, not just logging.

So the earlier review point that this knob was “defined and logged, but
not used” is no longer true.

---

## Remaining Findings

### 1. High: the default for `gcs_server_rpc_server_thread_num` is still wrong

Rust currently defines:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:230)

```rust
(num(u32), gcs_server_rpc_server_thread_num, 1),
```

C++ defines:

- `ray/src/ray/common/ray_config_def.h:382-384`

```cpp
RAY_CONFIG(uint32_t,
           gcs_server_rpc_server_thread_num,
           std::max(1U, std::thread::hardware_concurrency() / 4U))
```

That is not the same default.

On this machine, `os.cpu_count()` is `12`, so the C++ default would be:

- `max(1, 12 / 4) = 3`

while Rust defaults to:

- `1`

So the new Rust code does use the knob, but its default behavior is
still not C++-equivalent when the setting is not explicitly overridden.

Under a strict drop-in-replacement bar, that remains a real blocker.

### 2. Medium: some ported RayConfig settings still have no runtime consumer

I still do not find runtime consumers for at least these Rust
`RayConfig` entries:

- `emit_main_service_metrics`
- `gcs_pull_resource_loads_period_milliseconds`
- `debug_dump_period_milliseconds`
- `event_stats_print_interval_ms`

Search evidence:

- `rg -n "emit_main_service_metrics|gcs_pull_resource_loads_period_milliseconds|debug_dump_period_milliseconds|event_stats_print_interval_ms" ray/src/ray/rust/gcs/crates -g '!target/**'`

The matches are in `ray-config/src/lib.rs`, not in Rust runtime call
sites.

This matters because the new close-out report claims the reviewer’s
follow-up settings all now have real runtime consumers. That claim is
true for the three settings called out in the previous recheck:

- `gcs_redis_heartbeat_interval_milliseconds`
- `maximum_gcs_destroyed_actor_cached_count`
- `gcs_server_rpc_server_thread_num`

But it is still not true for every ported Rust `RayConfig` entry.

### 3. Low: the `main.rs` flag table still documents the wrong default port

The top-of-file flag table in `main.rs` still says:

- `gcs_server_port` default `6379`

at:

- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:10)

while the actual runtime default is `0`.

This is not a functional blocker, but it is stale and should be fixed to
avoid confusion in future reviews.

---

## Tests Run

I ran:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib redis_heartbeat_default_matches_cpp`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib actor_manager_honors_max_destroyed_actors_cached_from_ray_config`
  - passed

These confirm that the newly fixed Redis-heartbeat and destroyed-actor
cap paths are wired correctly.

They do not close the remaining default-mismatch issue for
`gcs_server_rpc_server_thread_num`.

---

## What Needs To Be Done To Close Blocker 3

At minimum:

1. Change the Rust default for `gcs_server_rpc_server_thread_num` to
   match the C++ formula:
   - `max(1, hardware_concurrency / 4)`

2. Add a regression test that checks Rust computes the same default as
   C++ for that field, ideally by deriving the expected value from the
   current machine’s available CPU count.

3. Either:
   - wire the remaining ported settings into real Rust runtime
     consumers, or
   - narrow the scope of the Rust `ray-config` crate documentation and
     any close-out report claims so they do not imply full config-surface
     parity where it does not yet exist.

Until at least item 1 is fixed, I would keep Blocker 3 open.
