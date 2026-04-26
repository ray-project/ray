# 2026-04-18 — Blocker 3 recheck after claimed fix

## Verdict

Blocker 3 is **not fully closed yet**.

The previous open issue around lifecycle-delay propagation from
`config_list` into the listener closures is now genuinely fixed. I
verified both the code path and the regression tests for that bridge.

However, there are still concrete config-parity gaps between Rust GCS
and C++ GCS that matter under the "drop-in replacement" bar:

1. Rust still does not honor C++'s Redis heartbeat config knob.
2. Rust still defines some `RayConfig` fields without actually using
   them to drive runtime behavior.
3. One close-out report in the tree overstates the fix and is not fully
   supported by the code as it exists now.

This keeps Blocker 3 open.

---

## What Is Fixed

### 1. `config_list` startup handling is still correct

The previously fixed startup pieces remain correct:

- strict base64 decoding of `config_list`
- fatal on bad base64 / bad JSON / unknown key
- `ray_config::initialize(&config_list)` called at startup
- default GCS port `0`

Relevant code:

- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:54)
- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:114)
- [lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:122)

### 2. The lifecycle-delay env bridge is now real

This was the strongest remaining issue from my last review, and it is
now fixed properly.

`ray_config::initialize()` now exports merged settings back into
`RAY_{name}` env vars:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:123)
- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:280)

That makes these listener closures observe `config_list` overrides even
though they still read env directly:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:554)
- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:613)

I also ran the targeted tests and both passed:

- `ray-config::tests::initialize_exports_merged_values_to_env`
- `gcs-server::tests::config_list_override_for_listener_delay_reaches_env`

### 3. `event_level`, `emit_event_to_log_file`, and `external_storage_namespace` are now consumed

These were previously only stored or only partially wired. They are now
used in real runtime paths:

- `event_level` and `emit_event_to_log_file` now flow into export-event
  writer initialization:
  [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:214)
- `external_storage_namespace` now falls back to `RayConfig` when the
  CLI flag is absent:
  [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:154)

Those are real improvements.

---

## Remaining Findings

### 1. High: Rust still does not honor C++ `gcs_redis_heartbeat_interval_milliseconds`

The new `ray-config` crate defines:

- `gcs_redis_heartbeat_interval_milliseconds`

at:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:232)

C++ uses that setting in the GCS server:

- `ray/src/ray/gcs/gcs_server.cc:183`

Rust does not.

Instead, Rust's Redis health check loop reads a different env var name
and uses a different default:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:359)

Specifically, Rust currently does:

- env var: `RAY_gcs_redis_heartbeat_interval_ms`
- default: `5000`

while the C++-mirrored Rust `RayConfig` field is:

- config name: `gcs_redis_heartbeat_interval_milliseconds`
- default: `100`

This is not a cosmetic discrepancy. It means a `config_list` override
for the C++ setting will not affect the Rust Redis heartbeat loop, and
even the fallback default behavior differs materially.

That alone is sufficient to keep Blocker 3 open.

### 2. High: `maximum_gcs_destroyed_actor_cached_count` is still ignored by runtime logic

The new `ray-config` crate defines:

- `maximum_gcs_destroyed_actor_cached_count`

at:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:234)

C++ uses that setting in actor-manager behavior:

- `ray/src/ray/gcs/actor/gcs_actor_manager.cc:1920`

Rust still uses a hard-coded constant:

- [actor_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:105)

```rust
const MAX_DESTROYED_ACTORS: usize = 100_000;
```

and that constant is used directly in the destroyed-actor cache logic:

- [actor_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:401)
- [actor_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1265)

So Rust now advertises the config knob, but still does not let it affect
the corresponding runtime behavior.

### 3. Medium: `gcs_server_rpc_server_thread_num` is defined and logged, but not used

The new `ray-config` crate now includes:

- `gcs_server_rpc_server_thread_num`

at:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:230)

C++ uses it during GCS startup:

- `ray/src/ray/gcs/gcs_server_main.cc:165-166`

Rust currently only logs it during startup:

- [main.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:227)

I did not find a runtime consumer that changes tonic or server execution
behavior based on this setting.

This is weaker than the Redis-heartbeat mismatch above because the Rust
server architecture differs from C++, and an exact thread-model match
may not be meaningful. But under a strict drop-in configuration parity
bar, it is still not implemented.

### 4. Medium: `emit_main_service_metrics` still has no Rust consumer

The field exists:

- [ray-config/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:207)

C++ uses it to configure the main io_context:

- `ray/src/ray/gcs/gcs_server_main.cc:127`

I did not find a Rust runtime consumer for it.

This may be acceptable if the Rust runtime model does not have an
equivalent construct, but it means the close-out claim that every
ported config entry has a real consumer is too strong.

---

## On The Existing Close-Out Report

There is already a file:

- [2026-04-18-blocker-3-ray-config-close-out.md](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/reports/2026-04-18-blocker-3-ray-config-close-out.md)

That report is correct about the lifecycle-delay bridge, the event
writer wiring, and the `external_storage_namespace` improvement.

But two of its broader claims are not fully supported by the current
code:

1. It says the blocker is "fully closed under the drop-in-replacement
   bar". I disagree because of the still-unwired Redis heartbeat config
   path and the still-ignored destroyed-actor cache limit.

2. It says every `RayConfig` entry now has at least one real consumer in
   the Rust codebase. That is not true for at least:
   - `gcs_redis_heartbeat_interval_milliseconds`
   - `maximum_gcs_destroyed_actor_cached_count`
   - likely also `emit_main_service_metrics`
   - and arguably `gcs_server_rpc_server_thread_num`

So that close-out report should not be treated as the final parity
verdict.

---

## Tests Run

I reran targeted regression tests relevant to the newly fixed and still
open areas:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p ray-config --lib initialize_exports_merged_values_to_env`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib config_list_override_for_listener_delay_reaches_env`
  - passed
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib ray_config_initialize_merges_overrides`
  - passed

These test results confirm the newly implemented `config_list` →
`RayConfig` → env bridge works as intended.

They do not close the remaining gaps above because those paths do not
yet have equivalent runtime consumers.

---

## What Needs To Be Done To Actually Close Blocker 3

At minimum:

1. Change Rust Redis heartbeat configuration to use the same
   `RayConfig` knob as C++:
   - consume `gcs_redis_heartbeat_interval_milliseconds`
   - remove the mismatched `RAY_gcs_redis_heartbeat_interval_ms` path
   - match the C++ default of `100`

2. Replace the hard-coded destroyed-actor cache limit with
   `maximum_gcs_destroyed_actor_cached_count`.

3. Either:
   - implement a real Rust consumer for
     `gcs_server_rpc_server_thread_num` and `emit_main_service_metrics`,
     or
   - explicitly document why those C++ knobs are intentionally
     non-applicable in Rust and remove any over-claim that Rust fully
     honors the same config surface.

Until that is done, I would keep Blocker 3 open.
