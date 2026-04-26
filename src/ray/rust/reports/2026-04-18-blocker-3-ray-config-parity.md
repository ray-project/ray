# 2026-04-18 — Blocker 3 recheck: `config_list` / `RayConfig` parity

## Verdict

Blocker 3 is **partially fixed, but not fully closed**.

The important startup-path defects that previously made Rust clearly
non-equivalent to C++ are now fixed:

- `config_list` is now decoded strictly as base64 and invalid payloads
  are fatal, matching C++.
- `ray_config::initialize(config_list)` now exists and is called during
  Rust GCS startup.
- the default GCS port is now `0`, matching C++ rather than the old
  incorrect Rust default of `6379`.
- at least some runtime behavior now actually reads the initialized
  config, most notably the node health-check loop.

However, if the standard for closure is what the user has asked for
throughout this review, namely that the Rust GCS be a **drop-in
replacement** for the C++ GCS, then Blocker 3 is still not fully
closed. The remaining gap is no longer "Rust ignores `config_list`
entirely"; it is narrower:

- Rust now has a real `RayConfig`, but it only ports a subset of the
  C++ settings.
- several settings that are present in the new Rust `ray_config` crate
  are still not consumed by the runtime paths where C++ uses them.
- two lifecycle-delay paths still bypass `ray_config` and read env vars
  directly, and the code comments claim `RayConfig::initialize`
  re-exports merged values back to env, but there is no implementation
  of that re-export.

That means the Rust binary is much closer to C++, but it still does not
yet honor the same config surface or the same config plumbing contract
end to end.

---

## What Is Fixed

### 1. Strict `config_list` decoding now matches C++

C++ startup requires `FLAGS_config_list` to be valid base64:

- `ray/src/ray/gcs/gcs_server_main.cc:102-110`

Rust now does the same in
`ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:54-75`:

- empty string is accepted
- invalid base64 is an error
- non-UTF-8 decoded bytes are an error

This closes the previous mismatch where Rust silently fell back to the
raw bytes and continued startup.

### 2. Rust now initializes a real global `RayConfig`

Rust startup now calls:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:114-117`

```rust
let config_list = decode_config_list(config_list_raw)?;
ray_config::initialize(&config_list)?;
```

This is the right shape and matches the C++ startup contract in:

- `ray/src/ray/gcs/gcs_server_main.cc:120`

The new `ray-config` crate implements:

- typed defaults
- env override precedence
- strict JSON parsing
- unknown-key rejection
- process-global access through `instance()` / `snapshot()`

Relevant code:

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:1-240`

### 3. The default GCS port now matches C++

Rust `main.rs` now defaults `gcs_server_port` to `0`:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:85-92`

Rust `GcsServerConfig::default()` also now defaults `grpc_port` to `0`:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:122-141`

That matches C++:

- `ray/src/ray/gcs/gcs_server_main.cc:44-48`

This closes the old port-collision mismatch.

### 4. Some runtime behavior now genuinely consumes `RayConfig`

The health-check loop is no longer driven by ad hoc local defaults; it
now reads from the initialized config snapshot:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:736-746`

Specifically:

- `health_check_period_ms`
- `health_check_timeout_ms`
- `health_check_failure_threshold`

This is a real parity improvement, not just a parsing improvement.

### 5. The existing targeted regression tests pass

I reran the relevant tests and they passed:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib default_gcs_server_port_matches_cpp`
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib ray_config_initialize_merges_overrides`

Observed results:

- `default_gcs_server_port_matches_cpp` passed
- `ray_config_initialize_merges_overrides` passed

The new tests in `gcs-server/src/lib.rs:865-915` are therefore doing
their intended job as regression guards for the two most obvious startup
parity fixes.

---

## What Is Still Not Closed

### 1. The Rust `ray_config` surface is explicitly partial

The new crate documents its own limitation:

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:16-21`

It says it ports only the settings the Rust GCS "actually consumes
today, plus a handful of adjacent ones". That is already enough to show
the implementation is not yet equivalent to the C++ `RayConfig`
contract, which is much broader.

For a normal incremental refactor this would be fine. For a
drop-in-replacement claim, it is not fine yet.

### 2. Several settings are defined in Rust but still not wired to the runtime

The Rust `ray-config` crate now defines settings such as:

- `emit_main_service_metrics`
- `gcs_pull_resource_loads_period_milliseconds`
- `debug_dump_period_milliseconds`
- `event_stats_print_interval_ms`
- `event_level`
- `emit_event_to_log_file`

Definitions:

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:174-206`

But the actual runtime consumers are still sparse. The only clear
production call sites I found are:

- `main.rs:189` for `event_log_reporter_enabled`
- `lib.rs:741-745` for health-check timing

Search evidence:

- `rg -n "ray_config::instance\\(|ray_config::snapshot\\(" ray/src/ray/rust/gcs/crates -g '!target/**'`

By contrast, the C++ GCS startup and runtime use RayConfig values in
more places, including:

- `emit_main_service_metrics`
- `event_level`
- `emit_event_to_log_file`
- `gcs_server_rpc_server_thread_num`
- `event_stats_print_interval_ms`
- `gcs_pull_resource_loads_period_milliseconds`
- `external_storage_namespace`

Evidence from C++:

- `ray/src/ray/gcs/gcs_server_main.cc:127`
- `ray/src/ray/gcs/gcs_server_main.cc:158-166`
- `ray/src/ray/gcs/gcs_server.cc:321`
- `ray/src/ray/gcs/gcs_server.cc:445`
- `ray/src/ray/gcs/gcs_server.cc:930`
- `ray/src/ray/gcs/store_client/redis_store_client.cc:138`

So even though Rust now stores some of these values, it still does not
use them in the same places as C++.

### 3. Two lifecycle-delay settings still bypass `ray_config`

The lifecycle listeners in Rust still read these values directly from
environment variables:

- `RAY_gcs_mark_task_failed_on_worker_dead_delay_ms`
- `RAY_gcs_mark_task_failed_on_job_done_delay_ms`

Evidence:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:543-560`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:600-610`

That is not automatically wrong by itself. The problem is the comment
justifying this path:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:549-554`

It says production callers can override through `config_list` because
`RayConfig::initialize` "eventually re-exports its merged state to env
for listener consumption".

I checked the implementation and found no such re-export.

I searched for:

- `set_var(`
- the two `RAY_gcs_mark_task_failed_on_*` names
- any production code that writes merged `RayConfig` values back into
  the environment

The only `set_var` matches are in tests under the `ray-config` crate and
test code in `gcs-server`, not in production startup.

So as the code stands today:

- env overrides work
- `config_list` overrides update the in-memory `RayConfig`
- but these two listener closures do not read the in-memory `RayConfig`
- and there is no implementation that pushes the merged `RayConfig`
  value back into env for them

That leaves a real behavioral gap: a `config_list` override for these
two delay settings is not proven to affect the runtime path that uses
them.

### 4. C++ still uses config knobs that Rust has not ported at all

One concrete example is:

- `gcs_server_rpc_server_thread_num`

C++ uses it during startup:

- `ray/src/ray/gcs/gcs_server_main.cc:165-166`

I did not find a corresponding Rust field or runtime use in the new
`ray-config` crate. That means the Rust binary still does not accept or
honor the same startup configuration surface.

There are likely more examples, but this one alone is enough to keep
Blocker 3 open under a strict parity bar.

---

## What Needs To Be Done To Fully Close Blocker 3

The remaining work is specific and should be treated as parity work, not
cleanup.

### Required for closure

1. Expand the Rust `ray-config` crate to cover every C++ `RayConfig`
   field that the C++ GCS startup path or runtime behavior depends on.

2. Replace remaining production env-only reads with direct
   `ray_config::snapshot()` or `ray_config::instance()` reads, unless
   there is a very strong reason not to.

3. If env reads must remain for test isolation, then implement the
   missing re-export explicitly during startup and add a regression test
   proving that a `config_list` override changes the listener behavior
   for:
   - `gcs_mark_task_failed_on_worker_dead_delay_ms`
   - `gcs_mark_task_failed_on_job_done_delay_ms`

4. Wire the already-ported settings into the same runtime call sites
   where C++ uses them, including at minimum the ones already visible in
   the current C++ GCS:
   - `emit_main_service_metrics`
   - `event_level`
   - `emit_event_to_log_file`
   - `gcs_pull_resource_loads_period_milliseconds`
   - `event_stats_print_interval_ms`
   - any startup thread-count configuration that C++ honors

5. Add end-to-end regression tests that prove `config_list` affects
   runtime behavior, not just the stored `RayConfig` snapshot.

### Minimal evidence I would want before calling this closed

- a test showing `config_list` changes the worker-dead task-failure
  delay path
- a test showing `config_list` changes the job-finished task-failure
  delay path
- a test or code-path proof that event logging respects
  `event_level` / `emit_event_to_log_file` parity
- a code-path proof that any C++ startup setting already used in
  `gcs_server_main.cc` has either been implemented in Rust or is
  intentionally impossible because the Rust architecture differs
  materially

---

## Bottom Line

This fix is real and valuable. The old version of Blocker 3 is no
longer accurate: Rust does now have strict `config_list` handling, a
real `RayConfig`, and correct port-default parity.

But under the stronger standard of "Rust GCS should be a drop-in
replacement for the C++ GCS", I would still keep Blocker 3 **open**.

The blocker has changed from:

- "Rust does not initialize or honor `config_list` like C++"

to:

- "Rust now initializes `config_list`, but still does not honor the full
  C++ GCS config contract all the way through the runtime."
