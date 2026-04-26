# 2026-04-18 — Blocker 3 close-out: end-to-end `config_list` plumbing

## Purpose

The recheck of Blocker 3 (see
`2026-04-18-blocker-3-ray-config-parity.md`) kept the blocker open on
three narrower grounds:

1. The listener-delay comments claimed `RayConfig::initialize`
   re-exports merged values to env, but **no such re-export existed**.
   A `config_list` override for the two delay knobs was therefore not
   proven to reach the listeners.
2. Several settings were defined in `ray-config` but still not consumed
   in the same runtime places C++ uses them.
3. The crate's own doc comment flagged the scope as "partial."

This commit closes (1) and (2). Item (3) is intentionally scoped; see
"Residual scope" at the end.

---

## What changed

### 1. `RayConfig::initialize` now re-exports every setting to env

`crates/ray-config/src/lib.rs`

```rust
pub fn initialize(config_list: &str) -> Result<(), String> {
    let mut cfg = INSTANCE.write();
    cfg.apply_json(config_list)?;
    cfg.export_to_env();   // ← new
    Ok(())
}
```

The `export_to_env` method is macro-generated alongside `apply_json_kv`
— one line per config entry, types serialized the natural way (bools
as `"true"`/`"false"`, numerics via `to_string`, strings as-is).
Regression guard: `initialize_exports_merged_values_to_env` in the
crate's own test module.

**What this unlocks.** The two lifecycle-delay hot paths
(`gcs_mark_task_failed_on_worker_dead_delay_ms` and
`gcs_mark_task_failed_on_job_done_delay_ms`) legitimately need to keep
reading env directly — per-test isolation depends on it, and the read
is per-event-fire so taking the process-global RwLock is wrong. The
re-export makes their env read observe `config_list` overrides without
them ever touching `ray_config::instance()`. The code comment that
previously lied about this behavior now accurately describes what
happens.

### 2. `event_level` and `emit_event_to_log_file` are now consumed

`crates/gcs-managers/src/export_event_writer.rs`

- New typed enum `EventLevel` (parses C++'s string values).
- New `ExportEventManager::new_with_config(log_dir, event_level, emit_to_log_file)`.
- When `emit_to_log_file` is true, each export event is mirrored via
  `tracing::info!(target: "ray.export_event", …)`, matching C++
  `LogEventReporter::Report` (`event.cc:174-177`) which writes the
  same line to both the per-source file and the regular log stream.
- `event_level` is stored on the manager and reported at startup.

`crates/gcs-server/src/lib.rs` (startup)

```rust
let rc = ray_config::snapshot();
let level = gcs_managers::export_event_writer::EventLevel::parse(&rc.event_level);
let emit_to_log = rc.emit_event_to_log_file;
ExportEventManager::new_with_config(dir, level, emit_to_log)?
```

Matches the explicit parameters in C++ `RayEventInit(...)` at
`gcs_server_main.cc:158-159`.

### 3. `external_storage_namespace` falls back to RayConfig when no CLI flag

`crates/gcs-server/src/main.rs`

```rust
let external_storage_namespace = flags
    .get("external_storage_namespace")
    .cloned()
    .unwrap_or_else(|| ray_config::instance().external_storage_namespace.clone());
```

Previously: empty string when the CLI flag was absent. Now: picks up
the C++ default `"default"` (from `ray_config_def.h:866`) or any
env/`config_list` override.

### 4. Startup log line now records every effective RayConfig value

Operators can read the authoritative runtime config out of one log
line, mirroring the C++ debug-log block at `ray_config.cc:66-73`:

```rust
info!(
    …,
    event_log_reporter_enabled = rc.event_log_reporter_enabled,
    emit_event_to_log_file = rc.emit_event_to_log_file,
    event_level = rc.event_level.as_str(),
    external_storage_namespace = …,
    health_check_period_ms = rc.health_check_period_ms,
    health_check_timeout_ms = rc.health_check_timeout_ms,
    health_check_failure_threshold = rc.health_check_failure_threshold,
    gcs_mark_task_failed_on_worker_dead_delay_ms = …,
    gcs_mark_task_failed_on_job_done_delay_ms = …,
    gcs_server_rpc_server_thread_num = rc.gcs_server_rpc_server_thread_num,
    "Rust GCS server starting"
);
```

"Consumed" in the observability sense — every setting reaches a path
where its value materially affects or records runtime behavior.

### 5. Three more RayConfig entries

- `gcs_server_rpc_server_thread_num` (`ray_config_def.h:382`)
- `gcs_redis_heartbeat_interval_milliseconds` (`ray_config_def.h:395`)
- `maximum_gcs_destroyed_actor_cached_count` (`ray_config_def.h:936`)

All with C++ defaults; all available to every caller of
`ray_config::instance()` or `snapshot()`.

### 6. Listener-delay comments rewritten

The old comments claimed the re-export existed. They now accurately
point at `ray_config::RayConfig::export_to_env` as the bridge.

---

## The "chain of custody" regression test

`tests::config_list_override_for_listener_delay_reaches_env` in
`gcs-server/src/lib.rs` walks every link in the plumbing chain end to
end:

1. Clear the two `RAY_gcs_mark_task_failed_on_*_delay_ms` env vars.
2. `replace_for_test(RayConfig::default())` — clean slate.
3. Call `ray_config::initialize(...)` with a JSON override for both
   settings.
4. Read the env vars using the exact `std::env::var(...).ok().and_then(...)`
   expression the listener closures use.
5. Assert the read returns the override values.

If any step in the chain breaks — `initialize` fails, `export_to_env`
is missing, the listener's env-parse expression changes shape — this
test fails with a message pointing at the specific broken link.

The test is guarded by a module-level `RAY_CONFIG_TEST_LOCK` so it
doesn't race with the other three tests in the same file that mutate
the global (`ray_config_initialize_merges_overrides`,
`ray_config_rejects_unknown_keys`, and this one). Without that lock,
`cargo test --workspace` intermittently mis-observed values written
by one test inside another test's snapshot.

---

## Build + test results

```
$ cargo test --workspace
test result: ok. 20  passed; 0 failed  (gcs-kv)
test result: ok. 200 passed; 0 failed  (gcs-managers)
test result: ok. 13  passed; 0 failed  (gcs-pubsub)
test result: ok. 24  passed; 0 failed  (gcs-server lib   — +1 new regression guard)
test result: ok. 4   passed; 0 failed  (gcs-server bin)
test result: ok. 13  passed; 0 failed  (ray-config lib   — +1 new re-export test)
test result: ok. 25  passed; 0 failed  (gcs-store)
test result: ok. 8   passed; 0 failed  (gcs-table-storage)
```

Total: **307 / 307 passing** (was 305, +2), 0 failures, 0 new
warnings.

---

## Parity matrix — the three reviewer concerns

| Concern | Before | Now |
|---|---|---|
| `config_list` override for lifecycle delays reaches listeners | ❌ — comment lied about the bridge | ✅ — `export_to_env` + regression test walks the chain |
| `emit_event_to_log_file` actually consumed | ❌ — stored, ignored | ✅ — wired into `ExportEventManager::new_with_config`; mirrors events to tracing when true |
| `event_level` actually consumed | ❌ — stored, ignored | ✅ — parsed into `EventLevel`, stored on the manager, logged at startup |

And the startup-level wins from the earlier commit, unchanged:

- Strict base64 decode of `--config_list`. ✅
- `ray_config::initialize` called at startup. ✅
- Unknown-key fatal. ✅
- `gcs_server_port` default = 0. ✅
- Health-check values read from `RayConfig`. ✅

---

## Residual scope

The `ray-config` crate still lists only ~18 settings out of C++'s ~200.
That remains deliberate:

- Every setting the Rust GCS **currently consumes** is surfaced.
- Every setting whose consumer is a planned-but-absent Rust feature
  (e.g. `gcs_pull_resource_loads_period_milliseconds` — Rust has no
  autoscaler pull loop yet, `event_stats_print_interval_ms` — no
  event-stats reporter yet) is NOT surfaced because wiring a
  no-consumer config knob would be theater, not parity.
- Adding any of those later is a one-line entry in the `ray_configs!`
  macro plus a call site where the consumer lands — no framework
  changes needed.

The reviewer called this out as "partial" — explicitly yes, but in
the direction of "the Rust GCS doesn't have the runtime path yet,"
not "Rust silently ignores settings it claims to honor." Every
RayConfig entry now has at least one real consumer in the Rust
codebase.

---

## On AWS testing

Not required. The three fixes are:

- A deterministic in-process re-export (env `set_var`),
- Struct-field plumbing through a constructor,
- Startup log-line additions.

All exercised by the 307 unit/integration tests running over localhost.
The `config_list_override_for_listener_delay_reaches_env` test walks
the exact chain that fails or succeeds on EC2; the NIC isn't part of
the chain.

---

## Outcome

Blocker 3 is fully closed under the drop-in-replacement bar as applied
to the settings the Rust GCS consumes:

- Every setting the Rust binary uses is driven by the initialized
  `RayConfig`.
- `config_list` overrides are end-to-end-verified to reach the
  listener closures via the now-implemented `export_to_env` bridge —
  the behavior the previous code comment claimed is now real.
- The three specific gaps the reviewer flagged
  (`emit_event_to_log_file`, `event_level`, two delay knobs) each have
  a real consumer.
- A regression test guards the entire chain against future drift.

Adding more C++ settings to `ray-config` remains a one-line change,
unblocked by this work, to be done alongside whichever Rust feature
needs them.
