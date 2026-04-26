//! Ray runtime configuration singleton.
//!
//! Ports C++ `RayConfig` (`ray/src/ray/common/ray_config.{h,cc}`) and its
//! macro-generated settings from `ray_config_def.h`. The contract is
//! identical to C++:
//!
//! 1. Each setting has a compile-time typed name and default value.
//! 2. On first access, the setting is loaded with this precedence:
//!    default → `RAY_{name}` env var → `config_list` JSON override.
//! 3. `initialize(config_list)` parses the JSON string and applies the
//!    overrides. Unknown keys are a hard error (matches C++
//!    `RAY_LOG(FATAL)` at `ray_config.cc:60`).
//! 4. Values are queried through `instance()` (which returns a read
//!    guard) or `snapshot()` (which clones).
//!
//! Scope policy: **only settings with a real Rust runtime consumer**
//! live here. C++'s `ray_config_def.h` has ~200 entries; Rust ports
//! them one-at-a-time, paired with the call site that actually
//! consumes the value. Shipping an entry without a consumer is
//! anti-parity — it implies configurability the runtime doesn't
//! provide. Adding more settings is a one-line change in the
//! `ray_configs!` macro plus the consumer call site.

use std::str::FromStr;

use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockReadGuard};
use tracing::warn;

/// Parse a `serde_json::Value` into `T`, honoring both native JSON types
/// and stringified values.
///
/// The Python/Ray layer historically sends some overrides as strings
/// (e.g. `{"event_log_reporter_enabled": "true"}`), which C++'s
/// `nlohmann::json::get<T>` accepts via implicit coercion. We match that.
fn parse_value<T>(value: &serde_json::Value) -> Option<T>
where
    T: FromStr + for<'de> serde::Deserialize<'de>,
{
    // Try structured JSON first (bool → bool, number → number, string → string).
    if let Ok(parsed) = serde_json::from_value::<T>(value.clone()) {
        return Some(parsed);
    }
    // Fall back to parsing a stringified value.
    if let Some(s) = value.as_str() {
        if let Ok(parsed) = T::from_str(s) {
            return Some(parsed);
        }
    }
    None
}

/// Read env var `RAY_{name}` with a typed default. Parses via `FromStr`.
fn read_env<T: FromStr>(name: &str, default: T) -> T {
    let key = format!("RAY_{name}");
    match std::env::var(&key) {
        Ok(raw) => match raw.parse::<T>() {
            Ok(v) => v,
            Err(_) => {
                warn!(env = %key, value = %raw, "Invalid env override, using default");
                default
            }
        },
        Err(_) => default,
    }
}

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

/// Specialized env reader for `bool`: accepts `"1"`, `"0"`, `"true"`,
/// `"false"` (case-insensitive), matching C++
/// `ConvertValue<bool>` (`ray_config.h:44-48`).
fn read_env_bool(name: &str, default: bool) -> bool {
    let key = format!("RAY_{name}");
    match std::env::var(&key) {
        Ok(raw) => {
            let lower = raw.to_ascii_lowercase();
            matches!(lower.as_str(), "1" | "true")
                || (!matches!(lower.as_str(), "0" | "false") && default)
        }
        Err(_) => default,
    }
}

/// Macro: `ray_configs!` declares the `RayConfig` struct, its default
/// impl (default → env override), and the JSON override logic.
///
/// One entry per C++ `RAY_CONFIG(kind, name, default)` — the `kind`
/// discriminator routes to the right parser:
///
/// - `bool!` → bool (accepts `true|false|1|0`, case-insensitive).
/// - `num!(T)` → numeric of type T (accepts JSON number or stringified number).
/// - `str!` → string (accepts JSON string or any JSON scalar stringified).
macro_rules! ray_configs {
    ( $( ($kind:ident $( ( $ty:ty ) )? , $name:ident , $default:expr) ),+ $(,)? ) => {
        #[derive(Debug, Clone)]
        pub struct RayConfig {
            $( pub $name: ray_configs!(@field_ty $kind $( $ty )?), )+
        }

        impl Default for RayConfig {
            fn default() -> Self {
                Self {
                    $( $name: ray_configs!(@load $kind $( $ty )?, $name, $default), )+
                }
            }
        }

        impl RayConfig {
            /// Apply a single JSON key/value override. Returns `Err`
            /// with the offending key name for unknown or malformed
            /// values — matching C++ `RAY_LOG(FATAL)` at
            /// `ray_config.cc:60`.
            fn apply_json_kv(&mut self, key: &str, value: &serde_json::Value) -> Result<(), String> {
                match key {
                    $(
                        stringify!($name) => {
                            ray_configs!(@apply $kind $( $ty )?, self.$name, key, value);
                            Ok(())
                        }
                    )+
                    _ => Err(format!("unknown config key: {}", key)),
                }
            }

            /// Write every setting to its `RAY_{name}` env variable.
            ///
            /// Production purpose — after `initialize(config_list)` merges
            /// overrides, this makes the merged value visible to any code
            /// path that still reads the raw `RAY_{name}` env var
            /// directly. That's how `config_list` overrides reach the
            /// GCS's hot-path listener closures (which read env on every
            /// event rather than taking the config RwLock): the
            /// closures see whatever `initialize` wrote here.
            ///
            /// This is the Rust analogue of C++'s "every setting lives in
            /// the process's RayConfig instance", except Rust splits the
            /// consumers into env-readers and `ray_config::instance()`
            /// readers, and we bridge the two here.
            fn export_to_env(&self) {
                $(
                    std::env::set_var(
                        format!("RAY_{}", stringify!($name)),
                        ray_configs!(@to_env_string $kind $( $ty )?, self.$name),
                    );
                )+
            }
        }
    };

    // Field-type helpers.
    (@field_ty bool) => { bool };
    (@field_ty num $ty:ty) => { $ty };
    (@field_ty str) => { String };

    // Load-from-default-plus-env helpers.
    (@load bool, $name:ident, $default:expr) => {
        read_env_bool(stringify!($name), $default)
    };
    (@load num $ty:ty, $name:ident, $default:expr) => {
        read_env::<$ty>(stringify!($name), $default)
    };
    (@load str, $name:ident, $default:expr) => {
        read_env::<String>(stringify!($name), ($default).to_string())
    };

    // Env-serialization helpers (used by `export_to_env`).
    (@to_env_string bool, $v:expr) => { if $v { "true".to_string() } else { "false".to_string() } };
    (@to_env_string num $ty:ty, $v:expr) => { ($v).to_string() };
    (@to_env_string str, $v:expr) => { ($v).clone() };

    // JSON-apply helpers.
    (@apply bool, $field:expr, $key:expr, $value:expr) => {
        {
            let parsed: bool = parse_value::<bool>($value)
                .or_else(|| $value.as_str().map(|s| {
                    let l = s.to_ascii_lowercase();
                    l == "true" || l == "1"
                }))
                .ok_or_else(|| format!("config key {} is not a bool", $key))?;
            $field = parsed;
        }
    };
    (@apply num $ty:ty, $field:expr, $key:expr, $value:expr) => {
        {
            let parsed: $ty = parse_value::<$ty>($value)
                .ok_or_else(|| format!("config key {} is not a {}", $key, stringify!($ty)))?;
            $field = parsed;
        }
    };
    (@apply str, $field:expr, $key:expr, $value:expr) => {
        {
            let parsed = match $value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            $field = parsed;
        }
    };
}

// The full ray_config_def.h has ~200 entries; we port the ones the Rust
// GCS actually uses plus obvious adjacent ones, matching C++ defaults.
// Each entry below is annotated with its `ray_config_def.h` source line.
ray_configs! {
    // ray_config_def.h:849 — log-reporter gate for export events.
    // Consumer: `GcsServer::new_with_store` export-writer init.
    (bool, event_log_reporter_enabled, true),
    // ray_config_def.h:853 — also emit export events to the log file.
    // Consumer: `ExportEventManager::new_with_config` mirror-to-tracing.
    (bool, emit_event_to_log_file, false),

    // ray_config_def.h:932 — raylet health-check cadence.
    // Consumer: `GcsNodeManager::start_health_check_loop`.
    (num(i64), health_check_period_ms, 3000),
    // ray_config_def.h:934 — per-attempt timeout. Same consumer.
    (num(i64), health_check_timeout_ms, 10000),
    // ray_config_def.h:936 — consecutive failures before FAILED. Same consumer.
    (num(i64), health_check_failure_threshold, 5),
    // ray_config_def.h:553 — delay before marking tasks on a dead worker FAILED.
    // Consumer: `start_lifecycle_listeners` worker-dead closure
    // (via `export_to_env` → `RAY_{name}` env read).
    (num(u64), gcs_mark_task_failed_on_worker_dead_delay_ms, 1000),
    // ray_config_def.h:548 — delay before marking tasks of a finished job FAILED.
    // Consumer: same pattern as above, job-finished closure.
    (num(u64), gcs_mark_task_failed_on_job_done_delay_ms, 15000),
    // ray_config_def.h:22 — debug state dump period (0 disables).
    // Consumer: `GcsServer::start_debug_dump_loop`.
    (num(u64), debug_dump_period_milliseconds, 10000),
    // ray_config_def.h:382-384 — number of GCS RPC server threads.
    // C++ default: `std::max(1U, std::thread::hardware_concurrency() / 4U)`.
    // Consumer: `main.rs` tokio runtime `worker_threads`.
    (num(u32), gcs_server_rpc_server_thread_num, cpu_quarter_default()),
    // ray_config_def.h:395 — Redis client health-check interval (ms, 0 disables).
    // Consumer: `GcsServer::start_redis_health_check`.
    (num(u64), gcs_redis_heartbeat_interval_milliseconds, 100),
    // ray_config_def.h:936 — maximum GCS destroyed-actor cache count.
    // Consumer: `GcsActorManager::max_destroyed_actors_cached`.
    (num(u64), maximum_gcs_destroyed_actor_cached_count, 100000),
    // ray_config_def.h:293 — graceful-shutdown timeout for actor destroy.
    // After this many ms the GCS escalates a `force_kill=false` destroy
    // to a force-kill RPC if the worker has not exited. Consumer:
    // `GcsActorManager::destroy_actor` graceful_shutdown_timers map
    // (parity with C++ `gcs_actor_manager.cc:1041-1080`).
    (num(i64), actor_graceful_shutdown_timeout_ms, 30000),
    // ray_config_def.h:62 — period between raylet GetResourceLoad polls.
    // Consumer: `GcsServer::start_raylet_load_pull_loop`.
    (num(u64), gcs_pull_resource_loads_period_milliseconds, 1000),
    // ray_config_def.h:498 — max number of task-event rows the GCS retains.
    // Oldest-first eviction kicks in above this cap. Consumer:
    // `GcsTaskManager::new` via `GcsServerConfig::max_task_events`.
    (num(i64), task_events_max_num_task_in_gcs, 100000),
    // ray_config_def.h:529 — per-task cap on profile events. Older entries
    // are dropped from the front on merge. Consumer:
    // `TaskEventStorage::add_or_replace` profile-events truncation path.
    (num(i64), task_events_max_num_profile_events_per_task, 1000),
    // ray_config_def.h:506 — per-job cap on the *tracked set* of
    // dropped task attempts. C++ evicts the oldest entries (+ 10% of
    // the overflow to mitigate thrashing) but preserves the total
    // count via `num_dropped_task_attempts_evicted_`. Consumer:
    // `TaskEventStorage::gc_job_summary`, driven by the server-side
    // 5s periodic loop parity with C++ `GcsTaskManager` ctor at
    // `gcs_task_manager.cc:50-52`.
    (num(i64), task_events_max_dropped_task_attempts_tracked_per_job_in_gcs, 1_000_000),

    // ray_config_def.h:856 — event severity threshold.
    // Consumer: `ExportEventManager::event_level`.
    (str, event_level, "warning"),
    // ray_config_def.h:866 — storage namespace.
    // Consumer: `main.rs` `external_storage_namespace` fallback.
    (str, external_storage_namespace, "default"),
}

impl RayConfig {
    /// Apply every override in a JSON object to this config. Strict:
    /// unknown keys abort with an error. Matches C++
    /// `RayConfig::initialize` (`ray_config.cc:31-78`).
    ///
    /// An empty string is a no-op (defaults stand) — same as C++ line
    /// 38-40.
    pub fn apply_json(&mut self, config_list: &str) -> Result<(), String> {
        if config_list.is_empty() {
            return Ok(());
        }
        let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(config_list)
            .map_err(|e| format!("config_list is not valid JSON: {e}"))?;
        for (k, v) in map {
            self.apply_json_kv(&k, &v)?;
        }
        Ok(())
    }
}

static INSTANCE: Lazy<RwLock<RayConfig>> = Lazy::new(|| RwLock::new(RayConfig::default()));

/// Initialize the global `RayConfig` from a decoded `config_list` JSON
/// string. Call exactly once at process startup, matching C++
/// `RayConfig::instance().initialize(config_list)` at
/// `gcs_server_main.cc:120`.
///
/// Side effect: after merging overrides, every setting is re-exported
/// to `RAY_{name}` via `std::env::set_var`. This bridges `config_list`
/// overrides to hot-path listener closures that read env directly
/// (see `gcs-server/src/lib.rs` lifecycle-delay reads). The in-memory
/// config and the env are guaranteed consistent after `initialize`
/// returns.
///
/// Returns `Err` if the JSON is malformed or contains an unknown key;
/// the caller should treat this as fatal. Empty strings are permitted
/// (no overrides).
pub fn initialize(config_list: &str) -> Result<(), String> {
    let mut cfg = INSTANCE.write();
    cfg.apply_json(config_list)?;
    cfg.export_to_env();
    Ok(())
}

/// Return a read-only handle to the global config. Brief reads only —
/// don't hold the guard across async awaits. Use `snapshot()` if you
/// need to keep the config around.
pub fn instance() -> RwLockReadGuard<'static, RayConfig> {
    INSTANCE.read()
}

/// Clone the current global config. Useful when you need to carry
/// config state across await points or into other threads without
/// holding the lock.
pub fn snapshot() -> RayConfig {
    INSTANCE.read().clone()
}

/// Replace the global config for the duration of a test. Returns the
/// previous value so it can be restored. Exposed unconditionally (not
/// just under `#[cfg(test)]`) so integration tests in sibling crates
/// can use it; downstream production code should call `initialize`.
pub fn replace_for_test(new_config: RayConfig) -> RayConfig {
    std::mem::replace(&mut *INSTANCE.write(), new_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests mutate the global; run them serially to avoid flake.
    // `#[serial_test]` isn't in the deps so we use a module-local
    // mutex to synchronize.
    use std::sync::Mutex;
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn fresh_default() -> RayConfig {
        // Clean any leaking env vars a prior test may have set.
        for name in [
            "RAY_event_log_reporter_enabled",
            "RAY_health_check_period_ms",
            "RAY_gcs_mark_task_failed_on_worker_dead_delay_ms",
        ] {
            std::env::remove_var(name);
        }
        RayConfig::default()
    }

    /// Parity with C++ `ray_config_def.h:382-384`:
    /// `std::max(1U, std::thread::hardware_concurrency() / 4U)`.
    /// Derives the expected value from the current machine's CPU
    /// count exactly the way C++ does, so we don't pin a number that
    /// drifts between CI runners.
    #[test]
    fn gcs_server_rpc_server_thread_num_default_matches_cpp_formula() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::remove_var("RAY_gcs_server_rpc_server_thread_num");

        let hardware_concurrency: u32 = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        let expected = (hardware_concurrency / 4).max(1);

        let cfg = RayConfig::default();
        assert_eq!(
            cfg.gcs_server_rpc_server_thread_num, expected,
            "parity with C++ std::max(1U, hardware_concurrency / 4U); \
             hardware_concurrency={hardware_concurrency}"
        );
        // Floor of 1 in the pathological 1-CPU case.
        assert!(cfg.gcs_server_rpc_server_thread_num >= 1);
    }

    #[test]
    fn defaults_match_cpp() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let cfg = fresh_default();
        // Spot-check the defaults that matter for the GCS startup path.
        assert!(cfg.event_log_reporter_enabled);
        assert!(!cfg.emit_event_to_log_file);
        assert_eq!(cfg.health_check_period_ms, 3000);
        assert_eq!(cfg.health_check_timeout_ms, 10000);
        assert_eq!(cfg.health_check_failure_threshold, 5);
        assert_eq!(cfg.gcs_mark_task_failed_on_worker_dead_delay_ms, 1000);
        assert_eq!(cfg.gcs_mark_task_failed_on_job_done_delay_ms, 15000);
        assert_eq!(cfg.event_level, "warning");
    }

    #[test]
    fn env_override_wins_over_default() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::set_var("RAY_health_check_period_ms", "12345");
        let cfg = RayConfig::default();
        assert_eq!(cfg.health_check_period_ms, 12345);
        std::env::remove_var("RAY_health_check_period_ms");
    }

    #[test]
    fn env_override_bool_accepts_numeric_and_word() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::set_var("RAY_event_log_reporter_enabled", "false");
        assert!(!RayConfig::default().event_log_reporter_enabled);
        std::env::set_var("RAY_event_log_reporter_enabled", "0");
        assert!(!RayConfig::default().event_log_reporter_enabled);
        std::env::set_var("RAY_event_log_reporter_enabled", "TRUE");
        assert!(RayConfig::default().event_log_reporter_enabled);
        std::env::remove_var("RAY_event_log_reporter_enabled");
    }

    #[test]
    fn apply_json_overrides_defaults() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = fresh_default();
        cfg.apply_json(
            r#"{
                "health_check_period_ms": 7777,
                "event_log_reporter_enabled": false,
                "event_level": "info"
            }"#,
        )
        .unwrap();
        assert_eq!(cfg.health_check_period_ms, 7777);
        assert!(!cfg.event_log_reporter_enabled);
        assert_eq!(cfg.event_level, "info");
    }

    #[test]
    fn apply_json_accepts_stringified_bools() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = fresh_default();
        cfg.apply_json(r#"{"event_log_reporter_enabled": "false"}"#)
            .unwrap();
        assert!(!cfg.event_log_reporter_enabled);
    }

    #[test]
    fn apply_json_empty_is_noop() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = fresh_default();
        cfg.apply_json("").unwrap();
        // Defaults preserved.
        assert!(cfg.event_log_reporter_enabled);
    }

    #[test]
    fn apply_json_unknown_key_is_fatal() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = fresh_default();
        let err = cfg
            .apply_json(r#"{"definitely_not_a_known_config": 42}"#)
            .unwrap_err();
        assert!(
            err.contains("unknown config key"),
            "expected unknown-key error, got: {err}"
        );
    }

    #[test]
    fn apply_json_malformed_is_error() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = fresh_default();
        let err = cfg.apply_json("not-json-at-all").unwrap_err();
        assert!(err.contains("not valid JSON"), "got: {err}");
    }

    #[test]
    fn apply_json_type_mismatch_is_error() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = fresh_default();
        let err = cfg
            .apply_json(r#"{"health_check_period_ms": "not-a-number"}"#)
            .unwrap_err();
        assert!(err.contains("health_check_period_ms"), "got: {err}");
    }

    #[test]
    fn precedence_config_list_overrides_env() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::set_var("RAY_health_check_period_ms", "1111");
        let mut cfg = RayConfig::default();
        assert_eq!(cfg.health_check_period_ms, 1111);
        cfg.apply_json(r#"{"health_check_period_ms": 2222}"#).unwrap();
        assert_eq!(cfg.health_check_period_ms, 2222);
        std::env::remove_var("RAY_health_check_period_ms");
    }

    #[test]
    fn global_initialize_and_instance() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = replace_for_test(RayConfig::default());
        initialize(r#"{"health_check_period_ms": 4242}"#).unwrap();
        assert_eq!(instance().health_check_period_ms, 4242);
        let _ = replace_for_test(prev);
    }

    /// Parity-glue test: `initialize` re-exports every setting to
    /// `RAY_{name}` so consumers that still read env (hot-path listener
    /// closures in `gcs-server`) see the merged `config_list` value.
    #[test]
    fn initialize_exports_merged_values_to_env() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Clean slate.
        std::env::remove_var("RAY_gcs_mark_task_failed_on_worker_dead_delay_ms");
        std::env::remove_var("RAY_gcs_mark_task_failed_on_job_done_delay_ms");
        std::env::remove_var("RAY_event_log_reporter_enabled");
        let prev = replace_for_test(RayConfig::default());

        initialize(
            r#"{
                "gcs_mark_task_failed_on_worker_dead_delay_ms": 4321,
                "gcs_mark_task_failed_on_job_done_delay_ms": 98765,
                "event_log_reporter_enabled": false
            }"#,
        )
        .unwrap();

        assert_eq!(
            std::env::var("RAY_gcs_mark_task_failed_on_worker_dead_delay_ms").unwrap(),
            "4321"
        );
        assert_eq!(
            std::env::var("RAY_gcs_mark_task_failed_on_job_done_delay_ms").unwrap(),
            "98765"
        );
        assert_eq!(
            std::env::var("RAY_event_log_reporter_enabled").unwrap(),
            "false"
        );

        // Restore.
        std::env::remove_var("RAY_gcs_mark_task_failed_on_worker_dead_delay_ms");
        std::env::remove_var("RAY_gcs_mark_task_failed_on_job_done_delay_ms");
        std::env::remove_var("RAY_event_log_reporter_enabled");
        let _ = replace_for_test(prev);
    }

    #[test]
    fn snapshot_does_not_hold_lock() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let snap = snapshot();
        // Can still take a write lock after — proves snapshot cloned.
        let _w = INSTANCE.write();
        drop(_w);
        let _reread = snap.health_check_period_ms;
    }
}
