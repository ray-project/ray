// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Ray configuration.
//!
//! Replaces `src/ray/common/ray_config.h/cc` and `ray_config_def.h`.
//!
//! In C++, config values are defined via X-macro `RAY_CONFIG(type, name, default)`.
//! Here, we use a plain struct with defaults and support:
//! - Base64-encoded JSON config string from the Python launcher
//! - Environment variable overrides: `RAY_<UPPER_SNAKE_CASE_NAME>`

use std::sync::OnceLock;

/// Global Ray configuration singleton.
static RAY_CONFIG: OnceLock<RayConfig> = OnceLock::new();

/// Get the global RayConfig. Panics if not initialized.
pub fn ray_config() -> &'static RayConfig {
    RAY_CONFIG
        .get()
        .expect("RayConfig not initialized. Call RayConfig::initialize() first.")
}

/// Initialize the global RayConfig from a base64-encoded JSON string.
/// Returns an error if already initialized.
pub fn initialize_config(config_str: Option<&str>) -> Result<(), String> {
    let config = match config_str {
        Some(s) if !s.is_empty() => RayConfig::from_base64_json(s)?,
        _ => RayConfig::default(),
    };
    RAY_CONFIG
        .set(config)
        .map_err(|_| "RayConfig already initialized".to_string())
}

/// Ray configuration parameters.
///
/// Each field corresponds to a `RAY_CONFIG(type, name, default)` entry
/// in the C++ `ray_config_def.h`. Only the most commonly used ones are
/// included here; the full set will be populated incrementally.
#[derive(Debug, Clone)]
pub struct RayConfig {
    // ─── Debug / Metrics ──────────────────────────────────────
    pub debug_dump_period_milliseconds: u64,
    pub event_stats: bool,
    pub emit_main_service_metrics: bool,
    pub event_stats_print_interval_ms: i64,

    // ─── Authentication ───────────────────────────────────────
    pub enable_cluster_auth: bool,
    pub auth_mode: String,
    pub enable_k8s_token_auth: bool,

    // ─── Timing / Scheduling ──────────────────────────────────
    pub ray_cookie: i64,
    pub handler_warning_timeout_ms: i64,
    pub gcs_pull_resource_loads_period_milliseconds: u64,
    pub raylet_report_resources_period_milliseconds: u64,
    pub raylet_check_gc_period_milliseconds: u64,

    // ─── Memory ───────────────────────────────────────────────
    pub memory_usage_threshold: f32,
    pub memory_monitor_refresh_ms: u64,
    pub min_memory_free_bytes: i64,
    pub system_memory_bytes_low: i64,
    pub user_memory_proportion_high: f32,

    // ─── Object Store ─────────────────────────────────────────
    pub object_store_memory: i64,
    pub object_spilling_threshold: f64,

    // ─── GCS ──────────────────────────────────────────────────
    pub gcs_server_port: i32,
    pub gcs_max_active_rpcs_per_handler: i64,

    // ─── Raylet ───────────────────────────────────────────────
    pub num_workers_soft_limit: i32,
    pub raylet_heartbeat_period_milliseconds: u64,

    // ─── Worker ───────────────────────────────────────────────
    pub worker_register_timeout_seconds: i64,
    pub task_retry_delay_ms: i64,
}

impl Default for RayConfig {
    fn default() -> Self {
        Self {
            debug_dump_period_milliseconds: 10_000,
            event_stats: true,
            emit_main_service_metrics: true,
            event_stats_print_interval_ms: 60_000,
            enable_cluster_auth: true,
            auth_mode: "disabled".to_string(),
            enable_k8s_token_auth: false,
            ray_cookie: 0x5241590000000000,
            handler_warning_timeout_ms: 1_000,
            gcs_pull_resource_loads_period_milliseconds: 1_000,
            raylet_report_resources_period_milliseconds: 100,
            raylet_check_gc_period_milliseconds: 100,
            memory_usage_threshold: 0.95,
            memory_monitor_refresh_ms: 250,
            min_memory_free_bytes: -1,
            system_memory_bytes_low: 0,
            user_memory_proportion_high: 1.0,
            object_store_memory: -1,
            object_spilling_threshold: 0.8,
            gcs_server_port: 6379,
            gcs_max_active_rpcs_per_handler: -1,
            num_workers_soft_limit: -1,
            raylet_heartbeat_period_milliseconds: 1_000,
            worker_register_timeout_seconds: 60,
            task_retry_delay_ms: 0,
        }
    }
}

impl RayConfig {
    /// Parse from base64-encoded JSON (as sent by Python launcher).
    pub fn from_base64_json(b64: &str) -> Result<Self, String> {
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
            .map_err(|e| format!("base64 decode error: {e}"))?;
        let json_str =
            String::from_utf8(decoded).map_err(|e| format!("UTF-8 decode error: {e}"))?;
        Self::from_json(&json_str)
    }

    /// Parse from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(json).map_err(|e| format!("JSON parse error: {e}"))?;

        let mut config = Self::default();

        // Apply JSON overrides
        macro_rules! set_field {
            ($field:ident, $key:expr, bool) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_bool()) {
                    config.$field = v;
                }
            };
            ($field:ident, $key:expr, u64) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_u64()) {
                    config.$field = v;
                }
            };
            ($field:ident, $key:expr, i64) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_i64()) {
                    config.$field = v;
                }
            };
            ($field:ident, $key:expr, i32) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_i64()) {
                    config.$field = v as i32;
                }
            };
            ($field:ident, $key:expr, f32) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_f64()) {
                    config.$field = v as f32;
                }
            };
            ($field:ident, $key:expr, f64) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_f64()) {
                    config.$field = v;
                }
            };
            ($field:ident, $key:expr, String) => {
                if let Some(v) = map.get($key).and_then(|v| v.as_str()) {
                    config.$field = v.to_string();
                }
            };
        }

        set_field!(
            debug_dump_period_milliseconds,
            "debug_dump_period_milliseconds",
            u64
        );
        set_field!(event_stats, "event_stats", bool);
        set_field!(emit_main_service_metrics, "emit_main_service_metrics", bool);
        set_field!(enable_cluster_auth, "enable_cluster_auth", bool);
        set_field!(auth_mode, "AUTH_MODE", String);
        set_field!(enable_k8s_token_auth, "ENABLE_K8S_TOKEN_AUTH", bool);
        set_field!(ray_cookie, "ray_cookie", i64);
        set_field!(
            handler_warning_timeout_ms,
            "handler_warning_timeout_ms",
            i64
        );
        set_field!(memory_usage_threshold, "memory_usage_threshold", f32);
        set_field!(memory_monitor_refresh_ms, "memory_monitor_refresh_ms", u64);
        set_field!(min_memory_free_bytes, "min_memory_free_bytes", i64);
        set_field!(gcs_server_port, "gcs_server_port", i32);
        set_field!(object_store_memory, "object_store_memory", i64);

        // Apply environment variable overrides (RAY_<NAME>)
        config.apply_env_overrides();

        Ok(config)
    }

    /// Apply environment variable overrides of the form `RAY_<NAME>`.
    fn apply_env_overrides(&mut self) {
        macro_rules! env_override {
            ($field:ident, bool) => {
                let env_key = concat!("RAY_", stringify!($field));
                if let Ok(val) = std::env::var(env_key) {
                    if let Ok(v) = val.parse::<bool>() {
                        self.$field = v;
                    }
                }
            };
            ($field:ident, u64) => {
                let env_key = concat!("RAY_", stringify!($field));
                if let Ok(val) = std::env::var(env_key) {
                    if let Ok(v) = val.parse::<u64>() {
                        self.$field = v;
                    }
                }
            };
            ($field:ident, i64) => {
                let env_key = concat!("RAY_", stringify!($field));
                if let Ok(val) = std::env::var(env_key) {
                    if let Ok(v) = val.parse::<i64>() {
                        self.$field = v;
                    }
                }
            };
            ($field:ident, f32) => {
                let env_key = concat!("RAY_", stringify!($field));
                if let Ok(val) = std::env::var(env_key) {
                    if let Ok(v) = val.parse::<f32>() {
                        self.$field = v;
                    }
                }
            };
            ($field:ident, String) => {
                let env_key = concat!("RAY_", stringify!($field));
                if let Ok(val) = std::env::var(env_key) {
                    self.$field = val;
                }
            };
        }

        env_override!(event_stats, bool);
        env_override!(enable_cluster_auth, bool);
        env_override!(auth_mode, String);
        env_override!(memory_usage_threshold, f32);
        env_override!(memory_monitor_refresh_ms, u64);
        env_override!(min_memory_free_bytes, i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RayConfig::default();
        assert!(config.event_stats);
        assert_eq!(config.memory_usage_threshold, 0.95);
        assert_eq!(config.ray_cookie, 0x5241590000000000);
    }

    #[test]
    fn test_json_parse() {
        let json = r#"{"event_stats": false, "memory_usage_threshold": 0.8}"#;
        let config = RayConfig::from_json(json).unwrap();
        assert!(!config.event_stats);
        assert_eq!(config.memory_usage_threshold, 0.8);
    }

    #[test]
    fn test_base64_json_roundtrip() {
        use base64::Engine;
        let json = r#"{"gcs_server_port": 8080}"#;
        let b64 = base64::engine::general_purpose::STANDARD.encode(json);
        let config = RayConfig::from_base64_json(&b64).unwrap();
        assert_eq!(config.gcs_server_port, 8080);
    }
}
