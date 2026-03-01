use crate::golden::{self, GoldenFile};
use ray_common::config::RayConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ConfigInput {
    key: String,
}

/// Expected value — exactly one of these will be set per case.
#[derive(Debug, Deserialize)]
struct ConfigExpected {
    #[serde(default)]
    value_i64: Option<i64>,
    #[serde(default)]
    value_u64: Option<u64>,
    #[serde(default)]
    value_i32: Option<i32>,
    #[serde(default)]
    value_f32: Option<f32>,
    #[serde(default)]
    value_f64: Option<f64>,
    #[serde(default)]
    value_bool: Option<bool>,
}

#[test]
fn test_config_default_values_conformance() {
    let golden: GoldenFile<ConfigInput, ConfigExpected> =
        golden::load_golden("config/default_values.json");

    let config = RayConfig::default();

    for case in &golden.cases {
        let key = &case.input.key;

        if let Some(expected) = case.expected.value_i64 {
            let actual = get_i64_field(&config, key);
            assert_eq!(
                actual, expected,
                "FAIL [{}]: config.{} = {}, expected {}",
                case.description, key, actual, expected
            );
        } else if let Some(expected) = case.expected.value_u64 {
            let actual = get_u64_field(&config, key);
            assert_eq!(
                actual, expected,
                "FAIL [{}]: config.{} = {}, expected {}",
                case.description, key, actual, expected
            );
        } else if let Some(expected) = case.expected.value_i32 {
            let actual = get_i32_field(&config, key);
            assert_eq!(
                actual, expected,
                "FAIL [{}]: config.{} = {}, expected {}",
                case.description, key, actual, expected
            );
        } else if let Some(expected) = case.expected.value_f32 {
            let actual = get_f32_field(&config, key);
            assert!(
                (actual - expected).abs() < 1e-6,
                "FAIL [{}]: config.{} = {}, expected {}",
                case.description, key, actual, expected
            );
        } else if let Some(expected) = case.expected.value_f64 {
            let actual = get_f64_field(&config, key);
            assert!(
                (actual - expected).abs() < 1e-10,
                "FAIL [{}]: config.{} = {}, expected {}",
                case.description, key, actual, expected
            );
        } else if let Some(expected) = case.expected.value_bool {
            let actual = get_bool_field(&config, key);
            assert_eq!(
                actual, expected,
                "FAIL [{}]: config.{} = {}, expected {}",
                case.description, key, actual, expected
            );
        } else {
            panic!("FAIL [{}]: no expected value set", case.description);
        }
    }
}

fn get_i64_field(config: &RayConfig, key: &str) -> i64 {
    match key {
        "ray_cookie" => config.ray_cookie,
        "worker_register_timeout_seconds" => config.worker_register_timeout_seconds,
        "task_retry_delay_ms" => config.task_retry_delay_ms,
        "handler_warning_timeout_ms" => config.handler_warning_timeout_ms,
        "event_stats_print_interval_ms" => config.event_stats_print_interval_ms,
        "min_memory_free_bytes" => config.min_memory_free_bytes,
        "object_store_memory" => config.object_store_memory,
        "gcs_max_active_rpcs_per_handler" => config.gcs_max_active_rpcs_per_handler,
        _ => panic!("unknown i64 config key: {key}"),
    }
}

fn get_u64_field(config: &RayConfig, key: &str) -> u64 {
    match key {
        "debug_dump_period_milliseconds" => config.debug_dump_period_milliseconds,
        "raylet_heartbeat_period_milliseconds" => config.raylet_heartbeat_period_milliseconds,
        "memory_monitor_refresh_ms" => config.memory_monitor_refresh_ms,
        "gcs_pull_resource_loads_period_milliseconds" => {
            config.gcs_pull_resource_loads_period_milliseconds
        }
        "raylet_report_resources_period_milliseconds" => {
            config.raylet_report_resources_period_milliseconds
        }
        "raylet_check_gc_period_milliseconds" => config.raylet_check_gc_period_milliseconds,
        _ => panic!("unknown u64 config key: {key}"),
    }
}

fn get_i32_field(config: &RayConfig, key: &str) -> i32 {
    match key {
        "gcs_server_port" => config.gcs_server_port,
        "num_workers_soft_limit" => config.num_workers_soft_limit,
        _ => panic!("unknown i32 config key: {key}"),
    }
}

fn get_f32_field(config: &RayConfig, key: &str) -> f32 {
    match key {
        "memory_usage_threshold" => config.memory_usage_threshold,
        "user_memory_proportion_high" => config.user_memory_proportion_high,
        _ => panic!("unknown f32 config key: {key}"),
    }
}

fn get_f64_field(config: &RayConfig, key: &str) -> f64 {
    match key {
        "object_spilling_threshold" => config.object_spilling_threshold,
        _ => panic!("unknown f64 config key: {key}"),
    }
}

fn get_bool_field(config: &RayConfig, key: &str) -> bool {
    match key {
        "event_stats" => config.event_stats,
        "enable_cluster_auth" => config.enable_cluster_auth,
        "emit_main_service_metrics" => config.emit_main_service_metrics,
        "enable_k8s_token_auth" => config.enable_k8s_token_auth,
        _ => panic!("unknown bool config key: {key}"),
    }
}
