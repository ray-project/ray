// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Raylet binary entry point.
//!
//! This binary replaces the C++ `raylet` when `RAY_USE_RUST_RAYLET=1`.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine;
use clap::Parser;
use ray_common::config::RayConfig;
use ray_raylet::node_manager::{NodeManager, RayletConfig};

#[derive(Parser, Debug)]
#[command(name = "raylet", about = "Ray Raylet (Rust)")]
struct Args {
    // ── Core args (used by Rust raylet) ──────────────────────────────

    /// Node IP address
    #[arg(long, alias = "node_ip_address")]
    node_ip_address: String,

    /// Node manager / raylet port
    #[arg(long, alias = "node_manager_port", default_value_t = 0)]
    port: u16,

    /// Object store socket path
    #[arg(long, alias = "store_socket_name")]
    object_store_socket_name: Option<String>,

    /// GCS address (host:port)
    #[arg(long, alias = "gcs-address")]
    gcs_address: String,

    /// Log directory
    #[arg(long, alias = "log_dir")]
    log_dir: Option<String>,

    /// Base64-encoded Ray config (alias: config_list)
    #[arg(long, alias = "config_list")]
    ray_config: Option<String>,

    /// Node ID (hex string)
    #[arg(long, alias = "node_id", default_value = "")]
    node_id: String,

    /// Resources as comma-separated key:value pairs (e.g. "CPU:4,GPU:2")
    /// Also accepts C++ format "CPU,4,GPU,2" via --static_resource_list.
    #[arg(long, default_value = "")]
    resources: String,

    /// Labels as comma-separated key:value pairs or JSON
    #[arg(long, default_value = "")]
    labels: String,

    /// Session name
    #[arg(long, alias = "session-name", default_value = "")]
    session_name: String,

    /// Python worker command (full shell command line)
    #[arg(long, alias = "python_worker_command")]
    python_worker_command: Option<String>,

    /// Static resource list in C++ format: "CPU,4,GPU,2,memory,8589934592"
    #[arg(long, alias = "static_resource_list")]
    static_resource_list: Option<String>,

    // ── Compatibility args (accepted for C++ parity, not used) ───────

    /// Raylet socket name (compatibility)
    #[arg(long, alias = "raylet_socket_name")]
    raylet_socket_name: Option<String>,

    /// Object manager port (compatibility)
    #[arg(long, alias = "object_manager_port", default_value_t = 0)]
    object_manager_port: u16,

    /// Min worker port (compatibility)
    #[arg(long, alias = "min_worker_port", default_value_t = 0)]
    min_worker_port: u16,

    /// Max worker port (compatibility)
    #[arg(long, alias = "max_worker_port", default_value_t = 0)]
    max_worker_port: u16,

    /// Worker port list (compatibility)
    #[arg(long, alias = "worker_port_list")]
    worker_port_list: Option<String>,

    /// Maximum startup concurrency (compatibility)
    #[arg(long, alias = "maximum_startup_concurrency", default_value_t = 1)]
    maximum_startup_concurrency: u32,

    /// Java worker command (compatibility)
    #[arg(long, alias = "java_worker_command")]
    java_worker_command: Option<String>,

    /// C++ worker command (compatibility)
    #[arg(long, alias = "cpp_worker_command")]
    cpp_worker_command: Option<String>,

    /// Native library path (compatibility)
    #[arg(long, alias = "native_library_path")]
    native_library_path: Option<String>,

    /// Temp directory (compatibility)
    #[arg(long, alias = "temp_dir")]
    temp_dir: Option<String>,

    /// Session directory (compatibility)
    #[arg(long, alias = "session_dir")]
    session_dir: Option<String>,

    /// Resource directory (compatibility)
    #[arg(long, alias = "resource_dir")]
    resource_dir: Option<String>,

    /// Metrics agent port (compatibility)
    #[arg(long, alias = "metrics-agent-port", default_value_t = 0)]
    metrics_agent_port: u16,

    /// Metrics export port (compatibility)
    #[arg(long, alias = "metrics_export_port", default_value_t = 0)]
    metrics_export_port: u16,

    /// Runtime env agent port (compatibility)
    #[arg(long, alias = "runtime_env_agent_port", default_value_t = 0)]
    runtime_env_agent_port: u16,

    /// Object store memory (compatibility)
    #[arg(long, alias = "object_store_memory", default_value_t = 0)]
    object_store_memory: u64,

    /// Plasma directory (compatibility)
    #[arg(long, alias = "plasma_directory")]
    plasma_directory: Option<String>,

    /// Fallback directory (compatibility)
    #[arg(long, alias = "fallback_directory")]
    fallback_directory: Option<String>,

    /// Ray debugger external (compatibility)
    #[arg(long, alias = "ray-debugger-external")]
    ray_debugger_external: Option<String>,

    /// Cluster ID (compatibility)
    #[arg(long, alias = "cluster-id")]
    cluster_id: Option<String>,

    /// Head node flag (compatibility)
    #[arg(long)]
    head: bool,

    /// Number of prestart Python workers (compatibility)
    #[arg(long, alias = "num_prestart_python_workers", default_value_t = 0)]
    num_prestart_python_workers: u32,

    /// Dashboard agent command (compatibility)
    #[arg(long, alias = "dashboard_agent_command")]
    dashboard_agent_command: Option<String>,

    /// Runtime env agent command (compatibility)
    #[arg(long, alias = "runtime_env_agent_command")]
    runtime_env_agent_command: Option<String>,

    /// Huge pages (compatibility)
    #[arg(long, alias = "huge_pages")]
    huge_pages: bool,

    /// Stdout filepath (compatibility)
    #[arg(long, alias = "stdout_filepath")]
    stdout_filepath: Option<String>,

    /// Stderr filepath (compatibility)
    #[arg(long, alias = "stderr_filepath")]
    stderr_filepath: Option<String>,

    /// Node name (compatibility)
    #[arg(long, alias = "node-name")]
    node_name: Option<String>,
}

/// Parse "CPU:4,GPU:2" format (colon-separated key:value pairs).
fn parse_kv_pairs(s: &str) -> HashMap<String, f64> {
    if s.is_empty() {
        return HashMap::new();
    }
    s.split(',')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, ':');
            let key = parts.next()?.trim().to_string();
            let value: f64 = parts.next()?.trim().parse().ok()?;
            Some((key, value))
        })
        .collect()
}

/// Parse C++ format "CPU,4,GPU,2" (alternating key,value).
fn parse_static_resource_list(s: &str) -> HashMap<String, f64> {
    if s.is_empty() {
        return HashMap::new();
    }
    let parts: Vec<&str> = s.split(',').collect();
    let mut map = HashMap::new();
    for chunk in parts.chunks(2) {
        if chunk.len() == 2 {
            if let Ok(val) = chunk[1].trim().parse::<f64>() {
                map.insert(chunk[0].trim().to_string(), val);
            }
        }
    }
    map
}

/// Parse labels: try JSON first, then "k:v,k:v" format.
fn parse_labels(s: &str) -> HashMap<String, String> {
    if s.is_empty() {
        return HashMap::new();
    }
    // Try JSON format first (what `ray start` passes)
    if s.starts_with('{') {
        if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(s) {
            return map;
        }
    }
    // Fall back to "k:v,k:v" format
    s.split(',')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, ':');
            let key = parts.next()?.trim().to_string();
            let value = parts.next()?.trim().to_string();
            Some((key, value))
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // Suppress unused variable warnings for compatibility flags.
    let _ = (
        args.object_manager_port,
        args.min_worker_port,
        args.max_worker_port,
        &args.worker_port_list,
        args.maximum_startup_concurrency,
        &args.java_worker_command,
        &args.cpp_worker_command,
        &args.native_library_path,
        &args.temp_dir,
        &args.session_dir,
        &args.resource_dir,
        args.metrics_agent_port,
        args.metrics_export_port,
        args.runtime_env_agent_port,
        args.object_store_memory,
        &args.plasma_directory,
        &args.fallback_directory,
        &args.ray_debugger_external,
        &args.cluster_id,
        args.head,
        args.num_prestart_python_workers,
        &args.dashboard_agent_command,
        &args.runtime_env_agent_command,
        args.huge_pages,
        &args.stdout_filepath,
        &args.stderr_filepath,
        &args.raylet_socket_name,
        &args.node_name,
    );

    ray_util::logging::init_ray_logging(
        "raylet",
        args.log_dir.as_ref().map(std::path::Path::new),
        0,
    );

    let ray_config = match &args.ray_config {
        Some(b64) => RayConfig::from_base64_json(b64).unwrap_or_default(),
        None => RayConfig::default(),
    };

    // Decode the raw config_list JSON for GetSystemConfig RPC.
    // We must return the original JSON (not our serialized RayConfig) because
    // the C++ CoreWorker fatally rejects unrecognized config keys.
    let raw_config_json = args.ray_config.as_ref().map(|b64| {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap_or_default();
        String::from_utf8(bytes).unwrap_or_default()
    }).unwrap_or_default();

    // Resources: prefer --static_resource_list (C++ format from ray start),
    // fall back to --resources (Rust colon format).
    let resources = if let Some(ref srl) = args.static_resource_list {
        parse_static_resource_list(srl)
    } else {
        parse_kv_pairs(&args.resources)
    };

    let labels = parse_labels(&args.labels);

    // Ensure GCS address has http:// scheme (ray start passes bare host:port).
    let gcs_address = if args.gcs_address.starts_with("http://") || args.gcs_address.starts_with("https://") {
        args.gcs_address
    } else {
        format!("http://{}", args.gcs_address)
    };

    let config = RayletConfig {
        node_ip_address: args.node_ip_address,
        port: args.port,
        object_store_socket: args.object_store_socket_name.unwrap_or_default(),
        gcs_address,
        log_dir: args.log_dir,
        ray_config,
        node_id: args.node_id,
        resources,
        labels,
        session_name: args.session_name,
        auth_token: None,
        python_worker_command: args.python_worker_command,
        raw_config_json,
    };

    let node_manager = Arc::new(NodeManager::new(config));
    node_manager.run().await
}
