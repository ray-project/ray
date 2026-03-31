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

    // ── Worker spawning args (used at runtime) ────────────────────────

    /// Min worker port. 0 = unconstrained.
    #[arg(long, alias = "min_worker_port", default_value_t = 0)]
    min_worker_port: u16,

    /// Max worker port. 0 = unconstrained.
    #[arg(long, alias = "max_worker_port", default_value_t = 0)]
    max_worker_port: u16,

    /// Explicit worker port list (comma-separated). Overrides min/max.
    #[arg(long, alias = "worker_port_list")]
    worker_port_list: Option<String>,

    /// Maximum number of workers starting concurrently.
    #[arg(long, alias = "maximum_startup_concurrency", default_value_t = 1)]
    maximum_startup_concurrency: u32,

    /// Number of Python workers to pre-start at raylet launch.
    #[arg(long, alias = "num_prestart_python_workers", default_value_t = 0)]
    num_prestart_python_workers: u32,

    // ── Agent subprocess management (used at runtime) ────────────────

    /// Command to launch the dashboard agent subprocess. Validated and
    /// launched by AgentManager with monitoring/respawn.
    #[arg(long, alias = "dashboard_agent_command")]
    dashboard_agent_command: Option<String>,

    /// Command to launch the runtime env agent subprocess. Validated and
    /// launched by AgentManager with monitoring/respawn.
    #[arg(long, alias = "runtime_env_agent_command")]
    runtime_env_agent_command: Option<String>,

    // ── Agent port resolution (used at runtime) ──────────────────────
    // CLI value > 0 is used directly; if 0, resolved from session_dir
    // port files (matching C++ WaitForDashboardAgentPorts pattern).

    /// Metrics agent port. Used to create MetricsAgentClient with
    /// readiness-gating before exporter initialization.
    /// PARITY STATUS: INTENTIONALLY DIFFERENT — sentinel value.
    /// C++ uses -1 (int32 gflags); Rust uses 0 (u16 clap).
    /// Both represent "not configured". `ray start` always provides
    /// explicit values, making this a binary-test-only divergence.
    /// This divergence is accepted and excluded from parity claims.
    #[arg(long, alias = "metrics-agent-port", default_value_t = 0)]
    metrics_agent_port: u16,

    /// Metrics export port. Published in GcsNodeInfo and used to start
    /// the Prometheus HTTP metrics endpoint.
    /// C++ default is 1 (meaning "will be resolved later"); 0 means "disabled".
    #[arg(long, alias = "metrics_export_port", default_value_t = 1)]
    metrics_export_port: u16,

    /// Runtime env agent port. Used to create RuntimeEnvAgentClient
    /// which is installed into the WorkerPool for env lifecycle ops.
    #[arg(long, alias = "runtime_env_agent_port", default_value_t = 0)]
    runtime_env_agent_port: u16,

    /// Dashboard agent listen port.
    #[arg(long, alias = "dashboard_agent_listen_port", default_value_t = 0)]
    dashboard_agent_listen_port: u16,

    // ── Object store configuration (used at runtime) ─────────────────
    // These values are passed to PlasmaAllocator -> PlasmaStore ->
    // ObjectManager during NodeManager construction.

    /// Object store memory limit in bytes. When > 0, constructs the
    /// local PlasmaAllocator/PlasmaStore/ObjectManager.
    /// PARITY STATUS: INTENTIONALLY DIFFERENT — sentinel value.
    /// C++ uses -1 (int64 gflags); Rust uses 0 (u64 clap).
    /// Both represent "not configured". `ray start` always provides
    /// explicit values, making this a binary-test-only divergence.
    /// This divergence is accepted and excluded from parity claims.
    #[arg(long, alias = "object_store_memory", default_value_t = 0)]
    object_store_memory: u64,

    /// Primary mmap directory for the object store (e.g. /dev/shm).
    #[arg(long, alias = "plasma_directory")]
    plasma_directory: Option<String>,

    /// Fallback mmap directory (disk overflow).
    #[arg(long, alias = "fallback_directory")]
    fallback_directory: Option<String>,

    /// Whether to use huge pages for the object store.
    #[arg(long, alias = "huge_pages")]
    huge_pages: bool,

    // ── Session and node metadata (used at runtime) ──────────────────

    /// Session directory. Used for port-file rendezvous when agent
    /// ports are not provided via CLI (reads <session_dir>/ports/<name>).
    #[arg(long, alias = "session_dir")]
    session_dir: Option<String>,

    /// Temp directory for Ray session. Published in GcsNodeInfo.
    #[arg(long, alias = "temp_dir")]
    temp_dir: Option<String>,

    /// Whether this node is the head node.
    #[arg(long)]
    head: bool,

    /// Human-readable node name. Published in GcsNodeInfo.
    #[arg(long, alias = "node-name")]
    node_name: Option<String>,

    /// Object manager port. Published in GcsNodeInfo for cluster
    /// discovery. Note: Rust raylet serves object manager RPCs on the
    /// main gRPC port; C++ uses a separate server on this port.
    /// PARITY STATUS: INTENTIONALLY DIFFERENT — sentinel value.
    /// C++ uses -1 (int32 gflags); Rust uses 0 (u16 clap).
    /// Both represent "not configured". `ray start` always provides
    /// explicit values, making this a binary-test-only divergence.
    /// This divergence is accepted and excluded from parity claims.
    #[arg(long, alias = "object_manager_port", default_value_t = 0)]
    object_manager_port: u16,

    /// Raylet socket name. Published in GcsNodeInfo.
    #[arg(long, alias = "raylet_socket_name")]
    raylet_socket_name: Option<String>,

    // ── Accepted but not used (C++ CLI compatibility only) ───────────
    // These are accepted so `ray start` can pass them without error,
    // but the Rust raylet does not act on them.

    /// Java worker command (not used — Rust raylet only spawns Python workers)
    #[arg(long, alias = "java_worker_command")]
    java_worker_command: Option<String>,

    /// C++ worker command (not used — Rust raylet only spawns Python workers)
    #[arg(long, alias = "cpp_worker_command")]
    cpp_worker_command: Option<String>,

    /// Native library path (not used)
    #[arg(long, alias = "native_library_path")]
    native_library_path: Option<String>,

    /// Resource directory (not used)
    #[arg(long, alias = "resource_dir")]
    resource_dir: Option<String>,

    /// Ray debugger external (not used)
    #[arg(long, alias = "ray-debugger-external")]
    ray_debugger_external: Option<String>,

    /// Cluster ID (not used)
    #[arg(long, alias = "cluster-id")]
    cluster_id: Option<String>,

    /// Stdout filepath (not used — logging configured separately)
    #[arg(long, alias = "stdout_filepath")]
    stdout_filepath: Option<String>,

    /// Stderr filepath (not used — logging configured separately)
    #[arg(long, alias = "stderr_filepath")]
    stderr_filepath: Option<String>,
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

    // Accept but do not use: language-specific worker commands and debugger
    // (Rust raylet spawns Python workers via python_worker_command).
    let _ = (
        &args.java_worker_command,
        &args.cpp_worker_command,
        &args.native_library_path,
        &args.resource_dir,
        &args.ray_debugger_external,
        &args.cluster_id,
        &args.stdout_filepath,
        &args.stderr_filepath,
        &args.raylet_socket_name,
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

    // Load auth token following C++ AuthenticationTokenLoader precedence:
    // 1. RAY_AUTH_TOKEN env var
    // 2. RAY_AUTH_TOKEN_PATH env var -> read file
    // 3. Default path: ~/.ray/auth_token
    let auth_token = std::env::var("RAY_AUTH_TOKEN")
        .ok()
        .or_else(|| {
            std::env::var("RAY_AUTH_TOKEN_PATH")
                .ok()
                .and_then(|path| std::fs::read_to_string(&path).ok())
                .map(|s| s.trim().to_string())
        })
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| std::path::PathBuf::from(h).join(".ray").join("auth_token"))
                .and_then(|p| std::fs::read_to_string(&p).ok())
                .map(|s| s.trim().to_string())
        })
        .filter(|t| !t.is_empty());

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
        auth_token,
        python_worker_command: args.python_worker_command,
        raw_config_json,
        object_manager_port: args.object_manager_port,
        min_worker_port: args.min_worker_port,
        max_worker_port: args.max_worker_port,
        worker_port_list: args.worker_port_list,
        maximum_startup_concurrency: args.maximum_startup_concurrency,
        metrics_agent_port: args.metrics_agent_port,
        metrics_export_port: args.metrics_export_port,
        runtime_env_agent_port: args.runtime_env_agent_port,
        object_store_memory: args.object_store_memory,
        plasma_directory: args.plasma_directory,
        fallback_directory: args.fallback_directory,
        huge_pages: args.huge_pages,
        dashboard_agent_listen_port: args.dashboard_agent_listen_port,
        head: args.head,
        num_prestart_python_workers: args.num_prestart_python_workers,
        dashboard_agent_command: args.dashboard_agent_command,
        runtime_env_agent_command: args.runtime_env_agent_command,
        temp_dir: args.temp_dir,
        session_dir: args.session_dir,
        node_name: args.node_name,
    };

    let node_manager = Arc::new(NodeManager::new(config));
    node_manager.run().await
}
