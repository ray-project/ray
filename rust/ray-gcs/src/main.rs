// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS server binary entry point.
//!
//! This binary replaces the C++ `gcs_server` when `RAY_USE_RUST_GCS=1`.

use base64::Engine;
use clap::Parser;
use ray_common::config::RayConfig;
use ray_gcs::server::{GcsServer, GcsServerConfig};

#[derive(Parser, Debug)]
#[command(name = "gcs_server", about = "Ray Global Control Service (Rust)")]
struct Args {
    /// GCS server port
    #[arg(long, alias = "gcs_server_port", default_value_t = 6379)]
    gcs_server_port: u16,

    /// Redis address (e.g., redis://host:port)
    #[arg(long, alias = "redis_address")]
    redis_address: Option<String>,

    /// Redis password
    #[arg(long, alias = "redis_password")]
    redis_password: Option<String>,

    /// Redis username
    #[arg(long, alias = "redis_username")]
    redis_username: Option<String>,

    /// Enable Redis SSL
    #[arg(long, alias = "redis_enable_ssl", default_value_t = false)]
    redis_enable_ssl: bool,

    /// Log directory
    #[arg(long, alias = "log_dir")]
    log_dir: Option<String>,

    /// Base64-encoded Ray config
    #[arg(long, alias = "config_list")]
    config_list: Option<String>,

    /// Node IP address
    #[arg(long, alias = "node_ip_address")]
    node_ip_address: Option<String>,

    /// Node ID
    #[arg(long, alias = "node_id")]
    node_id: Option<String>,

    /// Session name
    #[arg(long, alias = "session_name")]
    session_name: Option<String>,

    /// Session directory (for port persistence)
    #[arg(long, alias = "session_dir")]
    session_dir: Option<String>,

    /// Metrics agent port (accepted for compatibility, not used)
    #[arg(long, alias = "metrics_agent_port", default_value_t = 0)]
    metrics_agent_port: u16,

    /// Ray commit hash (accepted for compatibility, not used)
    #[arg(long, alias = "ray_commit", default_value = "")]
    ray_commit: String,

    /// Stdout filepath (accepted for compatibility, not used)
    #[arg(long, alias = "stdout_filepath")]
    stdout_filepath: Option<String>,

    /// Stderr filepath (accepted for compatibility, not used)
    #[arg(long, alias = "stderr_filepath")]
    stderr_filepath: Option<String>,

    /// Redis port (accepted for compatibility, not used)
    #[arg(long, alias = "redis_port")]
    redis_port: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Suppress unused variable warnings for compatibility flags
    let _ = (
        args.metrics_agent_port,
        &args.ray_commit,
        &args.stdout_filepath,
        &args.stderr_filepath,
        args.redis_port,
    );

    // Initialize logging
    ray_util::logging::init_ray_logging(
        "gcs_server",
        args.log_dir.as_ref().map(std::path::Path::new),
        0,
    );

    // Initialize config
    let ray_config = match &args.config_list {
        Some(b64) => RayConfig::from_base64_json(b64).unwrap_or_default(),
        None => RayConfig::default(),
    };

    // Decode base64-encoded config_list (matches C++ gcs_server_main.cc behavior)
    let decoded_config_list = args.config_list.as_ref().map(|b64| {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .expect("config_list is not a valid base64-encoded string");
        String::from_utf8(bytes).expect("config_list is not valid UTF-8")
    });

    let config = GcsServerConfig {
        port: args.gcs_server_port,
        redis_address: args.redis_address,
        redis_password: args.redis_password,
        redis_username: args.redis_username,
        enable_redis_ssl: args.redis_enable_ssl,
        node_ip_address: args.node_ip_address,
        node_id: args.node_id,
        log_dir: args.log_dir,
        session_name: args.session_name,
        session_dir: args.session_dir,
        raylet_config_list: decoded_config_list,
        ray_config,
    };

    let mut server = GcsServer::new(config);
    server.run().await
}
