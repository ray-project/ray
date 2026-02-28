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

use clap::Parser;
use ray_common::config::RayConfig;
use ray_raylet::node_manager::{NodeManager, RayletConfig};

#[derive(Parser, Debug)]
#[command(name = "raylet", about = "Ray Raylet (Rust)")]
struct Args {
    /// Node IP address
    #[arg(long)]
    node_ip_address: String,

    /// Raylet port
    #[arg(long, default_value_t = 0)]
    port: u16,

    /// Object store socket path
    #[arg(long)]
    object_store_socket_name: String,

    /// GCS address (host:port)
    #[arg(long)]
    gcs_address: String,

    /// Log directory
    #[arg(long)]
    log_dir: Option<String>,

    /// Base64-encoded Ray config
    #[arg(long)]
    ray_config: Option<String>,

    /// Node ID (hex string)
    #[arg(long, default_value = "")]
    node_id: String,

    /// Resources as comma-separated key:value pairs (e.g. "CPU:4,GPU:2")
    #[arg(long, default_value = "")]
    resources: String,

    /// Labels as comma-separated key:value pairs
    #[arg(long, default_value = "")]
    labels: String,

    /// Session name
    #[arg(long, default_value = "")]
    session_name: String,
}

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

fn parse_label_pairs(s: &str) -> HashMap<String, String> {
    if s.is_empty() {
        return HashMap::new();
    }
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    ray_util::logging::init_ray_logging(
        "raylet",
        args.log_dir.as_ref().map(std::path::Path::new),
        0,
    );

    let ray_config = match &args.ray_config {
        Some(b64) => RayConfig::from_base64_json(b64).unwrap_or_default(),
        None => RayConfig::default(),
    };

    let resources = parse_kv_pairs(&args.resources);
    let labels = parse_label_pairs(&args.labels);

    let config = RayletConfig {
        node_ip_address: args.node_ip_address,
        port: args.port,
        object_store_socket: args.object_store_socket_name,
        gcs_address: args.gcs_address,
        log_dir: args.log_dir,
        ray_config,
        node_id: args.node_id,
        resources,
        labels,
        session_name: args.session_name,
    };

    let node_manager = Arc::new(NodeManager::new(config));
    node_manager.run().await
}
