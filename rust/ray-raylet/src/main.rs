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

    let config = RayletConfig {
        node_ip_address: args.node_ip_address,
        port: args.port,
        object_store_socket: args.object_store_socket_name,
        gcs_address: args.gcs_address,
        log_dir: args.log_dir,
        ray_config,
    };

    let node_manager = NodeManager::new(config);
    node_manager.run().await
}
