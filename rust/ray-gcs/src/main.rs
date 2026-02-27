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

use clap::Parser;
use ray_common::config::RayConfig;
use ray_gcs::server::{GcsServer, GcsServerConfig};

#[derive(Parser, Debug)]
#[command(name = "gcs_server", about = "Ray Global Control Service (Rust)")]
struct Args {
    /// GCS server port
    #[arg(long, default_value_t = 6379)]
    gcs_server_port: u16,

    /// Redis address (host:port)
    #[arg(long)]
    redis_address: Option<String>,

    /// Redis password
    #[arg(long)]
    redis_password: Option<String>,

    /// Log directory
    #[arg(long)]
    log_dir: Option<String>,

    /// Base64-encoded Ray config
    #[arg(long)]
    ray_config: Option<String>,

    /// Node IP address
    #[arg(long)]
    node_ip_address: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    ray_util::logging::init_ray_logging(
        "gcs_server",
        args.log_dir.as_ref().map(std::path::Path::new),
        0,
    );

    // Initialize config
    let ray_config = match &args.ray_config {
        Some(b64) => RayConfig::from_base64_json(b64).unwrap_or_default(),
        None => RayConfig::default(),
    };

    let config = GcsServerConfig {
        port: args.gcs_server_port,
        redis_address: args.redis_address,
        redis_password: args.redis_password,
        log_dir: args.log_dir,
        ray_config,
    };

    let server = GcsServer::new(config);
    server.run().await
}
