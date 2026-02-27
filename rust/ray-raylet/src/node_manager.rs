// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Node manager — the central raylet class.

use ray_common::config::RayConfig;

/// Configuration for starting the raylet.
#[derive(Debug, Clone)]
pub struct RayletConfig {
    pub node_ip_address: String,
    pub port: u16,
    pub object_store_socket: String,
    pub gcs_address: String,
    pub log_dir: Option<String>,
    pub ray_config: RayConfig,
}

/// The main raylet node manager.
pub struct NodeManager {
    config: RayletConfig,
}

impl NodeManager {
    pub fn new(config: RayletConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &RayletConfig {
        &self.config
    }

    /// Start the raylet (Phase 8: full implementation).
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            port = self.config.port,
            node_ip = %self.config.node_ip_address,
            "Starting Rust raylet"
        );

        // Phase 8: Initialize worker pool, scheduling, object manager,
        // register with GCS, start gRPC server, handle heartbeats.

        tokio::signal::ctrl_c().await?;
        tracing::info!("Raylet shutting down");
        Ok(())
    }
}
