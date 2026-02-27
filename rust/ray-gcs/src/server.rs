// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS server startup and lifecycle.

use ray_common::config::RayConfig;

/// Configuration for starting the GCS server.
#[derive(Debug, Clone)]
pub struct GcsServerConfig {
    pub port: u16,
    pub redis_address: Option<String>,
    pub redis_password: Option<String>,
    pub log_dir: Option<String>,
    pub ray_config: RayConfig,
}

/// The main GCS server struct.
pub struct GcsServer {
    config: GcsServerConfig,
}

impl GcsServer {
    pub fn new(config: GcsServerConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &GcsServerConfig {
        &self.config
    }

    /// Start the GCS server (Phase 7: full implementation).
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(port = self.config.port, "Starting Rust GCS server");

        // Phase 7: Initialize all managers, register gRPC services,
        // start tonic server, handle graceful shutdown.

        // Placeholder: keep the server running
        tokio::signal::ctrl_c().await?;
        tracing::info!("GCS server shutting down");
        Ok(())
    }
}
