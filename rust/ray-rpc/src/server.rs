// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC server framework wrapping tonic.
//!
//! Replaces `src/ray/rpc/grpc_server.h/cc`.

use std::net::SocketAddr;

/// Configuration for a Ray gRPC server.
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Name of this server (for logging).
    pub name: String,
    /// Port to listen on (0 = pick a free port).
    pub port: u16,
    /// Whether to register the server with GCS.
    pub register_service: bool,
    /// Maximum concurrent RPCs per handler.
    pub max_active_rpcs_per_handler: i64,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            name: "RayServer".to_string(),
            port: 0,
            register_service: true,
            max_active_rpcs_per_handler: -1,
        }
    }
}

/// A Ray gRPC server wrapping tonic's Server.
///
/// In C++, this is a complex class with ServerCallFactory templates.
/// In Rust/tonic, services are added directly via the generated service traits,
/// making the server implementation much simpler.
pub struct GrpcServer {
    config: GrpcServerConfig,
    bound_addr: Option<SocketAddr>,
}

impl GrpcServer {
    pub fn new(config: GrpcServerConfig) -> Self {
        Self {
            config,
            bound_addr: None,
        }
    }

    pub fn config(&self) -> &GrpcServerConfig {
        &self.config
    }

    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr
    }

    pub fn port(&self) -> u16 {
        self.bound_addr
            .map(|a| a.port())
            .unwrap_or(self.config.port)
    }
}
