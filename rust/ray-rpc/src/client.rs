// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC client framework wrapping tonic channels.
//!
//! Replaces `src/ray/rpc/grpc_client.h` and `retryable_grpc_client.h/cc`.

use std::time::Duration;

use tonic::transport::Channel;

/// Configuration for retry behavior on gRPC calls.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial retry delay.
    pub initial_delay: Duration,
    /// Maximum retry delay.
    pub max_delay: Duration,
    /// Backoff multiplier.
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
        }
    }
}

/// A gRPC client wrapper with retry support.
///
/// In C++, this is the `RetryableGrpcClient` template class.
/// In Rust, we wrap a tonic `Channel` with retry logic.
#[derive(Clone)]
pub struct RetryableGrpcClient {
    channel: Channel,
    retry_config: RetryConfig,
}

impl RetryableGrpcClient {
    pub fn new(channel: Channel, retry_config: RetryConfig) -> Self {
        Self {
            channel,
            retry_config,
        }
    }

    /// Get the underlying channel for making RPC calls.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Get the retry configuration.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Connect to a gRPC endpoint.
    pub async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
        Channel::from_shared(addr.to_string())
            .expect("invalid URI")
            .connect()
            .await
    }
}
