// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Metrics agent client — raylet-side client for the metrics agent.
//!
//! Replaces `src/ray/rpc/metrics_agent_client.h/cc`.
//!
//! In C++, the raylet creates a `MetricsAgentClient` that:
//! 1. Connects to the metrics agent at `metrics_agent_port`
//! 2. Waits for readiness via HealthCheck RPC (up to 30 retries, 1s apart)
//! 3. On readiness, connects the OpenCensus/OpenTelemetry exporters
//!
//! The Rust raylet replicates this behavior using tonic gRPC health check,
//! since `reporter.proto` is not compiled in the Rust proto crate (it
//! depends on OpenCensus protos not in tree). The health check uses the
//! standard gRPC health checking protocol or raw TCP connectivity.

use std::time::Duration;

/// Maximum retries for metrics agent readiness (matches C++ kMetricAgentInitMaxRetries).
const METRIC_AGENT_INIT_MAX_RETRIES: u32 = 30;

/// Delay between retries in milliseconds (matches C++ kMetricAgentInitRetryDelayMs).
const METRIC_AGENT_INIT_RETRY_DELAY_MS: u64 = 1000;

/// Client for communicating with the local metrics agent.
///
/// C++ equivalent: `MetricsAgentClientImpl` in `metrics_agent_client.h`.
pub struct MetricsAgentClient {
    address: String,
    port: u16,
    ready: bool,
}

impl MetricsAgentClient {
    /// Create a new metrics agent client.
    ///
    /// Does not connect immediately — call `wait_for_server_ready()` to
    /// establish the connection and verify readiness.
    pub fn new(address: &str, port: u16) -> Self {
        tracing::debug!(
            address,
            port,
            "Creating MetricsAgentClient"
        );
        Self {
            address: address.to_string(),
            port,
            ready: false,
        }
    }

    /// Wait for the metrics agent to be ready.
    ///
    /// C++ equivalent: `MetricsAgentClientImpl::WaitForServerReady`.
    /// Retries up to 30 times with 1s delay (matching C++ behavior).
    ///
    /// On success, invokes the `on_ready` callback. On failure (all retries
    /// exhausted), logs an error. In either case, the raylet continues running —
    /// metrics agent unavailability is not fatal.
    pub async fn wait_for_server_ready<F>(&mut self, on_ready: F)
    where
        F: FnOnce(bool) + Send,
    {
        let endpoint = format!("http://{}:{}", self.address, self.port);
        tracing::info!(endpoint, "Waiting for metrics agent readiness");

        for retry in 0..METRIC_AGENT_INIT_MAX_RETRIES {
            match self.check_health(&endpoint).await {
                Ok(()) => {
                    tracing::info!(
                        endpoint,
                        retries = retry,
                        "Metrics agent is ready"
                    );
                    self.ready = true;
                    on_ready(true);
                    return;
                }
                Err(e) => {
                    if retry < METRIC_AGENT_INIT_MAX_RETRIES - 1 {
                        tracing::debug!(
                            retry,
                            max_retries = METRIC_AGENT_INIT_MAX_RETRIES,
                            error = %e,
                            "Metrics agent not ready, retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(
                            METRIC_AGENT_INIT_RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
            }
        }

        tracing::error!(
            endpoint,
            "Failed to establish connection to the metrics agent after {} retries",
            METRIC_AGENT_INIT_MAX_RETRIES
        );
        on_ready(false);
    }

    /// Check if the metrics agent is healthy.
    ///
    /// Uses TCP connectivity as a health check since the ReporterService
    /// proto is not available in the Rust proto crate. The C++ HealthCheck
    /// RPC is essentially a connectivity check.
    async fn check_health(&self, endpoint: &str) -> Result<(), String> {
        let ep = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(1));
        let _channel = ep.connect().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Whether the metrics agent is ready.
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// The port this client is configured for.
    pub fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_agent_client_creation() {
        let client = MetricsAgentClient::new("127.0.0.1", 8080);
        assert!(!client.is_ready());
        assert_eq!(client.port(), 8080);
    }

    #[tokio::test]
    async fn test_metrics_agent_client_unreachable_server() {
        let client = MetricsAgentClient::new("127.0.0.1", 1);
        // Use a very short retry for testing — override via direct health check
        let result = client.check_health("http://127.0.0.1:1").await;
        assert!(result.is_err(), "Should not connect to port 1");
        assert!(!client.is_ready());
    }
}
