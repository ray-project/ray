// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Health Check Manager — monitors node liveness.
//!
//! Replaces `src/ray/gcs/gcs_health_check_manager.h/cc`.
//!
//! Uses gRPC health checks to detect dead nodes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use ray_common::id::NodeID;
use tokio::sync::mpsc;

/// Callback invoked when a node is detected as dead.
pub type NodeDeathCallback = Arc<dyn Fn(NodeID) + Send + Sync>;

/// Configuration for health checking.
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Initial delay before first health check (ms).
    pub initial_delay_ms: u64,
    /// Timeout for each health check RPC (ms).
    pub timeout_ms: u64,
    /// Period between health checks (ms).
    pub period_ms: u64,
    /// Number of consecutive failures before declaring death.
    pub failure_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 5000,
            timeout_ms: 10000,
            period_ms: 5000,
            failure_threshold: 5,
        }
    }
}

/// Per-node health check state.
#[allow(dead_code)]
struct HealthCheckContext {
    /// gRPC address to check.
    address: String,
    /// Remaining failures before death.
    health_check_remaining: u32,
    /// Shutdown signal.
    cancel_tx: mpsc::Sender<()>,
}

/// The GCS health check manager periodically checks node liveness.
pub struct GcsHealthCheckManager {
    /// Health check configuration.
    config: HealthCheckConfig,
    /// Per-node health check contexts.
    contexts: Mutex<HashMap<NodeID, HealthCheckContext>>,
    /// Callback when a node is declared dead.
    on_node_death: NodeDeathCallback,
}

impl GcsHealthCheckManager {
    pub fn new(config: HealthCheckConfig, on_node_death: NodeDeathCallback) -> Arc<Self> {
        Arc::new(Self {
            config,
            contexts: Mutex::new(HashMap::new()),
            on_node_death,
        })
    }

    /// Start monitoring a node.
    pub fn add_node(self: &Arc<Self>, node_id: NodeID, address: String) {
        let (cancel_tx, mut cancel_rx) = mpsc::channel::<()>(1);

        let context = HealthCheckContext {
            address: address.clone(),
            health_check_remaining: self.config.failure_threshold,
            cancel_tx,
        };

        self.contexts.lock().insert(node_id, context);

        // Spawn health check loop
        let this = Arc::clone(self);
        let config = self.config.clone();
        tokio::spawn(async move {
            // Initial delay
            tokio::time::sleep(Duration::from_millis(config.initial_delay_ms)).await;

            loop {
                tokio::select! {
                    _ = cancel_rx.recv() => {
                        // Node removed — stop checking
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(config.period_ms)) => {
                        let alive = this.check_node_health(&address, &config).await;
                        let mut contexts = this.contexts.lock();
                        if let Some(ctx) = contexts.get_mut(&node_id) {
                            if alive {
                                ctx.health_check_remaining = config.failure_threshold;
                            } else {
                                ctx.health_check_remaining =
                                    ctx.health_check_remaining.saturating_sub(1);
                                if ctx.health_check_remaining == 0 {
                                    // Node is dead
                                    contexts.remove(&node_id);
                                    drop(contexts);
                                    tracing::warn!(?node_id, "Node declared dead by health check");
                                    (this.on_node_death)(node_id);
                                    break;
                                }
                            }
                        } else {
                            // Context was removed externally
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Stop monitoring a node.
    pub fn remove_node(&self, node_id: &NodeID) {
        if let Some(ctx) = self.contexts.lock().remove(node_id) {
            let _ = ctx.cancel_tx.try_send(());
        }
    }

    /// Get all monitored nodes.
    pub fn get_all_nodes(&self) -> Vec<NodeID> {
        self.contexts.lock().keys().copied().collect()
    }

    /// Mark a node as healthy (reset failure counter).
    pub fn mark_node_healthy(&self, node_id: &NodeID) {
        if let Some(ctx) = self.contexts.lock().get_mut(node_id) {
            ctx.health_check_remaining = self.config.failure_threshold;
        }
    }

    /// Check health of a single node via gRPC health check.
    async fn check_node_health(&self, address: &str, config: &HealthCheckConfig) -> bool {
        let endpoint = format!("http://{}", address);
        let channel = match tonic::transport::Endpoint::from_shared(endpoint) {
            Ok(ep) => ep.connect_lazy(),
            Err(_) => return false,
        };
        let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
        let result = tokio::time::timeout(
            Duration::from_millis(config.timeout_ms),
            client.check(tonic_health::pb::HealthCheckRequest {
                service: String::new(),
            }),
        )
        .await;
        matches!(result, Ok(Ok(_)))
    }

    pub fn num_monitored_nodes(&self) -> usize {
        self.contexts.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_remove_node() {
        let callback = Arc::new(|_: NodeID| {});
        let mgr = GcsHealthCheckManager::new(HealthCheckConfig::default(), callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 1;
        let nid = NodeID::from_binary(&nid_data);

        mgr.add_node(nid, "localhost:50051".to_string());
        assert_eq!(mgr.num_monitored_nodes(), 1);

        mgr.remove_node(&nid);
        // Give the cancel signal time to propagate
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mgr.num_monitored_nodes(), 0);
    }

    #[tokio::test]
    async fn test_get_all_nodes() {
        let callback = Arc::new(|_: NodeID| {});
        let mgr = GcsHealthCheckManager::new(HealthCheckConfig::default(), callback);

        for i in 1..=3u8 {
            let mut nid_data = [0u8; 28];
            nid_data[0] = i;
            let nid = NodeID::from_binary(&nid_data);
            mgr.add_node(nid, format!("localhost:5005{}", i));
        }

        assert_eq!(mgr.get_all_nodes().len(), 3);
    }
}
