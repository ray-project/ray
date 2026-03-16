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

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
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
    /// Per-node health check contexts — DashMap for per-node locking.
    contexts: DashMap<NodeID, HealthCheckContext>,
    /// Callback when a node is declared dead.
    on_node_death: NodeDeathCallback,
}

impl GcsHealthCheckManager {
    pub fn new(config: HealthCheckConfig, on_node_death: NodeDeathCallback) -> Arc<Self> {
        Arc::new(Self {
            config,
            contexts: DashMap::new(),
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

        self.contexts.insert(node_id, context);

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
                        if let Some(mut ctx) = this.contexts.get_mut(&node_id) {
                            if alive {
                                ctx.health_check_remaining = config.failure_threshold;
                            } else {
                                ctx.health_check_remaining =
                                    ctx.health_check_remaining.saturating_sub(1);
                                if ctx.health_check_remaining == 0 {
                                    // Node is dead — drop the ref before removing
                                    drop(ctx);
                                    this.contexts.remove(&node_id);
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
        if let Some((_, ctx)) = self.contexts.remove(node_id) {
            let _ = ctx.cancel_tx.try_send(());
        }
    }

    /// Get all monitored nodes.
    pub fn get_all_nodes(&self) -> Vec<NodeID> {
        self.contexts.iter().map(|entry| *entry.key()).collect()
    }

    /// Mark a node as healthy (reset failure counter).
    pub fn mark_node_healthy(&self, node_id: &NodeID) {
        if let Some(mut ctx) = self.contexts.get_mut(node_id) {
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
        self.contexts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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

    #[tokio::test]
    async fn test_mark_node_healthy() {
        let callback = Arc::new(|_: NodeID| {});
        let mgr = GcsHealthCheckManager::new(HealthCheckConfig::default(), callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 1;
        let nid = NodeID::from_binary(&nid_data);

        mgr.add_node(nid, "localhost:50051".to_string());
        // Should not panic
        mgr.mark_node_healthy(&nid);
        assert_eq!(mgr.num_monitored_nodes(), 1);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_node() {
        let callback = Arc::new(|_: NodeID| {});
        let mgr = GcsHealthCheckManager::new(HealthCheckConfig::default(), callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 99;
        let nid = NodeID::from_binary(&nid_data);

        // Should not panic
        mgr.remove_node(&nid);
        assert_eq!(mgr.num_monitored_nodes(), 0);
    }

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.initial_delay_ms, 5000);
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.period_ms, 5000);
        assert_eq!(config.failure_threshold, 5);
    }

    /// Test that with failure_threshold=1 and a very short period, the node death
    /// callback fires quickly after a single health check failure (no real gRPC
    /// server is running, so every check will fail).
    #[tokio::test]
    async fn test_rapid_failure_detection_threshold_one() {
        let (death_tx, mut death_rx) = tokio::sync::mpsc::channel::<NodeID>(4);
        let callback: NodeDeathCallback = Arc::new(move |node_id: NodeID| {
            let _ = death_tx.try_send(node_id);
        });

        let config = HealthCheckConfig {
            initial_delay_ms: 0,  // no initial delay
            timeout_ms: 50,       // very short timeout
            period_ms: 10,        // very short period
            failure_threshold: 1, // die after 1 failure
        };
        let mgr = GcsHealthCheckManager::new(config, callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 42;
        let nid = NodeID::from_binary(&nid_data);

        // Point health check at a port where nothing is listening
        mgr.add_node(nid, "127.0.0.1:19999".to_string());

        // The death callback should fire within a reasonable time
        let result = tokio::time::timeout(Duration::from_secs(5), death_rx.recv()).await;
        assert!(result.is_ok(), "Death callback should have fired");
        let dead_node = result.unwrap().unwrap();
        assert_eq!(dead_node, nid);

        // After death, the node should be removed from monitored set
        assert_eq!(mgr.num_monitored_nodes(), 0);
    }

    /// Test that when the health check RPC times out, it counts as a failure.
    /// With timeout_ms very short and no real server, the check should time out.
    #[tokio::test]
    async fn test_health_check_timeout_counts_as_failure() {
        let (death_tx, mut death_rx) = tokio::sync::mpsc::channel::<NodeID>(4);
        let callback: NodeDeathCallback = Arc::new(move |node_id: NodeID| {
            let _ = death_tx.try_send(node_id);
        });

        let config = HealthCheckConfig {
            initial_delay_ms: 0,
            timeout_ms: 10, // extremely short timeout
            period_ms: 10,
            failure_threshold: 2, // die after 2 failures
        };
        let mgr = GcsHealthCheckManager::new(config, callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 7;
        let nid = NodeID::from_binary(&nid_data);

        mgr.add_node(nid, "127.0.0.1:19998".to_string());

        // Should get death notification after 2 failed checks
        let result = tokio::time::timeout(Duration::from_secs(5), death_rx.recv()).await;
        assert!(
            result.is_ok(),
            "Death callback should have fired after 2 timeouts"
        );
        let dead_node = result.unwrap().unwrap();
        assert_eq!(dead_node, nid);
    }

    /// Test that multiple nodes can be monitored concurrently and each
    /// independently triggers its own death callback.
    #[tokio::test]
    async fn test_concurrent_health_checks_multiple_nodes() {
        let (death_tx, mut death_rx) = tokio::sync::mpsc::channel::<NodeID>(16);
        let callback: NodeDeathCallback = Arc::new(move |node_id: NodeID| {
            let _ = death_tx.try_send(node_id);
        });

        let config = HealthCheckConfig {
            initial_delay_ms: 0,
            timeout_ms: 50,
            period_ms: 10,
            failure_threshold: 1,
        };
        let mgr = GcsHealthCheckManager::new(config, callback);

        let mut expected_dead = HashSet::new();
        for i in 1..=3u8 {
            let mut nid_data = [0u8; 28];
            nid_data[0] = i;
            let nid = NodeID::from_binary(&nid_data);
            expected_dead.insert(nid);
            // Each node points to a different non-existent port
            mgr.add_node(nid, format!("127.0.0.1:{}", 19990 + i as u16));
        }

        assert_eq!(mgr.get_all_nodes().len(), 3);

        // Collect all 3 death notifications
        let mut actual_dead = HashSet::new();
        for _ in 0..3 {
            let result = tokio::time::timeout(Duration::from_secs(5), death_rx.recv()).await;
            assert!(
                result.is_ok(),
                "Should receive death notification for each node"
            );
            actual_dead.insert(result.unwrap().unwrap());
        }

        assert_eq!(actual_dead, expected_dead);
        assert_eq!(mgr.num_monitored_nodes(), 0);
    }

    // ---- Ported from gcs_health_check_manager_test.cc ----

    #[test]
    fn test_custom_health_check_config() {
        let config = HealthCheckConfig {
            initial_delay_ms: 100,
            timeout_ms: 200,
            period_ms: 300,
            failure_threshold: 10,
        };
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.timeout_ms, 200);
        assert_eq!(config.period_ms, 300);
        assert_eq!(config.failure_threshold, 10);
    }

    #[tokio::test]
    async fn test_mark_healthy_resets_failure_count() {
        let (death_tx, mut death_rx) = tokio::sync::mpsc::channel::<NodeID>(4);
        let callback: NodeDeathCallback = Arc::new(move |node_id: NodeID| {
            let _ = death_tx.try_send(node_id);
        });

        let config = HealthCheckConfig {
            initial_delay_ms: 0,
            timeout_ms: 50,
            period_ms: 50,
            failure_threshold: 3,
        };
        let mgr = GcsHealthCheckManager::new(config, callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 1;
        let nid = NodeID::from_binary(&nid_data);

        // Point to non-existent port (all checks fail)
        mgr.add_node(nid, "127.0.0.1:19997".to_string());

        // Wait a bit for some failures to accumulate
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Mark healthy — should reset failure counter
        mgr.mark_node_healthy(&nid);

        // The node should still be monitored (not dead yet)
        assert_eq!(mgr.num_monitored_nodes(), 1);

        // Eventually it will die because the port is unreachable
        let result = tokio::time::timeout(Duration::from_secs(5), death_rx.recv()).await;
        assert!(result.is_ok(), "Should eventually detect death");
    }

    #[tokio::test]
    async fn test_remove_node_prevents_death_callback() {
        let (death_tx, mut death_rx) = tokio::sync::mpsc::channel::<NodeID>(4);
        let callback: NodeDeathCallback = Arc::new(move |node_id: NodeID| {
            let _ = death_tx.try_send(node_id);
        });

        let config = HealthCheckConfig {
            initial_delay_ms: 0,
            timeout_ms: 50,
            period_ms: 10,
            failure_threshold: 2,
        };
        let mgr = GcsHealthCheckManager::new(config, callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 1;
        let nid = NodeID::from_binary(&nid_data);

        mgr.add_node(nid, "127.0.0.1:19996".to_string());

        // Remove immediately before it can fail enough times
        mgr.remove_node(&nid);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mgr.num_monitored_nodes(), 0);

        // Wait a bit and verify no death callback was fired
        let result = tokio::time::timeout(Duration::from_millis(300), death_rx.recv()).await;
        assert!(
            result.is_err(),
            "Should not receive death notification after removal"
        );
    }

    #[tokio::test]
    async fn test_add_multiple_then_remove_one() {
        let callback = Arc::new(|_: NodeID| {});
        let mgr = GcsHealthCheckManager::new(HealthCheckConfig::default(), callback);

        let mut ids = Vec::new();
        for i in 1..=5u8 {
            let mut nid_data = [0u8; 28];
            nid_data[0] = i;
            let nid = NodeID::from_binary(&nid_data);
            ids.push(nid);
            mgr.add_node(nid, format!("localhost:6000{}", i));
        }

        assert_eq!(mgr.num_monitored_nodes(), 5);

        mgr.remove_node(&ids[2]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mgr.num_monitored_nodes(), 4);

        let all = mgr.get_all_nodes();
        assert!(!all.contains(&ids[2]));
    }

    #[tokio::test]
    async fn test_failure_threshold_multiple() {
        // With failure_threshold=3, should take at least 3 check cycles to declare death
        let (death_tx, mut death_rx) = tokio::sync::mpsc::channel::<NodeID>(4);
        let callback: NodeDeathCallback = Arc::new(move |node_id: NodeID| {
            let _ = death_tx.try_send(node_id);
        });

        let config = HealthCheckConfig {
            initial_delay_ms: 0,
            timeout_ms: 10,
            period_ms: 10,
            failure_threshold: 3,
        };
        let mgr = GcsHealthCheckManager::new(config, callback);

        let mut nid_data = [0u8; 28];
        nid_data[0] = 55;
        let nid = NodeID::from_binary(&nid_data);

        mgr.add_node(nid, "127.0.0.1:19995".to_string());

        // Should eventually die after 3 failures
        let result = tokio::time::timeout(Duration::from_secs(5), death_rx.recv()).await;
        assert!(result.is_ok());
        let dead = result.unwrap().unwrap();
        assert_eq!(dead, nid);
        assert_eq!(mgr.num_monitored_nodes(), 0);
    }
}
