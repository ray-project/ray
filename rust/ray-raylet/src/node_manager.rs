// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Node manager — the central raylet class.
//!
//! Replaces `src/ray/raylet/node_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use ray_common::config::RayConfig;
use ray_common::id::NodeID;
use ray_common::scheduling::{FixedPoint, ResourceSet};
use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
use ray_proto::ray::rpc;
use ray_rpc::client::RetryConfig;

use crate::cluster_resource_manager::ClusterResourceManager;
use crate::cluster_resource_scheduler::ClusterResourceScheduler;
use crate::lease_manager::ClusterLeaseManager;
use crate::local_resource_manager::LocalResourceManager;
use crate::placement_group_resource_manager::PlacementGroupResourceManager;
use crate::wait_manager::WaitManager;
use crate::worker_pool::WorkerPool;

/// Wait for SIGTERM or SIGINT for graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}

/// Configuration for starting the raylet.
#[derive(Debug, Clone)]
pub struct RayletConfig {
    pub node_ip_address: String,
    pub port: u16,
    pub object_store_socket: String,
    pub gcs_address: String,
    pub log_dir: Option<String>,
    pub ray_config: RayConfig,
    pub node_id: String,
    pub resources: HashMap<String, f64>,
    pub labels: HashMap<String, String>,
    pub session_name: String,
}

/// The main raylet node manager.
pub struct NodeManager {
    config: RayletConfig,
    scheduler: Arc<ClusterResourceScheduler>,
    worker_pool: Arc<WorkerPool>,
    lease_manager: Arc<ClusterLeaseManager>,
    wait_manager: Arc<WaitManager>,
    placement_group_resource_manager: Arc<PlacementGroupResourceManager>,
}

impl NodeManager {
    pub fn new(config: RayletConfig) -> Self {
        // Build a ResourceSet from the config's resource map.
        let mut total_resources = ResourceSet::new();
        for (name, amount) in &config.resources {
            total_resources.set(name.clone(), FixedPoint::from_f64(*amount));
        }

        let local_resource_manager = Arc::new(LocalResourceManager::new(
            config.node_id.clone(),
            total_resources,
            config.labels.clone(),
        ));
        let cluster_resource_manager = Arc::new(ClusterResourceManager::new());

        let scheduler = Arc::new(ClusterResourceScheduler::new(
            config.node_id.clone(),
            local_resource_manager.clone(),
            cluster_resource_manager,
        ));

        let lease_manager = Arc::new(ClusterLeaseManager::new(scheduler.clone()));

        let worker_pool = Arc::new(WorkerPool::new(
            10,  // maximum_startup_concurrency (default)
            200, // num_workers_soft_limit (default)
        ));

        let wait_manager = Arc::new(WaitManager::new());

        let placement_group_resource_manager =
            Arc::new(PlacementGroupResourceManager::new(local_resource_manager));

        Self {
            config,
            scheduler,
            worker_pool,
            lease_manager,
            wait_manager,
            placement_group_resource_manager,
        }
    }

    pub fn config(&self) -> &RayletConfig {
        &self.config
    }

    pub fn scheduler(&self) -> &Arc<ClusterResourceScheduler> {
        &self.scheduler
    }

    pub fn worker_pool(&self) -> &Arc<WorkerPool> {
        &self.worker_pool
    }

    pub fn lease_manager(&self) -> &Arc<ClusterLeaseManager> {
        &self.lease_manager
    }

    pub fn wait_manager(&self) -> &Arc<WaitManager> {
        &self.wait_manager
    }

    pub fn placement_group_resource_manager(&self) -> &Arc<PlacementGroupResourceManager> {
        &self.placement_group_resource_manager
    }

    /// Handle a drain request.
    pub fn handle_drain(&self, deadline_ms: u64) {
        tracing::info!(deadline_ms, "Node drain requested");
        self.scheduler
            .local_resource_manager()
            .set_local_node_draining(deadline_ms);
    }

    /// Resolve the binary NodeID from the config's hex string, or generate a random one.
    fn resolve_node_id(&self) -> NodeID {
        let hex_str = &self.config.node_id;
        // A valid NodeID hex string is exactly 56 characters (28 bytes * 2).
        if hex_str.len() == NodeID::SIZE * 2 {
            let nid = NodeID::from_hex(hex_str);
            if !nid.is_nil() {
                return nid;
            }
        }
        NodeID::from_random()
    }

    /// Register this raylet node with the GCS server.
    ///
    /// Connects to GCS, retrieves the cluster ID, and sends a RegisterNode RPC
    /// with this node's address, port, resources, and labels.
    ///
    /// Returns the GCS client (for later unregistration) and the binary NodeID used.
    async fn register_with_gcs(
        &self,
        bound_port: u16,
    ) -> Result<(GcsRpcClient, NodeID), Box<dyn std::error::Error + Send + Sync>> {
        let gcs_address = &self.config.gcs_address;
        tracing::info!(gcs_address, "Connecting to GCS");

        let gcs_client = GcsRpcClient::connect(gcs_address, RetryConfig::default()).await?;

        // Verify GCS is alive by fetching the cluster ID.
        let cluster_reply = gcs_client
            .get_cluster_id(rpc::GetClusterIdRequest::default())
            .await?;
        tracing::info!(
            cluster_id = hex::encode(&cluster_reply.cluster_id),
            "Connected to GCS"
        );

        let node_id = self.resolve_node_id();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let node_info = rpc::GcsNodeInfo {
            node_id: node_id.binary(),
            node_manager_address: self.config.node_ip_address.clone(),
            node_manager_port: bound_port as i32,
            object_store_socket_name: self.config.object_store_socket.clone(),
            resources_total: self.config.resources.clone(),
            labels: self.config.labels.clone(),
            state: rpc::gcs_node_info::GcsNodeState::Alive as i32,
            start_time_ms: now_ms,
            ..Default::default()
        };

        gcs_client
            .register_node(rpc::RegisterNodeRequest {
                node_info: Some(node_info),
            })
            .await?;
        tracing::info!(node_id = %node_id.hex(), "Registered with GCS");

        Ok((gcs_client, node_id))
    }

    /// Unregister this node from GCS on shutdown.
    async fn unregister_from_gcs(gcs_client: &GcsRpcClient, node_id: &NodeID) {
        tracing::info!(node_id = %node_id.hex(), "Unregistering from GCS");
        let result = gcs_client
            .unregister_node(rpc::UnregisterNodeRequest {
                node_id: node_id.binary(),
                ..Default::default()
            })
            .await;
        match result {
            Ok(_) => tracing::info!("Unregistered from GCS"),
            Err(e) => tracing::warn!(error = %e, "Failed to unregister from GCS"),
        }
    }

    /// Start the raylet (full server).
    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!(
            port = self.config.port,
            node_ip = %self.config.node_ip_address,
            node_id = %self.config.node_id,
            session = %self.config.session_name,
            "Starting Rust raylet"
        );

        // Bind the TCP listener first to discover the actual port.
        let listener =
            tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        let bound_port = listener.local_addr()?.port();
        tracing::info!(port = bound_port, "Raylet gRPC server listening");

        // Wire the worker spawner callback now that we know the port.
        let spawner_config = crate::worker_spawner::WorkerSpawnerConfig {
            node_ip_address: self.config.node_ip_address.clone(),
            raylet_port: bound_port,
            gcs_address: self.config.gcs_address.clone(),
            node_id: self.config.node_id.clone(),
            session_name: self.config.session_name.clone(),
            python_worker_command: None,
        };
        self.worker_pool
            .set_start_worker_callback(crate::worker_spawner::make_spawn_callback(spawner_config));

        // Register with GCS if an address is configured.
        let gcs_state = if !self.config.gcs_address.is_empty() {
            match self.register_with_gcs(bound_port).await {
                Ok((client, node_id)) => Some((client, node_id)),
                Err(e) => {
                    tracing::error!(error = %e, "Failed to register with GCS, continuing without");
                    None
                }
            }
        } else {
            tracing::warn!("No GCS address configured, skipping registration");
            None
        };

        let svc = crate::grpc_service::NodeManagerServiceImpl {
            node_manager: Arc::clone(&self),
        };
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<rpc::node_manager_service_server::NodeManagerServiceServer<
                crate::grpc_service::NodeManagerServiceImpl,
            >>()
            .await;

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(
                rpc::node_manager_service_server::NodeManagerServiceServer::new(svc),
            )
            .add_service(health_service)
            .serve_with_incoming_shutdown(incoming, shutdown_signal())
            .await?;

        // Clean up: unregister from GCS.
        if let Some((gcs_client, node_id)) = &gcs_state {
            Self::unregister_from_gcs(gcs_client, node_id).await;
        }

        tracing::info!("Raylet shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> RayletConfig {
        RayletConfig {
            node_ip_address: "127.0.0.1".to_string(),
            port: 0,
            object_store_socket: "/tmp/plasma".to_string(),
            gcs_address: "127.0.0.1:6379".to_string(),
            log_dir: None,
            ray_config: RayConfig::default(),
            node_id: "test-node-1".to_string(),
            resources: HashMap::from([
                ("CPU".to_string(), 8.0),
                ("GPU".to_string(), 2.0),
            ]),
            labels: HashMap::from([("region".to_string(), "us-east".to_string())]),
            session_name: "test-session".to_string(),
        }
    }

    #[test]
    fn test_node_manager_creation() {
        let nm = NodeManager::new(make_config());
        assert_eq!(nm.config().node_id, "test-node-1");
        assert_eq!(nm.config().port, 0);
    }

    #[test]
    fn test_node_manager_components_initialized() {
        let nm = NodeManager::new(make_config());
        // All sub-managers should be accessible
        let _scheduler = nm.scheduler();
        let _worker_pool = nm.worker_pool();
        let _lease_manager = nm.lease_manager();
        let _wait_manager = nm.wait_manager();
        let _pg_manager = nm.placement_group_resource_manager();
    }

    #[test]
    fn test_node_manager_drain() {
        let nm = NodeManager::new(make_config());
        nm.handle_drain(5000);
        // After drain, node should be marked as draining
        assert!(nm.scheduler().local_resource_manager().is_local_node_draining());
    }

    #[test]
    fn test_node_manager_resources_configured() {
        let nm = NodeManager::new(make_config());
        let local_rm = nm.scheduler().local_resource_manager();
        let total = local_rm.get_local_total_resources();
        assert!(total.get("CPU") > FixedPoint::from_f64(0.0));
        assert!(total.get("GPU") > FixedPoint::from_f64(0.0));
    }
}
