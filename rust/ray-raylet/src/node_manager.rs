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
use ray_common::scheduling::{FixedPoint, ResourceSet};

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

    /// Start the raylet (full server).
    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            port = self.config.port,
            node_ip = %self.config.node_ip_address,
            node_id = %self.config.node_id,
            session = %self.config.session_name,
            "Starting Rust raylet"
        );

        let svc = crate::grpc_service::NodeManagerServiceImpl {
            node_manager: Arc::clone(&self),
        };
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ray_proto::ray::rpc::node_manager_service_server::NodeManagerServiceServer<
                crate::grpc_service::NodeManagerServiceImpl,
            >>()
            .await;

        let listener =
            tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        let bound_port = listener.local_addr()?.port();
        tracing::info!(port = bound_port, "Raylet gRPC server listening");

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(
                ray_proto::ray::rpc::node_manager_service_server::NodeManagerServiceServer::new(
                    svc,
                ),
            )
            .add_service(health_service)
            .serve_with_incoming_shutdown(incoming, shutdown_signal())
            .await?;

        tracing::info!("Raylet shutting down");
        Ok(())
    }
}
