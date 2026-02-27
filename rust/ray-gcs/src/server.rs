// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS server startup and lifecycle.
//!
//! Replaces `src/ray/gcs/gcs_server.h/cc`.

use std::sync::Arc;

use ray_common::config::RayConfig;

use crate::actor_manager::GcsActorManager;
use crate::autoscaler_state_manager::GcsAutoscalerStateManager;
use crate::health_check_manager::{GcsHealthCheckManager, HealthCheckConfig};
use crate::job_manager::GcsJobManager;
use crate::kv_manager::GcsInternalKVManager;
use crate::node_manager::GcsNodeManager;
use crate::placement_group_manager::GcsPlacementGroupManager;
use crate::pubsub_handler::InternalPubSubHandler;
use crate::resource_manager::GcsResourceManager;
use crate::store_client::{InMemoryInternalKV, InMemoryStoreClient, RedisStoreClient, StoreClient};
use crate::table_storage::GcsTableStorage;
use crate::task_manager::GcsTaskManager;
use crate::worker_manager::GcsWorkerManager;

/// Storage backend type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageType {
    /// In-memory only (no persistence across GCS restarts).
    InMemory,
    /// Redis-backed persistence for HA.
    Redis,
}

/// Configuration for starting the GCS server.
#[derive(Debug, Clone)]
pub struct GcsServerConfig {
    pub port: u16,
    pub redis_address: Option<String>,
    pub redis_password: Option<String>,
    pub redis_username: Option<String>,
    pub enable_redis_ssl: bool,
    pub node_ip_address: Option<String>,
    pub node_id: Option<String>,
    pub log_dir: Option<String>,
    pub session_name: Option<String>,
    pub raylet_config_list: Option<String>,
    pub ray_config: RayConfig,
}

impl Default for GcsServerConfig {
    fn default() -> Self {
        Self {
            port: 6379,
            redis_address: None,
            redis_password: None,
            redis_username: None,
            enable_redis_ssl: false,
            node_ip_address: None,
            node_id: None,
            log_dir: None,
            session_name: None,
            raylet_config_list: None,
            ray_config: RayConfig::default(),
        }
    }
}

/// The main GCS server struct.
pub struct GcsServer {
    config: GcsServerConfig,
    storage_type: StorageType,

    // Managers — initialized during start()
    table_storage: Option<Arc<GcsTableStorage>>,
    job_manager: Option<Arc<GcsJobManager>>,
    node_manager: Option<Arc<GcsNodeManager>>,
    actor_manager: Option<Arc<GcsActorManager>>,
    worker_manager: Option<Arc<GcsWorkerManager>>,
    resource_manager: Option<Arc<GcsResourceManager>>,
    placement_group_manager: Option<Arc<GcsPlacementGroupManager>>,
    task_manager: Option<Arc<GcsTaskManager>>,
    kv_manager: Option<Arc<GcsInternalKVManager>>,
    health_check_manager: Option<Arc<GcsHealthCheckManager>>,
    autoscaler_state_manager: Option<Arc<GcsAutoscalerStateManager>>,
    pubsub_handler: Option<Arc<InternalPubSubHandler>>,
}

impl GcsServer {
    pub fn new(config: GcsServerConfig) -> Self {
        let storage_type = if config.redis_address.is_some() {
            StorageType::Redis
        } else {
            StorageType::InMemory
        };

        Self {
            config,
            storage_type,
            table_storage: None,
            job_manager: None,
            node_manager: None,
            actor_manager: None,
            worker_manager: None,
            resource_manager: None,
            placement_group_manager: None,
            task_manager: None,
            kv_manager: None,
            health_check_manager: None,
            autoscaler_state_manager: None,
            pubsub_handler: None,
        }
    }

    pub fn config(&self) -> &GcsServerConfig {
        &self.config
    }

    /// Initialize all managers.
    async fn initialize(&mut self) -> anyhow::Result<()> {
        // 1. Create store client
        let store_client: Arc<dyn StoreClient> = match self.storage_type {
            StorageType::InMemory => {
                tracing::info!("Using in-memory storage");
                Arc::new(InMemoryStoreClient::new())
            }
            StorageType::Redis => {
                let addr = self
                    .config
                    .redis_address
                    .as_deref()
                    .unwrap_or("redis://127.0.0.1:6379");
                tracing::info!(address = addr, "Connecting to Redis");
                Arc::new(
                    RedisStoreClient::new(addr, String::new())
                        .map_err(|e| anyhow::anyhow!("Redis connection failed: {}", e))?,
                )
            }
        };

        // 2. Create table storage
        let table_storage = Arc::new(GcsTableStorage::new(store_client));

        // 3. Create KV manager
        let internal_kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(
            internal_kv,
            self.config.raylet_config_list.clone().unwrap_or_default(),
        ));

        // 4. Create managers in dependency order
        let node_manager = Arc::new(GcsNodeManager::new(table_storage.clone()));
        let resource_manager = Arc::new(GcsResourceManager::new());
        let job_manager = Arc::new(GcsJobManager::new(table_storage.clone()));
        let actor_manager = Arc::new(GcsActorManager::new(table_storage.clone()));
        let worker_manager = Arc::new(GcsWorkerManager::new(table_storage.clone()));
        let placement_group_manager =
            Arc::new(GcsPlacementGroupManager::new(table_storage.clone()));
        let task_manager = Arc::new(GcsTaskManager::new(None));
        let pubsub_handler = Arc::new(InternalPubSubHandler::new());

        // 5. Create health check manager
        let node_mgr_for_hc = Arc::clone(&node_manager);
        let health_check_manager = GcsHealthCheckManager::new(
            HealthCheckConfig::default(),
            Arc::new(move |node_id| {
                // On node death from health check, trigger node manager
                let mgr = Arc::clone(&node_mgr_for_hc);
                tokio::spawn(async move {
                    let _ = mgr.on_node_failure(&node_id).await;
                });
            }),
        );

        // 6. Create autoscaler state manager
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            self.config.session_name.clone().unwrap_or_default(),
            Arc::clone(&node_manager),
            Arc::clone(&resource_manager),
        ));

        // 7. Set cluster ID (generate if not provided)
        let cluster_id = self
            .config
            .node_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        node_manager.set_cluster_id(cluster_id.clone());
        tracing::info!(cluster_id = %cluster_id, "Cluster ID set");

        // 8. Initialize from persistent storage
        node_manager.initialize().await?;
        job_manager.initialize().await?;
        actor_manager.initialize().await?;
        placement_group_manager.initialize().await?;

        // Store references
        self.table_storage = Some(table_storage);
        self.job_manager = Some(job_manager);
        self.node_manager = Some(node_manager);
        self.actor_manager = Some(actor_manager);
        self.worker_manager = Some(worker_manager);
        self.resource_manager = Some(resource_manager);
        self.placement_group_manager = Some(placement_group_manager);
        self.task_manager = Some(task_manager);
        self.kv_manager = Some(kv_manager);
        self.health_check_manager = Some(health_check_manager);
        self.autoscaler_state_manager = Some(autoscaler_state_manager);
        self.pubsub_handler = Some(pubsub_handler);

        Ok(())
    }

    /// Start the GCS server.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(port = self.config.port, "Starting Rust GCS server");

        // Initialize all managers
        self.initialize().await?;

        tracing::info!(
            port = self.config.port,
            storage = ?self.storage_type,
            "GCS server initialized, all managers ready"
        );

        // Build tonic server with all services
        let addr: std::net::SocketAddr = format!("0.0.0.0:{}", self.config.port)
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid address: {}", e))?;

        tracing::info!(%addr, "GCS gRPC server listening");

        // For now, use a simple signal-based shutdown.
        // The full tonic server registration happens here.
        // Each tonic service would be registered using .add_service().
        //
        // Example (when tonic service traits are implemented):
        // tonic::transport::Server::builder()
        //     .add_service(JobInfoGcsServiceServer::new(job_svc))
        //     .add_service(NodeInfoGcsServiceServer::new(node_svc))
        //     .add_service(InternalKVGcsServiceServer::new(kv_svc))
        //     ... etc ...
        //     .serve_with_shutdown(addr, signal)
        //     .await?;

        tokio::signal::ctrl_c().await?;
        tracing::info!("GCS server shutting down");
        Ok(())
    }

    // ── Accessors ──────────────────────────────────────────────────────

    pub fn job_manager(&self) -> Option<&Arc<GcsJobManager>> {
        self.job_manager.as_ref()
    }

    pub fn node_manager(&self) -> Option<&Arc<GcsNodeManager>> {
        self.node_manager.as_ref()
    }

    pub fn actor_manager(&self) -> Option<&Arc<GcsActorManager>> {
        self.actor_manager.as_ref()
    }

    pub fn worker_manager(&self) -> Option<&Arc<GcsWorkerManager>> {
        self.worker_manager.as_ref()
    }

    pub fn resource_manager(&self) -> Option<&Arc<GcsResourceManager>> {
        self.resource_manager.as_ref()
    }

    pub fn placement_group_manager(&self) -> Option<&Arc<GcsPlacementGroupManager>> {
        self.placement_group_manager.as_ref()
    }

    pub fn task_manager(&self) -> Option<&Arc<GcsTaskManager>> {
        self.task_manager.as_ref()
    }

    pub fn kv_manager(&self) -> Option<&Arc<GcsInternalKVManager>> {
        self.kv_manager.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_initialization() {
        let config = GcsServerConfig {
            port: 0,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        assert!(server.job_manager().is_some());
        assert!(server.node_manager().is_some());
        assert!(server.actor_manager().is_some());
        assert!(server.worker_manager().is_some());
        assert!(server.resource_manager().is_some());
        assert!(server.placement_group_manager().is_some());
        assert!(server.task_manager().is_some());
        assert!(server.kv_manager().is_some());
    }

    #[tokio::test]
    async fn test_server_with_managers_integration() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        // Add a job
        let job_mgr = server.job_manager().unwrap();
        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            ..Default::default()
        };
        job_mgr.handle_add_job(job_data).await.unwrap();
        assert_eq!(job_mgr.num_running_jobs(), 1);

        // Register a node
        let node_mgr = server.node_manager().unwrap();
        let node_info = ray_proto::ray::rpc::GcsNodeInfo {
            node_id: vec![0u8; 28],
            state: 0,
            ..Default::default()
        };
        node_mgr.handle_register_node(node_info).await.unwrap();
        assert_eq!(node_mgr.num_alive_nodes(), 1);

        // Put/get KV
        let kv_mgr = server.kv_manager().unwrap();
        kv_mgr
            .handle_put("test", "key", "value".into(), true)
            .await
            .unwrap();
        let val = kv_mgr.handle_get("test", "key").await.unwrap();
        assert_eq!(val, Some("value".to_string()));
    }
}
