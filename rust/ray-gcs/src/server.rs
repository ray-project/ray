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

use std::path::Path;
use std::sync::Arc;

use ray_common::config::RayConfig;

use crate::actor_manager::GcsActorManager;
use crate::actor_scheduler::{GcsActorScheduler, GrpcCoreWorkerClient, GrpcRayletClient};
use crate::autoscaler_state_manager::GcsAutoscalerStateManager;
use crate::grpc_services::*;
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

use ray_proto::ray::rpc;

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
    pub session_dir: Option<String>,
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
            session_dir: None,
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

        // 4b. Create actor scheduler and wire into actor manager
        let raylet_client = Arc::new(GrpcRayletClient);
        let worker_client = Arc::new(GrpcCoreWorkerClient);
        let actor_scheduler = Arc::new(GcsActorScheduler::new(
            Arc::clone(&node_manager),
            raylet_client,
            worker_client,
        ));
        actor_manager.set_actor_scheduler(actor_scheduler);
        actor_manager.set_pubsub_handler(Arc::clone(&pubsub_handler));

        // 4c. Wire pubsub handlers to all managers
        node_manager.set_pubsub_handler(Arc::clone(&pubsub_handler));
        job_manager.set_pubsub_handler(Arc::clone(&pubsub_handler));
        worker_manager.set_pubsub_handler(Arc::clone(&pubsub_handler));

        // 4d. Wire node death cascade — when a node dies, notify all managers
        {
            let actor_mgr = Arc::clone(&actor_manager);
            let job_mgr = Arc::clone(&job_manager);
            let resource_mgr = Arc::clone(&resource_manager);
            let pg_mgr = Arc::clone(&placement_group_manager);
            node_manager.add_node_removed_listener(Box::new(move |node_info| {
                let node_id = ray_common::id::NodeID::from_binary(
                    node_info.node_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                );
                actor_mgr.on_node_dead(&node_id);
                job_mgr.on_node_dead(&node_id);
                resource_mgr.on_node_dead(&node_id);
                pg_mgr.on_node_dead(&node_id);
            }));

            // On node added, register with resource manager
            let resource_mgr = Arc::clone(&resource_manager);
            node_manager.add_node_added_listener(Box::new(move |node_info| {
                let node_id = ray_common::id::NodeID::from_binary(
                    node_info.node_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                );
                resource_mgr.on_node_add(&node_id);
            }));
        }

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

        // 7. Generate a random 28-byte cluster ID (matching C++ ClusterID::FromRandom)
        let cluster_id: Vec<u8> = (0..28).map(|_| rand::random::<u8>()).collect();
        node_manager.set_cluster_id(cluster_id.clone());
        tracing::info!(cluster_id = hex::encode(&cluster_id), "Cluster ID set");

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

    /// Persist the bound port to the session directory.
    ///
    /// Matches C++ format: `{session_dir}/gcs_server_port_{node_id_hex}`
    fn persist_port(&self, port: u16) -> anyhow::Result<()> {
        if let Some(ref session_dir) = self.config.session_dir {
            let dir = Path::new(session_dir);
            std::fs::create_dir_all(dir)?;
            let node_id = self.config.node_id.as_deref().unwrap_or("default");
            let filename = format!("gcs_server_port_{}", node_id);
            std::fs::write(dir.join(&filename), port.to_string())?;
            tracing::info!(port, filename, "Port file written");
        }
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

        // Construct tonic service wrappers from initialized managers
        let job_svc = JobInfoGcsServiceImpl {
            job_manager: Arc::clone(self.job_manager.as_ref().unwrap()),
        };
        let node_svc = NodeInfoGcsServiceImpl {
            node_manager: Arc::clone(self.node_manager.as_ref().unwrap()),
        };
        let actor_svc = ActorInfoGcsServiceImpl {
            actor_manager: Arc::clone(self.actor_manager.as_ref().unwrap()),
        };
        let kv_svc = InternalKVGcsServiceImpl {
            kv_manager: Arc::clone(self.kv_manager.as_ref().unwrap()),
        };
        let worker_svc = WorkerInfoGcsServiceImpl {
            worker_manager: Arc::clone(self.worker_manager.as_ref().unwrap()),
        };
        let pg_svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: Arc::clone(
                self.placement_group_manager.as_ref().unwrap(),
            ),
        };
        let resource_svc = NodeResourceInfoGcsServiceImpl {
            resource_manager: Arc::clone(self.resource_manager.as_ref().unwrap()),
        };
        let runtime_env_svc = RuntimeEnvGcsServiceImpl;
        // Publisher ID: random 28-byte NodeID, matches C++ Publisher behavior
        let publisher_id: Vec<u8> = (0..28).map(|_| rand::random::<u8>()).collect();
        let pubsub_svc = InternalPubSubGcsServiceImpl {
            pubsub_handler: Arc::clone(self.pubsub_handler.as_ref().unwrap()),
            publisher_id,
        };
        let task_svc = TaskInfoGcsServiceImpl {
            task_manager: Arc::clone(self.task_manager.as_ref().unwrap()),
        };
        let autoscaler_svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager: Arc::clone(
                self.autoscaler_state_manager.as_ref().unwrap(),
            ),
        };

        // Health service
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<rpc::job_info_gcs_service_server::JobInfoGcsServiceServer<
                JobInfoGcsServiceImpl,
            >>()
            .await;
        health_reporter
            .set_serving::<rpc::node_info_gcs_service_server::NodeInfoGcsServiceServer<
                NodeInfoGcsServiceImpl,
            >>()
            .await;
        health_reporter
            .set_serving::<rpc::actor_info_gcs_service_server::ActorInfoGcsServiceServer<
                ActorInfoGcsServiceImpl,
            >>()
            .await;
        health_reporter
            .set_serving::<rpc::internal_kv_gcs_service_server::InternalKvGcsServiceServer<
                InternalKVGcsServiceImpl,
            >>()
            .await;

        // Bind TCP listener
        let listener =
            tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        let bound_port = listener.local_addr()?.port();
        tracing::info!(port = bound_port, "GCS gRPC server listening");

        // Persist port file
        self.persist_port(bound_port)?;

        // Serve
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(rpc::job_info_gcs_service_server::JobInfoGcsServiceServer::new(job_svc))
            .add_service(rpc::node_info_gcs_service_server::NodeInfoGcsServiceServer::new(node_svc))
            .add_service(rpc::actor_info_gcs_service_server::ActorInfoGcsServiceServer::new(actor_svc))
            .add_service(rpc::internal_kv_gcs_service_server::InternalKvGcsServiceServer::new(kv_svc))
            .add_service(rpc::worker_info_gcs_service_server::WorkerInfoGcsServiceServer::new(worker_svc))
            .add_service(rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsServiceServer::new(pg_svc))
            .add_service(rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsServiceServer::new(resource_svc))
            .add_service(rpc::runtime_env_gcs_service_server::RuntimeEnvGcsServiceServer::new(runtime_env_svc))
            .add_service(rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsServiceServer::new(pubsub_svc))
            .add_service(rpc::task_info_gcs_service_server::TaskInfoGcsServiceServer::new(task_svc))
            .add_service(rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateServiceServer::new(autoscaler_svc))
            .add_service(health_service)
            .serve_with_incoming_shutdown(incoming, shutdown_signal())
            .await?;

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
        let job_data = rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            ..Default::default()
        };
        job_mgr.handle_add_job(job_data).await.unwrap();
        assert_eq!(job_mgr.num_running_jobs(), 1);

        // Register a node
        let node_mgr = server.node_manager().unwrap();
        let node_info = rpc::GcsNodeInfo {
            node_id: vec![0u8; 28],
            state: 0,
            ..Default::default()
        };
        node_mgr.handle_register_node(node_info).await.unwrap();
        assert_eq!(node_mgr.num_alive_nodes(), 1);

        // Put/get KV
        let kv_mgr = server.kv_manager().unwrap();
        kv_mgr
            .handle_put(b"test", b"key", b"value".to_vec(), true)
            .await
            .unwrap();
        let val = kv_mgr.handle_get(b"test", b"key").await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_node_death_cascade() {
        use crate::actor_manager::ActorState;

        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let node_mgr = server.node_manager().unwrap();
        let actor_mgr = server.actor_manager().unwrap();
        let resource_mgr = server.resource_manager().unwrap();

        // Register a node
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(node_mgr.num_alive_nodes(), 1);
        // Resource manager should have the node (via added listener)
        assert_eq!(resource_mgr.num_alive_nodes(), 1);

        // Register and place an actor on that node
        let mut actor_id = vec![0u8; 16];
        actor_id[0] = 1;
        let task_spec = rpc::TaskSpec {
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: actor_id.clone(),
                name: "test_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();
        assert_eq!(actor_mgr.num_registered_actors(), 1);

        // Manually place actor on the node (simulating scheduling)
        let aid = ray_common::id::ActorID::from_binary(
            actor_id.as_slice().try_into().unwrap(),
        );
        actor_mgr.on_actor_creation_success(
            &aid,
            rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: vec![42u8; 28],
            },
            1234,
            vec![],
            node_id.clone(),
        );

        // Verify actor is ALIVE
        let actor = actor_mgr.handle_get_actor_info(&actor_id).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Alive);

        // Kill the node — this should cascade to all managers
        let nid = ray_common::id::NodeID::from_binary(
            node_id.as_slice().try_into().unwrap(),
        );
        node_mgr.handle_unregister_node(&node_id).await.unwrap();

        // Verify cascade:
        // 1. Node should be dead
        assert_eq!(node_mgr.num_alive_nodes(), 0);
        assert!(node_mgr.is_node_dead(&nid));

        // 2. Actor should be DEAD (killed by cascade)
        let actor = actor_mgr.handle_get_actor_info(&actor_id).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Dead);

        // 3. Resource manager should have removed the node
        assert_eq!(resource_mgr.num_alive_nodes(), 0);
    }
}
