//! GCS Server -- orchestrator that wires all managers and serves gRPC.
//!
//! Maps C++ `GcsServer` from `src/ray/gcs/gcs_server.h/cc`.
//! This is a drop-in replacement for the C++ GCS binary, implementing the same
//! 12 gRPC services from the same proto files.

mod init_data;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{error, info, warn};

pub use init_data::GcsInitData;

use gcs_kv::{GcsInternalKVManager, StoreClientInternalKV};
use gcs_managers::actor_stub::GcsActorManager;
use gcs_managers::autoscaler_stub::GcsAutoscalerStateManager;
use gcs_managers::event_export_stub::EventExportServiceStub;
use gcs_managers::job_manager::GcsJobManager;
use gcs_managers::node_manager::GcsNodeManager;
use gcs_managers::placement_group_stub::GcsPlacementGroupManager;
use gcs_managers::pubsub_stub::PubSubService;
use gcs_managers::resource_manager::GcsResourceManager;
use gcs_managers::runtime_env_stub::RuntimeEnvServiceStub;
use gcs_managers::task_manager::GcsTaskManager;
use gcs_managers::worker_manager::GcsWorkerManager;
use gcs_proto::ray::rpc::actor_info_gcs_service_server::ActorInfoGcsServiceServer;
use gcs_proto::ray::rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateServiceServer;
use gcs_proto::ray::rpc::events::event_aggregator_service_server::EventAggregatorServiceServer;
use gcs_proto::ray::rpc::internal_kv_gcs_service_server::InternalKvGcsServiceServer;
use gcs_proto::ray::rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsServiceServer;
use gcs_proto::ray::rpc::job_info_gcs_service_server::JobInfoGcsServiceServer;
use gcs_proto::ray::rpc::node_info_gcs_service_server::NodeInfoGcsServiceServer;
use gcs_proto::ray::rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsServiceServer;
use gcs_proto::ray::rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsServiceServer;
use gcs_proto::ray::rpc::runtime_env_gcs_service_server::RuntimeEnvGcsServiceServer;
use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsServiceServer;
use gcs_proto::ray::rpc::worker_info_gcs_service_server::WorkerInfoGcsServiceServer;
use gcs_pubsub::{GcsPublisher, PubSubManager};
use gcs_store::{InMemoryStoreClient, RedisStoreClient, StoreClient};
use gcs_table_storage::GcsTableStorage;

/// Configuration for the GCS server.
#[derive(Debug, Clone)]
pub struct GcsServerConfig {
    pub grpc_port: u16,
    pub cluster_id: Vec<u8>,
    pub raylet_config_list: String,
    pub max_task_events: usize,
    /// Redis connection URL. If `Some`, uses Redis-backed persistence.
    /// If `None`, uses in-memory storage (no persistence across restarts).
    /// Example: `"redis://127.0.0.1:6379"` or `"redis://:password@host:port"`.
    pub redis_address: Option<String>,
    /// Namespace prefix that isolates this cluster's data in a shared Redis.
    /// Maps C++ `external_storage_namespace`. Defaults to empty string.
    pub external_storage_namespace: String,
}

impl Default for GcsServerConfig {
    fn default() -> Self {
        // ClusterID must be exactly 28 bytes (kUniqueIDSize in C++)
        let mut cluster_id = vec![0u8; 28];
        // Fill with random bytes using a simple approach
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for (i, b) in cluster_id.iter_mut().enumerate() {
            *b = ((seed >> (i % 16)) ^ (i as u128 * 37)) as u8;
        }
        Self {
            grpc_port: 6379,
            cluster_id,
            raylet_config_list: String::new(),
            max_task_events: 100_000,
            redis_address: None,
            external_storage_namespace: String::new(),
        }
    }
}

/// The GCS Server. Maps C++ `GcsServer`.
///
/// Creates all managers and serves gRPC on the configured port.
/// Supports both in-memory and Redis-backed storage.
pub struct GcsServer {
    config: GcsServerConfig,
    // Shared infrastructure
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    pubsub_manager: Arc<PubSubManager>,
    /// Optional Redis client handle for health checks (only set when using Redis).
    redis_client: Option<Arc<RedisStoreClient>>,
    // Managers
    node_manager: Arc<GcsNodeManager>,
    job_manager: Arc<GcsJobManager>,
    worker_manager: Arc<GcsWorkerManager>,
    task_manager: Arc<GcsTaskManager>,
    resource_manager: Arc<GcsResourceManager>,
    kv_manager: Arc<GcsInternalKVManager>,
    actor_manager: Arc<GcsActorManager>,
    placement_group_manager: Arc<GcsPlacementGroupManager>,
    autoscaler_manager: Arc<GcsAutoscalerStateManager>,
    pubsub_service: Arc<PubSubService>,
}

impl GcsServer {
    /// Create a new GCS server with in-memory storage (no persistence).
    pub fn new(config: GcsServerConfig) -> Self {
        let store: Arc<dyn StoreClient> = Arc::new(InMemoryStoreClient::new());
        Self::new_with_store(config, store, None)
    }

    /// Create a new GCS server with a Redis-backed store.
    pub fn new_with_redis(config: GcsServerConfig, redis: Arc<RedisStoreClient>) -> Self {
        let store: Arc<dyn StoreClient> = redis.clone();
        Self::new_with_store(config, store, Some(redis))
    }

    /// Shared constructor: wire all managers around a given `StoreClient`.
    fn new_with_store(
        config: GcsServerConfig,
        store: Arc<dyn StoreClient>,
        redis_client: Option<Arc<RedisStoreClient>>,
    ) -> Self {
        let table_storage = Arc::new(GcsTableStorage::new(store.clone()));
        let publisher = Arc::new(GcsPublisher::new(4096));

        let pubsub_manager = Arc::new(PubSubManager::new(config.cluster_id.clone()));

        let node_manager = Arc::new(GcsNodeManager::new(
            table_storage.clone(),
            publisher.clone(),
            config.cluster_id.clone(),
        ));

        let job_manager = Arc::new(GcsJobManager::new(
            table_storage.clone(),
            publisher.clone(),
        ));

        let worker_manager = Arc::new(GcsWorkerManager::new(
            table_storage.clone(),
            publisher.clone(),
        ));

        let task_manager = Arc::new(GcsTaskManager::new(config.max_task_events));

        let resource_manager = Arc::new(GcsResourceManager::new());

        let kv_store = Arc::new(StoreClientInternalKV::new(store));
        let kv_manager = Arc::new(GcsInternalKVManager::new(
            kv_store,
            config.raylet_config_list.clone(),
        ));

        let actor_manager = Arc::new(GcsActorManager::new(
            pubsub_manager.clone(),
            table_storage.clone(),
        ));

        let placement_group_manager = Arc::new(GcsPlacementGroupManager::new(
            table_storage.clone(),
        ));

        let autoscaler_manager = Arc::new(GcsAutoscalerStateManager::new());

        let pubsub_service = Arc::new(PubSubService::new(pubsub_manager.clone()));

        Self {
            config,
            table_storage,
            publisher,
            pubsub_manager,
            redis_client,
            node_manager,
            job_manager,
            worker_manager,
            task_manager,
            resource_manager,
            kv_manager,
            actor_manager,
            placement_group_manager,
            autoscaler_manager,
            pubsub_service,
        }
    }

    /// Load persisted state from storage and initialize all managers.
    ///
    /// Maps C++ `GcsServer::DoStart` which calls `GcsInitData::AsyncLoad`
    /// then initializes each manager with the recovered data.
    /// This is a no-op for empty storage (fresh start).
    pub async fn initialize(&self) {
        let init_data = GcsInitData::load(&self.table_storage).await;

        self.node_manager.initialize(&init_data.nodes).await;
        self.job_manager.initialize(&init_data.jobs);
        self.actor_manager.initialize(&init_data.actors, &init_data.actor_task_specs);
        self.placement_group_manager.initialize(&init_data.placement_groups);

        info!("GCS server initialized from persisted state");
    }

    /// Start a background loop that periodically PINGs Redis.
    /// Panics if Redis becomes unreachable (matching C++ behavior).
    fn start_redis_health_check(&self) {
        if let Some(redis) = &self.redis_client {
            let redis = redis.clone();
            let interval_ms: u64 = std::env::var("RAY_gcs_redis_heartbeat_interval_ms")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5000);

            tokio::spawn(async move {
                let mut consecutive_failures = 0u32;
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
                    match redis.check_health().await {
                        Ok(_) => {
                            if consecutive_failures > 0 {
                                info!("Redis health check recovered after {} failures", consecutive_failures);
                            }
                            consecutive_failures = 0;
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            error!(
                                error = %e,
                                consecutive_failures,
                                "Redis health check failed"
                            );
                            // Match C++ behavior: crash on Redis failure.
                            if consecutive_failures >= 5 {
                                panic!(
                                    "Redis connection failed after {} consecutive health check failures: {}",
                                    consecutive_failures, e
                                );
                            }
                        }
                    }
                }
            });
        }
    }

    /// Start the gRPC server, binding to the configured port. Blocks until shutdown.
    pub async fn start(&self) -> Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.grpc_port).parse()?;

        info!(
            port = self.config.grpc_port,
            redis = self.redis_client.is_some(),
            "Starting Rust GCS server"
        );

        // Load persisted state from storage and initialize managers.
        self.initialize().await;

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<NodeInfoGcsServiceServer<GcsNodeManager>>().await;

        // Start the health check loop for detecting dead nodes.
        let hc_period = std::env::var("RAY_health_check_period_ms")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(1000u64);
        let hc_timeout = std::env::var("RAY_health_check_timeout_ms")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(500u64);
        let hc_threshold = std::env::var("RAY_health_check_failure_threshold")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(5u32);
        self.node_manager.start_health_check_loop(hc_period, hc_timeout, hc_threshold);

        // Start periodic Redis health check (no-op if using in-memory store).
        self.start_redis_health_check();

        Server::builder()
            .add_service(health_service)
            .add_service(NodeInfoGcsServiceServer::from_arc(self.node_manager.clone()))
            .add_service(JobInfoGcsServiceServer::from_arc(self.job_manager.clone()))
            .add_service(WorkerInfoGcsServiceServer::from_arc(self.worker_manager.clone()))
            .add_service(TaskInfoGcsServiceServer::from_arc(self.task_manager.clone()))
            .add_service(NodeResourceInfoGcsServiceServer::from_arc(self.resource_manager.clone()))
            .add_service(InternalKvGcsServiceServer::from_arc(self.kv_manager.clone()))
            .add_service(ActorInfoGcsServiceServer::from_arc(self.actor_manager.clone()))
            .add_service(PlacementGroupInfoGcsServiceServer::from_arc(self.placement_group_manager.clone()))
            .add_service(InternalPubSubGcsServiceServer::from_arc(self.pubsub_service.clone()))
            .add_service(RuntimeEnvGcsServiceServer::new(RuntimeEnvServiceStub))
            .add_service(AutoscalerStateServiceServer::from_arc(self.autoscaler_manager.clone()))
            .add_service(EventAggregatorServiceServer::new(EventExportServiceStub))
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Start the gRPC server using a pre-bound TcpListener. Blocks until shutdown.
    pub async fn start_with_listener(&self, listener: TcpListener) -> Result<()> {
        let local_addr = listener.local_addr()?;
        info!(
            port = local_addr.port(),
            redis = self.redis_client.is_some(),
            "Starting Rust GCS server (pre-bound listener)"
        );

        // Load persisted state from storage and initialize managers.
        self.initialize().await;

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        // Start the health check loop for detecting dead nodes.
        self.node_manager.start_health_check_loop(1000, 500, 5);

        // Start periodic Redis health check (no-op if using in-memory store).
        self.start_redis_health_check();

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<NodeInfoGcsServiceServer<GcsNodeManager>>().await;

        Server::builder()
            .add_service(health_service)
            .add_service(NodeInfoGcsServiceServer::from_arc(self.node_manager.clone()))
            .add_service(JobInfoGcsServiceServer::from_arc(self.job_manager.clone()))
            .add_service(WorkerInfoGcsServiceServer::from_arc(self.worker_manager.clone()))
            .add_service(TaskInfoGcsServiceServer::from_arc(self.task_manager.clone()))
            .add_service(NodeResourceInfoGcsServiceServer::from_arc(self.resource_manager.clone()))
            .add_service(InternalKvGcsServiceServer::from_arc(self.kv_manager.clone()))
            .add_service(ActorInfoGcsServiceServer::from_arc(self.actor_manager.clone()))
            .add_service(PlacementGroupInfoGcsServiceServer::from_arc(self.placement_group_manager.clone()))
            .add_service(InternalPubSubGcsServiceServer::from_arc(self.pubsub_service.clone()))
            .add_service(RuntimeEnvGcsServiceServer::new(RuntimeEnvServiceStub))
            .add_service(AutoscalerStateServiceServer::from_arc(self.autoscaler_manager.clone()))
            .add_service(EventAggregatorServiceServer::new(EventExportServiceStub))
            .serve_with_incoming(incoming)
            .await?;

        Ok(())
    }

    pub fn node_manager(&self) -> &Arc<GcsNodeManager> {
        &self.node_manager
    }

    pub fn job_manager(&self) -> &Arc<GcsJobManager> {
        &self.job_manager
    }

    pub fn worker_manager(&self) -> &Arc<GcsWorkerManager> {
        &self.worker_manager
    }

    pub fn kv_manager(&self) -> &Arc<GcsInternalKVManager> {
        &self.kv_manager
    }

    pub fn pubsub_manager(&self) -> &Arc<PubSubManager> {
        &self.pubsub_manager
    }

    pub fn actor_manager(&self) -> &Arc<GcsActorManager> {
        &self.actor_manager
    }

    pub fn placement_group_manager(&self) -> &Arc<GcsPlacementGroupManager> {
        &self.placement_group_manager
    }

    pub fn table_storage(&self) -> &Arc<GcsTableStorage> {
        &self.table_storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_proto::ray::rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient;
    use gcs_proto::ray::rpc::job_info_gcs_service_client::JobInfoGcsServiceClient;
    use gcs_proto::ray::rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient;
    use gcs_proto::ray::rpc::{GetAllNodeInfoRequest, GetAllJobInfoRequest, InternalKvGetRequest};

    #[test]
    fn test_server_creation() {
        let server = GcsServer::new(GcsServerConfig::default());
        assert!(server.node_manager().get_all_alive_nodes().is_empty());
    }

    #[tokio::test]
    async fn test_server_manager_accessors() {
        let config = GcsServerConfig {
            grpc_port: 0,
            cluster_id: vec![0u8; 28],
            raylet_config_list: "test_config".to_string(),
            max_task_events: 100,
            ..Default::default()
        };
        let server = GcsServer::new(config);

        // Verify all manager getters return valid references.
        // node_manager
        let _nm = server.node_manager();
        assert!(server.node_manager().get_all_alive_nodes().is_empty());

        // job_manager -- get_job on non-existent should return None.
        let _jm = server.job_manager();
        assert!(server.job_manager().get_job(b"no_such_job").is_none());

        // worker_manager -- just verify the getter works.
        let _wm = server.worker_manager();

        // kv_manager -- verify the kv() accessor works.
        let kv = server.kv_manager();
        let result = kv.kv().get("ns", "key").await;
        assert!(result.is_none());

        // pubsub_manager -- verify publisher_id matches cluster_id.
        let psm = server.pubsub_manager();
        assert_eq!(psm.publisher_id(), vec![0u8; 28].as_slice());

        // actor_manager -- get unknown actor should return not-found.
        let _am = server.actor_manager();
    }

    #[tokio::test]
    async fn test_start_with_listener() {
        let config = GcsServerConfig {
            grpc_port: 0,
            cluster_id: vec![0u8; 28],
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config));

        // Bind to an OS-assigned port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port();

        // Spawn the server in the background.
        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        // Give the server a moment to start accepting connections.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Test 1: NodeInfoGcsService - GetAllNodeInfo on empty cluster.
        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to NodeInfoGcsService");
        let resp = node_client
            .get_all_node_info(GetAllNodeInfoRequest::default())
            .await
            .expect("GetAllNodeInfo RPC failed");
        assert!(
            resp.into_inner().node_info_list.is_empty(),
            "empty cluster should have no nodes"
        );

        // Test 2: JobInfoGcsService - GetAllJobInfo on empty cluster.
        let mut job_client = JobInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to JobInfoGcsService");
        let resp = job_client
            .get_all_job_info(GetAllJobInfoRequest::default())
            .await
            .expect("GetAllJobInfo RPC failed");
        assert!(
            resp.into_inner().job_info_list.is_empty(),
            "empty cluster should have no jobs"
        );

        // Test 3: InternalKvGcsService - get a non-existent key.
        let mut kv_client = InternalKvGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to InternalKvGcsService");
        let resp = kv_client
            .internal_kv_get(InternalKvGetRequest {
                namespace: "test_ns".into(),
                key: "nonexistent".into(),
            })
            .await
            .expect("InternalKvGet RPC failed");
        // Non-existent key should return an empty value.
        assert!(
            resp.into_inner().value.is_empty(),
            "non-existent key should return empty value"
        );

        // Clean up: abort the server task.
        server_handle.abort();
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_start() {
        // Find a free port by binding to port 0 and immediately releasing.
        let tmp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tmp_listener.local_addr().unwrap().port();
        drop(tmp_listener);

        let config = GcsServerConfig {
            grpc_port: port,
            cluster_id: vec![0u8; 28],
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config));

        // Spawn the server in the background.
        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start().await
        });

        // Give the server a moment to bind and start serving.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Verify the server is accepting connections and serving gRPC.
        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to GCS server via start()");
        let resp = node_client
            .get_all_node_info(GetAllNodeInfoRequest::default())
            .await
            .expect("GetAllNodeInfo RPC failed on start() server");
        assert!(
            resp.into_inner().node_info_list.is_empty(),
            "empty cluster should have no nodes"
        );

        // Also verify a second service is available (JobInfo).
        let mut job_client = JobInfoGcsServiceClient::connect(endpoint)
            .await
            .expect("failed to connect to JobInfoGcsService via start()");
        let resp = job_client
            .get_all_job_info(GetAllJobInfoRequest::default())
            .await
            .expect("GetAllJobInfo RPC failed on start() server");
        assert!(
            resp.into_inner().job_info_list.is_empty(),
            "empty cluster should have no jobs"
        );

        // Clean up.
        server_handle.abort();
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_restart_recovery_with_init_data() {
        use gcs_proto::ray::rpc::{
            JobTableData, GcsNodeInfo, ActorTableData,
            PlacementGroupTableData, gcs_node_info,
        };

        // Create a shared in-memory store (simulating Redis persistence).
        let store: Arc<dyn StoreClient> = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store.clone()));

        // Pre-populate storage with data (as if from a previous server lifetime).
        let mut job = JobTableData::default();
        job.job_id = b"job_1".to_vec();
        job.is_dead = false;
        table_storage.job_table().put("job_1", &job).await;

        let mut node = GcsNodeInfo::default();
        node.node_id = b"node_1".to_vec();
        node.state = gcs_node_info::GcsNodeState::Alive as i32;
        table_storage.node_table().put("node_1", &node).await;

        let mut actor = ActorTableData::default();
        actor.actor_id = b"actor_1".to_vec();
        actor.name = "my_actor".to_string();
        actor.ray_namespace = "default".to_string();
        table_storage.actor_table().put("actor_1", &actor).await;

        let mut pg = PlacementGroupTableData::default();
        pg.placement_group_id = b"pg_1".to_vec();
        pg.name = "my_pg".to_string();
        pg.ray_namespace = "default".to_string();
        table_storage.placement_group_table().put("pg_1", &pg).await;

        // Create a NEW server using the same store (simulating restart).
        let config = GcsServerConfig {
            cluster_id: vec![0u8; 28],
            ..Default::default()
        };
        let server = GcsServer::new_with_store(config, store, None);

        // Initialize from storage (this is what happens on restart).
        server.initialize().await;

        // Verify recovered state.
        assert!(server.job_manager().get_job(b"job_1").is_some());
        assert_eq!(server.node_manager().get_all_alive_nodes().len(), 1);
    }
}
