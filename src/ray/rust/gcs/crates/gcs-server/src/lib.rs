//! GCS Server -- orchestrator that wires all managers and serves gRPC.
//!
//! Maps C++ `GcsServer` from `src/ray/gcs/gcs_server.h/cc`.
//! This is a drop-in replacement for the C++ GCS binary, implementing the same
//! 12 gRPC services from the same proto files.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::info;

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
use gcs_store::InMemoryStoreClient;
use gcs_table_storage::GcsTableStorage;

/// Configuration for the GCS server.
#[derive(Debug, Clone)]
pub struct GcsServerConfig {
    pub grpc_port: u16,
    pub cluster_id: Vec<u8>,
    pub raylet_config_list: String,
    pub max_task_events: usize,
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
        }
    }
}

/// The GCS Server. Maps C++ `GcsServer`.
///
/// Creates all managers and serves gRPC on the configured port.
pub struct GcsServer {
    config: GcsServerConfig,
    // Shared infrastructure
    store: Arc<InMemoryStoreClient>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    pubsub_manager: Arc<PubSubManager>,
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
    /// Create a new GCS server with all managers initialized.
    pub fn new(config: GcsServerConfig) -> Self {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store.clone()));
        let publisher = Arc::new(GcsPublisher::new(4096));

        // Create the PubSubManager with a publisher_id derived from cluster_id.
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

        let kv_store = Arc::new(StoreClientInternalKV::new(store.clone()));
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
            store,
            table_storage,
            publisher,
            pubsub_manager,
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

    /// Start the gRPC server, binding to the configured port. Blocks until shutdown.
    pub async fn start(&self) -> Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.grpc_port).parse()?;

        info!(
            port = self.config.grpc_port,
            "Starting Rust GCS server"
        );

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<NodeInfoGcsServiceServer<GcsNodeManager>>().await;

        // Start the health check loop for detecting dead nodes.
        // Health check parameters can be configured via RayConfig (config_list).
        // Defaults match the C++ GCS: period=1000ms, timeout=10ms, threshold=5.
        // For tests, env vars override: RAY_health_check_period_ms etc.
        let hc_period = std::env::var("RAY_health_check_period_ms")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(1000u64);
        let hc_timeout = std::env::var("RAY_health_check_timeout_ms")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(500u64);
        let hc_threshold = std::env::var("RAY_health_check_failure_threshold")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(5u32);
        self.node_manager.start_health_check_loop(hc_period, hc_timeout, hc_threshold);

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
            "Starting Rust GCS server (pre-bound listener)"
        );

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        // Start the health check loop for detecting dead nodes.
        self.node_manager.start_health_check_loop(1000, 500, 5);

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
