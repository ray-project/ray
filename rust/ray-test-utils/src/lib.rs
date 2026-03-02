// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Shared test helpers for Ray Rust crates.

use std::net::SocketAddr;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub mod generators;
pub mod mock_clients;
pub mod proto_builders;
pub mod wait;

// Re-export generators at crate root for backward compatibility.
pub use generators::{random_actor_id, random_job_id};

/// Initialize tracing for tests.
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("debug")
        .try_init();
}

/// Create a temporary directory for test data.
pub fn test_temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}

/// A test GCS server that can be started and stopped.
pub struct TestGcsServer {
    pub addr: SocketAddr,
    pub shutdown_tx: oneshot::Sender<()>,
    pub join_handle: JoinHandle<()>,
}

/// Start a test GCS server on a random port with in-memory storage.
pub async fn start_test_gcs_server() -> TestGcsServer {
    start_test_gcs_server_with_config(ray_gcs::server::GcsServerConfig {
        port: 0,
        ..Default::default()
    })
    .await
}

/// Start a test GCS server with a custom config. Port 0 means random.
pub async fn start_test_gcs_server_with_config(
    _config: ray_gcs::server::GcsServerConfig,
) -> TestGcsServer {
    use ray_gcs::grpc_services::*;
    use ray_proto::ray::rpc;

    let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let join_handle = tokio::spawn(async move {
        // We need access to initialized managers, but GcsServer::run consumes them.
        // So we manually initialize and build the server.

        // Initialize via the server's initialize method (it's private, so we use a workaround).
        // Actually, GcsServer doesn't expose initialize() publicly. Let's build managers directly.
        use ray_gcs::store_client::{InMemoryInternalKV, InMemoryStoreClient};
        use ray_gcs::table_storage::GcsTableStorage;
        use std::sync::Arc;

        let store_client = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store_client));
        let internal_kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(ray_gcs::kv_manager::GcsInternalKVManager::new(
            internal_kv,
            String::new(),
        ));

        let node_manager = Arc::new(ray_gcs::node_manager::GcsNodeManager::new(
            table_storage.clone(),
        ));
        let resource_manager = Arc::new(ray_gcs::resource_manager::GcsResourceManager::new());
        let job_manager = Arc::new(ray_gcs::job_manager::GcsJobManager::new(
            table_storage.clone(),
        ));
        let actor_manager = Arc::new(ray_gcs::actor_manager::GcsActorManager::new(
            table_storage.clone(),
        ));
        let worker_manager = Arc::new(ray_gcs::worker_manager::GcsWorkerManager::new(
            table_storage.clone(),
        ));
        let placement_group_manager =
            Arc::new(ray_gcs::placement_group_manager::GcsPlacementGroupManager::new(
                table_storage.clone(),
            ));
        let task_manager = Arc::new(ray_gcs::task_manager::GcsTaskManager::new(None));
        let pubsub_handler = Arc::new(ray_gcs::pubsub_handler::InternalPubSubHandler::new());
        let autoscaler_state_manager =
            Arc::new(ray_gcs::autoscaler_state_manager::GcsAutoscalerStateManager::new(
                String::new(),
                Arc::clone(&node_manager),
                Arc::clone(&resource_manager),
            ));

        // Set cluster ID (random 28 bytes, matching C++ ClusterID format)
        let cluster_id: Vec<u8> = (0..28).map(|_| rand::random::<u8>()).collect();
        node_manager.set_cluster_id(cluster_id);

        // Initialize managers
        node_manager.initialize().await.unwrap();
        job_manager.initialize().await.unwrap();
        actor_manager.initialize().await.unwrap();
        placement_group_manager.initialize().await.unwrap();

        // Build services
        let job_svc = JobInfoGcsServiceImpl { job_manager };
        let node_mgr_for_autoscaler = Arc::clone(&node_manager);
        let node_svc = NodeInfoGcsServiceImpl { node_manager };
        let actor_svc = ActorInfoGcsServiceImpl { actor_manager };
        let kv_svc = InternalKVGcsServiceImpl { kv_manager };
        let worker_svc = WorkerInfoGcsServiceImpl { worker_manager };
        let pg_svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager,
        };
        let resource_svc = NodeResourceInfoGcsServiceImpl { resource_manager };
        let runtime_env_svc = RuntimeEnvGcsServiceImpl;
        let publisher_id: Vec<u8> = (0..28).map(|_| rand::random::<u8>()).collect();
        let pubsub_svc = InternalPubSubGcsServiceImpl {
            pubsub_handler,
            publisher_id,
        };
        let task_svc = TaskInfoGcsServiceImpl { task_manager };
        let autoscaler_svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager,
            node_manager: node_mgr_for_autoscaler,
        };

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<rpc::job_info_gcs_service_server::JobInfoGcsServiceServer<
                JobInfoGcsServiceImpl,
            >>()
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _ = addr_tx.send(addr);

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
            .serve_with_incoming_shutdown(incoming, async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let addr = addr_rx.await.unwrap();
    // Give the server a moment to be ready
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    TestGcsServer {
        addr,
        shutdown_tx,
        join_handle,
    }
}

/// A test Raylet server that can be started and stopped.
pub struct TestRayletServer {
    pub addr: SocketAddr,
    pub shutdown_tx: oneshot::Sender<()>,
    pub join_handle: JoinHandle<()>,
}

/// Start a test Raylet server on a random port.
pub async fn start_test_raylet_server() -> TestRayletServer {
    use ray_proto::ray::rpc;
    use ray_raylet::grpc_service::NodeManagerServiceImpl;
    use ray_raylet::node_manager::{NodeManager, RayletConfig};
    use std::collections::HashMap;
    use std::sync::Arc;

    let config = RayletConfig {
        node_ip_address: "127.0.0.1".to_string(),
        port: 0,
        object_store_socket: String::new(),
        gcs_address: String::new(),
        log_dir: None,
        ray_config: ray_common::config::RayConfig::default(),
        node_id: String::new(),
        resources: HashMap::new(),
        labels: HashMap::new(),
        session_name: String::new(),
    };

    let nm = Arc::new(NodeManager::new(config));
    let svc = NodeManagerServiceImpl {
        node_manager: Arc::clone(&nm),
    };

    let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let join_handle = tokio::spawn(async move {
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<rpc::node_manager_service_server::NodeManagerServiceServer<
                NodeManagerServiceImpl,
            >>()
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _ = addr_tx.send(addr);

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(
                rpc::node_manager_service_server::NodeManagerServiceServer::new(svc),
            )
            .add_service(health_service)
            .serve_with_incoming_shutdown(incoming, async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let addr = addr_rx.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    TestRayletServer {
        addr,
        shutdown_tx,
        join_handle,
    }
}
