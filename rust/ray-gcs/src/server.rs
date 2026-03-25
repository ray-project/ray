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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
use crate::recovery::{GcsRecovery, RecoveryConfig};
use crate::resource_manager::GcsResourceManager;
use crate::store_client::{
    InMemoryInternalKV, InMemoryStoreClient, RedisStoreClient, StoreClient,
    StoreClientInternalKV,
};
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
    /// Cluster auth token. When set, all gRPC requests must include a matching Bearer token.
    pub auth_token: Option<String>,
    /// Metrics agent port for event aggregator gRPC connection.
    /// C++ parity: `EventAggregatorClient::Connect(metrics_agent_port)`.
    /// When `Some(port)`, actor events are sent via gRPC to `127.0.0.1:<port>`.
    /// When `None`, exporter defers to late-init via head-node registration.
    /// No file fallback — events are simply not exported if no port is available.
    pub metrics_agent_port: Option<u16>,
}

impl Default for GcsServerConfig {
    fn default() -> Self {
        Self {
            port: ray_common::constants::DEFAULT_GCS_PORT,
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
            auth_token: None,
            metrics_agent_port: None,
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
    worker_client: Option<Arc<dyn crate::actor_scheduler::CoreWorkerClient>>,
    raylet_client: Option<Arc<dyn crate::actor_scheduler::RayletClient>>,
    /// Handle to the periodic actor-event export task.
    /// C++ parity: the recorder has explicit start/stop lifecycle;
    /// this handle allows `stop()` to abort the periodic loop.
    ///
    /// Shared via Arc<Mutex<>> so the late-init callback can store the handle.
    /// C++ parity: StartExportingEvents() is called only after exporter readiness
    /// succeeds — the loop must NOT run before a sink is attached.
    periodic_flush_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// C++ parity: `metrics_exporter_initialized_` — atomic flag ensuring
    /// `InitMetricsExporter` runs exactly once (startup OR late head-node path).
    metrics_exporter_initialized: Arc<std::sync::atomic::AtomicBool>,
    /// Flush interval for the periodic export loop (milliseconds).
    /// Stored at init time so the late-init callback can use the same value.
    flush_interval_ms: u64,
}

impl GcsServer {
    pub fn new(config: GcsServerConfig) -> Self {
        let storage_type = if config.redis_address.is_some() {
            StorageType::Redis
        } else {
            StorageType::InMemory
        };
        let flush_interval_ms = config.ray_config.ray_events_report_interval_ms;

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
            worker_client: None,
            raylet_client: None,
            periodic_flush_handle: Arc::new(parking_lot::Mutex::new(None)),
            metrics_exporter_initialized: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            flush_interval_ms,
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
                let default_addr = format!(
                    "redis://127.0.0.1:{}",
                    ray_common::constants::DEFAULT_GCS_PORT
                );
                let addr = self
                    .config
                    .redis_address
                    .as_deref()
                    .unwrap_or(&default_addr);
                tracing::info!(address = addr, "Connecting to Redis");
                Arc::new(
                    RedisStoreClient::new(addr, String::new())
                        .map_err(|e| anyhow::anyhow!("Redis connection failed: {}", e))?,
                )
            }
        };

        // 2. Create table storage
        let table_storage = Arc::new(GcsTableStorage::new(store_client.clone()));

        // 3. Create KV manager — use store-client-backed KV for Redis (GCS-1 fix),
        //    in-memory only for explicit in-memory mode.
        let internal_kv: Arc<dyn crate::store_client::InternalKVInterface> =
            match self.storage_type {
                StorageType::Redis => Arc::new(StoreClientInternalKV::new(store_client.clone())),
                StorageType::InMemory => Arc::new(InMemoryInternalKV::new()),
            };
        let kv_manager = Arc::new(GcsInternalKVManager::new(
            internal_kv,
            self.config.raylet_config_list.clone().unwrap_or_default(),
        ));

        // 4. Create managers in dependency order
        let node_manager = Arc::new(GcsNodeManager::new(table_storage.clone()));
        let resource_manager = Arc::new(GcsResourceManager::new());
        let job_manager = Arc::new(GcsJobManager::new(table_storage.clone()));
        let raylet_client: Arc<dyn crate::actor_scheduler::RayletClient> =
            Arc::new(GrpcRayletClient);
        // Build ActorExportConfig from RayConfig (C++ parity: gcs_server.cc initialization)
        let export_api_config = ray_observability::export::ExportApiConfig {
            enable_export_api_write: self.config.ray_config.enable_export_api_write,
            enable_export_api_write_config: self
                .config
                .ray_config
                .enable_export_api_write_config
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.trim().to_string())
                .collect(),
            enable_ray_event: self.config.ray_config.enable_ray_event,
            log_dir: self.config.log_dir.as_ref().map(std::path::PathBuf::from),
        };
        let actor_export_config = crate::actor_manager::ActorExportConfig {
            export_api_config,
        };
        let mut actor_manager_inner =
            GcsActorManager::with_export_config(table_storage.clone(), actor_export_config);
        actor_manager_inner.set_node_manager_and_raylet_client(
            Arc::clone(&node_manager),
            Arc::clone(&raylet_client),
        );

        // Wire live runtime sink and periodic flush for actor events.
        //
        // C++ parity (gcs_server.cc InitMetricsExporter):
        // - If metrics_agent_port known at startup → connect immediately
        // - If not known → defer to head-node registration callback
        // - Atomic flag prevents double-init from both paths
        // - On failure, log error, events NOT exported — no file fallback
        if self.config.ray_config.enable_ray_event {
            let node_id = self
                .config
                .node_id
                .as_deref()
                .and_then(|h| hex::decode(h).ok())
                .unwrap_or_default();
            let session_name = self
                .config
                .session_name
                .clone()
                .unwrap_or_default();

            // C++ parity: if port known at startup, initialize exporter now.
            if let Some(port) = self.config.metrics_agent_port {
                // C++ parity: atomic exchange(true) — first caller wins.
                if !self.metrics_exporter_initialized.swap(
                    true,
                    std::sync::atomic::Ordering::AcqRel,
                ) {
                    match ray_observability::export::GrpcEventAggregatorSink::connect(
                        port,
                        node_id,
                        session_name,
                    )
                    .await
                    {
                        Ok(sink) => {
                            actor_manager_inner
                                .event_exporter()
                                .set_sink(Box::new(sink));
                            // C++ parity: StartExportingEvents() only after
                            // readiness succeeds — start periodic loop now.
                            Self::start_periodic_flush_loop(
                                actor_manager_inner.event_exporter(),
                                self.flush_interval_ms,
                                &self.periodic_flush_handle,
                            );
                            tracing::info!(
                                port,
                                "Actor event gRPC sink connected to EventAggregatorService"
                            );
                        }
                        Err(e) => {
                            // C++ parity: on failure, log error, events not exported.
                            // No file fallback. No periodic loop started.
                            tracing::error!(
                                "Failed to connect to event aggregator service at port {}: {}. \
                                 Events and metrics will not be exported.",
                                port,
                                e
                            );
                        }
                    }
                }
            } else {
                // C++ parity: port not known at startup. Will be initialized
                // later when the head node registers and reports its
                // metrics_agent_port (see node_added_listener below).
                tracing::info!(
                    "enable_ray_event=true but no metrics_agent_port at startup. \
                     Exporter will initialize when head node registers."
                );
            }
        }

        let actor_manager = Arc::new(actor_manager_inner);
        let worker_manager = Arc::new(GcsWorkerManager::new(table_storage.clone()));
        let placement_group_manager =
            Arc::new(GcsPlacementGroupManager::new(table_storage.clone()));
        let task_manager = Arc::new(GcsTaskManager::new(None));
        let pubsub_handler = Arc::new(InternalPubSubHandler::new());

        // 4b. Create actor scheduler and wire into actor manager
        let worker_client: Arc<dyn crate::actor_scheduler::CoreWorkerClient> =
            Arc::new(GrpcCoreWorkerClient);
        let actor_scheduler = Arc::new(GcsActorScheduler::new(
            Arc::clone(&node_manager),
            Arc::clone(&raylet_client),
            Arc::clone(&worker_client),
        ));
        self.worker_client = Some(Arc::clone(&worker_client));
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
                    node_info
                        .node_id
                        .as_slice()
                        .try_into()
                        .unwrap_or(&[0u8; 28]),
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
                    node_info
                        .node_id
                        .as_slice()
                        .try_into()
                        .unwrap_or(&[0u8; 28]),
                );
                resource_mgr.on_node_add(&node_id);
            }));

            // C++ parity (gcs_server.cc lines 822-832): late-init metrics exporter
            // when the head node registers and reports its metrics_agent_port.
            // Only runs when enable_ray_event=true and exporter not yet initialized.
            if self.config.ray_config.enable_ray_event {
                let init_flag = Arc::clone(&self.metrics_exporter_initialized);
                let exporter = Arc::clone(actor_manager.event_exporter());
                let flush_handle = Arc::clone(&self.periodic_flush_handle);
                let interval_ms = self.flush_interval_ms;
                let node_id_bytes = self
                    .config
                    .node_id
                    .as_deref()
                    .and_then(|h| hex::decode(h).ok())
                    .unwrap_or_default();
                let sess_name = self
                    .config
                    .session_name
                    .clone()
                    .unwrap_or_default();
                node_manager.add_node_added_listener(Box::new(move |node_info| {
                    // C++ parity (gcs_server.cc:822-827): check preconditions
                    // in order BEFORE consuming the one-shot init flag.
                    // 1. Only head node triggers init
                    if !node_info.is_head_node {
                        return;
                    }
                    // 2. Already initialized? (read-only check, optimization)
                    if init_flag.load(std::sync::atomic::Ordering::Acquire) {
                        return;
                    }
                    // 3. Port must be > 0. If zero, do NOT consume the flag —
                    //    a later registration with a valid port must still work.
                    //    C++ (gcs_server.cc:826): logs info and returns without
                    //    calling InitMetricsExporter, so exchange(true) never fires.
                    let port = node_info.metrics_agent_port;
                    if port <= 0 {
                        tracing::info!(
                            "Head node registered but metrics_agent_port={}. \
                             Metrics agent not available. To enable metrics, install \
                             Ray with dashboard support: `pip install 'ray[default]'`.",
                            port
                        );
                        return;
                    }
                    // 4. All preconditions met — now claim the one-shot slot.
                    //    C++ (gcs_server.cc:941): exchange(true) inside InitMetricsExporter.
                    if init_flag.swap(true, std::sync::atomic::Ordering::AcqRel) {
                        // Another thread/call won the race
                        return;
                    }
                    // Spawn async task to connect gRPC sink and start periodic flush.
                    // C++ parity: StartExportingEvents() only after readiness succeeds.
                    let exporter = Arc::clone(&exporter);
                    let flush_handle = Arc::clone(&flush_handle);
                    let nid = node_id_bytes.clone();
                    let sn = sess_name.clone();
                    tokio::spawn(async move {
                        match ray_observability::export::GrpcEventAggregatorSink::connect(
                            port as u16, nid, sn,
                        )
                        .await
                        {
                            Ok(sink) => {
                                exporter.set_sink(Box::new(sink));
                                // Start periodic flush only after sink is attached.
                                Self::start_periodic_flush_loop(
                                    &exporter,
                                    interval_ms,
                                    &flush_handle,
                                );
                                tracing::info!(
                                    port,
                                    "Late-init: actor event gRPC sink connected via head-node registration"
                                );
                            }
                            Err(e) => {
                                // No sink, no periodic loop. Events stay buffered.
                                tracing::error!(
                                    "Late-init: failed to connect to event aggregator at port {}: {}. \
                                     Events will not be exported.",
                                    port, e
                                );
                            }
                        }
                    });
                }));
            }
        }

        // 4e. Wire worker death → actor death cascade
        {
            let actor_mgr = Arc::clone(&actor_manager);
            worker_manager.add_dead_listener(Box::new(move |worker_data| {
                if let Some(ref addr) = worker_data.worker_address {
                    let node_id = ray_common::id::NodeID::from_binary(
                        addr.node_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                    );
                    let worker_id = ray_common::id::WorkerID::from_binary(
                        addr.worker_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                    );
                    actor_mgr.on_worker_dead(&node_id, &worker_id);
                }
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

        // 7. Get or create cluster ID (persisted to Redis for HA)
        let recovery = GcsRecovery::new(
            RecoveryConfig {
                enable_recovery: self.storage_type == StorageType::Redis,
                ..Default::default()
            },
            store_client,
        );
        let cluster_id = recovery.get_or_create_cluster_id().await?;
        node_manager.set_cluster_id(cluster_id.clone());
        tracing::info!(cluster_id = hex::encode(&cluster_id), "Cluster ID set");

        // 8. Initialize from persistent storage
        node_manager.initialize().await?;
        job_manager.initialize().await?;
        actor_manager.initialize().await?;
        placement_group_manager.initialize().await?;

        // 4f. Wire node-draining listener → resource_manager (C++ parity: SetNodeDraining
        // listener updates scheduler-visible draining state).
        {
            let resource_mgr = Arc::clone(&resource_manager);
            node_manager.add_node_draining_listener(Box::new(move |node_id, is_draining, deadline_ms| {
                resource_mgr.set_node_draining(node_id, is_draining, deadline_ms);
            }));
        }

        // Store references
        self.table_storage = Some(table_storage);
        self.job_manager = Some(job_manager);
        self.node_manager = Some(node_manager);
        self.actor_manager = Some(actor_manager);
        self.worker_manager = Some(worker_manager);
        self.resource_manager = Some(resource_manager);
        self.raylet_client = Some(raylet_client);
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
            kv_manager: self.kv_manager.as_ref().map(Arc::clone),
            worker_client: self.worker_client.as_ref().map(Arc::clone),
        };
        let node_svc = NodeInfoGcsServiceImpl {
            node_manager: Arc::clone(self.node_manager.as_ref().unwrap()),
            autoscaler_state_manager: self.autoscaler_state_manager.as_ref().map(Arc::clone),
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
            placement_group_manager: Arc::clone(self.placement_group_manager.as_ref().unwrap()),
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
            autoscaler_state_manager: Arc::clone(self.autoscaler_state_manager.as_ref().unwrap()),
            node_manager: Arc::clone(self.node_manager.as_ref().unwrap()),
            placement_group_manager: Arc::clone(self.placement_group_manager.as_ref().unwrap()),
            raylet_client: self.raylet_client.as_ref().map(Arc::clone),
            actor_manager: self.actor_manager.as_ref().map(Arc::clone),
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
        let listener = tokio::net::TcpListener::bind(format!(
            "{}:{}",
            ray_common::constants::DEFAULT_SERVER_BIND_ADDRESS,
            self.config.port
        ))
        .await?;
        let bound_port = listener.local_addr()?.port();
        tracing::info!(port = bound_port, "GCS gRPC server listening");

        // Persist port file
        self.persist_port(bound_port)?;

        // Build the auth interceptor from config.
        let auth_mode = if self.config.auth_token.is_some() {
            ray_rpc::auth::AuthenticationMode::ClusterAuth
        } else {
            ray_rpc::auth::AuthenticationMode::Disabled
        };
        let auth = ray_rpc::auth::AuthInterceptor::new(auth_mode, self.config.auth_token.clone());

        // Serve with TCP_NODELAY to disable Nagle's algorithm (~40ms latency fix).
        let incoming = {
            let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
            NodelaySetter(stream)
        };
        tonic::transport::Server::builder()
            .initial_connection_window_size(2 * 1024 * 1024) // 2MB (default 64KB)
            .initial_stream_window_size(1024 * 1024) // 1MB (default 64KB)
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(20)))
            .add_service(rpc::job_info_gcs_service_server::JobInfoGcsServiceServer::with_interceptor(job_svc, auth.clone()))
            .add_service(rpc::node_info_gcs_service_server::NodeInfoGcsServiceServer::with_interceptor(node_svc, auth.clone()))
            .add_service(rpc::actor_info_gcs_service_server::ActorInfoGcsServiceServer::with_interceptor(actor_svc, auth.clone()))
            .add_service(rpc::internal_kv_gcs_service_server::InternalKvGcsServiceServer::with_interceptor(kv_svc, auth.clone()))
            .add_service(rpc::worker_info_gcs_service_server::WorkerInfoGcsServiceServer::with_interceptor(worker_svc, auth.clone()))
            .add_service(rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsServiceServer::with_interceptor(pg_svc, auth.clone()))
            .add_service(rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsServiceServer::with_interceptor(resource_svc, auth.clone()))
            .add_service(rpc::runtime_env_gcs_service_server::RuntimeEnvGcsServiceServer::with_interceptor(runtime_env_svc, auth.clone()))
            .add_service(rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsServiceServer::with_interceptor(pubsub_svc, auth.clone()))
            .add_service(rpc::task_info_gcs_service_server::TaskInfoGcsServiceServer::with_interceptor(task_svc, auth.clone()))
            .add_service(rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateServiceServer::with_interceptor(autoscaler_svc, auth.clone()))
            .add_service(health_service)
            .serve_with_incoming_shutdown(incoming, shutdown_signal())
            .await?;

        tracing::info!("GCS server shutting down");

        // C++ parity: GcsServer::Stop() calls ray_event_recorder_->StopExportingEvents()
        // which disables new events, waits for in-flight export, and does a final flush.
        self.stop();

        Ok(())
    }

    /// Start the periodic actor-event flush loop.
    ///
    /// C++ parity: `StartExportingEvents()` is called only after exporter
    /// readiness succeeds (gcs_server.cc:958). This method must only be
    /// called after a sink is successfully attached. The shared handle slot
    /// prevents starting multiple loops.
    fn start_periodic_flush_loop(
        exporter: &Arc<ray_observability::export::EventExporter>,
        flush_interval_ms: u64,
        handle_slot: &Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    ) {
        let mut slot = handle_slot.lock();
        if slot.is_some() {
            // Loop already running — do not start a second one.
            return;
        }
        let exp = Arc::clone(exporter);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                flush_interval_ms,
            ));
            loop {
                ticker.tick().await;
                exp.flush();
            }
        });
        *slot = Some(handle);
        tracing::info!(
            interval_ms = flush_interval_ms,
            "Periodic actor-event flush loop started"
        );
    }

    /// Stop the GCS server — flush remaining events and clean up.
    ///
    /// C++ parity: `GcsServer::Stop()` in `gcs_server.cc` (lines 324–331).
    /// Calls `ray_event_recorder_->StopExportingEvents()` for graceful shutdown
    /// with final flush before stopping IO contexts.
    pub fn stop(&mut self) {
        tracing::info!("GCS server stopping — flushing actor event exporter");

        // C++ parity: StopExportingEvents() — disable new events + final flush
        if let Some(ref actor_manager) = self.actor_manager {
            actor_manager.event_exporter().shutdown();
        }

        // C++ parity: the recorder lifecycle includes stopping the periodic export loop.
        // Abort the background tokio task so it no longer wakes up after shutdown.
        if let Some(handle) = self.periodic_flush_handle.lock().take() {
            handle.abort();
            tracing::info!("Periodic actor-event flush task aborted");
        }

        tracing::info!("GCS server stop complete");
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

    pub fn pubsub_handler(&self) -> Option<&Arc<InternalPubSubHandler>> {
        self.pubsub_handler.as_ref()
    }

    pub fn health_check_manager(&self) -> Option<&Arc<GcsHealthCheckManager>> {
        self.health_check_manager.as_ref()
    }

    pub fn autoscaler_state_manager(&self) -> Option<&Arc<GcsAutoscalerStateManager>> {
        self.autoscaler_state_manager.as_ref()
    }

    /// Whether the metrics exporter has been initialized (startup or late head-node path).
    /// C++ parity: `metrics_exporter_initialized_` atomic flag.
    pub fn metrics_exporter_initialized(&self) -> bool {
        self.metrics_exporter_initialized
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether the periodic flush loop is currently running.
    pub fn periodic_flush_running(&self) -> bool {
        self.periodic_flush_handle.lock().is_some()
    }

    pub fn storage_type(&self) -> StorageType {
        self.storage_type
    }

    /// Gracefully drain all in-flight operations.
    ///
    /// This should be called before shutdown to allow pending RPCs
    /// to complete and notify subscribers that GCS is going away.
    pub async fn graceful_drain(&self) {
        tracing::info!("GCS server draining...");

        // Mark all alive nodes as draining if we're shutting down permanently.
        // In a failover scenario, the new GCS instance will handle them.
        if let Some(ref node_manager) = self.node_manager {
            let alive = node_manager.get_all_alive_nodes();
            tracing::info!(
                num_alive_nodes = alive.len(),
                "Notifying alive nodes of GCS shutdown"
            );
        }

        tracing::info!("GCS server drain complete");
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

/// Stream wrapper that sets TCP_NODELAY on every accepted connection.
struct NodelaySetter(tokio_stream::wrappers::TcpListenerStream);

impl tokio_stream::Stream for NodelaySetter {
    type Item = Result<tokio::net::TcpStream, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_next(cx) {
            Poll::Ready(Some(Ok(stream))) => {
                let _ = stream.set_nodelay(true);
                Poll::Ready(Some(Ok(stream)))
            }
            other => other,
        }
    }
}

#[cfg(test)]
#[allow(clippy::needless_update)]
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
        let aid = ray_common::id::ActorID::from_binary(actor_id.as_slice().try_into().unwrap());
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
        let nid = ray_common::id::NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        node_mgr.handle_unregister_node(&node_id, None).await.unwrap();

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

    // ---- Additional tests ported from gcs_server_rpc_test.cc ----

    /// Port of TestJobInfo — add and finish a job.
    #[tokio::test]
    async fn test_job_info_lifecycle() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let job_mgr = server.job_manager().unwrap();

        // Add a job
        let job_data = rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            is_dead: false,
            ..Default::default()
        };
        job_mgr.handle_add_job(job_data).await.unwrap();
        assert_eq!(job_mgr.num_running_jobs(), 1);

        // Mark job finished
        job_mgr
            .handle_mark_job_finished(&[1, 0, 0, 0])
            .await
            .unwrap();
        assert_eq!(job_mgr.num_running_jobs(), 0);
    }

    /// Port of TestNodeInfo — register and unregister nodes.
    #[tokio::test]
    async fn test_node_info_lifecycle() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let node_mgr = server.node_manager().unwrap();

        // Register a node
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                state: 0, // ALIVE
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                ..Default::default()
            })
            .await
            .unwrap();

        let all_nodes = node_mgr.handle_get_all_node_info();
        assert_eq!(all_nodes.len(), 1);
        assert_eq!(all_nodes[0].state, 0); // ALIVE

        // Unregister node
        node_mgr.handle_unregister_node(&node_id, None).await.unwrap();

        let all_nodes = node_mgr.handle_get_all_node_info();
        assert_eq!(all_nodes.len(), 1);
        assert_eq!(all_nodes[0].state, 1); // DEAD
    }

    /// Port of TestWorkerInfo — report worker failure and add worker info.
    #[tokio::test]
    async fn test_worker_info_lifecycle() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let worker_mgr = server.worker_manager().unwrap();

        // Report worker failure
        let mut wid1 = vec![0u8; 28];
        wid1[0] = 1;
        let failure_data = rpc::WorkerTableData {
            worker_address: Some(rpc::Address {
                worker_id: wid1,
                node_id: vec![0u8; 28],
                ip_address: "127.0.0.1".to_string(),
                port: 5566,
                ..Default::default()
            }),
            ..Default::default()
        };
        worker_mgr
            .handle_report_worker_failure(failure_data)
            .await
            .unwrap();

        let (all, _, _) = worker_mgr
            .handle_get_all_worker_info(None, false, false)
            .await
            .unwrap();
        assert_eq!(all.len(), 1);

        // Add a worker info
        let mut wid2 = vec![0u8; 28];
        wid2[0] = 2;
        let worker_data = rpc::WorkerTableData {
            worker_address: Some(rpc::Address {
                worker_id: wid2,
                node_id: vec![0u8; 28],
                ..Default::default()
            }),
            ..Default::default()
        };
        worker_mgr
            .handle_add_worker_info(worker_data)
            .await
            .unwrap();

        let (all, _, _) = worker_mgr
            .handle_get_all_worker_info(None, false, false)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
    }

    /// Port of TestJobGarbageCollection — multiple jobs, finish triggers GC.
    #[tokio::test]
    async fn test_job_garbage_collection() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let job_mgr = server.job_manager().unwrap();

        // Add two jobs
        for i in 1..=2u8 {
            let job_data = rpc::JobTableData {
                job_id: vec![i, 0, 0, 0],
                is_dead: false,
                ..Default::default()
            };
            job_mgr.handle_add_job(job_data).await.unwrap();
        }
        assert_eq!(job_mgr.num_running_jobs(), 2);

        // Finish first job
        job_mgr
            .handle_mark_job_finished(&[1, 0, 0, 0])
            .await
            .unwrap();
        assert_eq!(job_mgr.num_running_jobs(), 1);

        // Finish second job
        job_mgr
            .handle_mark_job_finished(&[2, 0, 0, 0])
            .await
            .unwrap();
        assert_eq!(job_mgr.num_running_jobs(), 0);
    }

    /// Port of TestNodeInfoFilters — register multiple nodes, kill one, verify state queries.
    #[tokio::test]
    async fn test_node_info_filters() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let node_mgr = server.node_manager().unwrap();

        // Register 3 nodes
        for i in 1..=3u8 {
            let mut node_id = vec![0u8; 28];
            node_id[0] = i;
            node_mgr
                .handle_register_node(rpc::GcsNodeInfo {
                    node_id,
                    state: 0,
                    node_manager_address: format!("127.0.0.{}", i),
                    node_manager_port: 10000 + i as i32,
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        assert_eq!(node_mgr.num_alive_nodes(), 3);

        // Kill node 3
        let mut dead_node_id = vec![0u8; 28];
        dead_node_id[0] = 3;
        node_mgr
            .handle_unregister_node(&dead_node_id, None)
            .await
            .unwrap();

        // Get all nodes — should have 3 total (2 alive + 1 dead)
        let all_nodes = node_mgr.handle_get_all_node_info();
        assert_eq!(all_nodes.len(), 3);

        // Count alive and dead
        let alive_count = all_nodes.iter().filter(|n| n.state == 0).count();
        let dead_count = all_nodes.iter().filter(|n| n.state == 1).count();
        assert_eq!(alive_count, 2);
        assert_eq!(dead_count, 1);

        // Verify dead node has correct ID
        let dead_node = all_nodes.iter().find(|n| n.state == 1).unwrap();
        assert_eq!(dead_node.node_id, dead_node_id);
    }

    /// Test that multiple manager types work together via the server.
    #[tokio::test]
    async fn test_multi_manager_integration() {
        let config = GcsServerConfig::default();
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        // Add a job
        let job_mgr = server.job_manager().unwrap();
        job_mgr
            .handle_add_job(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                ..Default::default()
            })
            .await
            .unwrap();

        // Register a node
        let node_mgr = server.node_manager().unwrap();
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        // Add a worker
        let worker_mgr = server.worker_manager().unwrap();
        worker_mgr
            .handle_add_worker_info(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    worker_id: vec![1, 2, 3],
                    node_id: node_id.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        // Add task events
        let task_mgr = server.task_manager().unwrap();
        let mut tid = vec![0u8; 24];
        tid[0] = 1;
        task_mgr.handle_add_task_event_data(rpc::TaskEventData {
            events_by_task: vec![rpc::TaskEvents {
                task_id: tid,
                attempt_number: 0,
                ..Default::default()
            }],
            ..Default::default()
        });

        // Verify all managers have data
        assert_eq!(job_mgr.num_running_jobs(), 1);
        assert_eq!(node_mgr.num_alive_nodes(), 1);
        assert_eq!(
            worker_mgr
                .handle_get_all_worker_info(None, false, false)
                .await
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(task_mgr.num_events(), 1);
    }

    // === Round 14: Live runtime recorder lifecycle parity tests ===

    /// Helper: start a mock EventAggregatorService and return (port, received_count, server_handle).
    async fn start_mock_aggregator() -> (
        u16,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::atomic::AtomicUsize;
        use ray_proto::ray::rpc::events::{
            event_aggregator_service_server::{EventAggregatorService, EventAggregatorServiceServer},
            AddEventsRequest, AddEventsReply,
        };

        let received = Arc::new(AtomicUsize::new(0));
        let rc = received.clone();

        struct MockAgg(Arc<AtomicUsize>);

        #[tonic::async_trait]
        impl EventAggregatorService for MockAgg {
            async fn add_events(
                &self,
                request: tonic::Request<AddEventsRequest>,
            ) -> Result<tonic::Response<AddEventsReply>, tonic::Status> {
                if let Some(ref data) = request.into_inner().events_data {
                    self.0.fetch_add(data.events.len(), std::sync::atomic::Ordering::Relaxed);
                }
                Ok(tonic::Response::new(AddEventsReply { status: None }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let svc = tonic::transport::Server::builder()
            .add_service(EventAggregatorServiceServer::new(MockAgg(rc)));
        let handle = tokio::spawn(async move {
            svc.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        (port, received, handle)
    }

    #[tokio::test]
    async fn test_gcs_server_shutdown_flushes_actor_events() {
        // C++ GcsServer::Stop() calls ray_event_recorder_->StopExportingEvents()
        // which performs a final flush of remaining buffered events via gRPC.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_shutdown_flush".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 1],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![1u8; 16],
                name: "shutdown_flush_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        assert!(
            actor_mgr.event_exporter().buffer_len() > 0,
            "events must be buffered before shutdown"
        );

        server.stop();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            0,
            "live server stop() must flush all remaining buffered actor events"
        );
        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "mock aggregator must have received events via gRPC after shutdown flush"
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_gcs_server_shutdown_rejects_new_actor_events_after_shutdown_boundary() {
        // C++ StopExportingEvents() sets enabled_=false FIRST, then flushes.
        // After stop(), new AddEvents calls are rejected.
        //
        // This test proves the live server stop path prevents new events
        // from being buffered after shutdown.

        let tmp = tempfile::tempdir().unwrap();
        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_shutdown_boundary".to_string()),
            node_id: Some("aabbccdd".to_string()),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());

        // Stop the server
        server.stop();

        // After stop(), manually adding events to the exporter must be rejected
        let event = ray_observability::events::RayEvent::new(
            ray_observability::events::EventSourceType::Gcs,
            ray_observability::events::EventSeverity::Info,
            "AFTER_SHUTDOWN",
            "should be rejected",
        )
        .with_field("actor_id", "deadbeef")
        .with_field("event_kind", "lifecycle");
        actor_mgr.event_exporter().add_event(event);

        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            0,
            "events added after live server stop() must be rejected (C++ enabled_=false parity)"
        );
    }

    #[tokio::test]
    async fn test_actor_event_output_channel_matches_claimed_full_parity_contract() {
        // Round 17: updated to use gRPC path (no file fallback).
        // This test verifies events are delivered via gRPC to the aggregator service.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_output_channel".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 2],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![2u8; 16],
                name: "output_channel_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        server.stop();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "mock aggregator must have received events via gRPC"
        );

        server_handle.abort();
    }

    // === Round 15: Periodic loop lifecycle + output-channel parity tests ===

    #[tokio::test]
    async fn test_gcs_server_stop_stops_periodic_actor_event_export_loop() {
        // C++ recorder has explicit start/stop lifecycle — StopExportingEvents()
        // stops the periodic export loop. The Rust server must stop the periodic
        // flush task in stop(), not leave it running until runtime teardown.
        //
        // The periodic loop only starts after successful sink attach (C++ parity:
        // StartExportingEvents only after readiness). So we need a mock aggregator.

        let (port, _received, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_stop_loop".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        // The periodic flush handle must exist after successful sink attach
        assert!(
            server.periodic_flush_running(),
            "periodic flush task must be running after successful sink attach"
        );

        // Stop the server
        server.stop();

        // After stop(), the periodic flush handle must be taken/aborted
        assert!(
            !server.periodic_flush_running(),
            "periodic flush task handle must be consumed by stop() (task aborted)"
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_gcs_server_stop_cancels_or_quiesces_periodic_flush_task() {
        // After stop(), the periodic flush task must no longer execute.
        // Verify by checking that the task's JoinHandle is finished (aborted)
        // and that the exporter rejects new events.

        let (port, _received, agg_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 10, // Very short interval
                ..Default::default()
            },
            session_name: Some("test_cancel_task".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        assert!(
            server.periodic_flush_running(),
            "periodic flush must be running after successful sink attach"
        );

        // Let the periodic task run a few times to prove it's active
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Stop the server — this must abort the task
        server.stop();

        // Give the tokio runtime a moment to process the abort
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert!(
            !server.periodic_flush_running(),
            "periodic flush must be stopped after stop()"
        );

        // The exporter should be shut down (enabled=false)
        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        actor_mgr.event_exporter().add_event(
            ray_observability::events::RayEvent::new(
                ray_observability::events::EventSourceType::Gcs,
                ray_observability::events::EventSeverity::Info,
                "POST_STOP",
                "rejected",
            )
            .with_field("actor_id", "dead")
            .with_field("event_kind", "lifecycle"),
        );
        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            0,
            "exporter must reject events after stop — periodic task is cancelled, exporter is shut down"
        );

        agg_handle.abort();
    }

    #[tokio::test]
    async fn test_actor_event_output_channel_matches_full_parity_claim_without_transport_gap() {
        // Round 17: updated — gRPC is now the only path for enable_ray_event.
        // This test verifies events flow through gRPC to mock aggregator with sink attached.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_full_parity_channel".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        assert!(actor_mgr.event_exporter().has_sink(), "gRPC sink must be attached");

        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 3],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![3u8; 16],
                name: "full_parity_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        server.stop();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "mock aggregator must have received events via gRPC"
        );

        server_handle.abort();
    }

    // === Round 16: gRPC output-channel parity tests ===

    #[tokio::test]
    async fn test_actor_ray_event_path_uses_real_aggregator_service_client() {
        // C++ uses EventAggregatorClient to send AddEventsRequest over gRPC.
        // Rust must use GrpcEventAggregatorSink when metrics_agent_port is set.
        //
        // This test starts a mock EventAggregatorService server, configures
        // the GCS server with that port, and verifies events are delivered
        // via gRPC (not file).

        use std::sync::atomic::{AtomicUsize, Ordering};
        use ray_proto::ray::rpc::events::{
            event_aggregator_service_server::{EventAggregatorService, EventAggregatorServiceServer},
            AddEventsRequest, AddEventsReply,
        };

        // Mock EventAggregatorService that counts received events
        let received_count = Arc::new(AtomicUsize::new(0));
        let rc = received_count.clone();

        struct MockAggregator(Arc<AtomicUsize>);

        #[tonic::async_trait]
        impl EventAggregatorService for MockAggregator {
            async fn add_events(
                &self,
                request: tonic::Request<AddEventsRequest>,
            ) -> Result<tonic::Response<AddEventsReply>, tonic::Status> {
                let req = request.into_inner();
                if let Some(ref data) = req.events_data {
                    self.0.fetch_add(data.events.len(), Ordering::Relaxed);
                }
                Ok(tonic::Response::new(AddEventsReply { status: None }))
            }
        }

        // Start mock server on a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mock_server = tonic::transport::Server::builder()
            .add_service(EventAggregatorServiceServer::new(MockAggregator(rc)));

        let server_handle = tokio::spawn(async move {
            mock_server
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Configure GCS server with metrics_agent_port → should use gRPC sink
        let tmp = tempfile::tempdir().unwrap();
        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_grpc_sink".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        // Register an actor through the live pipeline
        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 10],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![10u8; 16],
                name: "grpc_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        // Stop → final flush sends events via gRPC
        server.stop();

        // Give gRPC a moment to deliver
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Mock server must have received events
        let count = received_count.load(Ordering::Relaxed);
        assert!(
            count > 0,
            "mock EventAggregatorService must have received events via gRPC, got {}",
            count
        );

        // The file-based output should NOT have been used (gRPC path is primary)
        let file_output = tmp.path().join("ray_events/actor_events.jsonl");
        assert!(
            !file_output.exists(),
            "file output must NOT exist when gRPC sink is used — events go through service path"
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_live_gcs_server_actor_events_flow_through_service_path() {
        // Verify the full end-to-end flow: GCS server → gRPC → mock service.
        // The mock server verifies the proto structure of received events.

        use std::sync::Mutex as StdMutex;
        use ray_proto::ray::rpc::events::{
            event_aggregator_service_server::{EventAggregatorService, EventAggregatorServiceServer},
            AddEventsRequest, AddEventsReply,
        };

        // Mock server that captures the full request
        let captured: Arc<StdMutex<Vec<AddEventsRequest>>> = Arc::new(StdMutex::new(Vec::new()));
        let cap = captured.clone();

        struct CapturingAggregator(Arc<StdMutex<Vec<AddEventsRequest>>>);

        #[tonic::async_trait]
        impl EventAggregatorService for CapturingAggregator {
            async fn add_events(
                &self,
                request: tonic::Request<AddEventsRequest>,
            ) -> Result<tonic::Response<AddEventsReply>, tonic::Status> {
                self.0.lock().unwrap().push(request.into_inner());
                Ok(tonic::Response::new(AddEventsReply { status: None }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mock_server = tonic::transport::Server::builder()
            .add_service(EventAggregatorServiceServer::new(CapturingAggregator(cap)));

        let server_handle = tokio::spawn(async move {
            mock_server
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_service_flow".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 11],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![11u8; 16],
                name: "service_flow_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        server.stop();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the captured request structure
        let requests = captured.lock().unwrap();
        assert!(
            !requests.is_empty(),
            "mock service must have received at least one AddEventsRequest"
        );

        let request = &requests[0];
        let events_data = request.events_data.as_ref().expect("must have events_data");
        assert!(
            !events_data.events.is_empty(),
            "events_data.events must not be empty"
        );

        for event in &events_data.events {
            assert_eq!(event.source_type, 2, "source_type must be GCS (2)");
            assert!(
                event.event_type == 9 || event.event_type == 10,
                "event_type must be definition (9) or lifecycle (10)"
            );
            assert!(!event.session_name.is_empty(), "session_name must be set");
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_no_longer_relies_on_file_output_for_full_parity() {
        // C++ parity: when metrics_agent_port is not available, enable_ray_event
        // does NOT fall back to file output. Events are simply not exported.
        // This matches C++ where StartExportingEvents() is never called if the
        // metrics agent connection fails.

        let tmp = tempfile::tempdir().unwrap();

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_no_file_fallback".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None, // No gRPC port → no export (C++ parity)
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());

        // No sink should be attached — no file fallback
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink must be attached when metrics_agent_port is None (C++ parity: no file fallback)"
        );

        // Events are buffered but not delivered
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 12],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![12u8; 16],
                name: "no_fallback_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();
        server.stop();

        // File output must NOT exist — no file fallback for enable_ray_event
        let file_output = tmp.path().join("ray_events/actor_events.jsonl");
        assert!(
            !file_output.exists(),
            "file output must NOT exist when metrics_agent_port is None \
             (C++ parity: no file fallback for enable_ray_event)"
        );
    }

    // === Round 17: Fallback semantics parity tests ===

    #[tokio::test]
    async fn test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file() {
        // C++ behavior: if the event aggregator connection fails,
        // StartExportingEvents() is never called. Events are not exported.
        // C++ does NOT fall back to file output for enable_ray_event.
        //
        // This test configures a port that has no server listening, verifying
        // the Rust path does not fall back to file output.

        let tmp = tempfile::tempdir().unwrap();

        // Use a port with no server → connection will fail
        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_no_fallback_on_failure".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(19999), // Port with no server
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());

        // Register an actor — events buffered
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 13],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![13u8; 16],
                name: "no_fallback_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();
        server.stop();

        // File output must NOT exist — no file fallback on gRPC failure
        let file_output = tmp.path().join("ray_events/actor_events.jsonl");
        assert!(
            !file_output.exists(),
            "file output must NOT exist when gRPC connection fails \
             (C++ parity: no file fallback for enable_ray_event)"
        );
    }

    #[tokio::test]
    async fn test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path() {
        // C++ behavior: without a metrics agent port, InitMetricsExporter is
        // never called and StartExportingEvents() is never called.
        // Events are simply not exported.
        //
        // Rust must match: no sink, no export, no file fallback.

        let tmp = tempfile::tempdir().unwrap();

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_no_port_no_export".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());

        // No sink → events buffered but not delivered
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink when metrics_agent_port is None"
        );

        // Flush returns 0 (no sink)
        actor_mgr.event_exporter().add_event(
            ray_observability::events::RayEvent::new(
                ray_observability::events::EventSourceType::Gcs,
                ray_observability::events::EventSeverity::Info,
                "TEST",
                "test",
            )
            .with_field("actor_id", "aabb")
            .with_field("event_kind", "lifecycle"),
        );
        let flushed = actor_mgr.event_exporter().flush();
        assert_eq!(flushed, 0, "flush must return 0 when no sink (no export)");

        server.stop();

        // No file output
        let file_output = tmp.path().join("ray_events/actor_events.jsonl");
        assert!(
            !file_output.exists(),
            "no file output when no metrics_agent_port"
        );

        // No ray_events directory at all
        let events_dir = tmp.path().join("ray_events");
        assert!(
            !events_dir.exists(),
            "no ray_events directory when no metrics_agent_port"
        );
    }

    // ===== TESTS: Late-init exporter path (C++ parity: dynamic head-node port) =====

    #[tokio::test]
    async fn test_late_init_head_node_with_port_initializes_exporter() {
        // C++ parity: when startup has no metrics_agent_port, exporter initializes
        // later when the head node registers with a valid port.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_late_init".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None, // No port at startup — triggers late-init path
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        // Before head node registers: no sink, not initialized
        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink before head node registers"
        );
        assert!(
            !server.metrics_exporter_initialized(),
            "exporter not initialized before head node registers"
        );

        // Register head node with metrics_agent_port
        let node_mgr = Arc::clone(server.node_manager().unwrap());
        let mut head_node_id = vec![0u8; 28];
        head_node_id[0] = 0x10;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head_node_id,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        // Wait for the async init task to complete
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // After head node registers: sink attached, flag set
        assert!(
            server.metrics_exporter_initialized(),
            "exporter must be initialized after head node registers with port"
        );
        assert!(
            actor_mgr.event_exporter().has_sink(),
            "gRPC sink must be attached after late-init"
        );

        // Verify events actually flow through gRPC
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 1],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![2u8; 16],
                name: "late_init_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();
        actor_mgr.event_exporter().flush();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "mock aggregator must receive events via gRPC after late-init"
        );

        server.stop();
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_late_init_non_head_node_does_not_initialize() {
        // C++ parity: only head node registration triggers late init.
        // Non-head nodes are ignored.

        let (port, _received, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_non_head".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // Register non-head node with a valid port
        let mut worker_node_id = vec![0u8; 28];
        worker_node_id[0] = 0x20;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: worker_node_id,
                state: 0,
                is_head_node: false,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Non-head node must NOT trigger initialization
        assert!(
            !server.metrics_exporter_initialized(),
            "exporter must NOT initialize for non-head node"
        );
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink after non-head node registers"
        );

        server.stop();
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_late_init_head_node_with_zero_port_does_not_initialize() {
        // C++ parity: head node with port=0 means metrics agent not available.
        // Exporter does NOT initialize.

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_head_zero_port".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // Register head node with port 0
        let mut head_node_id = vec![0u8; 28];
        head_node_id[0] = 0x30;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head_node_id,
                state: 0,
                is_head_node: true,
                metrics_agent_port: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // C++ parity: head node with port=0 does NOT consume the init flag.
        // C++ (gcs_server.cc:826) logs info and returns without calling
        // InitMetricsExporter, so exchange(true) never fires.
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink when head node has port=0"
        );
        assert!(
            !server.metrics_exporter_initialized(),
            "init flag must NOT be consumed by head node with port=0"
        );
        assert!(
            !server.periodic_flush_running(),
            "no periodic loop when no sink attached"
        );

        server.stop();
    }

    #[tokio::test]
    async fn test_late_init_no_double_initialization() {
        // C++ parity: metrics_exporter_initialized_ atomic flag prevents
        // double initialization. Second head-node registration must not
        // replace the sink.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_no_double_init".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // First head-node registration → initializes exporter
        let mut head1 = vec![0u8; 28];
        head1[0] = 0x40;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head1,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            server.metrics_exporter_initialized(),
            "initialized after first head node"
        );
        assert!(actor_mgr.event_exporter().has_sink(), "sink attached");

        // Send an event, verify it goes through
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 2],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![3u8; 16],
                name: "first_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();
        actor_mgr.event_exporter().flush();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let count_after_first = received_count.load(std::sync::atomic::Ordering::Relaxed);
        assert!(count_after_first > 0, "events received after first init");

        // Second head-node registration → must NOT re-initialize
        let mut head2 = vec![0u8; 28];
        head2[0] = 0x41;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head2,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Sink still works (original sink, not replaced)
        let task_spec2 = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 3],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![4u8; 16],
                name: "second_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec2).await.unwrap();
        actor_mgr.event_exporter().flush();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let count_after_second = received_count.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            count_after_second > count_after_first,
            "events still flow through original sink after second head-node registration"
        );

        server.stop();
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_late_init_unreachable_port_no_sink_no_file_fallback() {
        // C++ parity: late-discovered unreachable port → no sink, no file fallback.

        let tmp = tempfile::tempdir().unwrap();
        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_late_unreachable".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // Register head node with unreachable port
        let mut head_id = vec![0u8; 28];
        head_id[0] = 0x50;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head_id,
                state: 0,
                is_head_node: true,
                metrics_agent_port: 19999, // No server here
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // No sink despite head node registration (connection failed)
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink when late-init port is unreachable"
        );

        // No file fallback
        let file_output = tmp.path().join("ray_events/actor_events.jsonl");
        assert!(
            !file_output.exists(),
            "no file fallback on late-init connection failure"
        );

        server.stop();
    }

    #[tokio::test]
    async fn test_startup_known_port_sets_initialized_flag() {
        // C++ parity: when port is known at startup, metrics_exporter_initialized_
        // is set immediately, preventing late-init from running.

        let (port, _received, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_startup_flag".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(port),
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        // Flag set immediately after startup init
        assert!(
            server.metrics_exporter_initialized(),
            "flag must be set after startup with known port"
        );

        // Sink attached
        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        assert!(
            actor_mgr.event_exporter().has_sink(),
            "sink must be attached after startup with known port"
        );

        // Late head-node registration must NOT re-init (flag already set)
        let node_mgr = Arc::clone(server.node_manager().unwrap());
        let mut head_id = vec![0u8; 28];
        head_id[0] = 0x60;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head_id,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Still has sink (same one, not replaced)
        assert!(
            actor_mgr.event_exporter().has_sink(),
            "sink still attached after redundant head-node registration"
        );

        server.stop();
        server_handle.abort();
    }

    // ===== Round 19: Tests proving Bug 1 (flag ordering) and Bug 2 (premature flush) are fixed =====

    #[tokio::test]
    async fn test_zero_port_does_not_consume_future_valid_init() {
        // Bug 1 regression test: head node with port=0 must NOT consume the
        // one-shot init flag. A later head-node registration with a valid port
        // must still initialize the exporter.
        //
        // Old code: swap(true) happened BEFORE checking port > 0, so port=0
        // permanently burned the flag. This test would FAIL on the old code.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_zero_then_valid".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // Step 1: Register head node with port=0
        let mut head1 = vec![0u8; 28];
        head1[0] = 0x70;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head1,
                state: 0,
                is_head_node: true,
                metrics_agent_port: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Flag must NOT be consumed
        assert!(
            !server.metrics_exporter_initialized(),
            "init flag must NOT be consumed by port=0 head node"
        );
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink after port=0 head node"
        );

        // Step 2: Register another head node with valid port
        let mut head2 = vec![0u8; 28];
        head2[0] = 0x71;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head2,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Now the exporter must be initialized
        assert!(
            server.metrics_exporter_initialized(),
            "init flag must be set after valid port head node"
        );
        assert!(
            actor_mgr.event_exporter().has_sink(),
            "sink must be attached after valid port head node"
        );

        // Verify events flow through gRPC
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 5],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![7u8; 16],
                name: "after_zero_port_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();
        actor_mgr.event_exporter().flush();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "events must flow through gRPC after zero-port then valid-port sequence"
        );

        server.stop();
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_pre_attach_events_survive_flush_intervals_and_export_after_late_attach() {
        // Bug 2 regression test: events generated before sink attach must NOT
        // be lost by premature periodic flushing.
        //
        // Uses a VERY short flush interval (10ms) to provoke the race.
        // Old code: periodic loop started immediately, flush_inner() drained
        // buffer into void when no sink was present. This test would FAIL.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 10, // Very short — provokes the race
                ..Default::default()
            },
            session_name: Some("test_pre_attach_survive".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None, // No port at startup → late-init path
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // No periodic loop should be running yet
        assert!(
            !server.periodic_flush_running(),
            "periodic loop must NOT run before sink attach"
        );

        // Emit events BEFORE any sink is attached
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 6],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![8u8; 16],
                name: "pre_attach_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        // Wait LONG ENOUGH for many flush intervals to fire (if the loop were running)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Events must still be buffered — not drained into void
        assert!(
            actor_mgr.event_exporter().buffer_len() > 0,
            "pre-attach events must survive multiple flush intervals without a sink"
        );

        // Now attach via late head-node registration
        let mut head_id = vec![0u8; 28];
        head_id[0] = 0x80;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head_id,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        // Wait for async chain: spawn → gRPC connect → set_sink → periodic flush → gRPC send
        // The chain involves multiple async hops and a spawned OS thread for the gRPC call.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // If the periodic loop hasn't flushed yet, do a manual flush to be deterministic
        if received_count.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            actor_mgr.event_exporter().flush();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        // Events must have been exported
        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "pre-attach events must be exported after late sink attach"
        );

        server.stop();
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_no_periodic_draining_without_sink() {
        // Bug 2 defense-in-depth: even if flush() is called manually when no
        // sink is present, events must NOT be drained from the buffer.
        // This tests the flush_inner() hardening directly.

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 10, // Short interval
                ..Default::default()
            },
            session_name: Some("test_no_drain_no_sink".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());

        // Emit events
        actor_mgr.event_exporter().add_event(
            ray_observability::events::RayEvent::new(
                ray_observability::events::EventSourceType::Gcs,
                ray_observability::events::EventSeverity::Info,
                "TEST",
                "buffered event",
            )
            .with_field("actor_id", "aabb")
            .with_field("event_kind", "lifecycle"),
        );

        assert_eq!(actor_mgr.event_exporter().buffer_len(), 1);

        // Flush manually — should return 0 AND preserve the buffer
        let flushed = actor_mgr.event_exporter().flush();
        assert_eq!(flushed, 0, "flush must return 0 when no sink");
        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            1,
            "buffer must be preserved when flush finds no sink"
        );

        // Wait for many potential periodic flush intervals
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Events still buffered
        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            1,
            "events must survive multiple flush intervals without a sink"
        );

        server.stop();
    }

    #[tokio::test]
    async fn test_startup_unreachable_port_no_destructive_loss() {
        // Startup with configured but unreachable port, short interval.
        // Events must not be silently lost.

        let tmp = tempfile::tempdir().unwrap();
        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 10, // Short interval
                ..Default::default()
            },
            log_dir: Some(tmp.path().to_string_lossy().to_string()),
            session_name: Some("test_unreachable_no_loss".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: Some(19999), // Unreachable port
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());

        // Connection failed → no sink, no periodic loop
        assert!(
            !actor_mgr.event_exporter().has_sink(),
            "no sink when port is unreachable"
        );
        assert!(
            !server.periodic_flush_running(),
            "no periodic loop when connection failed"
        );

        // Emit events
        actor_mgr.event_exporter().add_event(
            ray_observability::events::RayEvent::new(
                ray_observability::events::EventSourceType::Gcs,
                ray_observability::events::EventSeverity::Info,
                "TEST",
                "event after unreachable port",
            )
            .with_field("actor_id", "ccdd")
            .with_field("event_kind", "lifecycle"),
        );

        // Wait for potential flush intervals
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Events must still be buffered (not drained by sinkless flush)
        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            1,
            "events must be preserved when startup port is unreachable"
        );

        // No file fallback
        assert!(
            !tmp.path().join("ray_events/actor_events.jsonl").exists(),
            "no file fallback"
        );

        server.stop();
    }

    #[tokio::test]
    async fn test_late_attach_starts_loop_exactly_once() {
        // Late-init must start the periodic flush loop exactly once.
        // Repeated head-node registrations must not spawn additional loops.

        let (port, _received, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000,
                ..Default::default()
            },
            session_name: Some("test_loop_once".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // No loop before attach
        assert!(!server.periodic_flush_running(), "no loop before attach");

        // First head-node → attach
        let mut head1 = vec![0u8; 28];
        head1[0] = 0x90;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head1,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Loop now running
        assert!(server.periodic_flush_running(), "loop started after attach");
        assert!(actor_mgr.event_exporter().has_sink(), "sink attached");

        // Second head-node → no additional loop, no sink replacement
        let mut head2 = vec![0u8; 28];
        head2[0] = 0x91;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head2,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Still running (same handle, not replaced)
        assert!(server.periodic_flush_running(), "loop still running");
        assert!(actor_mgr.event_exporter().has_sink(), "sink still attached");

        server.stop();
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_shutdown_still_flushes_after_late_attach() {
        // After successful late attach, shutdown must flush remaining
        // buffered events and reject new ones.

        let (port, received_count, server_handle) = start_mock_aggregator().await;

        let config = GcsServerConfig {
            port: 0,
            ray_config: RayConfig {
                enable_ray_event: true,
                ray_events_report_interval_ms: 60_000, // Long interval — rely on shutdown flush
                ..Default::default()
            },
            session_name: Some("test_shutdown_late".to_string()),
            node_id: Some("aabbccdd".to_string()),
            metrics_agent_port: None,
            ..Default::default()
        };
        let mut server = GcsServer::new(config);
        server.initialize().await.unwrap();

        let actor_mgr = Arc::clone(server.actor_manager().unwrap());
        let node_mgr = Arc::clone(server.node_manager().unwrap());

        // Late attach
        let mut head_id = vec![0u8; 28];
        head_id[0] = 0xA0;
        node_mgr
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: head_id,
                state: 0,
                is_head_node: true,
                metrics_agent_port: port as i32,
                ..Default::default()
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Emit events after attach (long interval — they will be buffered)
        let task_spec = rpc::TaskSpec {
            job_id: vec![0, 0, 0, 7],
            caller_address: Some(rpc::Address::default()),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: vec![9u8; 16],
                name: "shutdown_late_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        actor_mgr.handle_register_actor(task_spec).await.unwrap();

        assert!(
            actor_mgr.event_exporter().buffer_len() > 0,
            "events buffered before shutdown"
        );

        // Shutdown should flush
        server.stop();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            0,
            "shutdown must flush all buffered events"
        );
        assert!(
            received_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "events must reach mock aggregator via gRPC after shutdown flush"
        );

        // New events must be rejected after shutdown
        actor_mgr.event_exporter().add_event(
            ray_observability::events::RayEvent::new(
                ray_observability::events::EventSourceType::Gcs,
                ray_observability::events::EventSeverity::Info,
                "TEST",
                "post-shutdown event",
            ),
        );
        assert_eq!(
            actor_mgr.event_exporter().buffer_len(),
            0,
            "new events must be rejected after shutdown"
        );

        server_handle.abort();
    }
}
