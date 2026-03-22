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

use parking_lot::RwLock;
use ray_common::config::RayConfig;
use ray_common::id::NodeID;
use ray_common::scheduling::{FixedPoint, ResourceSet};
use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
use ray_proto::ray::rpc;
use ray_rpc::client::RetryConfig;

use crate::cluster_resource_manager::ClusterResourceManager;
use crate::cluster_resource_scheduler::ClusterResourceScheduler;
use crate::demand_calculator::{DemandCalculator, DemandCalculatorConfig};
use crate::dependency_manager::DependencyManager;
use crate::lease_manager::{ClusterLeaseManager, SchedulingClass, WorkerLeaseTracker};
use crate::local_object_manager::{LocalObjectManager, LocalObjectManagerConfig};
use crate::local_resource_manager::LocalResourceManager;
use crate::placement_group_resource_manager::PlacementGroupResourceManager;
use crate::wait_manager::WaitManager;
use crate::worker_pool::WorkerPool;
use crate::worker_reaper::{WorkerReaper, WorkerReaperConfig};

/// Tracks per-worker, per-scheduling-class backlog counts.
///
/// C++ contract: `HandleReportWorkerBacklog` validates the worker exists,
/// clears old backlog for that worker, then sets new per-(worker, class) entries.
/// The scheduler sees an aggregate view keyed by scheduling class.
pub struct BacklogTracker {
    /// Per-worker backlog: worker_id → (scheduling_class → backlog_size)
    per_worker: RwLock<HashMap<ray_common::id::WorkerID, HashMap<SchedulingClass, i64>>>,
}

impl BacklogTracker {
    pub fn new() -> Self {
        Self {
            per_worker: RwLock::new(HashMap::new()),
        }
    }

    /// Report backlog for a worker: clear old entries, then set new ones.
    /// Matches C++ clear-then-set semantics.
    pub fn report_worker_backlog(
        &self,
        worker_id: ray_common::id::WorkerID,
        entries: Vec<(SchedulingClass, i64)>,
    ) {
        let mut guard = self.per_worker.write();
        // Clear previous backlog for this worker (C++ contract)
        let worker_entry = guard.entry(worker_id).or_default();
        worker_entry.clear();
        for (class, size) in entries {
            worker_entry.insert(class, size);
        }
    }

    /// Remove all backlog for a worker (e.g., on disconnect).
    pub fn remove_worker(&self, worker_id: &ray_common::id::WorkerID) {
        self.per_worker.write().remove(worker_id);
    }

    /// Update the backlog for a given scheduling class (legacy aggregate API).
    pub fn update(&self, scheduling_class: SchedulingClass, backlog_size: i64) {
        // Use a synthetic worker ID for backwards compatibility
        let synthetic = ray_common::id::WorkerID::nil();
        let mut guard = self.per_worker.write();
        let entry = guard.entry(synthetic).or_default();
        entry.insert(scheduling_class, backlog_size);
    }

    /// Get the aggregated backlog map (sum across all workers per scheduling class).
    pub fn get_backlog(&self) -> HashMap<SchedulingClass, i64> {
        let guard = self.per_worker.read();
        let mut result = HashMap::new();
        for worker_backlogs in guard.values() {
            for (&class, &size) in worker_backlogs {
                *result.entry(class).or_insert(0) += size;
            }
        }
        result
    }

    /// Get per-worker backlog (for testing/debugging).
    pub fn get_worker_backlog(
        &self,
        worker_id: &ray_common::id::WorkerID,
    ) -> HashMap<SchedulingClass, i64> {
        self.per_worker
            .read()
            .get(worker_id)
            .cloned()
            .unwrap_or_default()
    }
}

/// Per-worker stats snapshot, reported by core workers.
/// C++ collects these via GetCoreWorkerStats RPCs.
#[derive(Debug, Clone, Default)]
pub struct WorkerStatsSnapshot {
    pub num_pending_tasks: i32,
    pub num_running_tasks: i64,
    pub used_object_store_memory: i64,
    pub num_owned_objects: i64,
}

/// Trait for collecting live per-worker stats.
/// C++ contract: HandleGetNodeStats sends GetCoreWorkerStats RPC to each alive worker.
/// Implementations may be:
/// - gRPC-based (real worker RPC fanout)
/// - In-process (direct query for in-process workers)
/// - Test mock
pub trait WorkerStatsProvider: Send + Sync {
    /// Collect stats from a specific worker (sync path for simple lookups).
    fn get_worker_stats(
        &self,
        addr: &str,
        worker_id: &ray_common::id::WorkerID,
        include_memory_info: bool,
    ) -> Option<WorkerStatsSnapshot>;

    /// Whether this provider uses live RPC collection (vs. local tracker only).
    fn is_live_rpc_provider(&self) -> bool {
        false
    }
}

/// Default implementation that reads from the local WorkerStatsTracker.
/// This serves in-process workers that report their stats directly.
pub struct TrackerBasedStatsProvider {
    tracker: Arc<WorkerStatsTracker>,
}

impl TrackerBasedStatsProvider {
    pub fn new(tracker: Arc<WorkerStatsTracker>) -> Self {
        Self { tracker }
    }
}

impl WorkerStatsProvider for TrackerBasedStatsProvider {
    fn get_worker_stats(
        &self,
        _addr: &str,
        worker_id: &ray_common::id::WorkerID,
        _include_memory_info: bool,
    ) -> Option<WorkerStatsSnapshot> {
        self.tracker.get_stats(worker_id)
    }
}

/// gRPC-based worker stats provider that sends GetCoreWorkerStats RPCs.
/// C++ contract: HandleGetNodeStats sends GetCoreWorkerStats to each alive worker.
/// This is the real live-RPC path that replaces the tracker-only approach.
/// For workers reachable via gRPC, it queries them directly.
/// For workers without a reachable endpoint, it falls back to the tracker.
pub struct GrpcWorkerStatsProvider {
    /// Fallback tracker for in-process workers.
    tracker: Arc<WorkerStatsTracker>,
}

impl GrpcWorkerStatsProvider {
    pub fn new(tracker: Arc<WorkerStatsTracker>) -> Self {
        Self { tracker }
    }

    /// Query a worker via gRPC GetCoreWorkerStats RPC.
    /// This is the C++ `worker->rpc_client()->GetCoreWorkerStats()` equivalent.
    pub async fn query_worker_rpc(
        addr: &str,
        worker_id: &ray_common::id::WorkerID,
        include_memory_info: bool,
    ) -> Option<WorkerStatsSnapshot> {
        use ray_proto::ray::rpc;
        let endpoint =
            tonic::transport::Endpoint::from_shared(format!("http://{}", addr)).ok()?;
        let channel = endpoint
            .connect_timeout(std::time::Duration::from_secs(1))
            .connect()
            .await
            .ok()?;
        let mut client =
            rpc::core_worker_service_client::CoreWorkerServiceClient::new(channel);
        let request = rpc::GetCoreWorkerStatsRequest {
            intended_worker_id: worker_id.binary(),
            include_memory_info,
            ..Default::default()
        };
        let reply = client
            .get_core_worker_stats(request)
            .await
            .ok()?
            .into_inner();
        let cw_stats = reply.core_worker_stats?;
        Some(WorkerStatsSnapshot {
            num_pending_tasks: cw_stats.num_pending_tasks,
            num_running_tasks: cw_stats.num_running_tasks,
            used_object_store_memory: cw_stats.used_object_store_memory,
            num_owned_objects: cw_stats.num_owned_objects,
        })
    }
}

impl WorkerStatsProvider for GrpcWorkerStatsProvider {
    fn get_worker_stats(
        &self,
        _addr: &str,
        worker_id: &ray_common::id::WorkerID,
        _include_memory_info: bool,
    ) -> Option<WorkerStatsSnapshot> {
        // Sync fallback: use tracker. The async gRPC path is called from
        // handle_get_node_stats directly.
        self.tracker.get_stats(worker_id)
    }

    fn is_live_rpc_provider(&self) -> bool {
        true
    }
}

/// Tracks per-worker runtime stats for node stats reporting.
/// C++ contract: HandleGetNodeStats sends GetCoreWorkerStats RPC to each worker.
/// Workers report their stats here so the raylet can aggregate them.
pub struct WorkerStatsTracker {
    stats: RwLock<HashMap<ray_common::id::WorkerID, WorkerStatsSnapshot>>,
}

impl WorkerStatsTracker {
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Update stats for a worker.
    pub fn report_stats(
        &self,
        worker_id: ray_common::id::WorkerID,
        snapshot: WorkerStatsSnapshot,
    ) {
        self.stats.write().insert(worker_id, snapshot);
    }

    /// Get stats for a specific worker.
    pub fn get_stats(&self, worker_id: &ray_common::id::WorkerID) -> Option<WorkerStatsSnapshot> {
        self.stats.read().get(worker_id).cloned()
    }

    /// Remove a worker's stats (e.g. on disconnect).
    pub fn remove_worker(&self, worker_id: &ray_common::id::WorkerID) {
        self.stats.write().remove(worker_id);
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
    /// Cluster auth token. When set, all gRPC requests must include a matching Bearer token.
    pub auth_token: Option<String>,
    /// Custom Python worker command (full shell command). If None, defaults to `python -m ray.worker`.
    pub python_worker_command: Option<String>,
    /// Raw config_list JSON string from `ray start` (returned by GetSystemConfig RPC).
    /// This must be the original JSON so the C++ CoreWorker can parse it without
    /// encountering Rust-only config fields.
    pub raw_config_json: String,
}

/// The main raylet node manager.
pub struct NodeManager {
    config: RayletConfig,
    scheduler: Arc<ClusterResourceScheduler>,
    worker_pool: Arc<WorkerPool>,
    lease_manager: Arc<ClusterLeaseManager>,
    worker_lease_tracker: Arc<WorkerLeaseTracker>,
    worker_reaper: Arc<WorkerReaper>,
    dependency_manager: Arc<DependencyManager>,
    wait_manager: Arc<WaitManager>,
    local_object_manager: Arc<parking_lot::Mutex<LocalObjectManager>>,
    demand_calculator: Arc<DemandCalculator>,
    placement_group_resource_manager: Arc<PlacementGroupResourceManager>,
    backlog_tracker: Arc<BacklogTracker>,
    worker_stats_tracker: Arc<WorkerStatsTracker>,
    worker_stats_provider: Arc<dyn WorkerStatsProvider>,
    /// Subscriber for owner eviction events (WORKER_OBJECT_EVICTION channel).
    /// C++ equivalent: `core_worker_subscriber_` in `LocalObjectManager`.
    eviction_subscriber: Arc<ray_pubsub::Subscriber>,
    /// Runtime-owned transport driver for the eviction subscriber.
    /// C++ equivalent: the subscriber's internal poll loop that drives
    /// SendCommandBatchIfPossible + MakeLongPollingPubsubConnection +
    /// HandleLongPollingResponse.
    eviction_transport: ray_pubsub::SubscriberTransport,
    /// Active poll loop handles per publisher. The runtime spawns a background
    /// tokio task for each owner that has pending subscriptions.
    eviction_poll_handles: parking_lot::Mutex<HashMap<Vec<u8>, tokio::task::JoinHandle<()>>>,
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

        let worker_lease_tracker = Arc::new(WorkerLeaseTracker::default());
        let worker_reaper = Arc::new(WorkerReaper::new(WorkerReaperConfig::default()));
        let dependency_manager = Arc::new(DependencyManager::new());

        let wait_manager = Arc::new(WaitManager::new());

        let local_object_manager = Arc::new(parking_lot::Mutex::new(LocalObjectManager::new(
            LocalObjectManagerConfig::default(),
        )));

        let demand_calculator = Arc::new(DemandCalculator::new(DemandCalculatorConfig::default()));

        let placement_group_resource_manager =
            Arc::new(PlacementGroupResourceManager::new(local_resource_manager));

        let backlog_tracker = Arc::new(BacklogTracker::new());
        let worker_stats_tracker = Arc::new(WorkerStatsTracker::new());
        // C++ contract: HandleGetNodeStats sends GetCoreWorkerStats RPC to each worker.
        // The default provider uses gRPC for reachable workers, with tracker fallback.
        let worker_stats_provider: Arc<dyn WorkerStatsProvider> =
            Arc::new(GrpcWorkerStatsProvider::new(Arc::clone(&worker_stats_tracker)));

        // C++ contract: core_worker_subscriber_ manages WORKER_OBJECT_EVICTION
        // subscriptions for pinned objects. Uses real ray-pubsub Subscriber
        // infrastructure with command batching and long-poll message delivery.
        let eviction_subscriber = Arc::new(ray_pubsub::Subscriber::new(
            config.node_id.as_bytes().to_vec(),
            ray_pubsub::SubscriberConfig::default(),
        ));
        let eviction_transport =
            ray_pubsub::SubscriberTransport::new(Arc::clone(&eviction_subscriber));

        Self {
            config,
            scheduler,
            worker_pool,
            lease_manager,
            worker_lease_tracker,
            worker_reaper,
            dependency_manager,
            wait_manager,
            local_object_manager,
            demand_calculator,
            placement_group_resource_manager,
            backlog_tracker,
            worker_stats_tracker,
            worker_stats_provider,
            eviction_subscriber,
            eviction_transport,
            eviction_poll_handles: parking_lot::Mutex::new(HashMap::new()),
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

    pub fn worker_lease_tracker(&self) -> &Arc<WorkerLeaseTracker> {
        &self.worker_lease_tracker
    }

    pub fn worker_reaper(&self) -> &Arc<WorkerReaper> {
        &self.worker_reaper
    }

    pub fn dependency_manager(&self) -> &Arc<DependencyManager> {
        &self.dependency_manager
    }

    pub fn wait_manager(&self) -> &Arc<WaitManager> {
        &self.wait_manager
    }

    pub fn local_object_manager(&self) -> &Arc<parking_lot::Mutex<LocalObjectManager>> {
        &self.local_object_manager
    }

    pub fn demand_calculator(&self) -> &Arc<DemandCalculator> {
        &self.demand_calculator
    }

    pub fn placement_group_resource_manager(&self) -> &Arc<PlacementGroupResourceManager> {
        &self.placement_group_resource_manager
    }

    pub fn backlog_tracker(&self) -> &Arc<BacklogTracker> {
        &self.backlog_tracker
    }

    pub fn worker_stats_tracker(&self) -> &Arc<WorkerStatsTracker> {
        &self.worker_stats_tracker
    }

    pub fn worker_stats_provider(&self) -> &Arc<dyn WorkerStatsProvider> {
        &self.worker_stats_provider
    }

    /// The real subscriber for WORKER_OBJECT_EVICTION subscriptions.
    /// C++ equivalent: `core_worker_subscriber_` in `LocalObjectManager`.
    pub fn eviction_subscriber(&self) -> &Arc<ray_pubsub::Subscriber> {
        &self.eviction_subscriber
    }

    /// The runtime-owned transport driver for the eviction subscriber.
    pub fn eviction_transport(&self) -> &ray_pubsub::SubscriberTransport {
        &self.eviction_transport
    }

    /// Start the runtime poll loop for an owner publisher.
    ///
    /// Spawns a background tokio task that runs the subscriber transport
    /// lifecycle in a loop: drain commands → send to publisher → long-poll →
    /// dispatch/failure. The loop continues until the publisher fails or
    /// all subscriptions to that publisher are removed.
    ///
    /// C++ equivalent: the subscriber's internal poll loop started by
    /// `MakeLongPollingConnectionIfNotConnected()` when Subscribe() is called.
    ///
    /// `client` is the transport to the owner — in production a gRPC client,
    /// in tests an `InProcessSubscriberClient`.
    pub fn start_eviction_poll_loop(
        &self,
        publisher_id: Vec<u8>,
        client: Arc<dyn ray_pubsub::SubscriberClient + 'static>,
    ) {
        let mut handles = self.eviction_poll_handles.lock();
        // Don't start a second loop for the same publisher.
        if handles.contains_key(&publisher_id) {
            return;
        }
        let transport =
            ray_pubsub::SubscriberTransport::new(Arc::clone(&self.eviction_subscriber));
        let pid = publisher_id.clone();
        let handle = tokio::spawn(async move {
            loop {
                match transport.poll_publisher(&pid, client.as_ref()).await {
                    Ok(_) => {} // Message dispatched, continue loop.
                    Err(ray_pubsub::TransportError::PublisherFailure) => break,
                    Err(_) => break,
                }
                // Stop if no more subscriptions to this publisher.
                if !transport.subscriber().has_subscriptions_to(&pid) {
                    break;
                }
            }
        });
        handles.insert(publisher_id, handle);
    }

    /// Check if a poll loop is running for a given publisher.
    pub fn has_eviction_poll_loop(&self, publisher_id: &[u8]) -> bool {
        let handles = self.eviction_poll_handles.lock();
        handles
            .get(publisher_id)
            .is_some_and(|h| !h.is_finished())
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
        let listener = tokio::net::TcpListener::bind(format!(
            "{}:{}",
            ray_common::constants::DEFAULT_SERVER_BIND_ADDRESS,
            self.config.port
        ))
        .await?;
        let bound_port = listener.local_addr()?.port();
        tracing::info!(port = bound_port, "Raylet gRPC server listening");

        // Wire the worker spawner callback now that we know the port.
        let spawner_config = crate::worker_spawner::WorkerSpawnerConfig {
            node_ip_address: self.config.node_ip_address.clone(),
            raylet_port: bound_port,
            gcs_address: self.config.gcs_address.clone(),
            node_id: self.config.node_id.clone(),
            session_name: self.config.session_name.clone(),
            python_worker_command: self.config.python_worker_command.clone(),
        };
        // Create cgroup manager if enabled.
        let cgroup_manager: Option<std::sync::Arc<dyn ray_common::cgroup::CgroupManager>> = {
            let cgm = ray_common::cgroup::create_cgroup_manager(
                &self.config.node_id,
                self.config.ray_config.enable_cgroup,
            );
            if cgm.is_cgroup_v2_available() {
                if let Err(e) = cgm.initialize() {
                    tracing::warn!(error = %e, "Failed to initialize cgroup hierarchy");
                }
                Some(std::sync::Arc::from(cgm))
            } else {
                None
            }
        };

        self.worker_pool
            .set_start_worker_callback(crate::worker_spawner::make_spawn_callback(
                spawner_config,
                cgroup_manager,
            ));

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
            subscriber_client_factory: parking_lot::Mutex::new(None),
        };
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<rpc::node_manager_service_server::NodeManagerServiceServer<
                crate::grpc_service::NodeManagerServiceImpl,
            >>()
            .await;

        // Build the auth interceptor from config.
        let auth_mode = if self.config.auth_token.is_some() {
            ray_rpc::auth::AuthenticationMode::ClusterAuth
        } else {
            ray_rpc::auth::AuthenticationMode::Disabled
        };
        let auth = ray_rpc::auth::AuthInterceptor::new(auth_mode, self.config.auth_token.clone());

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(
                rpc::node_manager_service_server::NodeManagerServiceServer::with_interceptor(
                    svc, auth,
                ),
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
            resources: HashMap::from([("CPU".to_string(), 8.0), ("GPU".to_string(), 2.0)]),
            labels: HashMap::from([("region".to_string(), "us-east".to_string())]),
            session_name: "test-session".to_string(),
            auth_token: None,
            python_worker_command: None,
            raw_config_json: "{}".to_string(),
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
        assert!(nm
            .scheduler()
            .local_resource_manager()
            .is_local_node_draining());
    }

    #[test]
    fn test_node_manager_resources_configured() {
        let nm = NodeManager::new(make_config());
        let local_rm = nm.scheduler().local_resource_manager();
        let total = local_rm.get_local_total_resources();
        assert!(total.get("CPU") > FixedPoint::from_f64(0.0));
        assert!(total.get("GPU") > FixedPoint::from_f64(0.0));
    }

    #[test]
    fn test_node_manager_labels() {
        let nm = NodeManager::new(make_config());
        let local_rm = nm.scheduler().local_resource_manager();
        let labels = local_rm.get_labels();
        assert_eq!(labels.get("region"), Some(&"us-east".to_string()));
    }

    #[test]
    fn test_node_manager_drain_deadline() {
        let nm = NodeManager::new(make_config());

        // Initially not draining
        assert!(!nm
            .scheduler()
            .local_resource_manager()
            .is_local_node_draining());

        // Drain with a deadline
        nm.handle_drain(10000);
        assert!(nm
            .scheduler()
            .local_resource_manager()
            .is_local_node_draining());
    }

    #[test]
    fn test_node_manager_lease_manager_accessible() {
        let nm = NodeManager::new(make_config());
        let lease_mgr = nm.lease_manager();

        // Queue a lease through the node manager's lease manager
        let mut req = ray_common::scheduling::ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let mut rx = lease_mgr.queue_and_schedule_lease(
            req,
            crate::scheduling_resources::SchedulingOptions::hybrid(),
            crate::lease_manager::SchedulingClass(1),
        );

        match rx.try_recv().unwrap() {
            crate::lease_manager::LeaseReply::Granted { node_id, .. } => {
                assert_eq!(node_id, "test-node-1");
            }
            _ => panic!("expected lease grant"),
        }
    }

    #[test]
    fn test_node_manager_worker_pool_accessible() {
        let nm = NodeManager::new(make_config());
        let pool = nm.worker_pool();
        assert_eq!(pool.num_idle_workers(), 0);
        assert_eq!(pool.num_registered_workers(), 0);
    }

    #[test]
    fn test_node_manager_placement_group_manager() {
        let nm = NodeManager::new(make_config());
        let pg_mgr = nm.placement_group_resource_manager();
        assert_eq!(pg_mgr.num_bundles(), 0);
    }

    #[test]
    fn test_node_manager_local_object_manager() {
        let nm = NodeManager::new(make_config());
        let obj_mgr = nm.local_object_manager();
        let locked = obj_mgr.lock();
        assert_eq!(locked.num_pinned(), 0);
        assert_eq!(locked.pinned_bytes(), 0);
    }

    #[test]
    fn test_node_manager_custom_resources() {
        let config = RayletConfig {
            resources: HashMap::from([
                ("CPU".to_string(), 4.0),
                ("GPU".to_string(), 2.0),
                ("memory".to_string(), 1024.0),
                ("custom_resource".to_string(), 10.0),
            ]),
            ..make_config()
        };
        let nm = NodeManager::new(config);
        let local_rm = nm.scheduler().local_resource_manager();
        let total = local_rm.get_local_total_resources();
        assert_eq!(total.get("custom_resource"), FixedPoint::from_f64(10.0));
        assert_eq!(total.get("memory"), FixedPoint::from_f64(1024.0));
    }

    #[test]
    fn test_node_manager_no_resources() {
        let config = RayletConfig {
            resources: HashMap::new(),
            ..make_config()
        };
        let nm = NodeManager::new(config);
        let local_rm = nm.scheduler().local_resource_manager();
        let total = local_rm.get_local_total_resources();
        assert_eq!(total.get("CPU"), FixedPoint::from_f64(0.0));
    }

    #[test]
    fn test_node_manager_demand_calculator() {
        let nm = NodeManager::new(make_config());
        let _dc = nm.demand_calculator();
        // Just verifying it's accessible and doesn't panic
    }

    #[test]
    fn test_node_manager_config_preserved() {
        let nm = NodeManager::new(make_config());
        assert_eq!(nm.config().node_ip_address, "127.0.0.1");
        assert_eq!(nm.config().object_store_socket, "/tmp/plasma");
        assert_eq!(nm.config().gcs_address, "127.0.0.1:6379");
        assert_eq!(nm.config().session_name, "test-session");
        assert!(nm.config().auth_token.is_none());
    }
}
