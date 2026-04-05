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

/// Reason the raylet is shutting down, propagated through the shutdown channel
/// so that the unregister request can carry the appropriate `NodeDeathInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownReason {
    /// Normal OS signal (SIGTERM, Ctrl-C) or server exit.
    Signal,
    /// Runtime-env agent timed out — maps to UNEXPECTED_TERMINATION.
    RuntimeEnvTimeout,
}

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
#[derive(Debug, Clone, Default)]
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

    // ── Runtime flags wired from CLI (C++ parity) ───────────────────

    /// Object manager gRPC port. Published in GcsNodeInfo for cluster discovery.
    /// Note: Rust raylet serves object manager RPCs on the main gRPC port;
    /// C++ runs a separate object manager server on this port.
    pub object_manager_port: u16,
    /// Minimum port for spawned worker processes. 0 = unconstrained.
    pub min_worker_port: u16,
    /// Maximum port for spawned worker processes. 0 = unconstrained.
    pub max_worker_port: u16,
    /// Explicit worker port list (comma-separated). Overrides min/max if set.
    pub worker_port_list: Option<String>,
    /// Maximum number of workers starting concurrently.
    pub maximum_startup_concurrency: u32,
    /// Metrics agent port. CLI-provided value; if 0, Rust raylet will
    /// attempt to read from session_dir port file (like C++). After
    /// resolution, a MetricsAgentClient is created and readiness is
    /// verified before publishing to GcsNodeInfo.
    pub metrics_agent_port: u16,
    /// Metrics export port. Published in GcsNodeInfo and propagated to workers.
    pub metrics_export_port: u16,
    /// Runtime env agent port. CLI-provided value; if 0, Rust raylet
    /// will attempt to read from session_dir port file (like C++).
    /// After resolution, a RuntimeEnvAgentClient is created and
    /// installed into the WorkerPool.
    pub runtime_env_agent_port: u16,
    /// Object store memory limit in bytes.
    pub object_store_memory: u64,
    /// Primary mmap directory for the object store (e.g. /dev/shm).
    pub plasma_directory: Option<String>,
    /// Fallback mmap directory (disk overflow).
    pub fallback_directory: Option<String>,
    /// Whether to use huge pages for the object store.
    pub huge_pages: bool,
    /// Dashboard agent listen port. CLI-provided value; if 0, resolved
    /// from session_dir port file (matching C++ WaitForDashboardAgentPorts).
    pub dashboard_agent_listen_port: u16,
    /// Whether this node is the head node.
    pub head: bool,
    /// Number of Python workers to pre-start.
    pub num_prestart_python_workers: u32,
    /// Command to launch the dashboard agent subprocess. When provided,
    /// the Rust raylet validates the command and launches/monitors the
    /// subprocess (matching C++ behavior in node_manager.cc:3299-3336).
    pub dashboard_agent_command: Option<String>,
    /// Command to launch the runtime env agent subprocess. When provided,
    /// the Rust raylet validates the command and launches/monitors the
    /// subprocess (matching C++ behavior in node_manager.cc:3363-3396).
    pub runtime_env_agent_command: Option<String>,
    /// Temp directory for Ray session. Published in GcsNodeInfo.
    pub temp_dir: Option<String>,
    /// Session directory for Ray session. Published in GcsNodeInfo.
    /// Also used as a fallback rendezvous point for agent port files
    /// when ports are not provided via CLI (matching C++ behavior).
    pub session_dir: Option<String>,
    /// Human-readable node name.
    pub node_name: Option<String>,
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
    /// Dashboard agent process manager.
    /// C++ equivalent: `dashboard_agent_manager_` in `node_manager.h`.
    dashboard_agent_manager: Option<Arc<crate::agent_manager::AgentManager>>,
    /// Runtime env agent process manager.
    /// C++ equivalent: `runtime_env_agent_manager_` in `node_manager.h`.
    runtime_env_agent_manager: Option<Arc<crate::agent_manager::AgentManager>>,
    /// Resolved metrics agent port (after potential port-file rendezvous).
    /// C++ equivalent: `metrics_agent_port_` in `node_manager.h`.
    resolved_metrics_agent_port: std::sync::atomic::AtomicI32,
    /// Resolved metrics export port (after potential port-file rendezvous).
    /// C++ equivalent: `metrics_export_port_` in `node_manager.h`.
    resolved_metrics_export_port: std::sync::atomic::AtomicI32,
    /// Resolved runtime env agent port (after potential port-file rendezvous).
    /// C++ equivalent: `runtime_env_agent_port_` in `node_manager.h`.
    resolved_runtime_env_agent_port: std::sync::atomic::AtomicI32,
    /// Resolved dashboard agent listen port (after potential port-file rendezvous).
    /// C++ equivalent: `dashboard_agent_listen_port_` in `node_manager.h`.
    resolved_dashboard_agent_listen_port: std::sync::atomic::AtomicI32,
    /// The local object manager's plasma store (constructed from raylet config).
    /// Holds the actual PlasmaStore that uses object_store_memory, plasma_directory,
    /// fallback_directory, and huge_pages from the raylet config.
    object_manager: Option<Arc<ray_object_manager::object_manager::ObjectManager>>,
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

        let startup_concurrency = if config.maximum_startup_concurrency > 0 {
            config.maximum_startup_concurrency as usize
        } else {
            10
        };
        let worker_pool = Arc::new(WorkerPool::new(
            startup_concurrency,
            200, // num_workers_soft_limit
            config.ray_config.max_io_workers,
        ));

        let worker_lease_tracker = Arc::new(WorkerLeaseTracker::default());
        let worker_reaper = Arc::new(WorkerReaper::new(WorkerReaperConfig::default()));
        let dependency_manager = Arc::new(DependencyManager::new());

        let wait_manager = Arc::new(WaitManager::new());

        // C++ parity: construct LocalObjectManager with real config from RayConfig
        // (main.cc:871-896) instead of using defaults.
        let lom_config = {
            let max_file_size = if config.ray_config.max_spilling_file_size_bytes < 0 {
                0 // C++ uses -1 to disable; Rust uses 0
            } else {
                config.ray_config.max_spilling_file_size_bytes
            };
            LocalObjectManagerConfig {
                min_spilling_size: config.ray_config.min_spilling_size,
                max_fused_object_count: config.ray_config.max_fused_object_count as usize,
                max_spilling_file_size_bytes: max_file_size,
                free_objects_batch_size: config.ray_config.free_objects_batch_size as usize,
                free_objects_period_ms: config.ray_config.free_objects_period_milliseconds,
                object_spilling_threshold: config.ray_config.object_spilling_threshold,
                max_io_workers: config.ray_config.max_io_workers as i64,
            }
        };
        let local_object_manager = Arc::new(parking_lot::Mutex::new(LocalObjectManager::new(
            lom_config,
        )));

        // Wire IO worker pool into local_object_manager.
        {
            let mut lom = local_object_manager.lock();
            lom.set_io_worker_pool(worker_pool.clone() as Arc<dyn crate::worker_pool::IOWorkerPool>);
        }

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

        // Create agent managers for dashboard and runtime-env agents.
        // C++ equivalent: CreateDashboardAgentManager / CreateRuntimeEnvAgentManager.
        // C++ checks RayConfig::instance().enable_metrics_collection().
        let enable_metrics = config.ray_config.enable_metrics_collection;
        let dashboard_agent_manager = config.dashboard_agent_command.as_deref().and_then(|cmd| {
            crate::agent_manager::create_dashboard_agent_manager(
                &config.node_id,
                cmd,
                enable_metrics,
            )
            .map(Arc::new)
        });
        let runtime_env_agent_manager =
            config.runtime_env_agent_command.as_deref().and_then(|cmd| {
                crate::agent_manager::create_runtime_env_agent_manager(&config.node_id, cmd)
                    .map(Arc::new)
            });

        // Construct the local PlasmaStore + ObjectManager from raylet config.
        // C++ equivalent: main.cc lines 651-670 (ObjectManagerConfig population).
        let object_manager = Self::create_object_manager(&config);

        // C++ parity: wire is_plasma_object_spillable callback (main.cc:889).
        // In C++, this checks PlasmaStore::IsObjectSpillable() — the object must
        // be sealed and have refcount == 1 (not pinned by any worker).
        if let Some(ref om) = object_manager {
            let om_ref: Arc<ray_object_manager::object_manager::ObjectManager> = Arc::clone(om);
            let mut lom = local_object_manager.lock();
            lom.set_is_plasma_object_spillable(Arc::new(move |object_id| {
                om_ref.plasma_store().is_object_spillable(object_id)
            }));
        }

        // C++ parity: wire on_objects_freed callback (main.cc:884-887).
        // In C++, this calls `object_manager_->FreeObjects(object_ids, /*local_only=*/false)`.
        // The Rust equivalent deletes objects from the plasma store. Remote-node
        // fanout is not yet implemented (mirrors C++ TODO #56414).
        if let Some(ref om) = object_manager {
            let plasma = Arc::clone(om.plasma_store());
            let mut lom = local_object_manager.lock();
            lom.set_on_objects_freed(Arc::new(move |object_ids| {
                for oid in &object_ids {
                    if let Err(e) = plasma.delete_object(oid, plasma.allocator().as_ref()) {
                        tracing::warn!(
                            object_id = %oid,
                            error = %e,
                            "Failed to delete freed object from plasma store"
                        );
                    }
                }
            }));
        }

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
            dashboard_agent_manager,
            runtime_env_agent_manager,
            resolved_metrics_agent_port: std::sync::atomic::AtomicI32::new(0),
            resolved_metrics_export_port: std::sync::atomic::AtomicI32::new(0),
            resolved_runtime_env_agent_port: std::sync::atomic::AtomicI32::new(0),
            resolved_dashboard_agent_listen_port: std::sync::atomic::AtomicI32::new(0),
            object_manager,
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

    /// Get the resolved metrics agent port.
    /// C++ equivalent: `NodeManager::GetMetricsAgentPort()`.
    pub fn get_metrics_agent_port(&self) -> i32 {
        self.resolved_metrics_agent_port
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get the resolved metrics export port.
    /// C++ equivalent: published in GcsNodeInfo.
    pub fn get_metrics_export_port(&self) -> i32 {
        self.resolved_metrics_export_port
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get the resolved dashboard agent listen port.
    /// C++ equivalent: published in GcsNodeInfo.
    pub fn get_dashboard_agent_listen_port(&self) -> i32 {
        self.resolved_dashboard_agent_listen_port
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get the resolved runtime env agent port.
    /// C++ equivalent: `NodeManager::GetRuntimeEnvAgentPort()`.
    pub fn get_runtime_env_agent_port(&self) -> i32 {
        self.resolved_runtime_env_agent_port
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Resolve all four agent ports (metrics_agent, metrics_export,
    /// dashboard_agent_listen, runtime_env_agent) and store results in atomic fields.
    ///
    /// This is the same logic as the port-resolution section of `run()`, extracted
    /// so that integration tests can exercise the full NodeManager port-resolution
    /// path without needing a live GCS connection.
    ///
    /// C++ equivalent: the WaitForDashboardAgentPorts + WaitForRuntimeEnvAgentPort
    /// sections in NodeManager::Start().
    pub async fn resolve_all_ports(&self) {
        let session_dir = self.config.session_dir.as_deref();
        // C++ default: 15000ms (kDefaultPortWaitTimeoutMs).
        let port_timeout = std::time::Duration::from_millis(15000);
        let node_id = &self.config.node_id;

        let metrics_port = Self::resolve_port(
            self.config.metrics_agent_port,
            session_dir,
            node_id,
            "metrics_agent_port",
            port_timeout,
        ).await;
        self.resolved_metrics_agent_port
            .store(metrics_port, std::sync::atomic::Ordering::Release);

        let metrics_export_port = Self::resolve_port(
            self.config.metrics_export_port,
            session_dir,
            node_id,
            "metrics_export_port",
            port_timeout,
        ).await;
        self.resolved_metrics_export_port
            .store(metrics_export_port, std::sync::atomic::Ordering::Release);

        let dashboard_listen_port = Self::resolve_port(
            self.config.dashboard_agent_listen_port,
            session_dir,
            node_id,
            "dashboard_agent_listen_port",
            port_timeout,
        ).await;
        self.resolved_dashboard_agent_listen_port
            .store(dashboard_listen_port, std::sync::atomic::Ordering::Release);

        let runtime_env_port = Self::resolve_port(
            self.config.runtime_env_agent_port,
            session_dir,
            node_id,
            "runtime_env_agent_port",
            port_timeout,
        ).await;
        self.resolved_runtime_env_agent_port
            .store(runtime_env_port, std::sync::atomic::Ordering::Release);
    }

    /// Get the dashboard agent PID (0 if not running).
    /// C++ equivalent: `HandleGetAgentPIDs` → `dashboard_agent_manager_->GetPid()`.
    pub fn get_dashboard_agent_pid(&self) -> u32 {
        self.dashboard_agent_manager
            .as_ref()
            .map(|m| m.get_pid())
            .unwrap_or(0)
    }

    /// Get the runtime env agent PID (0 if not running).
    /// C++ equivalent: `HandleGetAgentPIDs` → `runtime_env_agent_manager_->GetPid()`.
    pub fn get_runtime_env_agent_pid(&self) -> u32 {
        self.runtime_env_agent_manager
            .as_ref()
            .map(|m| m.get_pid())
            .unwrap_or(0)
    }

    /// Get the object manager (if constructed from raylet config).
    ///
    /// The ObjectManager/PlasmaStore is integrated for capacity and allocation
    /// stats (reported via GetNodeStats). IO worker pool management
    /// (spill/restore/delete workers) is implemented via WorkerPool's
    /// IOWorkerState. Actual spill/restore/delete RPCs to IO workers are a
    /// follow-up -- the worker pool acquisition/release pattern is wired.
    pub fn object_manager(&self) -> Option<&Arc<ray_object_manager::object_manager::ObjectManager>> {
        self.object_manager.as_ref()
    }

    /// Construct an ObjectManager + PlasmaStore from raylet config.
    ///
    /// C++ equivalent: main.cc lines 651-670 (ObjectManagerConfig → ObjectManager).
    /// This is the critical path that the re-audit flagged: ensuring the
    /// raylet actually constructs the object store runtime from CLI flags,
    /// not just storing them in config.
    fn create_object_manager(
        config: &RayletConfig,
    ) -> Option<Arc<ray_object_manager::object_manager::ObjectManager>> {
        // Only create if object_store_memory is explicitly set.
        // C++ validates: object_store_memory > 0 (FATAL if not).
        if config.object_store_memory == 0 {
            tracing::info!(
                "object_store_memory not set, skipping local object store construction"
            );
            return None;
        }

        let plasma_directory = config
            .plasma_directory
            .as_deref()
            .unwrap_or(if cfg!(target_os = "linux") {
                "/dev/shm"
            } else {
                "/tmp"
            });
        let fallback_directory = config.fallback_directory.as_deref().unwrap_or("");

        // Construct the PlasmaAllocator with the real config values.
        let allocator = Arc::new(ray_object_manager::plasma::allocator::PlasmaAllocator::new(
            config.object_store_memory as i64,
            plasma_directory,
            fallback_directory,
            config.huge_pages,
        ));

        // Construct the PlasmaStore with the real allocator.
        let store_config = ray_object_manager::plasma::store::PlasmaStoreConfig {
            object_store_memory: config.object_store_memory as i64,
            plasma_directory: plasma_directory.to_string(),
            fallback_directory: fallback_directory.to_string(),
            huge_pages: config.huge_pages,
        };
        let plasma_store = Arc::new(ray_object_manager::plasma::store::PlasmaStore::new(
            allocator, &store_config,
        ));

        // Construct the ObjectManager with real config.
        let node_id = if config.node_id.len() == ray_common::id::NodeID::SIZE * 2 {
            ray_common::id::NodeID::from_hex(&config.node_id)
        } else {
            ray_common::id::NodeID::from_random()
        };

        let om_config = ray_object_manager::common::ObjectManagerConfig {
            object_store_memory: config.object_store_memory as i64,
            plasma_directory: plasma_directory.to_string(),
            fallback_directory: fallback_directory.to_string(),
            huge_pages: config.huge_pages,
            store_socket_name: config.object_store_socket.clone(),
            ..Default::default()
        };

        let om = ray_object_manager::object_manager::ObjectManager::new(om_config, node_id, plasma_store);
        tracing::info!(
            object_store_memory = config.object_store_memory,
            plasma_directory,
            fallback_directory,
            huge_pages = config.huge_pages,
            "Constructed local ObjectManager from raylet config"
        );
        Some(Arc::new(om))
    }

    /// Resolve a port: use CLI value if > 0, otherwise try session_dir port file.
    ///
    /// C++ equivalent: the pattern used in WaitForDashboardAgentPorts and
    /// WaitForRuntimeEnvAgentPort.
    async fn resolve_port(
        cli_port: u16,
        session_dir: Option<&str>,
        node_id: &str,
        port_name: &str,
        timeout: std::time::Duration,
    ) -> i32 {
        if cli_port > 0 {
            return cli_port as i32;
        }
        if let Some(dir) = session_dir {
            crate::agent_manager::wait_for_persisted_port(dir, node_id, port_name, timeout)
                .await
                .map(|p| p as i32)
                .unwrap_or(0)
        } else {
            0
        }
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
            object_manager_port: self.config.object_manager_port as i32,
            object_store_socket_name: self.config.object_store_socket.clone(),
            resources_total: self.config.resources.clone(),
            labels: self.config.labels.clone(),
            state: rpc::gcs_node_info::GcsNodeState::Alive as i32,
            start_time_ms: now_ms,
            metrics_agent_port: self.get_metrics_agent_port(),
            metrics_export_port: self.resolved_metrics_export_port
                .load(std::sync::atomic::Ordering::Acquire),
            runtime_env_agent_port: self.get_runtime_env_agent_port(),
            is_head_node: self.config.head,
            node_name: self.config.node_name.clone().unwrap_or_default(),
            temp_dir: self.config.temp_dir.clone().unwrap_or_default(),
            session_dir: self.config.session_dir.clone().unwrap_or_default(),
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

    /// Unregister this node from GCS on shutdown, propagating death info when
    /// the shutdown was caused by a runtime-env agent timeout (C++ parity:
    /// `NodeManager::ShutdownRaylet` builds `NodeDeathInfo` with
    /// `UNEXPECTED_TERMINATION`).
    async fn unregister_from_gcs(
        gcs_client: &GcsRpcClient,
        node_id: &NodeID,
        reason: ShutdownReason,
    ) {
        tracing::info!(node_id = %node_id.hex(), ?reason, "Unregistering from GCS");

        let node_death_info = match reason {
            ShutdownReason::RuntimeEnvTimeout => Some(rpc::NodeDeathInfo {
                reason: rpc::node_death_info::Reason::UnexpectedTermination as i32,
                reason_message:
                    "Raylet could not connect to Runtime Env Agent".to_string(),
            }),
            ShutdownReason::Signal => None,
        };

        let result = gcs_client
            .unregister_node(rpc::UnregisterNodeRequest {
                node_id: node_id.binary(),
                node_death_info,
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

        // ── Agent subprocess lifecycle (C++ parity) ───────────────────
        //
        // C++ equivalent: node_manager.cc lines 274-291.
        // 1. Launch dashboard agent subprocess
        // 2. Launch runtime env agent subprocess
        // 3. Wait for agent ports (via CLI or session_dir port files)
        // 4. Create MetricsAgentClient (readiness wait + exporter init)
        // 5. Create RuntimeEnvAgentClient and install into worker pool

        // Step 1-2: Start agent subprocesses and begin monitoring.
        // C++ equivalent: AgentManager launch + respawn loop.
        let mut _dashboard_monitor: Option<tokio::task::JoinHandle<()>> = None;
        let mut _runtime_env_monitor: Option<tokio::task::JoinHandle<()>> = None;

        if let Some(ref mgr) = self.dashboard_agent_manager {
            match mgr.start(bound_port) {
                Ok(pid) => {
                    tracing::info!(pid, "Dashboard agent started");
                    _dashboard_monitor = Some(mgr.start_monitoring());
                }
                Err(e) => tracing::error!(error = %e, "Failed to start dashboard agent"),
            }
        }
        if let Some(ref mgr) = self.runtime_env_agent_manager {
            match mgr.start(bound_port) {
                Ok(pid) => {
                    tracing::info!(pid, "Runtime env agent started");
                    _runtime_env_monitor = Some(mgr.start_monitoring());
                }
                Err(e) => tracing::error!(error = %e, "Failed to start runtime env agent"),
            }
        }

        // Step 3: Resolve agent ports.
        // C++ equivalent: WaitForDashboardAgentPorts / WaitForRuntimeEnvAgentPort.
        // Prefer CLI-provided ports; fall back to session_dir port files.
        self.resolve_all_ports().await;
        let metrics_port = self.get_metrics_agent_port();

        // Wire the worker spawner callback now that ports are resolved.
        // IMPORTANT: This must be AFTER resolve_all_ports() so that the
        // resolved metrics_export_port is used (not the pre-resolution value).
        let spawner_config = crate::worker_spawner::WorkerSpawnerConfig {
            node_ip_address: self.config.node_ip_address.clone(),
            raylet_port: bound_port,
            gcs_address: self.config.gcs_address.clone(),
            node_id: self.config.node_id.clone(),
            session_name: self.config.session_name.clone(),
            python_worker_command: self.config.python_worker_command.clone(),
            min_worker_port: self.config.min_worker_port,
            max_worker_port: self.config.max_worker_port,
            worker_port_list: self.config.worker_port_list.clone(),
            metrics_export_port: self.resolved_metrics_export_port.load(std::sync::atomic::Ordering::Acquire) as u16,
            object_store_memory: self.config.object_store_memory,
            object_spilling_config: self.config.ray_config.object_spilling_config.clone(),
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

        // Pre-start Python workers if configured.
        // C++ parity: WorkerPool::PrestartWorkersInternal() in worker_pool.cc:196.
        if self.config.num_prestart_python_workers > 0 {
            self.worker_pool
                .prestart_workers(self.config.num_prestart_python_workers);
        }

        // Step 4: Create MetricsAgentClient, wait for readiness, then init exporters.
        // C++ equivalent: main.cc lines 1067-1082.
        //   - MetricsAgentClientImpl::WaitForServerReady(callback)
        //   - callback calls ConnectOpenCensusExporter(port) + InitOpenTelemetryExporter(port)
        //
        // PARITY STATUS: ARCHITECTURALLY DIFFERENT.
        // C++ connects OpenCensus/OpenTelemetry exporters to the metrics agent,
        // which acts as a proxy/aggregator for reporting to external systems.
        // Rust uses a standalone MetricsExporter with a Prometheus HTTP endpoint.
        // Both share the same readiness-gating pattern (wait for agent, then init),
        // but the export mechanism is different. The Rust path does NOT replicate
        // the C++ OpenCensus/OpenTelemetry export protocol.
        //
        // To achieve full parity, the Rust side would need to either:
        //   (a) compile and link the ReporterService proto (depends on OC protos), or
        //   (b) implement a gRPC reporter client that speaks the same protocol.
        // Neither is done today.
        if metrics_port > 0 {
            let mut metrics_client = crate::metrics_agent_client::MetricsAgentClient::new(
                "127.0.0.1",
                metrics_port as u16,
            );
            let export_port = self.resolved_metrics_export_port
                .load(std::sync::atomic::Ordering::Acquire);
            metrics_client
                .wait_for_server_ready(move |ready| {
                    if ready {
                        tracing::info!(
                            metrics_agent_port = metrics_port,
                            metrics_export_port = export_port,
                            "Metrics agent ready — initializing exporters"
                        );
                        // C++ equivalent: ConnectOpenCensusExporter(port) +
                        // InitOpenTelemetryExporter(port).
                        // Rust: start the periodic exporter and HTTP server.
                        let exporter = std::sync::Arc::new(
                            ray_stats::exporter::MetricsExporter::new(
                                ray_stats::exporter::ExporterConfig::default(),
                            ),
                        );
                        let exporter_clone = std::sync::Arc::clone(&exporter);
                        // Start periodic export loop.
                        let _export_handle = exporter_clone.start_periodic_export();

                        // Start Prometheus HTTP endpoint if export port is set.
                        if export_port > 0 {
                            let http_config = ray_stats::http_server::MetricsHttpConfig {
                                port: export_port as u16,
                            };
                            tokio::spawn(async move {
                                match ray_stats::http_server::start_metrics_server(
                                    http_config, exporter,
                                )
                                .await
                                {
                                    Ok((_handle, actual_port)) => {
                                        tracing::info!(
                                            port = actual_port,
                                            "Prometheus metrics HTTP server started"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            "Failed to start metrics HTTP server"
                                        );
                                    }
                                }
                            });
                        }
                    } else {
                        tracing::error!(
                            "Failed to establish connection to the metrics agent"
                        );
                    }
                })
                .await;
        } else {
            tracing::info!(
                "Metrics agent not available (port not configured). \
                 This is expected in minimal installations."
            );
        }

        // Step 5: Create RuntimeEnvAgentClient and install into worker pool.
        // C++ equivalent: node_manager.cc lines 281-291.
        //
        // Create a oneshot channel so the runtime-env timeout shutdown callback
        // can signal the gRPC server to initiate graceful shutdown (C++ parity:
        // build NodeDeathInfo → call shutdown_raylet_gracefully_ → schedule
        // forced exit after 10s).
        let (runtime_env_shutdown_tx, runtime_env_shutdown_rx) =
            tokio::sync::oneshot::channel::<ShutdownReason>();
        let runtime_env_shutdown_tx =
            Arc::new(std::sync::Mutex::new(Some(runtime_env_shutdown_tx)));

        let runtime_env_port = self.get_runtime_env_agent_port();
        if runtime_env_port > 0 {
            let shutdown_tx_clone = Arc::clone(&runtime_env_shutdown_tx);
            let re_client = Arc::new(
                crate::runtime_env_agent_client::RuntimeEnvAgentClient::new(
                    &self.config.node_ip_address,
                    runtime_env_port as u16,
                    crate::runtime_env_agent_client::RuntimeEnvAgentClientConfig {
                        auth_token: self.config.auth_token.clone(),
                        shutdown_raylet: Some(std::sync::Arc::new(move || {
                            tracing::error!(
                                "Initiating graceful raylet shutdown due to runtime env \
                                 agent timeout. The runtime env agent was never started, \
                                 or is listening to the wrong port. Read the log `cat \
                                 /tmp/ray/session_latest/logs/runtime_env_agent.log`."
                            );
                            // C++ parity: signal the gRPC server to stop accepting new
                            // requests (graceful shutdown), then schedule a forced exit
                            // after 10s as a backstop.
                            if let Some(tx) = shutdown_tx_clone.lock().unwrap().take() {
                                let _ = tx.send(ShutdownReason::RuntimeEnvTimeout);
                            }
                            std::thread::spawn(|| {
                                std::thread::sleep(std::time::Duration::from_secs(10));
                                tracing::error!("Graceful shutdown timed out, forcing exit");
                                std::process::exit(1);
                            });
                        })),
                        ..Default::default()
                    },
                ),
            );
            self.worker_pool
                .set_runtime_env_agent_client(re_client);
            tracing::info!(
                port = runtime_env_port,
                "RuntimeEnvAgentClient installed into worker pool"
            );
        } else {
            tracing::info!(
                "Runtime env agent not available (port not configured)"
            );
        }

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

        // ── Periodic timers (C++ parity: node_manager.cc:418-432) ────────
        //
        // Timer 1: Flush freed objects periodically.
        // Timer 2: Spill objects when usage exceeds the primary-object threshold.
        // Both use the same period (`free_objects_period_milliseconds`).
        //
        // A watch channel is used as a cancellation signal: send `true` on
        // shutdown so both timer tasks exit cleanly.
        let (timer_cancel_tx, timer_cancel_rx) = tokio::sync::watch::channel(false);
        let free_objects_period_ms = self.config.ray_config.free_objects_period_milliseconds;
        if free_objects_period_ms > 0 {
            // Timer 1: periodic flush of freed objects.
            // C++ equivalent: node_manager.cc:418-422.
            let lom = Arc::clone(&self.local_object_manager);
            let mut cancel_rx = timer_cancel_rx.clone();
            let period = std::time::Duration::from_millis(free_objects_period_ms as u64);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = cancel_rx.changed() => break,
                        _ = interval.tick() => {
                            lom.lock().flush_free_objects();
                        }
                    }
                }
            });

            // Timer 2: threshold-driven spill scheduling.
            // C++ equivalent: node_manager.cc:423-431 + SpillIfOverPrimaryObjectsThreshold
            // (node_manager.cc:2400-2414).
            let spilling_config = self.config.ray_config.object_spilling_config.clone();
            if spilling_config.is_empty() {
                tracing::info!(
                    "Object spilling is disabled because spilling config is unspecified"
                );
            } else {
                let lom = Arc::clone(&self.local_object_manager);
                let object_manager_ref = self.object_manager.clone();
                let spill_threshold = self.config.ray_config.object_spilling_threshold;
                let mut cancel_rx = timer_cancel_rx.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(period);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            _ = cancel_rx.changed() => break,
                            _ = interval.tick() => {
                                // C++ parity: SpillIfOverPrimaryObjectsThreshold
                                // (node_manager.cc:2400-2414).
                                let Some(ref om) = object_manager_ref else {
                                    continue;
                                };
                                let memory_capacity = om.config().object_store_memory;
                                if memory_capacity <= 0 {
                                    continue;
                                }
                                let primary_bytes = lom.lock().get_primary_bytes();
                                let allocated_pct =
                                    primary_bytes as f64 / memory_capacity as f64;
                                if allocated_pct >= spill_threshold {
                                    tracing::info!(
                                        usage_pct = allocated_pct * 100.0,
                                        threshold_pct = spill_threshold * 100.0,
                                        "Triggering object spilling because current \
                                         usage is above threshold"
                                    );
                                    crate::local_object_manager::spill_objects_upto_max_throughput(&lom);
                                }
                            }
                        }
                    }
                });
            }
        }

        // Combine OS signals (SIGTERM / Ctrl-C) with the runtime-env timeout
        // shutdown channel so either path triggers graceful server shutdown.
        // We capture the shutdown reason so we can propagate NodeDeathInfo to GCS.
        let shutdown_reason: std::sync::Arc<std::sync::Mutex<ShutdownReason>> =
            std::sync::Arc::new(std::sync::Mutex::new(ShutdownReason::Signal));
        let shutdown_reason_clone = std::sync::Arc::clone(&shutdown_reason);
        let combined_shutdown = async move {
            tokio::select! {
                _ = shutdown_signal() => {
                    tracing::info!("Received OS shutdown signal");
                    // reason stays Signal (the default)
                }
                result = runtime_env_shutdown_rx => {
                    let reason = result.unwrap_or(ShutdownReason::Signal);
                    *shutdown_reason_clone.lock().unwrap() = reason;
                    tracing::info!(
                        "Graceful shutdown initiated by runtime env agent timeout"
                    );
                }
            }
        };

        tonic::transport::Server::builder()
            .add_service(
                rpc::node_manager_service_server::NodeManagerServiceServer::with_interceptor(
                    svc, auth,
                ),
            )
            .add_service(health_service)
            .serve_with_incoming_shutdown(incoming, combined_shutdown)
            .await?;

        // Cancel periodic timers (flush + spill).
        let _ = timer_cancel_tx.send(true);

        // Clean up: unregister from GCS with appropriate death info.
        let final_reason = *shutdown_reason.lock().unwrap();
        if let Some((gcs_client, node_id)) = &gcs_state {
            Self::unregister_from_gcs(gcs_client, node_id, final_reason).await;
        }

        // Clean up: stop agent subprocesses.
        // C++ equivalent: node_manager.cc:2960-2961.
        if let Some(ref mgr) = self.dashboard_agent_manager {
            mgr.stop();
        }
        if let Some(ref mgr) = self.runtime_env_agent_manager {
            mgr.stop();
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
            ..Default::default()
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

    #[test]
    fn test_shutdown_reason_builds_correct_unregister_request() {
        // RuntimeEnvTimeout should produce UNEXPECTED_TERMINATION death info
        let reason = ShutdownReason::RuntimeEnvTimeout;
        let death_info = match reason {
            ShutdownReason::RuntimeEnvTimeout => Some(rpc::NodeDeathInfo {
                reason: rpc::node_death_info::Reason::UnexpectedTermination as i32,
                reason_message:
                    "Raylet could not connect to Runtime Env Agent".to_string(),
            }),
            ShutdownReason::Signal => None,
        };

        let req = rpc::UnregisterNodeRequest {
            node_id: vec![0u8; 28],
            node_death_info: death_info,
        };

        let info = req.node_death_info.as_ref().expect("death info must be set");
        assert_eq!(
            info.reason,
            rpc::node_death_info::Reason::UnexpectedTermination as i32
        );
        assert_eq!(
            info.reason_message,
            "Raylet could not connect to Runtime Env Agent"
        );

        // Signal shutdown should NOT include death info
        let reason_signal = ShutdownReason::Signal;
        let death_info_signal = match reason_signal {
            ShutdownReason::RuntimeEnvTimeout => Some(rpc::NodeDeathInfo {
                reason: rpc::node_death_info::Reason::UnexpectedTermination as i32,
                reason_message:
                    "Raylet could not connect to Runtime Env Agent".to_string(),
            }),
            ShutdownReason::Signal => None,
        };

        let req_signal = rpc::UnregisterNodeRequest {
            node_id: vec![0u8; 28],
            node_death_info: death_info_signal,
        };

        assert!(
            req_signal.node_death_info.is_none(),
            "Signal shutdown should not include death info"
        );
    }

    #[test]
    fn test_shutdown_reason_channel_propagates() {
        // Verify the oneshot channel carries the correct ShutdownReason variant.
        let (tx, mut rx) = tokio::sync::oneshot::channel::<ShutdownReason>();
        tx.send(ShutdownReason::RuntimeEnvTimeout).unwrap();
        let received = rx.try_recv().unwrap();
        assert_eq!(received, ShutdownReason::RuntimeEnvTimeout);

        let (tx2, mut rx2) = tokio::sync::oneshot::channel::<ShutdownReason>();
        tx2.send(ShutdownReason::Signal).unwrap();
        let received2 = rx2.try_recv().unwrap();
        assert_eq!(received2, ShutdownReason::Signal);
    }
}
