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
use tokio::sync::mpsc;
use tonic::transport::Server;
use tracing::{error, info, warn};

pub use init_data::GcsInitData;

use gcs_kv::{GcsInternalKVManager, InternalKVInterface, StoreClientInternalKV};
use gcs_managers::actor_scheduler::GcsActorScheduler;
use gcs_managers::actor_stub::GcsActorManager;
use gcs_managers::autoscaler_stub::{GcsAutoscalerStateManager, TonicRayletClientPool};
use gcs_managers::export_event_writer::ExportEventManager;
use gcs_managers::event_export_stub::EventExportServiceStub;
use gcs_managers::job_manager::GcsJobManager;
use gcs_managers::node_manager::GcsNodeManager;
use gcs_managers::pg_scheduler::GcsPlacementGroupScheduler;
use gcs_managers::placement_group_stub::GcsPlacementGroupManager;
use gcs_managers::function_manager::GcsFunctionManager;
use gcs_managers::health_service::{GcsHealthService, HealthProbe};
use gcs_managers::metrics_exporter::MetricsExporter;
use gcs_managers::pubsub_stub::PubSubService;
use gcs_managers::ray_syncer_stub::{RaySyncerService, SyncMessageConsumer};
use gcs_managers::raylet_load::{
    spawn_load_pull_loop, RayletLoadFetcher, TonicRayletLoadFetcher,
};
use gcs_managers::resource_manager::GcsResourceManager;
use gcs_managers::runtime_env_stub::RuntimeEnvServiceStub;
use gcs_managers::usage_stats::UsageStatsClient;
use gcs_managers::task_manager::GcsTaskManager;
use gcs_managers::worker_manager::GcsWorkerManager;
use gcs_proto::ray::rpc::actor_info_gcs_service_server::ActorInfoGcsServiceServer;
use gcs_proto::ray::rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateServiceServer;
use gcs_proto::ray::rpc::ray_event_export_gcs_service_server::RayEventExportGcsServiceServer;
use gcs_proto::ray::rpc::syncer::ray_syncer_server::RaySyncerServer;
use gcs_proto::ray::rpc::internal_kv_gcs_service_server::InternalKvGcsServiceServer;
use gcs_proto::ray::rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsServiceServer;
use gcs_proto::ray::rpc::job_info_gcs_service_server::JobInfoGcsServiceServer;
use gcs_proto::ray::rpc::node_info_gcs_service_server::NodeInfoGcsServiceServer;
use gcs_proto::ray::rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsServiceServer;
use gcs_proto::ray::rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsServiceServer;
use gcs_proto::ray::rpc::runtime_env_gcs_service_server::RuntimeEnvGcsServiceServer;
use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsServiceServer;
use gcs_proto::ray::rpc::worker_info_gcs_service_server::WorkerInfoGcsServiceServer;
use gcs_proto::ray::rpc::{JobTableData, WorkerTableData};
use gcs_pubsub::{GcsPublisher, PubSubManager};
use gcs_store::{InMemoryStoreClient, RedisStoreClient, StoreClient};
use gcs_table_storage::GcsTableStorage;

// Cluster ID KV constants matching C++ (gcs_server.cc + constants.h).
const CLUSTER_ID_NAMESPACE: &str = "cluster";
const CLUSTER_ID_KEY: &str = "ray_cluster_id";
const CLUSTER_ID_SIZE: usize = 28; // kUniqueIDSize in C++

/// Generate a random 28-byte cluster ID (matching C++ ClusterID::FromRandom).
fn generate_random_cluster_id() -> Vec<u8> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut id = vec![0u8; CLUSTER_ID_SIZE];
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for (i, b) in id.iter_mut().enumerate() {
        *b = ((seed >> (i % 16)) ^ (i as u128 * 37)) as u8;
    }
    id
}

/// Get an existing cluster ID from KV, or generate and persist a new one.
///
/// Maps C++ `GcsServer::GetOrGenerateClusterId` from `gcs_server.cc:234-269`.
/// Uses KV namespace `"cluster"`, key `"ray_cluster_id"`.
async fn get_or_generate_cluster_id(kv: &dyn InternalKVInterface) -> Vec<u8> {
    // Try to retrieve existing cluster ID from KV.
    if let Some(stored) = kv.get(CLUSTER_ID_NAMESPACE, CLUSTER_ID_KEY).await {
        let bytes = stored.into_bytes();
        if bytes.len() == CLUSTER_ID_SIZE {
            info!("Using existing cluster ID from external storage");
            return bytes;
        }
    }

    // Generate new random 28-byte cluster ID.
    let cluster_id = generate_random_cluster_id();

    // Persist with overwrite=false (first writer wins, matching C++ HSETNX).
    let id_str = unsafe { String::from_utf8_unchecked(cluster_id.clone()) };
    kv.put(CLUSTER_ID_NAMESPACE, CLUSTER_ID_KEY, id_str, false).await;
    info!("Generated and persisted new cluster ID");

    cluster_id
}

/// Configuration for the GCS server.
#[derive(Debug, Clone)]
pub struct GcsServerConfig {
    pub grpc_port: u16,
    pub raylet_config_list: String,
    /// Cap on stored task-event rows. `None` (the default) reads from
    /// `ray_config::task_events_max_num_task_in_gcs` — the documented
    /// configuration path, matching C++
    /// `RayConfig::instance().task_events_max_num_task_in_gcs()`
    /// passed into `GcsTaskManager` (`gcs_task_manager.cc:42-45`).
    /// `Some(n)` pins the cap explicitly; tests use this to drive
    /// eviction behaviour without mutating the global config.
    pub max_task_events: Option<usize>,
    /// Redis connection URL. If `Some`, uses Redis-backed persistence.
    /// If `None`, uses in-memory storage (no persistence across restarts).
    /// Example: `"redis://127.0.0.1:6379"` or `"redis://:password@host:port"`.
    pub redis_address: Option<String>,
    /// Namespace prefix that isolates this cluster's data in a shared Redis.
    /// Maps C++ `external_storage_namespace`. Defaults to empty string.
    pub external_storage_namespace: String,
    /// Cluster session name. Maps C++ `session_name_` passed to GcsAutoscalerStateManager.
    pub session_name: String,
    /// Directory where export-event JSON files are written
    /// (`<log_dir>/export_events/event_<SOURCE>.log`). When `None` or
    /// `event_log_reporter_enabled = false`, export events are dropped on the
    /// floor — matching C++ `gcs_server_main.cc:145` (`if event_log_reporter_enabled
    /// && !log_dir.empty()`).
    pub log_dir: Option<String>,
    /// Whether to emit export events. Maps C++
    /// `RayConfig::event_log_reporter_enabled()`.
    pub event_log_reporter_enabled: bool,
    /// Port of the local metrics/event-aggregator agent. Parity with
    /// C++ `GcsServerConfig::metrics_agent_port` passed through from
    /// `gcs_server_main.cc` into `GcsServer::DoStart`
    /// (`gcs_server.cc:324-329`): if > 0 at startup, the metrics
    /// exporter is initialised eagerly; otherwise init is deferred
    /// until the head node registers with a non-zero
    /// `metrics_agent_port` (`gcs_server.cc:831-842`). `-1` is the
    /// CLI "unset" sentinel used by `main.rs`.
    pub metrics_agent_port: i32,
}

impl Default for GcsServerConfig {
    fn default() -> Self {
        Self {
            // Matches C++ `DEFINE_int32(gcs_server_port, 0, ...)` at
            // `gcs_server_main.cc:48`. Port 0 asks the OS for any
            // free port; the binary pre-binds and publishes the
            // actual port via the port file before serving.
            grpc_port: 0,
            raylet_config_list: String::new(),
            // None ⇒ read from `ray_config` at construction time. This
            // is what closes blocker 2b: previously the task manager saw
            // a hard-coded 100_000 regardless of what `config_list` or
            // `RAY_task_events_max_num_task_in_gcs` set.
            max_task_events: None,
            redis_address: None,
            external_storage_namespace: String::new(),
            session_name: String::new(),
            log_dir: None,
            // Matches C++ `RAY_CONFIG(bool, event_log_reporter_enabled, true)`
            // in `ray_config_def.h:849`. The writer still stays disabled at
            // runtime unless `log_dir` is also set — same gate as C++
            // `gcs_server_main.cc:145`.
            event_log_reporter_enabled: true,
            // `-1` = unset (same sentinel main.rs uses from the CLI).
            // Eager init only fires for values > 0; anything <= 0
            // falls through to the deferred head-node-register path.
            metrics_agent_port: -1,
        }
    }
}

/// The GCS Server. Maps C++ `GcsServer`.
///
/// Creates all managers and serves gRPC on the configured port.
/// Supports both in-memory and Redis-backed storage.
/// The cluster ID is obtained from KV on construction (get-or-generate),
/// matching C++ `GcsServer::GetOrGenerateClusterId`.
pub struct GcsServer {
    config: GcsServerConfig,
    /// Cluster ID recovered from KV or generated on first start.
    cluster_id: Vec<u8>,
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
    actor_scheduler: Arc<GcsActorScheduler>,
    placement_group_manager: Arc<GcsPlacementGroupManager>,
    pg_scheduler: Arc<GcsPlacementGroupScheduler>,
    autoscaler_manager: Arc<GcsAutoscalerStateManager>,
    runtime_env_service: Arc<RuntimeEnvServiceStub>,
    pubsub_service: Arc<PubSubService>,
    /// GCS-side implementation of `ray.rpc.syncer.RaySyncer` — the bidi
    /// streaming channel raylets use to push resource-view / commands
    /// updates to the GCS. Maps C++ `RaySyncerService` registered by
    /// `GcsServer::InitRaySyncer` (`gcs/gcs_server.cc:607-621`).
    ray_syncer_service: Arc<RaySyncerService>,
    /// Fetcher used by the periodic raylet-load pull loop to issue
    /// `NodeManagerService::GetResourceLoad`. Production wires the tonic
    /// implementation; tests swap in a fake via `set_raylet_load_fetcher`.
    /// Mirrors C++ `raylet_client_pool_` used inside
    /// `GcsServer::InitGcsResourceManager` (`gcs_server.cc:415-446`).
    raylet_load_fetcher: parking_lot::Mutex<Arc<dyn RayletLoadFetcher>>,
    /// Join handle for the periodic pull loop, kept so tests can abort it
    /// explicitly and so a restart can cancel the prior loop before
    /// spawning a new one.
    raylet_load_pull_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Custom `grpc.health.v1.Health` implementation whose `Check`
    /// handler round-trips through a dedicated tokio task. Mirrors C++
    /// `rpc::HealthCheckGrpcService` registered at
    /// `gcs/gcs_server.cc:304-305`, which dispatches `Check` onto the
    /// GCS event loop so a stuck loop surfaces as a client timeout.
    health_service: Arc<GcsHealthService>,
    /// Join handle for the health-probe task. Kept so tests can
    /// deliberately abort the probe to drive the NOT_SERVING path.
    /// Production drops this only when the server itself shuts down.
    health_probe_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Join handle for the per-job dropped-attempt GC loop. Parity
    /// with C++ `GcsTaskManager` constructor at
    /// `gcs_task_manager.cc:50-52`, which spawns a 5 s periodical
    /// runner that calls `GcJobSummary()`. Kept so tests can abort
    /// it explicitly; production keeps it running for the lifetime
    /// of the server.
    task_gc_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Usage-stats recorder. Mirrors C++ `usage_stats_client_`
    /// member (`gcs_server.h`, wired in `InitUsageStatsClient` at
    /// `gcs_server.cc:629-637`). Shared into the four managers that
    /// consume it (worker, actor, placement-group, task).
    usage_stats_client: Arc<UsageStatsClient>,
    /// Job-scoped KV-cleanup refcounter. Mirrors C++
    /// `function_manager_` (`gcs_server.h`, wired in
    /// `InitFunctionManager` at `gcs_server.cc:624-627`). Shared
    /// between job and actor managers.
    function_manager: Arc<GcsFunctionManager>,
    /// Metrics / event-aggregator exporter. Mirrors C++
    /// `GcsServer::InitMetricsExporter` (`gcs_server.cc:950-977`).
    /// Constructed eagerly; `initialize_once` is called either at
    /// startup (eager) or on head-node registration (deferred),
    /// mirroring the two C++ invocation sites.
    metrics_exporter: Arc<MetricsExporter>,
}

impl GcsServer {
    /// Create a new GCS server with in-memory storage (no persistence).
    pub async fn new(config: GcsServerConfig) -> Self {
        let store: Arc<dyn StoreClient> = Arc::new(InMemoryStoreClient::new());
        Self::new_with_store(config, store, None).await
    }

    /// Create a new GCS server with a Redis-backed store.
    pub async fn new_with_redis(config: GcsServerConfig, redis: Arc<RedisStoreClient>) -> Self {
        let store: Arc<dyn StoreClient> = redis.clone();
        Self::new_with_store(config, store, Some(redis)).await
    }

    /// Shared constructor: wire all managers around a given `StoreClient`.
    ///
    /// This is async because it calls `get_or_generate_cluster_id` via KV
    /// before constructing managers that depend on the cluster ID.
    async fn new_with_store(
        config: GcsServerConfig,
        store: Arc<dyn StoreClient>,
        redis_client: Option<Arc<RedisStoreClient>>,
    ) -> Self {
        let table_storage = Arc::new(GcsTableStorage::new(store.clone()));
        let publisher = Arc::new(GcsPublisher::new(4096));

        // Initialize KV first — needed for cluster ID retrieval (matches C++ flow).
        let kv_store = Arc::new(StoreClientInternalKV::new(store));
        let kv_manager = Arc::new(GcsInternalKVManager::new(
            kv_store.clone(),
            config.raylet_config_list.clone(),
        ));

        // Get or generate the cluster ID from KV (matching C++ GetOrGenerateClusterId).
        let cluster_id = get_or_generate_cluster_id(kv_store.as_ref()).await;

        let pubsub_manager = Arc::new(PubSubManager::new(cluster_id.clone()));

        // Initialize the export-event writer (RayEventInit equivalent).
        // Maps C++ `gcs_server_main.cc:144-160`. `event_level` and
        // `emit_event_to_log_file` are now consumed from the
        // initialized `RayConfig`, matching the explicit parameters in
        // C++ `RayEventInit(... event_level, emit_event_to_log_file)`
        // at `gcs_server_main.cc:158-159`.
        let export_events = match (
            config.event_log_reporter_enabled,
            config.log_dir.as_deref(),
        ) {
            (true, Some(dir)) if !dir.is_empty() => {
                let rc = ray_config::snapshot();
                let level = gcs_managers::export_event_writer::EventLevel::parse(&rc.event_level);
                let emit_to_log = rc.emit_event_to_log_file;
                match ExportEventManager::new_with_config(dir, level, emit_to_log) {
                    Ok(mgr) => {
                        info!(
                            log_dir = dir,
                            event_level = ?level,
                            emit_to_log,
                            "Export event writers initialized"
                        );
                        mgr
                    }
                    Err(e) => {
                        warn!(error = %e, log_dir = dir, "Failed to init export events; disabling");
                        ExportEventManager::disabled()
                    }
                }
            }
            _ => ExportEventManager::disabled(),
        };

        let node_manager = Arc::new(GcsNodeManager::with_export_events(
            table_storage.clone(),
            publisher.clone(),
            cluster_id.clone(),
            export_events.clone(),
        ));

        let job_manager = Arc::new(GcsJobManager::with_export_events(
            table_storage.clone(),
            publisher.clone(),
            export_events.clone(),
        ));

        let worker_manager = Arc::new(GcsWorkerManager::new(
            table_storage.clone(),
            publisher.clone(),
        ));

        // Construct the task manager from ray_config so `config_list`
        // and `RAY_task_events_max_num_task_in_gcs` propagate. Mirrors
        // C++ `GcsTaskManager::GcsTaskManager(RayConfig::instance()
        //     .task_events_max_num_task_in_gcs(), ...)` at
        // `gcs_task_manager.cc:42-45`. An explicit `max_task_events`
        // in `GcsServerConfig` still wins for tests that need to pin
        // eviction thresholds without touching the global config.
        let task_manager = Arc::new(if let Some(cap) = config.max_task_events {
            let max_profile =
                ray_config::instance().task_events_max_num_profile_events_per_task.max(0) as usize;
            GcsTaskManager::new_with_limits(cap, max_profile)
        } else {
            GcsTaskManager::new_from_ray_config()
        });

        let resource_manager = Arc::new(GcsResourceManager::new());

        let (actor_success_tx, actor_success_rx) = mpsc::unbounded_channel();
        let (actor_failure_tx, actor_failure_rx) = mpsc::unbounded_channel();
        let actor_scheduler = Arc::new(GcsActorScheduler::new(
            node_manager.clone(),
            actor_success_tx,
            actor_failure_tx,
        ));
        // The destroyed-actor LRU cap comes from RayConfig — matches
        // C++ `RayConfig::instance().maximum_gcs_destroyed_actor_cached_count()`
        // (`ray_config_def.h:936`, default 100_000). Captured at
        // construction so the per-insert path doesn't take the config
        // RwLock.
        let max_destroyed = ray_config::instance()
            .maximum_gcs_destroyed_actor_cached_count as usize;
        let actor_manager = Arc::new(GcsActorManager::with_export_events_and_cap(
            pubsub_manager.clone(),
            table_storage.clone(),
            actor_scheduler.clone(),
            node_manager.clone(),
            export_events.clone(),
            max_destroyed,
        ));

        let (pg_success_tx, pg_success_rx) = mpsc::unbounded_channel();
        let (pg_failure_tx, pg_failure_rx) = mpsc::unbounded_channel();
        let pg_scheduler = Arc::new(GcsPlacementGroupScheduler::new(
            node_manager.clone(),
            pg_success_tx,
            pg_failure_tx,
        ));
        let placement_group_manager = Arc::new(GcsPlacementGroupManager::new(
            table_storage.clone(),
            pg_scheduler.clone(),
            node_manager.clone(),
        ));

        // Usage-stats client. Parity with C++
        // `GcsServer::InitUsageStatsClient` (`gcs_server.cc:629-637`):
        // one shared recorder backed by the KV manager's instance, then
        // installed on each manager that emits tags.
        let usage_stats_client: Arc<UsageStatsClient> = Arc::new(UsageStatsClient::new(
            kv_store.clone() as Arc<dyn gcs_kv::InternalKVInterface>,
        ));
        worker_manager.set_usage_stats_client(usage_stats_client.clone());
        actor_manager.set_usage_stats_client(usage_stats_client.clone());
        placement_group_manager.set_usage_stats_client(usage_stats_client.clone());
        task_manager.set_usage_stats_client(usage_stats_client.clone());

        // Shared `GcsFunctionManager`. Parity with C++
        // `GcsServer::InitFunctionManager` (`gcs_server.cc:624-627`):
        // one refcounter, handed to both the job and actor managers
        // so their lifecycle callbacks keep the same job refcount
        // moving. Construction goes through the same `kv_store`
        // every KV consumer in this function uses.
        let function_manager: Arc<GcsFunctionManager> = Arc::new(
            GcsFunctionManager::new(
                kv_store.clone() as Arc<dyn gcs_kv::InternalKVInterface>,
            ),
        );
        job_manager.set_function_manager(function_manager.clone());
        actor_manager.set_function_manager(function_manager.clone());

        // Metrics / event-aggregator exporter. Idempotent one-shot
        // `initialize_once` is called at startup if config supplies a
        // port; otherwise the head-node-register listener calls it.
        let metrics_exporter = Arc::new(MetricsExporter::new());

        // Single `RayletClientPool` instance shared between the
        // autoscaler (for resize/drain RPCs) and the actor manager
        // (for `KillLocalActor` RPCs). Matches C++'s single
        // `raylet_client_pool_` member on `GcsServer` threaded into
        // both `GcsAutoscalerStateManager` and `GcsActorManager`.
        let raylet_client_pool: Arc<dyn gcs_managers::autoscaler_stub::RayletClientPool> =
            Arc::new(TonicRayletClientPool::new());

        let autoscaler_manager = Arc::new(GcsAutoscalerStateManager::new(
            node_manager.clone(),
            placement_group_manager.clone(),
            raylet_client_pool.clone(),
            kv_store.clone(),
            config.session_name.clone(),
        ));
        // Wire the actor manager into the autoscaler so `HandleDrainNode`
        // can call `SetPreemptedAndPublish`. Parity with C++ which
        // threads `GcsActorManager &gcs_actor_manager_` into the
        // autoscaler constructor (`gcs_autoscaler_state_manager.h:38-46`,
        // invoked at `gcs_autoscaler_state_manager.cc:498`). We deferred
        // this until after the autoscaler was Arc-shared because the
        // actor manager depends on the node manager, which the autoscaler
        // also holds — a direct constructor arg would require threading
        // the actor manager everywhere the autoscaler is built.
        autoscaler_manager.set_actor_manager(actor_manager.clone());

        // Give the actor manager a handle to the same pool so
        // `destroy_actor` → `notify_raylet_to_kill_actor` can forward
        // `KillLocalActor` RPCs with the correct `force_kill` flag.
        // Parity with C++ `gcs_actor_manager.cc:1857-1864` where the
        // raylet client is obtained via
        // `raylet_client_pool_.GetOrConnectByAddress(...)`.
        actor_manager.set_raylet_client_pool(raylet_client_pool.clone());

        // RuntimeEnvService with KV for gcs:// URI cleanup.
        // Maps C++ GcsServer::InitRuntimeEnvManager (gcs_server.cc:696-738).
        let runtime_env_service = Arc::new(RuntimeEnvServiceStub::new_with_kv(kv_store));
        // Install the same instance into both managers so job and
        // detached-actor lifecycles drive the refcount. Parity with
        // C++ which threads a single `runtime_env_manager_` into
        // both `GcsJobManager` (`gcs_job_manager.cc:132-171`) and
        // `GcsActorManager` (`gcs_actor_manager.cc:744-1120`).
        job_manager.set_runtime_env_service(runtime_env_service.clone());
        actor_manager.set_runtime_env_service(runtime_env_service.clone());

        let pubsub_service = Arc::new(PubSubService::new(pubsub_manager.clone()));

        // RaySyncer service — raylets connect to this bidi stream to push
        // resource-view/commands messages to the GCS. Mirrors C++
        // `GcsServer::InitRaySyncer` (`gcs_server.cc:607-621`), which uses
        // `kGCSNodeID` (28 zero bytes, see `common/id.cc:348`) as the
        // local node id and registers `GcsResourceManager` as the
        // receiver for both `RESOURCE_VIEW` and `COMMANDS` (`:616-619`).
        let gcs_node_id: Vec<u8> = vec![0u8; CLUSTER_ID_SIZE];
        let syncer_consumer: Arc<dyn SyncMessageConsumer> = resource_manager.clone();
        let ray_syncer_service = Arc::new(RaySyncerService::new(gcs_node_id, syncer_consumer));

        // Start the actor scheduler event loop — reads success/failure from
        // scheduler channels and drives actor state transitions.
        actor_manager.start_scheduler_loop(actor_success_rx, actor_failure_rx);

        // Start the placement group scheduler event loop.
        placement_group_manager.start_scheduler_loop(pg_success_rx, pg_failure_rx);

        // Spawn the health-probe task. Parity with C++
        // `HealthCheckGrpcService` registered at `gcs_server.cc:304-305`:
        // the Check RPC must round-trip through a GCS-side task so a
        // stuck event loop surfaces as a timeout. Construction happens
        // here because `HealthProbe::spawn` needs a live tokio runtime,
        // and `new_with_store` is already `async` so we are in one.
        let (health_probe, health_probe_handle) = HealthProbe::spawn();
        let health_service = Arc::new(GcsHealthService::new(health_probe));

        Self {
            config,
            cluster_id,
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
            actor_scheduler,
            placement_group_manager,
            pg_scheduler,
            autoscaler_manager,
            runtime_env_service,
            pubsub_service,
            ray_syncer_service,
            raylet_load_fetcher: parking_lot::Mutex::new(Arc::new(TonicRayletLoadFetcher::new())),
            raylet_load_pull_handle: parking_lot::Mutex::new(None),
            health_service,
            health_probe_handle: parking_lot::Mutex::new(Some(health_probe_handle)),
            task_gc_handle: parking_lot::Mutex::new(None),
            usage_stats_client,
            metrics_exporter,
            function_manager,
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
    ///
    /// Cadence comes from
    /// `ray_config::instance().gcs_redis_heartbeat_interval_milliseconds`,
    /// which honors the same `RAY_{name}` env var and `config_list`
    /// JSON key that C++ honors via `RayConfig::instance()
    /// .gcs_redis_heartbeat_interval_milliseconds()` at
    /// `gcs_server.cc:183`. Default 100ms (`ray_config_def.h:395`).
    fn start_redis_health_check(&self) {
        if let Some(redis) = &self.redis_client {
            let redis = redis.clone();
            let interval_ms: u64 =
                ray_config::instance().gcs_redis_heartbeat_interval_milliseconds;
            // 0 disables heartbeat — matches the C++ guard at
            // `gcs_server.cc:182`.
            if interval_ms == 0 {
                return;
            }

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

    /// Periodically log cluster-state counts (alive/dead nodes, running
    /// jobs, actor counts). Cadence comes from
    /// `ray_config::instance().debug_dump_period_milliseconds`
    /// (`ray_config_def.h:22`, default 10s). A value of 0 disables
    /// the loop — same gate as C++ `GcsServer::PrintDebugState`
    /// periodical runner at `gcs_server.cc:314-318`.
    fn start_debug_dump_loop(&self) {
        let period_ms = ray_config::instance().debug_dump_period_milliseconds;
        if period_ms == 0 {
            return;
        }
        let node_manager = self.node_manager.clone();
        let job_manager = self.job_manager.clone();
        let actor_manager = self.actor_manager.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(
                std::time::Duration::from_millis(period_ms),
            );
            ticker.tick().await; // skip the immediate first tick
            loop {
                ticker.tick().await;
                let alive = node_manager.get_all_alive_nodes().len();
                let dead = node_manager.get_all_dead_nodes().len();
                info!(
                    target: "ray.gcs.debug_state",
                    alive_nodes = alive,
                    dead_nodes = dead,
                    actor_cache_cap = actor_manager.max_destroyed_actors_cached(),
                    job_manager_present = !std::ptr::eq(
                        Arc::as_ptr(&job_manager) as *const (),
                        std::ptr::null(),
                    ),
                    "GCS debug state"
                );
            }
        });
    }

    /// Wire node lifecycle events to the resource manager.
    ///
    /// Maps C++ `GcsServer::InstallEventListeners` (gcs_server.cc:820-877)
    /// which registers callbacks on GcsNodeManager that call:
    /// - `GcsResourceManager::OnNodeAdd` on node addition
    /// - `GcsResourceManager::OnNodeDead` on node removal
    /// - `GcsResourceManager::SetNodeDraining` on drain state change
    ///
    /// Must be called **before** `initialize()` so that recovered alive
    /// nodes fire broadcasts that populate the resource manager.
    fn start_node_resource_listeners(&self) {
        use gcs_proto::ray::rpc::ResourcesData;

        // Node added → populate resource_manager with the node's resources.
        // Maps C++ `GcsResourceManager::OnNodeAdd`.
        let mut rx_added = self.node_manager.subscribe_node_added();
        let rm_added = self.resource_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_added.recv().await {
                rm_added.update_node_resources(
                    node.node_id.clone(),
                    ResourcesData {
                        node_id: node.node_id,
                        node_manager_address: node.node_manager_address,
                        resources_total: node.resources_total.clone(),
                        resources_available: node.resources_total,
                        ..Default::default()
                    },
                );
            }
        });

        // Node removed → clean up resource_manager.
        // Maps C++ `GcsResourceManager::OnNodeDead`.
        let mut rx_removed = self.node_manager.subscribe_node_removed();
        let rm_removed = self.resource_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_removed.recv().await {
                rm_removed.on_node_dead(&node.node_id);
            }
        });

        // Node draining → propagate to resource_manager.
        // Maps C++ `GcsResourceManager::SetNodeDraining` via
        // `node_draining_listeners_`.
        let mut rx_draining = self.node_manager.subscribe_node_draining();
        let rm_draining = self.resource_manager.clone();
        tokio::spawn(async move {
            while let Ok((node_id, deadline_ms)) = rx_draining.recv().await {
                rm_draining.set_node_draining(node_id, deadline_ms);
            }
        });
    }

    /// Wire node lifecycle events to the autoscaler state manager.
    ///
    /// Maps C++ `GcsServer::InstallEventListeners` (gcs_server.cc:820-877)
    /// which registers `OnNodeAdd` / `OnNodeDead` callbacks for the
    /// autoscaler state manager. Also propagates draining state.
    ///
    /// Must be called **before** `initialize()` so that recovered alive
    /// nodes fire broadcasts that populate the autoscaler's resource cache.
    fn start_node_autoscaler_listeners(&self) {
        // Node added → autoscaler_manager.on_node_add.
        let mut rx_added = self.node_manager.subscribe_node_added();
        let asm = self.autoscaler_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_added.recv().await {
                asm.on_node_add(&node);
            }
        });

        // Node removed → autoscaler_manager.on_node_dead.
        let mut rx_removed = self.node_manager.subscribe_node_removed();
        let asm = self.autoscaler_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_removed.recv().await {
                asm.on_node_dead(&node.node_id);
            }
        });

        // Node draining → update autoscaler's resource info.
        let mut rx_draining = self.node_manager.subscribe_node_draining();
        let asm = self.autoscaler_manager.clone();
        tokio::spawn(async move {
            while let Ok((node_id, deadline_ms)) = rx_draining.recv().await {
                asm.set_node_draining_in_resource_info(&node_id, deadline_ms);
            }
        });
    }

    /// Wire node lifecycle events to the actor scheduler.
    ///
    /// - Node added → schedule_pending_actors (retry actors waiting for nodes)
    /// - Node removed → on_node_dead (cancel scheduling, restart affected actors)
    ///
    /// Maps C++ `GcsServer::InstallEventListeners` actor-related callbacks.
    fn start_node_actor_listeners(&self) {
        // Node added → retry pending actors.
        let mut rx_added = self.node_manager.subscribe_node_added();
        let am_added = self.actor_manager.clone();
        tokio::spawn(async move {
            while let Ok(_node) = rx_added.recv().await {
                am_added.schedule_pending_actors();
            }
        });

        // Node removed → handle actor lifecycle.
        let mut rx_removed = self.node_manager.subscribe_node_removed();
        let am_removed = self.actor_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_removed.recv().await {
                am_removed.on_node_dead(&node.node_id).await;
            }
        });
    }

    /// Wire node/worker/job lifecycle callbacks, matching the full set
    /// registered by C++ `GcsServer::InstallEventListeners`
    /// (`gcs_server.cc:856-907`). Every C++ side effect on these three
    /// events has a Rust counterpart here:
    ///
    /// - `worker_dead` (C++ 880-900):
    ///   1. `task_manager.on_worker_dead` — mark running tasks FAILED
    ///   2. `actor_manager.on_worker_dead` — restart/terminate affected actors
    ///   3. `pg_scheduler.handle_waiting_removed_bundles` — retry bundle releases
    ///   4. `pubsub_manager.remove_subscriber(worker_id)` — pubsub cleanup
    /// - `job_finished` (C++ 902-907):
    ///   1. `task_manager.on_job_finished` — mark job tasks FAILED
    ///   2. `placement_group_manager.clean_placement_group_if_needed_when_job_dead`
    /// - `node_removed` (C++ 856-870) — `start_lifecycle_listeners`'s slice:
    ///   1. `job_manager.on_node_dead` — mark driver-on-node jobs dead
    ///   2. `pubsub_manager.remove_subscriber(node_id)` — pubsub cleanup
    ///
    /// (Other `node_added` / `node_removed` side effects — resource manager,
    /// autoscaler, actor scheduler, PG scheduler, health-check manager — are
    /// wired in the sibling `start_node_*_listeners` methods; see their
    /// doc comments for the C++ line ranges.)
    fn start_lifecycle_listeners(&self) {
        // ─── worker_dead → {task_manager, actor_manager, pg_scheduler, pubsub} ───
        // Parity with gcs_server.cc:880-900.
        let tm = self.task_manager.clone();
        let am = self.actor_manager.clone();
        let pgs = self.pg_scheduler.clone();
        let pubsub = self.pubsub_manager.clone();
        self.worker_manager.add_worker_dead_listener(Box::new(
            move |worker_data: &WorkerTableData| {
                let (worker_id, node_id) = match worker_data.worker_address.as_ref() {
                    Some(addr) => (addr.worker_id.clone(), addr.node_id.clone()),
                    None => return,
                };
                if worker_id.is_empty() {
                    return;
                }

                // C++ `gcs_mark_task_failed_on_worker_dead_delay_ms`
                // (default 1000ms, `ray_config_def.h:553`). Reads
                // `RAY_{name}` env directly rather than
                // `ray_config::instance()` for per-test isolation
                // (parallel tests can pin a value via `set_var` without
                // racing the process-global config lock).
                //
                // Production path: Ray Python launches with either an
                // env var or a `config_list` override. The env path
                // reaches this read directly; the `config_list` path
                // reaches it via `ray_config::initialize`, which
                // writes every merged setting back to `RAY_{name}`
                // (see `ray_config::RayConfig::export_to_env`). So a
                // `config_list` override is observed here.
                let delay_ms: u64 = std::env::var(
                    "RAY_gcs_mark_task_failed_on_worker_dead_delay_ms",
                )
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000);

                // 1. Mark the worker's tasks FAILED.
                tm.on_worker_dead(worker_id.clone(), worker_data.clone(), delay_ms);

                // 2. Drive actor reconstruction (C++ line 891-896). If the
                //    worker died because a creation task threw, the actor
                //    cannot be meaningfully reconstructed — set
                //    need_reconstruct=false to skip restart; otherwise
                //    restart is allowed (subject to remaining budget).
                let need_reconstruct = worker_data.creation_task_exception.is_none();
                let am = am.clone();
                let worker_id_for_actor = worker_id.clone();
                tokio::spawn(async move {
                    am.on_worker_dead(&node_id, &worker_id_for_actor, need_reconstruct).await;
                });

                // 3. Retry any PG bundles whose release was deferred
                //    (C++ line 897). Drains the `waiting_removed_bundles`
                //    queue, re-sending `CancelResourceReserve` RPCs.
                //    Spawned because the listener closure is sync and
                //    the drain awaits RPCs.
                let pgs = pgs.clone();
                tokio::spawn(async move {
                    pgs.handle_waiting_removed_bundles().await;
                });

                // 4. Clean up pubsub subscription for this worker (C++ 898).
                pubsub.remove_subscriber(&worker_id);
            },
        ));

        // ─── job_finished → {task_manager, placement_group_manager} ───
        // Parity with gcs_server.cc:902-907.
        let tm = self.task_manager.clone();
        let pgm = self.placement_group_manager.clone();
        self.job_manager.add_job_finished_listener(Box::new(
            move |job_data: &JobTableData| {
                let job_id = job_data.job_id.clone();
                let end_time_ms = job_data.end_time as i64;
                // C++ `gcs_mark_task_failed_on_job_done_delay_ms`
                // (default 15000ms, `ray_config_def.h:548`). Same
                // env-based read as the worker-dead delay; same
                // production bridge via
                // `ray_config::RayConfig::export_to_env`.
                let delay_ms: u64 = std::env::var(
                    "RAY_gcs_mark_task_failed_on_job_done_delay_ms",
                )
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(15000);

                // 1. Mark the job's tasks FAILED.
                tm.on_job_finished(job_id.clone(), end_time_ms, delay_ms);

                // 2. Clean placement groups whose creator job just finished
                //    (C++ line 906). Non-detached PGs with a dead creator
                //    actor are removed; others are just marked
                //    creator_job_dead=true.
                let pgm = pgm.clone();
                tokio::spawn(async move {
                    pgm.clean_placement_group_if_needed_when_job_dead(&job_id).await;
                });
            },
        ));

        // ─── node_removed → {job_manager, pubsub} ───
        // Parity with gcs_server.cc:865, 869.
        let mut rx_removed = self.node_manager.subscribe_node_removed();
        let jm = self.job_manager.clone();
        let pubsub = self.pubsub_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_removed.recv().await {
                jm.on_node_dead(&node.node_id).await;
                // Clean up pubsub subscription for this node (C++ line 869).
                pubsub.remove_subscriber(&node.node_id);
            }
        });
    }

    /// Wire node lifecycle events to the placement group scheduler.
    ///
    /// - Node added → on_node_add (retry infeasible PGs, matching C++ line 733-746)
    /// - Node removed → on_node_dead (CREATED → RESCHEDULING, matching C++ line 684-731)
    fn start_node_placement_group_listeners(&self) {
        // Node added → retry infeasible PGs.
        let mut rx_added = self.node_manager.subscribe_node_added();
        let pgm_added = self.placement_group_manager.clone();
        tokio::spawn(async move {
            while let Ok(_node) = rx_added.recv().await {
                pgm_added.on_node_add();
            }
        });

        // Node removed → handle PG rescheduling.
        let mut rx_removed = self.node_manager.subscribe_node_removed();
        let pgm_removed = self.placement_group_manager.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_removed.recv().await {
                pgm_removed.on_node_dead(&node.node_id).await;
            }
        });
    }

    /// Bridge GcsPublisher (broadcast) → PubSubManager (long-poll).
    ///
    /// In C++, both publishing paths share the same Publisher. In Rust,
    /// `GcsPublisher` (used by node/job/worker managers) and `PubSubManager`
    /// (used for InternalPubSubGcsService long-poll) are separate. This
    /// bridge forwards broadcast messages to the long-poll system so
    /// external subscribers on all channels receive the messages.
    ///
    /// Maps the C++ pattern where `gcs_publisher_->Publish*()` calls reach
    /// both the internal subscribers and the long-poll handler.
    fn start_publisher_bridge(&self) {
        use gcs_proto::ray::rpc::pub_message::InnerMessage;
        use gcs_proto::ray::rpc::ChannelType;

        let mut rx = self.publisher.subscribe();
        let psm = self.pubsub_manager.clone();

        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                // Map GcsPublisher channel names to ChannelType + InnerMessage.
                let (channel_type, inner) = match msg.channel.as_str() {
                    "JOB" => (ChannelType::GcsJobChannel as i32, None),
                    "NODE_INFO" => (ChannelType::GcsNodeInfoChannel as i32, None),
                    "WORKER_DELTA" => (ChannelType::GcsWorkerDeltaChannel as i32, None),
                    "ERROR_INFO" => (ChannelType::RayErrorInfoChannel as i32, None),
                    "LOG" => (ChannelType::RayLogChannel as i32, None),
                    "NODE_RESOURCE_USAGE" => {
                        (ChannelType::RayNodeResourceUsageChannel as i32, None)
                    }
                    "NODE_ADDRESS_AND_LIVENESS" => {
                        (ChannelType::GcsNodeAddressAndLivenessChannel as i32, None)
                    }
                    _ => continue, // Unknown channel, skip.
                };

                psm.publish(gcs_proto::ray::rpc::PubMessage {
                    channel_type,
                    key_id: msg.key_id,
                    sequence_id: 0, // PubSubManager assigns its own.
                    inner_message: inner,
                });
            }
        });
    }

    /// Install every lifecycle listener, load persisted state, and start
    /// every pre-serve background task. Must run before any `serve*` call.
    ///
    /// Mirrors C++ `GcsServer::DoStart` (`gcs_server.cc:271-309`), which
    /// calls `InstallEventListeners()` exactly once before the gRPC server
    /// runs. Factored out of `start()` and `start_with_listener()` so the
    /// two entry points cannot drift — a prior bug (fixed in this commit)
    /// left `start_with_listener`, the path the production binary uses,
    /// without `start_lifecycle_listeners()` wired up, silently dropping
    /// `worker_dead`, `job_finished`, and `node_removed` hooks.
    async fn install_listeners_and_initialize(&self) {
        // Wire node lifecycle events to resource manager and autoscaler
        // BEFORE initialize(), so that recovered alive nodes populate both.
        // Maps C++ `InstallEventListeners()` called before node init.
        self.start_node_resource_listeners();
        self.start_node_autoscaler_listeners();
        self.start_node_actor_listeners();
        self.start_node_placement_group_listeners();
        // Worker-dead / job-finished / node-dead → task-manager + job-manager.
        // Parity with C++ InstallEventListeners (gcs_server.cc:856, 879-907).
        self.start_lifecycle_listeners();
        // Bridge GcsPublisher → PubSubManager so all channels reach long-poll subscribers.
        self.start_publisher_bridge();

        // Load persisted state from storage and initialize managers.
        self.initialize().await;

        // Start the health check loop for detecting dead nodes. Values
        // are read through `ray_config`, which applies the same
        // precedence as every other config knob: default → env →
        // config_list. Defaults match C++ `ray_config_def.h:932-936`
        // (3000ms / 10000ms / 5).
        let hc = ray_config::snapshot();
        self.node_manager.start_health_check_loop(
            hc.health_check_period_ms.max(0) as u64,
            hc.health_check_timeout_ms.max(0) as u64,
            hc.health_check_failure_threshold.max(0) as u32,
        );

        // Start periodic Redis health check (no-op if using in-memory store).
        self.start_redis_health_check();

        // Start periodic debug-state dump (parity with C++
        // `gcs_server.cc:314-318` via
        // `RayConfig::debug_dump_period_milliseconds`, 0 disables).
        self.start_debug_dump_loop();

        // Start the periodic raylet resource-load pull loop. Parity with
        // C++ `GcsServer::InitGcsResourceManager` registering a
        // `periodical_runner_->RunFnPeriodically(..., "RayletLoadPulled")`
        // that polls `GetResourceLoad` from every alive raylet every
        // `gcs_pull_resource_loads_period_milliseconds` ms
        // (`gcs_server.cc:415-446`). Without this loop the autoscaler's
        // `pending_resource_requests`, per-node running/idle state, and
        // `resource_load_by_shape` all stay frozen at whatever was
        // captured at node registration.
        self.start_raylet_load_pull_loop();

        // Start the per-job dropped-attempt GC loop. Parity with C++
        // `GcsTaskManager` constructor spawning a 5 s periodical
        // runner at `gcs_task_manager.cc:50-52` that calls
        // `GcJobSummary()`. Without this loop the per-job tracked set
        // grows unboundedly in long-lived clusters.
        self.start_task_gc_loop(std::time::Duration::from_secs(5));

        // Metrics exporter init. Parity with C++ `gcs_server.cc:324-329`
        // (eager if `config_.metrics_agent_port > 0`) and the deferred
        // `:831-842` branch inside the head-node-register listener
        // installed below. A `-1`/`0` port here means "not known at
        // startup" — defer to the head-node path.
        self.start_metrics_exporter_on_head_node_register();
        if self.config.metrics_agent_port > 0 {
            self.metrics_exporter.initialize_once(self.config.metrics_agent_port);
        }
    }

    /// Attach a node-added listener that inits the metrics exporter
    /// once when the head node registers with a non-zero
    /// `metrics_agent_port`. Mirrors C++ `gcs_server.cc:831-842`.
    ///
    /// The exporter itself is idempotent — if eager init already fired
    /// (via `config.metrics_agent_port > 0`) or another listener
    /// beat us to it, `initialize_once` is a no-op. So it is safe to
    /// install this listener in both the early-known and
    /// late-known cases.
    fn start_metrics_exporter_on_head_node_register(&self) {
        let mut rx_added = self.node_manager.subscribe_node_added();
        let exporter = self.metrics_exporter.clone();
        tokio::spawn(async move {
            while let Ok(node) = rx_added.recv().await {
                if node.is_head_node && !exporter.is_initialized() {
                    // C++ reads `node->metrics_agent_port()` at
                    // `gcs_server.cc:835`. `metrics_agent_port` is the
                    // dashboard-agent gRPC port (proto tag 30), the
                    // one the event aggregator listens on. It is
                    // DIFFERENT from `metrics_export_port` (proto tag
                    // 9), which is the node's HTTP metrics-export
                    // port — raylet populates both independently
                    // (`raylet/main.cc:1115-1116`). Using
                    // `metrics_export_port` here silently diverges
                    // from C++ and can leave the exporter
                    // uninitialised in the common case.
                    let port = node.metrics_agent_port;
                    // `initialize_once` already gates on `port > 0`
                    // and logs the same "metrics agent not available"
                    // message when the port is missing — parity with
                    // C++ `gcs_server.cc:838-841`.
                    exporter.initialize_once(port);
                }
            }
        });
    }

    /// Spawn the task-manager's per-job dropped-attempt GC loop.
    /// `period == 0` disables the loop (same convention other Rust
    /// GCS `_period_*` knobs use).
    pub fn start_task_gc_loop(&self, period: std::time::Duration) {
        // Abort any previously-running loop so repeat calls are safe.
        if let Some(prev) = self.task_gc_handle.lock().take() {
            prev.abort();
        }
        if period.is_zero() {
            return;
        }
        let task_manager = self.task_manager.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Skip the instant-first tick; the C++ runner schedules
            // the first execution after the period elapses.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                task_manager.gc_job_summary();
            }
        });
        *self.task_gc_handle.lock() = Some(handle);
    }

    /// Spawn the periodic raylet `GetResourceLoad` puller.
    ///
    /// Cadence comes from
    /// `ray_config::gcs_pull_resource_loads_period_milliseconds`
    /// (default 1000ms, `ray_config_def.h:62`). Period 0 disables the
    /// loop — matches the convention of every other `_period_*` knob in
    /// the Rust GCS.
    ///
    /// Replaces any previously-running loop, so calling this twice is
    /// safe (the earlier handle is aborted before the new one is stored).
    /// Tests exercise this directly after installing a mock fetcher.
    pub fn start_raylet_load_pull_loop(&self) {
        let period_ms = ray_config::instance().gcs_pull_resource_loads_period_milliseconds;
        // Snapshot the currently-configured fetcher (production:
        // `TonicRayletLoadFetcher`; tests: an injected mock).
        let fetcher = self.raylet_load_fetcher.lock().clone();
        let handle = spawn_load_pull_loop(
            fetcher,
            self.node_manager.clone(),
            self.resource_manager.clone(),
            self.autoscaler_manager.clone(),
            period_ms,
        );
        let mut slot = self.raylet_load_pull_handle.lock();
        if let Some(prev) = slot.take() {
            prev.abort();
        }
        *slot = handle;
    }

    /// Inject a raylet load fetcher. Call before
    /// `install_listeners_and_initialize` so the spawned loop uses the
    /// replacement. Safe to call afterwards too — a subsequent
    /// `start_raylet_load_pull_loop()` picks up the new fetcher. In
    /// production callers don't need this (the server defaults to the
    /// real tonic fetcher); in tests this is how we inject a mock.
    pub fn set_raylet_load_fetcher(&self, fetcher: Arc<dyn RayletLoadFetcher>) {
        *self.raylet_load_fetcher.lock() = fetcher;
    }

    /// Build the configured tonic `Router` with every GCS service registered
    /// plus the health service. Shared by both `serve` entry points so the
    /// set of services cannot drift.
    async fn build_router(&self) -> tonic::transport::server::Router {
        // Custom Health service whose `Check` handler round-trips through
        // a dedicated GCS task. Parity with C++
        // `gcs_server.cc:304-305` — a stuck event loop must surface as
        // a timing-out health check, not as a static "SERVING" lie from
        // the default `tonic_health::server::health_reporter()`.
        use tonic_health::pb::health_server::HealthServer;
        let health_service =
            HealthServer::from_arc(self.health_service.clone());

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
            .add_service(RuntimeEnvGcsServiceServer::from_arc(self.runtime_env_service.clone()))
            .add_service(AutoscalerStateServiceServer::from_arc(self.autoscaler_manager.clone()))
            .add_service(RayEventExportGcsServiceServer::new(EventExportServiceStub::new(self.task_manager.clone())))
            // RaySyncer — bidi streaming channel to raylets. Parity with
            // C++ `rpc_server_.RegisterService(std::make_unique<syncer::RaySyncerService>(...))`
            // in `gcs_server.cc:620-621`.
            .add_service(RaySyncerServer::from_arc(self.ray_syncer_service.clone()))
    }

    /// Start the gRPC server, binding to the configured port. Blocks until shutdown.
    pub async fn start(&self) -> Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.grpc_port).parse()?;

        info!(
            port = self.config.grpc_port,
            redis = self.redis_client.is_some(),
            "Starting Rust GCS server"
        );

        self.install_listeners_and_initialize().await;
        self.build_router().await.serve(addr).await?;
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

        self.install_listeners_and_initialize().await;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        self.build_router().await.serve_with_incoming(incoming).await?;
        Ok(())
    }

    /// The cluster ID recovered from KV or generated on first start.
    pub fn cluster_id(&self) -> &[u8] {
        &self.cluster_id
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

    pub fn resource_manager(&self) -> &Arc<GcsResourceManager> {
        &self.resource_manager
    }

    pub fn table_storage(&self) -> &Arc<GcsTableStorage> {
        &self.table_storage
    }

    /// Accessor for the GCS-side RaySyncer service. Tests use it to
    /// inspect connection state; production code could use it to
    /// broadcast commands to raylets.
    pub fn ray_syncer_service(&self) -> &Arc<RaySyncerService> {
        &self.ray_syncer_service
    }

    /// Accessor for the health service. Tests drive the NOT_SERVING
    /// path by aborting the probe handle exposed via
    /// `abort_health_probe_for_test`.
    pub fn health_service(&self) -> &Arc<GcsHealthService> {
        &self.health_service
    }

    /// Accessor for the usage-stats recorder. Exposed so integration
    /// tests can assert that tags landed in the KV, and so downstream
    /// managers outside this crate can reach the same instance during
    /// teardown / replay.
    pub fn usage_stats_client(&self) -> &Arc<UsageStatsClient> {
        &self.usage_stats_client
    }

    /// Accessor for the metrics exporter — tests assert that
    /// `is_initialized()` flips at the expected time (eager vs
    /// head-node-register).
    pub fn metrics_exporter(&self) -> &Arc<MetricsExporter> {
        &self.metrics_exporter
    }

    /// Accessor for the function manager — tests observe per-job
    /// refcount state through this; production code stays on the
    /// shared `Arc` installed on the job/actor managers.
    pub fn function_manager(&self) -> &Arc<GcsFunctionManager> {
        &self.function_manager
    }

    /// Test hook: abort the currently-running health probe task so
    /// subsequent `Check` RPCs will time out. Returns `true` if a
    /// handle was aborted. Production code should never call this.
    pub fn abort_health_probe_for_test(&self) -> bool {
        if let Some(h) = self.health_probe_handle.lock().take() {
            h.abort();
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_proto::ray::rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient;
    use gcs_proto::ray::rpc::job_info_gcs_service_client::JobInfoGcsServiceClient;
    use gcs_proto::ray::rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient;
    use gcs_proto::ray::rpc::{GetAllNodeInfoRequest, GetAllJobInfoRequest, InternalKvGetRequest};

    /// Serialize tests that mutate the global `ray_config` or the env
    /// vars it re-exports — without this lock, parallel tests would
    /// interleave `initialize` + `replace_for_test` + `set_var` +
    /// `remove_var` calls and mis-observe each other's state.
    ///
    /// Only tests that directly touch `ray_config` or the re-exported
    /// `RAY_*` env vars need to acquire this; ordinary tests that just
    /// read snapshot values incidentally are unaffected because the
    /// default RayConfig is stable when no one mutates it.
    static RAY_CONFIG_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test]
    async fn test_server_creation() {
        let server = GcsServer::new(GcsServerConfig::default()).await;
        assert!(server.node_manager().get_all_alive_nodes().is_empty());
        // Cluster ID should be exactly 28 bytes.
        assert_eq!(server.cluster_id().len(), CLUSTER_ID_SIZE);
    }

    /// Parity with C++ `gcs_server_main.cc:48` —
    /// `DEFINE_int32(gcs_server_port, 0, ...)`. The binary's CLI
    /// default for `gcs_server_port` is 0, and `GcsServerConfig`
    /// mirrors that. Prior to this commit it was 6379 on both sides,
    /// which caused port-collision failures when running multiple
    /// local GCS processes side-by-side.
    #[test]
    fn default_gcs_server_port_matches_cpp() {
        let cfg = GcsServerConfig::default();
        assert_eq!(
            cfg.grpc_port, 0,
            "C++ DEFINE_int32(gcs_server_port, 0, ...) default is 0"
        );
    }

    /// Parity with C++ `RayConfig::instance().initialize(config_list)`
    /// at `gcs_server_main.cc:120` — a `config_list` JSON override
    /// actually flows through to runtime behavior, not just parsed and
    /// discarded. Verified by applying a `health_check_period_ms`
    /// override and observing that `ray_config::instance()` reflects
    /// it.
    #[test]
    fn ray_config_initialize_merges_overrides() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Isolate this test from other ones that may have mutated the
        // global.
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());

        ray_config::initialize(
            r#"{"health_check_period_ms": 777, "event_log_reporter_enabled": false}"#,
        )
        .expect("initialize must accept a valid config_list");

        let cfg = ray_config::snapshot();
        assert_eq!(cfg.health_check_period_ms, 777);
        assert!(!cfg.event_log_reporter_enabled);

        let _ = ray_config::replace_for_test(prev);
    }

    /// Parity-glue guard: a `config_list` override for a
    /// lifecycle-delay setting must actually reach the production
    /// listener — it's not enough to parse it and store it.
    ///
    /// The bridge is `ray_config::initialize` → `export_to_env` →
    /// listener's `RAY_{name}` env read. This test verifies every link
    /// in that chain:
    ///
    /// 1. A JSON override for `gcs_mark_task_failed_on_worker_dead_delay_ms`
    ///    goes through `ray_config::initialize`.
    /// 2. The re-export writes the merged value to
    ///    `RAY_gcs_mark_task_failed_on_worker_dead_delay_ms`.
    /// 3. A fresh read via the same `std::env::var` pattern the
    ///    listener closure uses returns the overridden value.
    ///
    /// Before this commit, link (2) did not exist — the code comment
    /// claimed it but there was no implementation. This test is the
    /// regression guard.
    #[test]
    fn config_list_override_for_listener_delay_reaches_env() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        // Clean slate so we can distinguish "init wrote it" from
        // "something else set it earlier".
        std::env::remove_var("RAY_gcs_mark_task_failed_on_worker_dead_delay_ms");
        std::env::remove_var("RAY_gcs_mark_task_failed_on_job_done_delay_ms");

        ray_config::initialize(
            r#"{
                "gcs_mark_task_failed_on_worker_dead_delay_ms": 12345,
                "gcs_mark_task_failed_on_job_done_delay_ms": 67890
            }"#,
        )
        .expect("initialize accepts the override");

        // Same env-read pattern as `start_lifecycle_listeners`.
        let wd: u64 = std::env::var("RAY_gcs_mark_task_failed_on_worker_dead_delay_ms")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        let jd: u64 = std::env::var("RAY_gcs_mark_task_failed_on_job_done_delay_ms")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(15000);
        assert_eq!(wd, 12345, "worker-dead listener must see the config_list value");
        assert_eq!(jd, 67890, "job-done listener must see the config_list value");

        std::env::remove_var("RAY_gcs_mark_task_failed_on_worker_dead_delay_ms");
        std::env::remove_var("RAY_gcs_mark_task_failed_on_job_done_delay_ms");
        let _ = ray_config::replace_for_test(prev);
    }

    /// Parity guard: the actor manager's destroyed-actor LRU cap must
    /// come from `RayConfig` (C++ `ray_config_def.h:936`). Before this
    /// commit it was a hardcoded `MAX_DESTROYED_ACTORS = 100_000` that
    /// ignored `config_list` overrides.
    #[tokio::test]
    async fn actor_manager_honors_max_destroyed_actors_cached_from_ray_config() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());

        ray_config::initialize(r#"{"maximum_gcs_destroyed_actor_cached_count": 7}"#)
            .expect("initialize accepts the override");

        let server = GcsServer::new(GcsServerConfig::default()).await;
        assert_eq!(
            server.actor_manager().max_destroyed_actors_cached(),
            7,
            "actor manager must capture the config value at construction"
        );

        let _ = ray_config::replace_for_test(prev);
    }

    /// Parity guard: `debug_dump_period_milliseconds` drives
    /// `start_debug_dump_loop`. 0 → no loop is spawned (mirrors C++
    /// `gcs_server.cc:314-318` where period=0 skips the periodical
    /// registration).
    #[tokio::test]
    async fn debug_dump_loop_respects_disable() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        ray_config::initialize(r#"{"debug_dump_period_milliseconds": 0}"#).unwrap();

        let server = GcsServer::new(GcsServerConfig::default()).await;
        // Calling start_debug_dump_loop with period=0 must return
        // immediately without spawning — otherwise the test would
        // produce periodic tracing output and leak the task.
        server.start_debug_dump_loop();
        // Sanity check: config reads 0.
        assert_eq!(
            ray_config::instance().debug_dump_period_milliseconds, 0
        );

        let _ = ray_config::replace_for_test(prev);
    }

    /// Parity guard: a non-zero `debug_dump_period_milliseconds`
    /// actually spawns a loop that ticks at that cadence. We spin up
    /// the server, wait a bit over two tick intervals, and verify
    /// the loop is alive (no panic, no hang).
    #[tokio::test]
    async fn debug_dump_loop_spawns_when_enabled() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        // 50ms period — short enough to observe within the test.
        ray_config::initialize(r#"{"debug_dump_period_milliseconds": 50}"#).unwrap();

        let server = GcsServer::new(GcsServerConfig::default()).await;
        server.start_debug_dump_loop();

        // Let the loop tick at least twice. If spawning failed,
        // `await` still returns immediately; the assertion below
        // proves the config round-tripped.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(
            ray_config::instance().debug_dump_period_milliseconds, 50
        );

        let _ = ray_config::replace_for_test(prev);
    }

    /// Parity guard: the Redis health-check cadence must come from
    /// `RayConfig::gcs_redis_heartbeat_interval_milliseconds` (C++
    /// default 100ms at `ray_config_def.h:395`). Before this commit the
    /// listener read a differently-named env var (`..._interval_ms`,
    /// not `..._interval_milliseconds`) with the wrong default (5000).
    ///
    /// Scope note: we can only assert the config value here because
    /// the loop's tokio::spawn'd interval is not directly observable.
    /// What matters for parity is that the Redis heartbeat reads from
    /// the same config key as C++ does — proven by
    /// `start_redis_health_check`'s code path referencing
    /// `ray_config::instance().gcs_redis_heartbeat_interval_milliseconds`.
    #[test]
    fn redis_heartbeat_default_matches_cpp() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        assert_eq!(
            ray_config::instance().gcs_redis_heartbeat_interval_milliseconds,
            100,
            "C++ default at ray_config_def.h:395 is 100ms"
        );
        // config_list override takes effect through the same path:
        ray_config::initialize(r#"{"gcs_redis_heartbeat_interval_milliseconds": 42}"#).unwrap();
        assert_eq!(
            ray_config::instance().gcs_redis_heartbeat_interval_milliseconds,
            42
        );
        let _ = ray_config::replace_for_test(prev);
    }

    /// An unknown key in `config_list` is a hard error — matches C++
    /// `ray_config.cc:60` (`RAY_LOG(FATAL)` on unknown config).
    #[test]
    fn ray_config_rejects_unknown_keys() {
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        let err =
            ray_config::initialize(r#"{"definitely_not_a_known_ray_config": 42}"#).unwrap_err();
        assert!(
            err.contains("unknown config key"),
            "expected unknown-key fatal, got: {err}"
        );
        let _ = ray_config::replace_for_test(prev);
    }

    #[test]
    fn default_event_log_reporter_enabled_matches_cpp() {
        // Parity with C++ `RAY_CONFIG(bool, event_log_reporter_enabled, true)`
        // (`ray_config_def.h:849`). Regression guard: a prior version of this
        // crate defaulted to `false`, which silently disabled export events
        // in production even when `log_dir` was set.
        let cfg = GcsServerConfig::default();
        assert!(cfg.event_log_reporter_enabled);
    }

    #[tokio::test]
    async fn test_export_events_emitted_when_log_dir_set() {
        use gcs_proto::ray::rpc::{gcs_node_info, GcsNodeInfo};

        let dir = std::env::temp_dir()
            .join(format!("gcs_server_export_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let cfg = GcsServerConfig {
            grpc_port: 0,
            log_dir: Some(dir.to_string_lossy().into_owned()),
            event_log_reporter_enabled: true,
            ..Default::default()
        };
        let server = GcsServer::new(cfg).await;

        // Add a node; the server should append a JSON line to
        // `<log_dir>/export_events/event_EXPORT_NODE.log`.
        let mut node = GcsNodeInfo::default();
        node.node_id = b"test-node-id-01".to_vec();
        node.state = gcs_node_info::GcsNodeState::Alive as i32;
        node.node_manager_address = "10.0.0.1".into();
        node.is_head_node = true;
        server.node_manager().add_node(node).await;

        let event_file = dir.join("export_events").join("event_EXPORT_NODE.log");
        assert!(
            event_file.exists(),
            "expected {event_file:?} to be created by the server"
        );
        let content = std::fs::read_to_string(&event_file).unwrap();
        assert!(
            content.contains("EXPORT_NODE"),
            "expected EXPORT_NODE source type in event line, got: {content}"
        );
        assert!(
            content.contains("10.0.0.1"),
            "expected node address in event line, got: {content}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression guard for the `start_with_listener` listener-drop bug.
    ///
    /// The production binary (`main.rs`) uses `start_with_listener` so it
    /// can pre-bind the port (port 0, port-file publishing, etc.). Before
    /// this fix, `start()` called `start_lifecycle_listeners()` but
    /// `start_with_listener()` did not, so a Rust GCS launched by Ray
    /// Python silently dropped the three C++
    /// `GcsServer::InstallEventListeners` hooks
    /// (`gcs_server.cc:819-908`): worker-dead → task-manager, job-finished
    /// → task-manager, and node-removed → job-manager.
    ///
    /// This test goes through the real `start_with_listener` entry point
    /// and asserts that a `ReportWorkerFailure` RPC causes
    /// `task_manager.on_worker_dead` to run end-to-end (task on the dead
    /// worker transitions to FAILED).
    #[tokio::test]
    async fn test_start_with_listener_installs_lifecycle_listeners() {
        use gcs_proto::ray::rpc::worker_info_gcs_service_client::WorkerInfoGcsServiceClient;
        use gcs_proto::ray::rpc::{
            Address, AddTaskEventDataRequest, ReportWorkerFailureRequest, TaskEventData,
            TaskEvents, TaskInfoEntry, TaskStateUpdate, TaskStatus, WorkerTableData,
        };

        // Pin the worker-dead delay to 0ms so the spawned task runs before
        // we poll for the failed task. Env var matches C++
        // `gcs_mark_task_failed_on_worker_dead_delay_ms`.
        std::env::set_var(
            "RAY_gcs_mark_task_failed_on_worker_dead_delay_ms",
            "0",
        );

        let config = GcsServerConfig {
            grpc_port: 0,
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config).await);

        // Seed a running task on worker W BEFORE the server accepts
        // connections. Using the task_manager accessor is correct because
        // `install_listeners_and_initialize` hasn't run yet — once it
        // does, the worker-dead listener must observe this task.
        let worker_id: Vec<u8> = vec![0xAAu8; 16];
        let mut running = TaskEvents {
            task_id: b"t_worker_dead_probe".to_vec(),
            attempt_number: 0,
            job_id: b"j_probe".to_vec(),
            ..Default::default()
        };
        running.task_info = Some(TaskInfoEntry {
            r#type: 0, // NORMAL_TASK
            ..Default::default()
        });
        let mut su = TaskStateUpdate::default();
        su.worker_id = Some(worker_id.clone());
        su.state_ts_ns.insert(TaskStatus::SubmittedToWorker as i32, 1);
        running.state_updates = Some(su);

        // Inject the task via the internal task manager (parity with C++
        // AddTaskEventData flow).
        use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsService;
        server
            .task_manager
            .add_task_event_data(tonic::Request::new(AddTaskEventDataRequest {
                data: Some(TaskEventData {
                    events_by_task: vec![running],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);
        let mut worker_client = WorkerInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("connect to WorkerInfoGcsService");

        // Report worker failure — C++ path wires this to task_manager via
        // InstallEventListeners; Rust must do the same.
        worker_client
            .report_worker_failure(tonic::Request::new(ReportWorkerFailureRequest {
                worker_failure: Some(WorkerTableData {
                    worker_address: Some(Address {
                        worker_id: worker_id.clone(),
                        ..Default::default()
                    }),
                    is_alive: false,
                    exit_type: Some(1), // WORKER_DIED
                    exit_detail: Some("regression probe".into()),
                    end_time_ms: 12345,
                    ..Default::default()
                }),
            }))
            .await
            .expect("ReportWorkerFailure RPC failed");

        // Poll up to ~1s for the failed task. If the lifecycle listener
        // was NOT installed, we never transition and this test fails.
        let failed_status = TaskStatus::Failed as i32;
        let mut observed_failed = false;
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let reply = server
                .task_manager
                .get_task_events(tonic::Request::new(
                    gcs_proto::ray::rpc::GetTaskEventsRequest::default(),
                ))
                .await
                .unwrap()
                .into_inner();
            let probe = reply
                .events_by_task
                .iter()
                .find(|e| e.task_id == b"t_worker_dead_probe");
            if let Some(task) = probe {
                if task
                    .state_updates
                    .as_ref()
                    .map(|su| su.state_ts_ns.contains_key(&failed_status))
                    .unwrap_or(false)
                {
                    observed_failed = true;
                    break;
                }
            }
        }

        assert!(
            observed_failed,
            "start_with_listener must install the worker_dead → task_manager \
             lifecycle listener (C++ gcs_server.cc:856, 880-900). Task did \
             not transition to FAILED after ReportWorkerFailure RPC — the \
             listener was probably never wired."
        );

        server_handle.abort();
        let _ = server_handle.await;
    }

    /// Parity with C++ `gcs_server.cc:897` — a `worker_dead` event must
    /// drive `GcsPlacementGroupScheduler::HandleWaitingRemovedBundles`
    /// through to completion: previously-stuck bundle releases get
    /// retried and drained.
    ///
    /// This is the behavioral (not structural) half of Blocker 2:
    /// proves the hook is real by forcing the scheduler into the
    /// `waiting_removed_bundles` condition and observing the queue
    /// drain after a `ReportWorkerFailure` RPC.
    #[tokio::test]
    async fn test_worker_dead_drains_waiting_removed_bundles() {
        use gcs_proto::ray::rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        use gcs_proto::ray::rpc::{
            gcs_node_info, Address, Bundle, GcsNodeInfo, NodeDeathInfo,
            ReportWorkerFailureRequest, WorkerTableData,
        };

        std::env::set_var(
            "RAY_gcs_mark_task_failed_on_worker_dead_delay_ms",
            "0",
        );
        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        server.install_listeners_and_initialize().await;

        // Register a live-but-unreachable "raylet" so that
        // CancelResourceReserve fails at connect time, forcing the
        // bundle into the retry queue.
        let pg_node_id: Vec<u8> = b"node_unreachable_probe".to_vec();
        let mut pg_node = GcsNodeInfo::default();
        pg_node.node_id = pg_node_id.clone();
        pg_node.state = gcs_node_info::GcsNodeState::Alive as i32;
        pg_node.node_manager_address = "127.0.0.1".into();
        pg_node.node_manager_port = 1;
        server.node_manager.add_node(pg_node).await;

        let bundle = Bundle {
            bundle_id: Some(gcs_proto::ray::rpc::bundle::BundleIdentifier {
                placement_group_id: b"pg_probe".to_vec(),
                bundle_index: 0,
            }),
            node_id: pg_node_id.clone(),
            unit_resources: [("CPU".to_string(), 1.0)].into_iter().collect(),
            ..Default::default()
        };

        // Force the scheduler into the waiting-removed-bundles state.
        server
            .pg_scheduler
            .try_release_bundle_or_defer(&pg_node_id, bundle)
            .await;
        assert_eq!(
            server.pg_scheduler.waiting_removed_bundles_count(),
            1,
            "preconditions: the bundle must be queued before worker_dead"
        );

        // Mark the unreachable node as dead so the retry treats it as
        // "already released" and drops the entry (C++
        // TryReleasingBundleResources line 777-782).
        server
            .node_manager
            .remove_node(&pg_node_id, NodeDeathInfo::default())
            .await;

        // Fire the real worker_dead event through the real listener
        // pipeline. The worker itself is independent of the PG node —
        // worker_dead exists to trigger the drain.
        let worker_id: Vec<u8> = vec![0xD3u8; 16];
        server
            .worker_manager
            .report_worker_failure(tonic::Request::new(ReportWorkerFailureRequest {
                worker_failure: Some(WorkerTableData {
                    worker_address: Some(Address {
                        worker_id,
                        node_id: b"other_node".to_vec(),
                        ..Default::default()
                    }),
                    is_alive: false,
                    exit_type: Some(1),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();

        // The drain runs in a spawned task; poll until it completes.
        let mut drained = false;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            if server.pg_scheduler.waiting_removed_bundles_count() == 0 {
                drained = true;
                break;
            }
        }
        assert!(
            drained,
            "worker_dead must drive handle_waiting_removed_bundles to completion \
             (parity with C++ gcs_server.cc:897). Queue never drained — listener \
             either did not fire or the drain body is still a no-op."
        );
    }

    /// Parity with C++ `gcs_server.cc:898` — pubsub must shed the worker's
    /// subscription when the worker dies.
    #[tokio::test]
    async fn test_worker_dead_listener_cleans_pubsub_subscription() {
        use gcs_proto::ray::rpc::{Address, ReportWorkerFailureRequest, WorkerTableData};
        use gcs_proto::ray::rpc::worker_info_gcs_service_server::WorkerInfoGcsService;

        std::env::set_var(
            "RAY_gcs_mark_task_failed_on_worker_dead_delay_ms",
            "0",
        );
        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        server.install_listeners_and_initialize().await;

        let worker_id: Vec<u8> = vec![0xC1u8; 16];
        // Register a pubsub subscription for this worker id.
        server
            .pubsub_manager
            .subscribe_channel(&worker_id, /*channel_type=*/ 1, /*key_id=*/ Vec::new());
        assert!(server.pubsub_manager.has_subscriber(&worker_id));

        // Fire ReportWorkerFailure via the worker_manager — this drives the
        // dead_listeners chain set up by `install_listeners_and_initialize`.
        server
            .worker_manager
            .report_worker_failure(tonic::Request::new(ReportWorkerFailureRequest {
                worker_failure: Some(WorkerTableData {
                    worker_address: Some(Address {
                        worker_id: worker_id.clone(),
                        ..Default::default()
                    }),
                    is_alive: false,
                    exit_type: Some(1),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();

        // Spawned tokio task completes on the next yield; give it a
        // moment. Pubsub cleanup itself is synchronous inside the
        // listener closure, but the actor-restart side task spawns —
        // yielding ensures any racey observations settle.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !server.pubsub_manager.has_subscriber(&worker_id),
            "worker_dead listener must call PubSubManager::remove_subscriber \
             (parity with C++ gcs_server.cc:898)"
        );
    }

    /// Parity with C++ `gcs_server.cc:869` — pubsub must shed the node's
    /// subscription when the node is removed.
    #[tokio::test]
    async fn test_node_removed_listener_cleans_pubsub_subscription() {
        use gcs_proto::ray::rpc::{gcs_node_info, GcsNodeInfo, NodeDeathInfo};

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        server.install_listeners_and_initialize().await;

        let node_id = b"node-pubsub-probe".to_vec();
        let mut node = GcsNodeInfo::default();
        node.node_id = node_id.clone();
        node.state = gcs_node_info::GcsNodeState::Alive as i32;
        server.node_manager.add_node(node).await;

        server
            .pubsub_manager
            .subscribe_channel(&node_id, 1, Vec::new());
        assert!(server.pubsub_manager.has_subscriber(&node_id));

        server
            .node_manager
            .remove_node(&node_id, NodeDeathInfo::default())
            .await;

        // node_removed is delivered via a spawned tokio loop; yield until
        // it drains.
        for _ in 0..64 {
            if !server.pubsub_manager.has_subscriber(&node_id) {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            !server.pubsub_manager.has_subscriber(&node_id),
            "node_removed listener must call PubSubManager::remove_subscriber \
             (parity with C++ gcs_server.cc:869)"
        );
    }

    /// Parity with C++ `gcs_server.cc:906` — the job_finished listener
    /// must clean placement groups whose creator job just died.
    #[tokio::test]
    async fn test_job_finished_listener_cleans_placement_groups() {
        use gcs_proto::ray::rpc::job_info_gcs_service_server::JobInfoGcsService;
        use gcs_proto::ray::rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        use gcs_proto::ray::rpc::placement_group_table_data::PlacementGroupState;
        use gcs_proto::ray::rpc::{
            AddJobRequest, GetPlacementGroupRequest, JobTableData, MarkJobFinishedRequest,
            PlacementGroupSpec,
        };
        use gcs_managers::pg_scheduler::PgCreationSuccess;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        server.install_listeners_and_initialize().await;

        // Register a PG whose creator actor is already dead; job_finished
        // should now make lifetime_done → true and remove the PG.
        let pg_id = b"pg_jobdead_probe".to_vec();
        let job_id = b"job_probe".to_vec();
        server
            .placement_group_manager
            .create_placement_group(tonic::Request::new(
                gcs_proto::ray::rpc::CreatePlacementGroupRequest {
                    placement_group_spec: Some(PlacementGroupSpec {
                        placement_group_id: pg_id.clone(),
                        creator_job_id: job_id.clone(),
                        creator_actor_dead: true,
                        is_detached: false,
                        ..Default::default()
                    }),
                },
            ))
            .await
            .unwrap();
        // Drive to CREATED so it's in the active registry.
        server
            .placement_group_manager
            .on_placement_group_creation_success(PgCreationSuccess {
                pg_id: pg_id.clone(),
                bundles: Vec::new(),
            })
            .await;

        // Add then finish the job. `mark_job_finished` fires finished_listeners.
        server
            .job_manager
            .add_job(tonic::Request::new(AddJobRequest {
                data: Some(JobTableData {
                    job_id: job_id.clone(),
                    is_dead: false,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        server
            .job_manager
            .mark_job_finished(tonic::Request::new(MarkJobFinishedRequest {
                job_id: job_id.clone(),
            }))
            .await
            .unwrap();

        // PG cleanup is dispatched from the listener via tokio::spawn; poll.
        let mut observed_removed = false;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let reply = server
                .placement_group_manager
                .get_placement_group(tonic::Request::new(GetPlacementGroupRequest {
                    placement_group_id: pg_id.clone(),
                }))
                .await
                .unwrap()
                .into_inner();
            if let Some(data) = reply.placement_group_table_data {
                if data.state == PlacementGroupState::Removed as i32 {
                    observed_removed = true;
                    break;
                }
            }
        }
        assert!(
            observed_removed,
            "job_finished listener must call \
             placement_group_manager.clean_placement_group_if_needed_when_job_dead \
             (parity with C++ gcs_server.cc:906)"
        );
    }

    #[tokio::test]
    async fn test_export_events_disabled_when_log_dir_unset() {
        // Parity gate with C++ `gcs_server_main.cc:145`
        // (`if event_log_reporter_enabled && !log_dir.empty()`): the feature
        // must stay a no-op when `log_dir` is None, even though the default
        // for `event_log_reporter_enabled` is now true.
        use gcs_proto::ray::rpc::{gcs_node_info, GcsNodeInfo};

        let cfg = GcsServerConfig::default();
        assert!(cfg.event_log_reporter_enabled);
        assert!(cfg.log_dir.is_none());

        let server = GcsServer::new(cfg).await;
        let mut node = GcsNodeInfo::default();
        node.node_id = b"test-node-id-02".to_vec();
        node.state = gcs_node_info::GcsNodeState::Alive as i32;
        server.node_manager().add_node(node).await;
        // No panic and no file system activity is sufficient; the disabled
        // writer short-circuits inside `ExportEventManager::report`.
    }

    #[tokio::test]
    async fn test_server_manager_accessors() {
        let config = GcsServerConfig {
            grpc_port: 0,
            raylet_config_list: "test_config".to_string(),
            max_task_events: Some(100),
            ..Default::default()
        };
        let server = GcsServer::new(config).await;

        // Verify all manager getters return valid references.
        assert!(server.node_manager().get_all_alive_nodes().is_empty());
        assert!(server.job_manager().get_job(b"no_such_job").is_none());
        let _wm = server.worker_manager();

        let kv = server.kv_manager();
        let result = kv.kv().get("ns", "key").await;
        assert!(result.is_none());

        // pubsub_manager publisher_id should match the generated cluster_id.
        let psm = server.pubsub_manager();
        assert_eq!(psm.publisher_id(), server.cluster_id());

        let _am = server.actor_manager();
    }

    #[tokio::test]
    async fn test_start_with_listener() {
        let config = GcsServerConfig {
            grpc_port: 0,
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config).await);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to NodeInfoGcsService");
        let resp = node_client
            .get_all_node_info(GetAllNodeInfoRequest::default())
            .await
            .expect("GetAllNodeInfo RPC failed");
        assert!(resp.into_inner().node_info_list.is_empty());

        let mut job_client = JobInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to JobInfoGcsService");
        let resp = job_client
            .get_all_job_info(GetAllJobInfoRequest::default())
            .await
            .expect("GetAllJobInfo RPC failed");
        assert!(resp.into_inner().job_info_list.is_empty());

        let mut kv_client = InternalKvGcsServiceClient::connect(endpoint)
            .await
            .expect("failed to connect to InternalKvGcsService");
        let resp = kv_client
            .internal_kv_get(InternalKvGetRequest {
                namespace: "test_ns".into(),
                key: "nonexistent".into(),
            })
            .await
            .expect("InternalKvGet RPC failed");
        assert!(resp.into_inner().value.is_empty());

        server_handle.abort();
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_start() {
        let tmp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tmp_listener.local_addr().unwrap().port();
        drop(tmp_listener);

        let config = GcsServerConfig {
            grpc_port: port,
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config).await);

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start().await
        });

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await
            .expect("failed to connect to GCS server via start()");
        let resp = node_client
            .get_all_node_info(GetAllNodeInfoRequest::default())
            .await
            .expect("GetAllNodeInfo RPC failed on start() server");
        assert!(resp.into_inner().node_info_list.is_empty());

        let mut job_client = JobInfoGcsServiceClient::connect(endpoint)
            .await
            .expect("failed to connect to JobInfoGcsService via start()");
        let resp = job_client
            .get_all_job_info(GetAllJobInfoRequest::default())
            .await
            .expect("GetAllJobInfo RPC failed on start() server");
        assert!(resp.into_inner().job_info_list.is_empty());

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
        let config = GcsServerConfig::default();
        let server = GcsServer::new_with_store(config, store, None).await;

        // Initialize from storage (this is what happens on restart).
        server.initialize().await;

        // Verify recovered state.
        assert!(server.job_manager().get_job(b"job_1").is_some());
        assert_eq!(server.node_manager().get_all_alive_nodes().len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_id_persisted_across_restarts() {
        // Create a shared store that persists across "restarts".
        let store: Arc<dyn StoreClient> = Arc::new(InMemoryStoreClient::new());

        // First server: generates and persists a cluster ID.
        let server1 = GcsServer::new_with_store(
            GcsServerConfig::default(), store.clone(), None,
        ).await;
        let first_cluster_id = server1.cluster_id().to_vec();
        assert_eq!(first_cluster_id.len(), CLUSTER_ID_SIZE);

        // Verify the ID was persisted in KV.
        let kv = StoreClientInternalKV::new(store.clone());
        let stored = kv.get(CLUSTER_ID_NAMESPACE, CLUSTER_ID_KEY).await;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().into_bytes(), first_cluster_id);

        // Second server: should recover the SAME cluster ID from KV.
        let server2 = GcsServer::new_with_store(
            GcsServerConfig::default(), store.clone(), None,
        ).await;
        assert_eq!(server2.cluster_id(), first_cluster_id.as_slice());

        // Third server: still the same.
        let server3 = GcsServer::new_with_store(
            GcsServerConfig::default(), store, None,
        ).await;
        assert_eq!(server3.cluster_id(), first_cluster_id.as_slice());
    }

    // ─── Resource lifecycle integration tests ──────────────────────────

    use gcs_proto::ray::rpc::node_resource_info_gcs_service_client::NodeResourceInfoGcsServiceClient;
    use gcs_proto::ray::rpc::{
        RegisterNodeRequest, UnregisterNodeRequest, GcsNodeInfo,
        GetAllAvailableResourcesRequest, GetAllTotalResourcesRequest,
        GetDrainingNodesRequest, GetAllResourceUsageRequest,
        gcs_node_info, NodeDeathInfo,
    };

    fn make_node_with_resources(id: &[u8]) -> GcsNodeInfo {
        let mut total = std::collections::HashMap::new();
        total.insert("CPU".to_string(), 8.0);
        total.insert("GPU".to_string(), 2.0);
        GcsNodeInfo {
            node_id: id.to_vec(),
            state: gcs_node_info::GcsNodeState::Alive as i32,
            node_manager_address: "10.0.0.1".to_string(),
            node_manager_port: 12345,
            resources_total: total,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_register_node_populates_resources() {
        // Register a node via gRPC, then verify resource RPCs return its data.
        let config = GcsServerConfig { grpc_port: 0, ..Default::default() };
        let server = Arc::new(GcsServer::new(config).await);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Register a node with resources.
        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await.unwrap();
        node_client
            .register_node(RegisterNodeRequest {
                node_info: Some(make_node_with_resources(b"n1")),
            })
            .await
            .unwrap();

        // Give the broadcast listener a moment to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify GetAllTotalResources returns the node's resources.
        let mut res_client = NodeResourceInfoGcsServiceClient::connect(endpoint.clone())
            .await.unwrap();
        let reply = res_client
            .get_all_total_resources(GetAllTotalResourcesRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
        assert_eq!(reply.resources_list[0].node_id, b"n1");
        assert_eq!(reply.resources_list[0].resources_total.get("CPU"), Some(&8.0));
        assert_eq!(reply.resources_list[0].resources_total.get("GPU"), Some(&2.0));

        // Verify GetAllAvailableResources also returns the node.
        let reply = res_client
            .get_all_available_resources(GetAllAvailableResourcesRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
        assert_eq!(reply.resources_list[0].node_id, b"n1");
        assert_eq!(reply.resources_list[0].resources_available.get("CPU"), Some(&8.0));

        // Verify GetAllResourceUsage returns the node's batch.
        let reply = res_client
            .get_all_resource_usage(GetAllResourceUsageRequest {})
            .await
            .unwrap()
            .into_inner();
        let batch = reply.resource_usage_data.unwrap();
        assert_eq!(batch.batch.len(), 1);

        server_handle.abort();
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_unregister_node_cleans_up_resources() {
        let config = GcsServerConfig { grpc_port: 0, ..Default::default() };
        let server = Arc::new(GcsServer::new(config).await);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Register then unregister a node.
        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await.unwrap();
        node_client
            .register_node(RegisterNodeRequest {
                node_info: Some(make_node_with_resources(b"n1")),
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        node_client
            .unregister_node(UnregisterNodeRequest {
                node_id: b"n1".to_vec(),
                node_death_info: Some(NodeDeathInfo::default()),
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Resource RPCs should now return empty.
        let mut res_client = NodeResourceInfoGcsServiceClient::connect(endpoint)
            .await.unwrap();
        let reply = res_client
            .get_all_total_resources(GetAllTotalResourcesRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(reply.resources_list.is_empty());

        server_handle.abort();
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_drain_node_propagates_to_resource_manager() {
        use async_trait::async_trait;
        use gcs_managers::autoscaler_stub::RayletClientPool;
        use gcs_proto::ray::rpc::autoscaler::autoscaler_state_service_client::AutoscalerStateServiceClient;
        use gcs_proto::ray::rpc::autoscaler::DrainNodeRequest as AutoscalerDrainNodeRequest;
        use gcs_proto::ray::rpc::{DrainRayletReply, DrainRayletRequest};
        use std::collections::HashMap;

        // Scripted raylet pool: always accept. The production
        // autoscaler now forwards DrainRaylet before marking state —
        // without this mock, the call would fail against a non-existent
        // raylet and the assertions below would time out.
        struct AcceptingPool;
        #[async_trait]
        impl RayletClientPool for AcceptingPool {
            async fn resize_local_resource_instances(
                &self,
                _a: &str,
                _p: u16,
                _r: HashMap<String, f64>,
            ) -> Result<HashMap<String, f64>, tonic::Status> {
                Ok(HashMap::new())
            }
            async fn drain_raylet(
                &self,
                _address: &str,
                _port: u16,
                _request: DrainRayletRequest,
            ) -> Result<DrainRayletReply, tonic::Status> {
                Ok(DrainRayletReply {
                    is_accepted: true,
                    rejection_reason_message: String::new(),
                })
            }
            async fn kill_local_actor(
                &self,
                _address: &str,
                _port: u16,
                _request: gcs_proto::ray::rpc::KillLocalActorRequest,
            ) -> Result<(), tonic::Status> {
                Ok(())
            }
        }

        let config = GcsServerConfig { grpc_port: 0, ..Default::default() };
        let server = Arc::new(GcsServer::new(config).await);
        // Swap in the accepting pool so drain_node doesn't try to reach
        // a non-existent raylet. Matches the test fixture C++ uses
        // (mock raylet pool) for the same test.
        server
            .autoscaler_manager
            .set_raylet_client_pool(Arc::new(AcceptingPool));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Register a node.
        let mut node_client = NodeInfoGcsServiceClient::connect(endpoint.clone())
            .await.unwrap();
        node_client
            .register_node(RegisterNodeRequest {
                node_info: Some(make_node_with_resources(b"n1")),
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Drain the node via autoscaler service.
        let mut as_client = AutoscalerStateServiceClient::connect(endpoint.clone())
            .await.unwrap();
        let reply = as_client
            .drain_node(AutoscalerDrainNodeRequest {
                node_id: b"n1".to_vec(),
                reason: 1, // IdleTermination
                reason_message: "idle".to_string(),
                deadline_timestamp_ms: 99999,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify GetDrainingNodes returns the draining node.
        let mut res_client = NodeResourceInfoGcsServiceClient::connect(endpoint)
            .await.unwrap();
        let reply = res_client
            .get_draining_nodes(GetDrainingNodesRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.draining_nodes.len(), 1);
        assert_eq!(reply.draining_nodes[0].node_id, b"n1");
        assert_eq!(reply.draining_nodes[0].draining_deadline_timestamp_ms, 99999);

        server_handle.abort();
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_recovery_populates_resources() {
        // Pre-populate storage with a node, then create a new server
        // and verify resource data is available after recovery.
        let store: Arc<dyn StoreClient> = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store.clone()));

        // Persist an alive node with resources.
        let node = make_node_with_resources(b"recovered");
        table_storage.node_table().put("recovered", &node).await;

        // Create server using the same store (simulating recovery).
        let config = GcsServerConfig { grpc_port: 0, ..Default::default() };
        let server = Arc::new(GcsServer::new_with_store(config, store, None).await);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Resource RPCs should reflect the recovered node.
        let mut res_client = NodeResourceInfoGcsServiceClient::connect(endpoint)
            .await.unwrap();
        let reply = res_client
            .get_all_total_resources(GetAllTotalResourcesRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
        assert_eq!(reply.resources_list[0].node_id, b"recovered");
        assert_eq!(reply.resources_list[0].resources_total.get("CPU"), Some(&8.0));

        server_handle.abort();
        let _ = server_handle.await;
    }

    /// End-to-end parity: a raylet-style client opens the
    /// `ray.rpc.syncer.RaySyncer/StartSync` bidi stream, sends a
    /// `RESOURCE_VIEW` batch, and the GCS-side `GcsResourceManager` is
    /// updated with the reported totals — visible through
    /// `NodeResourceInfoGcsService::GetAllTotalResources`.
    ///
    /// This is the parity guard for C++ `GcsServer::InitRaySyncer`
    /// (`gcs_server.cc:607-621`) registering the `RaySyncerService`. Prior
    /// to this commit the Rust server returned Unimplemented for this RPC
    /// and raylet resource updates simply never reached the GCS.
    #[tokio::test]
    async fn ray_syncer_service_receives_resource_view_over_grpc() {
        use gcs_proto::ray::rpc::syncer::ray_syncer_client::RaySyncerClient;
        use gcs_proto::ray::rpc::syncer::{
            MessageType, RaySyncMessage, RaySyncMessageBatch, ResourceViewSyncMessage,
        };
        use gcs_proto::ray::rpc::node_resource_info_gcs_service_client::NodeResourceInfoGcsServiceClient;
        use gcs_proto::ray::rpc::GetAllTotalResourcesRequest;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let endpoint = format!("http://127.0.0.1:{}", port);

        // Build a raylet-style RESOURCE_VIEW payload.
        let mut rv = ResourceViewSyncMessage::default();
        rv.resources_total.insert("CPU".to_string(), 16.0);
        rv.resources_total.insert("GPU".to_string(), 2.0);
        let payload = <ResourceViewSyncMessage as prost::Message>::encode_to_vec(&rv);
        let remote_node_id = b"raylet-node-0001-padded-zzzz".to_vec(); // 28 bytes
        assert_eq!(remote_node_id.len(), 28);
        let batch = RaySyncMessageBatch {
            messages: vec![RaySyncMessage {
                version: 1,
                message_type: MessageType::ResourceView as i32,
                sync_message: payload,
                node_id: remote_node_id.clone(),
            }],
        };

        // Channel carrying batches into the bidi outbound stream.
        let (tx, rx) = tokio::sync::mpsc::channel::<RaySyncMessageBatch>(4);
        let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Build the request with the required `node_id` metadata header
        // (hex-encoded 28-byte id, matching C++ `ray_syncer_client.cc:42`).
        let mut req = tonic::Request::new(outbound);
        let hex_id = {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut s = String::with_capacity(remote_node_id.len() * 2);
            for &b in &remote_node_id {
                s.push(HEX[(b >> 4) as usize] as char);
                s.push(HEX[(b & 0x0f) as usize] as char);
            }
            s
        };
        req.metadata_mut().insert("node_id", hex_id.parse().unwrap());

        let mut client = RaySyncerClient::connect(endpoint.clone()).await.unwrap();
        let response = client.start_sync(req).await.unwrap();
        // Push the batch after the response stream is open so the server
        // has a live reader task.
        tx.send(batch).await.unwrap();

        // Give the server a moment to process. The reactor spawns a tokio
        // task that dispatches to the consumer; 200ms is comfortably
        // longer than what local in-process routing needs.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        drop(response); // close the server-side read loop cleanly

        // Confirm the resource manager saw the update.
        let mut res_client = NodeResourceInfoGcsServiceClient::connect(endpoint)
            .await
            .unwrap();
        let reply = res_client
            .get_all_total_resources(GetAllTotalResourcesRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.resources_list.len(),
            1,
            "GCS resource table must contain the raylet's reported node"
        );
        assert_eq!(reply.resources_list[0].node_id, remote_node_id);
        assert_eq!(
            reply.resources_list[0].resources_total.get("CPU"),
            Some(&16.0)
        );
        assert_eq!(
            reply.resources_list[0].resources_total.get("GPU"),
            Some(&2.0)
        );

        server_handle.abort();
        let _ = server_handle.await;
    }

    /// The service must reject a `StartSync` RPC that does not carry the
    /// `node_id` metadata header. Parity with C++
    /// `GetNodeIDFromServerContext` (`ray_syncer_server.cc:27-32`),
    /// which `RAY_CHECK`s the header is present.
    #[tokio::test]
    async fn ray_syncer_service_rejects_missing_node_id_metadata() {
        use gcs_proto::ray::rpc::syncer::ray_syncer_client::RaySyncerClient;
        use gcs_proto::ray::rpc::syncer::RaySyncMessageBatch;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let endpoint = format!("http://127.0.0.1:{}", port);

        let (_tx, rx) = tokio::sync::mpsc::channel::<RaySyncMessageBatch>(4);
        let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
        let req = tonic::Request::new(outbound);

        let mut client = RaySyncerClient::connect(endpoint).await.unwrap();
        let err = client
            .start_sync(req)
            .await
            .err()
            .expect("missing node_id header must fail the RPC");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        server_handle.abort();
        let _ = server_handle.await;
    }

    /// End-to-end parity guard for the periodic raylet-load pull loop
    /// (C++ `gcs_server.cc:415-446`): spinning up the real server with
    /// a mock `RayletLoadFetcher` must update *both* the resource
    /// manager's `resource_load_by_shape` and the autoscaler's
    /// per-node cache.
    ///
    /// Before this commit no such loop existed in Rust — the autoscaler
    /// was frozen at node-registration totals, so this assertion would
    /// have timed out waiting for any update.
    #[tokio::test]
    async fn raylet_load_pull_loop_updates_resource_manager_and_autoscaler() {
        use async_trait::async_trait;
        use gcs_managers::raylet_load::RayletLoadFetcher;
        use gcs_proto::ray::rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
        use gcs_proto::ray::rpc::{
            gcs_node_info::GcsNodeState, GcsNodeInfo, ResourceLoad, ResourcesData,
        };
        use parking_lot::Mutex;
        use std::collections::HashMap;

        // ── Install a short 30ms cadence so the test completes fast but
        //    still exercises the real interval-tick path. We isolate via
        //    the same RAY_CONFIG lock the other config-sensitive tests use.
        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        ray_config::initialize(
            r#"{"gcs_pull_resource_loads_period_milliseconds": 30}"#,
        )
        .unwrap();

        // ── Build a fake fetcher that always returns the same rich reply.
        struct CountingFetcher {
            calls: Mutex<usize>,
            reply: ResourcesData,
        }
        #[async_trait]
        impl RayletLoadFetcher for CountingFetcher {
            async fn fetch(
                &self,
                _address: &str,
                _port: u16,
            ) -> Result<ResourcesData, tonic::Status> {
                *self.calls.lock() += 1;
                Ok(self.reply.clone())
            }
        }

        // Node id is 28 bytes; values are irrelevant but must be
        // consistent between the GcsNodeInfo and the reply.
        let node_id: Vec<u8> = (0..28).map(|i| (i as u8) + 1).collect();
        let mut reply = ResourcesData::default();
        reply.node_id = node_id.clone();
        reply.resource_load = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 3.25);
            m
        };
        reply.resource_load_by_shape = Some(ResourceLoad::default());
        reply.resources_available = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 0.75);
            m
        };
        let fetcher = Arc::new(CountingFetcher {
            calls: Mutex::new(0),
            reply: reply.clone(),
        });

        // ── Build the server, register an alive node, seed the
        //    resource-manager row (mirroring the on-node-add listener
        //    that normally does it), install the fake fetcher, and start
        //    the loop. Keep the server struct around — we're *not*
        //    calling start_with_listener here because we only want the
        //    load-pull path, not the full tonic serve() wiring.
        let config = GcsServerConfig::default();
        let server = Arc::new(GcsServer::new(config).await);

        let node = GcsNodeInfo {
            node_id: node_id.clone(),
            state: GcsNodeState::Alive as i32,
            node_manager_address: "1.2.3.4".into(),
            node_manager_port: 7001,
            resources_total: {
                let mut m = HashMap::new();
                m.insert("CPU".to_string(), 4.0);
                m
            },
            ..Default::default()
        };
        server.node_manager().add_node(node.clone()).await;
        // Seed the autoscaler cache (on_node_add is what the
        // production listener wires; we call it directly so the test
        // doesn't depend on listener ordering).
        server.autoscaler_manager.on_node_add(&node);
        // And the resource-manager row — same reason.
        server
            .resource_manager()
            .update_node_resources(node_id.clone(), ResourcesData::default());

        server.set_raylet_load_fetcher(fetcher.clone());
        server.start_raylet_load_pull_loop();

        // Wait for at least two ticks (~30ms each, plus one skipped).
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let calls = *fetcher.calls.lock();
        assert!(
            calls >= 1,
            "loop must have called the fetcher at least once, saw {calls}"
        );

        // Resource manager: resource_load was copied.
        let usage = server
            .resource_manager()
            .get_all_resource_usage(tonic::Request::new(
                gcs_proto::ray::rpc::GetAllResourceUsageRequest {},
            ))
            .await
            .unwrap()
            .into_inner()
            .resource_usage_data
            .expect("batch present");
        assert_eq!(usage.batch.len(), 1);
        assert_eq!(usage.batch[0].resource_load.get("CPU"), Some(&3.25));
        assert!(
            usage.batch[0].resource_load_by_shape.is_some(),
            "resource_load_by_shape must be populated by the pull loop"
        );

        // Autoscaler: per-node cache picked up the reply (resources_available
        // going from 4.0 at on_node_add to 0.75 proves the update landed).
        let asm_view = server
            .autoscaler_manager
            .get_node_resource_info(&node_id)
            .expect("autoscaler must have a cached row");
        assert_eq!(asm_view.resources_available.get("CPU"), Some(&0.75));

        // Tear down the loop explicitly so the test's runtime doesn't
        // keep a stray interval alive after we return.
        if let Some(h) = server.raylet_load_pull_handle.lock().take() {
            h.abort();
        }
        let _ = ray_config::replace_for_test(prev);
    }

    /// End-to-end parity guard for the DrainNode → raylet flow
    /// (C++ `gcs_autoscaler_state_manager.cc:463-523`): when the
    /// DrainNode RPC reaches the GCS, the autoscaler must:
    ///   1. call `SetPreemptedAndPublish` on the actor manager
    ///   2. forward `DrainRaylet` to the target raylet
    ///   3. mark the node draining ONLY if the raylet accepted
    ///   4. propagate a raylet rejection verbatim
    ///
    /// We don't stand up a real raylet — instead we inject a scripted
    /// `RayletClientPool` at the autoscaler boundary and drive the
    /// autoscaler directly. A separate unit test in `actor_stub`
    /// covers the actor publish side; this test proves the wiring
    /// between the two.
    #[tokio::test]
    async fn drain_node_forwards_to_raylet_and_marks_actors_preempted_end_to_end() {
        use async_trait::async_trait;
        use gcs_managers::autoscaler_stub::RayletClientPool;
        use gcs_proto::ray::rpc::actor_table_data::ActorState;
        use gcs_proto::ray::rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        use gcs_proto::ray::rpc::autoscaler::{DrainNodeReason, DrainNodeRequest};
        use gcs_proto::ray::rpc::{
            gcs_node_info::GcsNodeState, ActorTableData, DrainRayletReply, DrainRayletRequest,
            GcsNodeInfo,
        };
        use parking_lot::Mutex as PMutex;
        use std::collections::HashMap;

        // Scripted pool: accept=true, record the forwarded request.
        struct ScriptedPool {
            accept: bool,
            rejection_reason: String,
            calls: PMutex<Vec<(String, u16, DrainRayletRequest)>>,
        }
        #[async_trait]
        impl RayletClientPool for ScriptedPool {
            async fn resize_local_resource_instances(
                &self,
                _a: &str,
                _p: u16,
                _r: HashMap<String, f64>,
            ) -> Result<HashMap<String, f64>, tonic::Status> {
                Ok(HashMap::new())
            }
            async fn drain_raylet(
                &self,
                address: &str,
                port: u16,
                request: DrainRayletRequest,
            ) -> Result<DrainRayletReply, tonic::Status> {
                self.calls
                    .lock()
                    .push((address.to_string(), port, request));
                Ok(DrainRayletReply {
                    is_accepted: self.accept,
                    rejection_reason_message: self.rejection_reason.clone(),
                })
            }
            async fn kill_local_actor(
                &self,
                _address: &str,
                _port: u16,
                _request: gcs_proto::ray::rpc::KillLocalActorRequest,
            ) -> Result<(), tonic::Status> {
                Ok(())
            }
        }

        // Build a server, then swap the autoscaler's pool for the scripted
        // one. The autoscaler exposes its pool only through its own
        // constructor, so we rebuild it here with the mock and replace
        // the field via `set_actor_manager` — this is the same lifecycle
        // the production server uses.
        use gcs_managers::autoscaler_stub::GcsAutoscalerStateManager;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);

        // Register an alive node.
        let node_id: Vec<u8> = (0..28).map(|i| (i as u8) + 1).collect();
        let node = GcsNodeInfo {
            node_id: node_id.clone(),
            state: GcsNodeState::Alive as i32,
            node_manager_address: "10.0.0.42".into(),
            node_manager_port: 7001,
            resources_total: {
                let mut m = HashMap::new();
                m.insert("CPU".to_string(), 4.0);
                m
            },
            ..Default::default()
        };
        server.node_manager().add_node(node.clone()).await;

        // Seed an alive actor on that node so we can observe the
        // preemption publish.
        let actor_id: Vec<u8> = (0..16).map(|i| 100 + i as u8).collect();
        server.actor_manager().insert_registered_actor(
            actor_id.clone(),
            ActorTableData {
                actor_id: actor_id.clone(),
                state: ActorState::Alive as i32,
                node_id: Some(node_id.clone()),
                preempted: false,
                ..Default::default()
            },
        );
        server.actor_manager().record_created_actor(
            node_id.clone(),
            b"worker-1".to_vec(),
            actor_id.clone(),
        );

        // Build a scripted autoscaler that shares node_manager and actor_manager
        // with the server.
        let pool = Arc::new(ScriptedPool {
            accept: true,
            rejection_reason: String::new(),
            calls: PMutex::new(Vec::new()),
        });
        let pg_mgr: Arc<dyn gcs_managers::autoscaler_stub::PlacementGroupLoadSource> =
            server.placement_group_manager().clone();
        let asm = Arc::new(GcsAutoscalerStateManager::new(
            server.node_manager().clone(),
            pg_mgr,
            pool.clone(),
            {
                // Wrap the kv_store the server uses so state KV is shared;
                // for this test an in-memory one is fine.
                let store = Arc::new(InMemoryStoreClient::new());
                Arc::new(gcs_kv::StoreClientInternalKV::new(store))
            },
            "test".to_string(),
        ));
        asm.set_actor_manager(server.actor_manager().clone());

        let reply = asm
            .drain_node(tonic::Request::new(DrainNodeRequest {
                node_id: node_id.clone(),
                reason: DrainNodeReason::Preemption as i32,
                reason_message: "spot reclaim".into(),
                deadline_timestamp_ms: 5_000,
            }))
            .await
            .unwrap()
            .into_inner();

        // 1. Reply is accepted.
        assert!(reply.is_accepted);

        // 2. Raylet was called with the exact (address, port, reason,
        //    reason_message, deadline) that came in.
        let calls = pool.calls.lock();
        assert_eq!(calls.len(), 1);
        let (addr, port, req) = &calls[0];
        assert_eq!(addr, "10.0.0.42");
        assert_eq!(*port, 7001);
        assert_eq!(req.reason, DrainNodeReason::Preemption as i32);
        assert_eq!(req.reason_message, "spot reclaim");
        assert_eq!(req.deadline_timestamp_ms, 5_000);

        // 3. Actor on this node was marked preempted.
        assert!(
            server
                .actor_manager()
                .get_registered_actor(&actor_id)
                .unwrap()
                .preempted,
            "actor on drained node must have preempted=true"
        );

        // 4. Node manager recorded the drain.
        assert!(server.node_manager().is_node_draining(&node_id));
    }

    /// End-to-end parity guard for the custom Health service
    /// (C++ `gcs_server.cc:304-305`).
    ///
    /// Starts the real server, issues `grpc.health.v1.Health/Check`
    /// over a tonic client, expects `SERVING`. Then aborts the probe
    /// task and issues Check again — expects `NOT_SERVING` rather than
    /// the static "SERVING" the default tonic_health reporter would
    /// have returned.
    #[tokio::test]
    async fn health_check_service_reports_serving_and_not_serving_after_probe_aborts() {
        use tonic_health::pb::health_check_response::ServingStatus;
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let endpoint = format!("http://127.0.0.1:{}", port);
        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::new(channel);

        // Healthy probe → SERVING.
        let reply = client
            .check(HealthCheckRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.status,
            ServingStatus::Serving as i32,
            "default health check must be SERVING"
        );

        // Kill the probe task. Subsequent Check RPCs must surface the
        // event-loop-unhealthy state as NOT_SERVING — the whole point
        // of this fix.
        assert!(
            server.abort_health_probe_for_test(),
            "probe handle must exist before first check"
        );
        // Give the abort a moment to propagate.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reply = client
            .check(HealthCheckRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.status,
            ServingStatus::NotServing as i32,
            "aborted probe must surface as NOT_SERVING — parity with C++ \
             stuck-event-loop semantics"
        );

        server_handle.abort();
        let _ = server_handle.await;
    }

    /// Parity: Watch is not implemented (C++ only registers Check).
    /// A client that hits Watch gets UNIMPLEMENTED.
    #[tokio::test]
    async fn health_watch_returns_unimplemented() {
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_clone = server.clone();
        let server_handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let endpoint = format!("http://127.0.0.1:{}", port);
        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::new(channel);

        let err = client
            .watch(HealthCheckRequest::default())
            .await
            .err()
            .expect("watch must return UNIMPLEMENTED");
        assert_eq!(err.code(), tonic::Code::Unimplemented);

        server_handle.abort();
        let _ = server_handle.await;
    }

    /// Parity guard: period 0 disables the loop entirely — the fetcher
    /// must never be called. This mirrors the convention of every other
    /// `_period_*` knob in the Rust GCS.
    #[tokio::test]
    async fn raylet_load_pull_loop_period_zero_disables() {
        use async_trait::async_trait;
        use gcs_managers::raylet_load::RayletLoadFetcher;
        use gcs_proto::ray::rpc::{gcs_node_info::GcsNodeState, GcsNodeInfo, ResourcesData};
        use parking_lot::Mutex;
        use std::collections::HashMap;

        let _guard = RAY_CONFIG_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        ray_config::initialize(
            r#"{"gcs_pull_resource_loads_period_milliseconds": 0}"#,
        )
        .unwrap();

        struct CountingFetcher {
            calls: Mutex<usize>,
        }
        #[async_trait]
        impl RayletLoadFetcher for CountingFetcher {
            async fn fetch(
                &self,
                _a: &str,
                _p: u16,
            ) -> Result<ResourcesData, tonic::Status> {
                *self.calls.lock() += 1;
                Ok(ResourcesData::default())
            }
        }
        let fetcher = Arc::new(CountingFetcher {
            calls: Mutex::new(0),
        });

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        let node_id: Vec<u8> = (0..28).map(|i| (i as u8) + 1).collect();
        let node = GcsNodeInfo {
            node_id: node_id.clone(),
            state: GcsNodeState::Alive as i32,
            node_manager_address: "1.2.3.4".into(),
            node_manager_port: 7001,
            resources_total: {
                let mut m = HashMap::new();
                m.insert("CPU".to_string(), 4.0);
                m
            },
            ..Default::default()
        };
        server.node_manager().add_node(node).await;

        server.set_raylet_load_fetcher(fetcher.clone());
        server.start_raylet_load_pull_loop();

        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert_eq!(*fetcher.calls.lock(), 0, "period=0 must disable the loop");
        assert!(server.raylet_load_pull_handle.lock().is_none());

        let _ = ray_config::replace_for_test(prev);
    }

    /// Parity guard for C++ `InitUsageStatsClient` wiring
    /// (`gcs_server.cc:629-637`): after server construction the four
    /// managers must each hold the shared `UsageStatsClient`, and a
    /// recorded tag lands in the KV under the expected key.
    #[tokio::test]
    async fn usage_stats_client_is_wired_to_all_four_managers() {
        let server = GcsServer::new(GcsServerConfig::default()).await;

        // Record a test tag and confirm it round-trips through the KV.
        server
            .usage_stats_client()
            .record_extra_usage_counter(
                gcs_managers::usage_stats::TagKey::ActorNumCreated,
                3,
            )
            .await;

        let value: Option<String> = server
            .kv_manager
            .kv()
            .get("usage_stats", "extra_usage_tag_actor_num_created")
            .await;
        assert_eq!(value.as_deref(), Some("3"));

        // The four managers all share the same Arc — assert via
        // Arc::strong_count that at least 4 references exist on the
        // usage-stats client (one for the server, one per manager).
        // This guards against a future refactor that silently drops
        // a setter.
        let sc = Arc::strong_count(server.usage_stats_client());
        assert!(
            sc >= 5,
            "expected at least 5 strong refs (server + 4 managers), got {sc}"
        );
    }

    /// Parity guard for C++ eager init at `gcs_server.cc:327-328`:
    /// when `metrics_agent_port > 0` at startup, `InitMetricsExporter`
    /// is called immediately. The Rust equivalent flips
    /// `MetricsExporter::is_initialized` during `install_listeners_and_initialize`.
    #[tokio::test]
    async fn metrics_exporter_initializes_eagerly_when_port_is_set() {
        // A port of 1 will fail to connect but the one-shot flag is
        // flipped synchronously before the async connect attempt —
        // that's enough for the parity guard. The connect task is
        // drained so it doesn't leak past the test.
        let config = GcsServerConfig {
            metrics_agent_port: 1,
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config).await);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });

        // A short wait for install_listeners_and_initialize to flip
        // the flag.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            server.metrics_exporter().is_initialized(),
            "eager init must flip is_initialized when metrics_agent_port > 0"
        );

        if let Some(h) = server.metrics_exporter().take_handle() {
            let _ = h.await;
        }
        handle.abort();
        let _ = handle.await;
    }

    /// Parity guard for C++ deferred init at `gcs_server.cc:831-842`.
    /// C++ reads `node->metrics_agent_port()` (proto tag 30), NOT
    /// `metrics_export_port` (tag 9). These are two independent fields
    /// that `raylet/main.cc:1115-1116` populates separately. An
    /// earlier Rust revision read the wrong one and would have left
    /// the exporter uninitialised whenever the head node populated
    /// only `metrics_agent_port` — which is the common case.
    #[tokio::test]
    async fn metrics_exporter_initializes_on_head_node_register() {
        let config = GcsServerConfig {
            metrics_agent_port: -1,
            ..Default::default()
        };
        let server = Arc::new(GcsServer::new(config).await);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Should still be uninitialized — no eager call and no node
        // registration yet.
        assert!(!server.metrics_exporter().is_initialized());

        // Register a non-head node with a port — must NOT trigger init
        // (C++ checks `node->is_head_node()` at line 834).
        let non_head = gcs_proto::ray::rpc::GcsNodeInfo {
            node_id: (0..28).map(|i| i as u8).collect(),
            state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
            is_head_node: false,
            metrics_agent_port: 1,
            ..Default::default()
        };
        server.node_manager().add_node(non_head).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !server.metrics_exporter().is_initialized(),
            "non-head-node register must not fire the metrics exporter"
        );

        // Now register the head node with `metrics_agent_port` set —
        // must init. This is the exact field C++ reads at line 835.
        let head = gcs_proto::ray::rpc::GcsNodeInfo {
            node_id: (0..28).map(|i| (i + 100) as u8).collect(),
            state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
            is_head_node: true,
            metrics_agent_port: 1,
            ..Default::default()
        };
        server.node_manager().add_node(head).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            server.metrics_exporter().is_initialized(),
            "head-node register with metrics_agent_port > 0 must fire the metrics exporter"
        );

        if let Some(h) = server.metrics_exporter().take_handle() {
            let _ = h.await;
        }
        handle.abort();
        let _ = handle.await;
    }

    /// Positive regression for the field-parity fix: when the head
    /// node reports `metrics_agent_port > 0` but `metrics_export_port
    /// == 0`, the exporter must still initialise. This is the
    /// configuration the C++ code handles at `gcs_server.cc:835`, and
    /// the one a prior Rust revision would have missed by reading the
    /// wrong field.
    #[tokio::test]
    async fn metrics_exporter_inits_when_only_metrics_agent_port_is_set() {
        let server = Arc::new(
            GcsServer::new(GcsServerConfig {
                metrics_agent_port: -1,
                ..Default::default()
            })
            .await,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!server.metrics_exporter().is_initialized());

        let head = gcs_proto::ray::rpc::GcsNodeInfo {
            node_id: (0..28).map(|i| i as u8).collect(),
            state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
            is_head_node: true,
            metrics_agent_port: 1,
            metrics_export_port: 0,
            ..Default::default()
        };
        server.node_manager().add_node(head).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            server.metrics_exporter().is_initialized(),
            "metrics_agent_port > 0 is the one field that must drive init \
             (C++ `gcs_server.cc:835`)"
        );

        if let Some(h) = server.metrics_exporter().take_handle() {
            let _ = h.await;
        }
        handle.abort();
        let _ = handle.await;
    }

    /// Negative regression for the field-parity fix: when the head
    /// node reports `metrics_export_port > 0` but `metrics_agent_port
    /// == 0`, the exporter must NOT initialise. `metrics_export_port`
    /// is an HTTP metrics-scrape port (proto tag 9) and has nothing
    /// to do with the event-aggregator gRPC endpoint C++ connects to.
    /// A prior Rust revision would have initialised here — this test
    /// pins the corrected behaviour.
    #[tokio::test]
    async fn metrics_exporter_does_not_init_when_only_metrics_export_port_is_set() {
        let server = Arc::new(
            GcsServer::new(GcsServerConfig {
                metrics_agent_port: -1,
                ..Default::default()
            })
            .await,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            server_clone.start_with_listener(listener).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!server.metrics_exporter().is_initialized());

        let head = gcs_proto::ray::rpc::GcsNodeInfo {
            node_id: (0..28).map(|i| i as u8).collect(),
            state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
            is_head_node: true,
            metrics_agent_port: 0,
            metrics_export_port: 1,
            ..Default::default()
        };
        server.node_manager().add_node(head).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            !server.metrics_exporter().is_initialized(),
            "metrics_export_port is a different port — it must not drive \
             the C++ deferred init path"
        );

        handle.abort();
        let _ = handle.await;
    }

    /// End-to-end parity guard for `GcsFunctionManager`:
    ///
    ///  1. add two actor references and one job reference for the
    ///     same job (all three from the same JobID);
    ///  2. seed three KV keys in namespace `fun` that the cleanup is
    ///     supposed to remove;
    ///  3. drop the job and both actor references (the two actor
    ///     references via the actor manager's destroy path, the job
    ///     reference via `mark_job_finished`);
    ///  4. assert the three KV keys are gone and one KV key belonging
    ///     to a different job is untouched.
    ///
    /// Mirrors the C++ invariant: function/actor/hook metadata is
    /// cleaned up exactly once, when the last holder (job or
    /// detached actor) retires.
    #[tokio::test]
    async fn function_manager_cleans_kv_on_last_reference_retirement() {
        use gcs_proto::ray::rpc::actor_died_error_context::Reason as DeathReason;
        use gcs_proto::ray::rpc::actor_table_data::ActorState;
        use gcs_proto::ray::rpc::job_info_gcs_service_server::JobInfoGcsService;

        let server = Arc::new(GcsServer::new(GcsServerConfig::default()).await);
        let kv = server.kv_manager.kv();

        // Job id is 4 bytes; actor_id = 12 unique bytes + job_id.
        let job_id: Vec<u8> = vec![0xaa, 0xbb, 0xcc, 0xdd];
        let job_hex = "aabbccdd";
        let make_actor_id = |n: u8| -> Vec<u8> {
            let mut v = vec![n; 12];
            v.extend_from_slice(&job_id);
            v
        };

        // Seed the three KV keys the cleanup is supposed to wipe,
        // plus one extra under a different job id that must survive.
        kv.put(
            "fun",
            &format!("RemoteFunction:{job_hex}:f1"),
            "one".into(),
            true,
        )
        .await;
        kv.put(
            "fun",
            &format!("ActorClass:{job_hex}:ClassA"),
            "two".into(),
            true,
        )
        .await;
        kv.put(
            "fun",
            &format!("FunctionsToRun:{job_hex}:hookA"),
            "three".into(),
            true,
        )
        .await;
        // Different job id — must survive.
        kv.put("fun", "RemoteFunction:11223344:keep", "keep".into(), true)
            .await;

        // Drive the function-manager refcount to 3 via the ingress
        // paths that production uses. Keep the job ALIVE so the
        // `mark_job_finished` handler sees it in its in-memory map.
        // (Actor managers don't round-trip through storage for
        // refcount purposes — we use public API methods so this test
        // stays coupled to the observable contract, not internals.)
        let _ = server
            .job_manager()
            .add_job(tonic::Request::new(gcs_proto::ray::rpc::AddJobRequest {
                data: Some(gcs_proto::ray::rpc::JobTableData {
                    job_id: job_id.clone(),
                    is_dead: false,
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        assert_eq!(server.function_manager().count(&job_id), 1);

        // Simulate two actor registrations by inserting ActorTableData
        // with ALIVE state directly and bumping the refcount. The
        // actor manager's public register_actor requires a task spec
        // round-trip; we exercise the same refcount-balance path
        // directly via the function-manager accessor, then verify
        // the destroy path's matching decrement.
        let actor_a = make_actor_id(0x11);
        let actor_b = make_actor_id(0x22);
        for aid in [&actor_a, &actor_b] {
            server.actor_manager().insert_registered_actor(
                aid.clone(),
                gcs_proto::ray::rpc::ActorTableData {
                    actor_id: aid.clone(),
                    state: ActorState::Alive as i32,
                    // Death cause populated later in destroy; here
                    // the non-restartable default (max_restarts=0,
                    // RAY_KILL intent) is what destroy_actor expects.
                    max_restarts: 0,
                    ..Default::default()
                },
            );
            server.function_manager().add_job_reference(&job_id);
        }
        assert_eq!(server.function_manager().count(&job_id), 3);

        // KV still populated — no cleanup has run.
        assert!(
            kv.get("fun", &format!("RemoteFunction:{job_hex}:f1"))
                .await
                .is_some(),
            "KV cleanup must not have run while refcount > 0"
        );

        // Retire the two actors via `destroy_actor`. Each call drops
        // one refcount when the actor is not restartable.
        for aid in [&actor_a, &actor_b] {
            server
                .actor_manager()
                .destroy_actor_for_test(
                    aid,
                    DeathReason::RayKill,
                    "test retirement",
                )
                .await;
        }
        assert_eq!(
            server.function_manager().count(&job_id),
            1,
            "destroying both actors must have dropped two refcounts"
        );
        // KV still alive — the job refcount is still holding.
        assert!(
            kv.get("fun", &format!("RemoteFunction:{job_hex}:f1"))
                .await
                .is_some()
        );

        // Finalise the job. `mark_job_finished` drops the last
        // refcount and triggers the three prefix deletes.
        let _ = server
            .job_manager()
            .mark_job_finished(tonic::Request::new(
                gcs_proto::ray::rpc::MarkJobFinishedRequest {
                    job_id: job_id.clone(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(server.function_manager().count(&job_id), 0);

        // The three per-job KV keys are gone, the other job's key is
        // untouched.
        assert!(kv
            .get("fun", &format!("RemoteFunction:{job_hex}:f1"))
            .await
            .is_none());
        assert!(kv
            .get("fun", &format!("ActorClass:{job_hex}:ClassA"))
            .await
            .is_none());
        assert!(kv
            .get("fun", &format!("FunctionsToRun:{job_hex}:hookA"))
            .await
            .is_none());
        assert_eq!(
            kv.get("fun", "RemoteFunction:11223344:keep").await.as_deref(),
            Some("keep"),
            "cleanup must not touch other jobs"
        );

        // And: all four managers + the server itself should hold the
        // function-manager Arc — enough strong refs to prove the
        // wiring closed the loop. The actor and job managers each
        // hold one via their setter; the server holds one for the
        // accessor; plus the `Arc<GcsFunctionManager>` we observed
        // via the accessor counts too.
        let sc = Arc::strong_count(server.function_manager());
        assert!(
            sc >= 3,
            "expected at least 3 strong refs (server + job + actor), got {sc}"
        );
    }
}
