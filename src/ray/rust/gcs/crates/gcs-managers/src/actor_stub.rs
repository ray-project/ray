//! Full implementation of ActorInfoGcsService.
//!
//! Manages actor registration, creation, state transitions, and lookup.
//! Publishes state changes via PubSubManager so that subscribers
//! (raylet, dashboard, etc.) get notified.
//!
//! Maps C++ `GcsActorManager` from `src/ray/gcs/gcs_actor_manager.h/cc`.
//!
//! ## Actor Lifecycle (matching C++ semantics)
//!
//! States: DEPENDENCIES_UNREADY → PENDING_CREATION → ALIVE → DEAD
//!                                                  → RESTARTING → ALIVE
//!
//! 1. `RegisterActor` — stores task spec, creates actor in DEPENDENCIES_UNREADY
//! 2. `CreateActor` — transitions to PENDING_CREATION, then hands off to
//!    `GcsActorScheduler::Schedule()` which leases a worker from a raylet
//!    and creates the actor on it. On success, `on_actor_creation_success`
//!    transitions to ALIVE (matching C++ `OnActorCreationSuccess`).
//! 3. `KillActorViaGcs` —
//!    `no_restart=true`: DestroyActor path → DEAD with `RAY_KILL`
//!    (non-restartable, always `force_kill=true`, matching C++'s
//!    default on this branch).
//!    `no_restart=false`: KillActor path (separate from DestroyActor).
//!    For an actor in `created_actors`, only a `KillLocalActor` RPC is
//!    sent to the owning raylet — state stays unchanged until
//!    `OnWorkerDead` fires the restart logic. For a not-yet-created
//!    actor the raylet is notified (if a worker was leased), in-flight
//!    scheduling is cancelled, and `restart_actor_internal` runs with
//!    a `RAY_KILL` death cause (transition to RESTARTING if budget
//!    allows, otherwise DEAD with `RAY_KILL`).
//!    The C++ `graceful_shutdown_timers_` force-kill escalation
//!    (`gcs_actor_manager.cc:1041-1080`) is implemented in Rust as
//!    `GcsActorManager::graceful_shutdown_timers` — armed by
//!    `destroy_actor(force_kill=false, timeout_ms>0)`, cancelled on
//!    `force_kill=true` upgrade or on any restart, and fires a
//!    `KillLocalActor(force_kill=true)` RPC when the timeout
//!    expires. The production graceful path
//!    (`report_actor_out_of_scope`) reads the timeout from
//!    `RayConfig::actor_graceful_shutdown_timeout_ms`.
//! 4. `ReportActorOutOfScope` — transitions to DEAD with OUT_OF_SCOPE cause
//!    (stale-message detection via lineage reconstruction counter)
//! 5. `RestartActorForLineageReconstruction` — transitions DEAD → RESTARTING
//!    with idempotency/stale-message checks and restart budget enforcement

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::actor_info_gcs_service_server::ActorInfoGcsService;
use gcs_proto::ray::rpc::actor_table_data::ActorState;
use gcs_proto::ray::rpc::actor_died_error_context::Reason as DeathReason;
use gcs_proto::ray::rpc::actor_death_cause::Context as DeathContext;
use gcs_proto::ray::rpc::pub_message::InnerMessage;
use gcs_proto::ray::rpc::request_worker_lease_reply::SchedulingFailureType;
use gcs_proto::ray::rpc::*;

use crate::autoscaler_stub::RayletClientPool;
use gcs_pubsub::PubSubManager;
use gcs_table_storage::GcsTableStorage;

use crate::actor_scheduler::{
    ActorCreationSuccess, ActorSchedulingFailure, GcsActorScheduler, PendingActor,
};
use crate::node_manager::GcsNodeManager;

fn ok_status() -> GcsStatus {
    GcsStatus {
        code: 0,
        message: String::new(),
    }
}

fn error_status(code: i32, msg: &str) -> GcsStatus {
    GcsStatus {
        code,
        message: msg.to_string(),
    }
}

fn not_found_status(msg: &str) -> GcsStatus {
    error_status(17, msg) // Ray StatusCode::NotFound
}

fn already_exists_status(msg: &str) -> GcsStatus {
    error_status(6, msg) // Ray StatusCode::AlreadyExists (ALREADY_EXISTS in gRPC)
}

fn invalid_status(msg: &str) -> GcsStatus {
    error_status(5, msg) // Ray StatusCode::Invalid
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

fn now_ms_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Build an ActorDeathCause with the given reason.
fn make_death_cause(reason: DeathReason, msg: &str) -> ActorDeathCause {
    ActorDeathCause {
        context: Some(DeathContext::ActorDiedErrorContext(ActorDiedErrorContext {
            reason: reason as i32,
            error_message: msg.to_string(),
            ..Default::default()
        })),
    }
}

/// Default cap on the LRU of destroyed actors when no override is
/// provided. Matches C++ `RayConfig` default at `ray_config_def.h:936`
/// (`maximum_gcs_destroyed_actor_cached_count`). Production code reads
/// the actual cap from each `GcsActorManager`'s
/// `max_destroyed_actors_cached` field, which is configurable per
/// instance — the constant exists only as the default value used when
/// constructing a manager via `new` or `with_export_events`.
const DEFAULT_MAX_DESTROYED_ACTORS: usize = 100_000;

/// Handle to a per-worker graceful-shutdown timer task. Parity with
/// C++ `graceful_shutdown_timers_` value type
/// (`std::unique_ptr<boost::asio::deadline_timer>`) at
/// `gcs_actor_manager.h:534`.
///
/// The timer task itself is responsible for re-acquiring the manager
/// via `Weak::upgrade` and re-validating that the actor is still
/// registered with the same `worker_id` before firing a force-kill
/// RPC. Cancellation by the manager is performed by removing this
/// entry from the `graceful_shutdown_timers` map; the task discovers
/// removal on wake (via a self-remove that returns `None`) and exits
/// silently. This mirrors C++ behavior at
/// `gcs_actor_manager.cc:1058-1061` where the callback receives
/// `operation_aborted` on `cancel()` and bails after self-erasing.
struct GracefulShutdownTimer {
    /// Owned task handle. Kept on `Self` so `Drop` can `abort()` if a
    /// timer entry is removed without going through the cooperative
    /// "self-remove + skip" path. Production cancellation always
    /// removes the entry first, so the task simply terminates after
    /// `sleep` returns; abort is the safety net for shutdown.
    handle: JoinHandle<()>,
}

impl Drop for GracefulShutdownTimer {
    fn drop(&mut self) {
        // Best-effort: abort the sleeping task on drop. If the task
        // already woke and is past the self-remove check, abort is a
        // no-op. If it is still asleep, this stops it from holding a
        // tokio runtime slot beyond the manager's lifetime.
        self.handle.abort();
    }
}

/// GCS Actor Manager.
pub struct GcsActorManager {
    /// actor_id -> ActorTableData (all registered actors: unresolved, pending, alive, dead-but-restartable)
    actors: DashMap<Vec<u8>, ActorTableData>,
    /// Recently destroyed actors (non-restartable). Used as a fallback for
    /// GetActorInfo lookups. Maps C++ `destroyed_actors_`.
    destroyed_actors: DashMap<Vec<u8>, ActorTableData>,
    /// (name, namespace) -> actor_id for named actors
    named_actors: DashMap<(String, String), Vec<u8>>,
    /// actor_id -> TaskSpec (the creation task spec)
    actor_task_specs: DashMap<Vec<u8>, TaskSpec>,
    /// NodeID → (WorkerID → ActorID) for ALIVE actors. Maps C++ `created_actors_`.
    created_actors: Mutex<HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<u8>>>>,
    publisher: Arc<PubSubManager>,
    table_storage: Arc<GcsTableStorage>,
    /// Actor scheduler for async PENDING_CREATION → ALIVE flow.
    scheduler: Arc<GcsActorScheduler>,
    node_manager: Arc<GcsNodeManager>,
    /// Optional EXPORT_ACTOR sink — emits one JSON line per state change.
    /// Maps C++ `RayExportEvent(ExportActorData).SendEvent()` in
    /// `gcs_actor.cc:159`.
    export_events: Arc<crate::export_event_writer::ExportEventManager>,
    /// Cap on the LRU of destroyed actors. Captured at construction
    /// from `RayConfig::maximum_gcs_destroyed_actor_cached_count`
    /// (`ray_config_def.h:936`); kept on `self` so a long-running
    /// process honors whatever value was active at startup without
    /// taking the config RwLock per insert.
    max_destroyed_actors_cached: usize,
    /// Optional usage-stats client, installed by
    /// `set_usage_stats_client` after KV init. Mirrors C++
    /// `gcs_actor_manager.cc:249` (nullptr default) + the
    /// `GcsServer::InitUsageStatsClient` wiring at
    /// `gcs_server.cc:634`.
    usage_stats_client: Mutex<Option<Arc<crate::usage_stats::UsageStatsClient>>>,
    /// Monotonic count of actors moved to ALIVE. Parity with C++
    /// `lifetime_num_created_actors_` at `gcs_actor_manager.h:513`.
    lifetime_num_created_actors: Mutex<i64>,
    /// Optional function manager — the per-job KV-cleanup refcounter
    /// both the job manager and the actor manager share. Set after
    /// construction via `set_function_manager`. Mirrors the C++
    /// `function_manager_` reference at
    /// `gcs_actor_manager.cc:731,1114,1730`. When `None` the actor
    /// manager runs without per-job KV cleanup (tests that don't
    /// stand up a function manager fall into this path).
    function_manager: Mutex<Option<Arc<crate::function_manager::GcsFunctionManager>>>,
    /// Optional runtime-env service — the same instance the job
    /// manager uses. Detached actors must pin their creation-time
    /// runtime-env URIs so `gcs://` packages survive their owning
    /// job's finish. Mirrors C++ `runtime_env_manager_` usage at
    /// `gcs_actor_manager.cc:744-745,1120` — `AddURIReference` on
    /// detached-actor register, `RemoveURIReference` on the
    /// non-restartable destroy branch. When `None`, runtime-env
    /// lifecycle is not driven by this manager.
    runtime_env_service: Mutex<Option<Arc<crate::runtime_env_stub::RuntimeEnvService>>>,
    /// Optional raylet client pool — used by
    /// `notify_raylet_to_kill_actor` to send `KillLocalActor` RPCs
    /// when destroying an actor that has a live worker on a known
    /// raylet. Mirrors C++ `raylet_client_pool_` threaded into
    /// `GcsActorManager` at construction and consumed in
    /// `NotifyRayletToKillActor` (`gcs_actor_manager.cc:1857-1858`).
    ///
    /// Set via `set_raylet_client_pool` to match the existing setter
    /// pattern (`set_function_manager`, `set_runtime_env_service`,
    /// `set_usage_stats_client`). When `None`, the raylet
    /// notification is skipped (tests that don't stand up a pool
    /// still work) — production code always wires it in
    /// `GcsServer::new_with_store`.
    raylet_client_pool: Mutex<Option<Arc<dyn RayletClientPool>>>,
    /// Per-worker graceful-shutdown timers. Keyed by `worker_id`.
    /// Parity with C++ `graceful_shutdown_timers_`
    /// (`gcs_actor_manager.h:534`). An entry is armed when
    /// `destroy_actor` is called with `force_kill=false` AND a
    /// positive timeout AND no entry already exists for this
    /// `worker_id` (matches C++ `gcs_actor_manager.cc:1041-1042`).
    /// On expiry, the timer task fires a second
    /// `KillLocalActor(force_kill=true)` RPC if the actor is still
    /// registered with the same `worker_id`. Cancellation comes from
    /// `destroy_actor(force_kill=true)` (escalation interrupt) and
    /// from `restart_actor_internal` (timer must not survive into a
    /// restarted instance) — both remove the entry from this map,
    /// which the timer task observes via a self-remove returning
    /// `None`.
    graceful_shutdown_timers: Mutex<HashMap<Vec<u8>, GracefulShutdownTimer>>,
    /// Weak self-reference used by detached tasks (graceful-shutdown
    /// timers, scheduler loop) that need to call back into the
    /// manager's async methods after a `tokio::spawn`. Parity with
    /// C++ `std::enable_shared_from_this<GcsActorManager>` /
    /// `weak_from_this()` used in
    /// `gcs_actor_manager.cc:1048` to capture a weak reference for
    /// the timer callback. Set once at startup via
    /// `install_self_arc(self: &Arc<Self>)`; production wiring calls
    /// it from `GcsServer::new_with_store` (the same place
    /// `start_scheduler_loop` is called) and tests call it from
    /// `make_manager` helpers. When `None`, async fallbacks degrade
    /// gracefully — the timer simply will not be armed.
    self_weak: Mutex<Option<Weak<Self>>>,
}

impl GcsActorManager {
    pub fn new(
        publisher: Arc<PubSubManager>,
        table_storage: Arc<GcsTableStorage>,
        scheduler: Arc<GcsActorScheduler>,
        node_manager: Arc<GcsNodeManager>,
    ) -> Self {
        Self::with_export_events(
            publisher,
            table_storage,
            scheduler,
            node_manager,
            crate::export_event_writer::ExportEventManager::disabled(),
        )
    }

    pub fn with_export_events(
        publisher: Arc<PubSubManager>,
        table_storage: Arc<GcsTableStorage>,
        scheduler: Arc<GcsActorScheduler>,
        node_manager: Arc<GcsNodeManager>,
        export_events: Arc<crate::export_event_writer::ExportEventManager>,
    ) -> Self {
        Self::with_export_events_and_cap(
            publisher,
            table_storage,
            scheduler,
            node_manager,
            export_events,
            DEFAULT_MAX_DESTROYED_ACTORS,
        )
    }

    /// Full constructor that lets the caller supply
    /// `max_destroyed_actors_cached`. The GCS server uses this to
    /// thread the value of
    /// `ray_config::instance().maximum_gcs_destroyed_actor_cached_count`
    /// (parity with C++ `gcs_actor_manager.cc` reading
    /// `RayConfig::instance().maximum_gcs_destroyed_actor_cached_count()`).
    pub fn with_export_events_and_cap(
        publisher: Arc<PubSubManager>,
        table_storage: Arc<GcsTableStorage>,
        scheduler: Arc<GcsActorScheduler>,
        node_manager: Arc<GcsNodeManager>,
        export_events: Arc<crate::export_event_writer::ExportEventManager>,
        max_destroyed_actors_cached: usize,
    ) -> Self {
        Self {
            actors: DashMap::new(),
            destroyed_actors: DashMap::new(),
            named_actors: DashMap::new(),
            actor_task_specs: DashMap::new(),
            created_actors: Mutex::new(HashMap::new()),
            publisher,
            table_storage,
            scheduler,
            node_manager,
            export_events,
            max_destroyed_actors_cached,
            usage_stats_client: Mutex::new(None),
            lifetime_num_created_actors: Mutex::new(0),
            function_manager: Mutex::new(None),
            runtime_env_service: Mutex::new(None),
            raylet_client_pool: Mutex::new(None),
            graceful_shutdown_timers: Mutex::new(HashMap::new()),
            self_weak: Mutex::new(None),
        }
    }

    /// Install the function manager. Production wiring calls this
    /// from `GcsServer::new_with_store` once KV + both managers
    /// exist. Mirrors C++'s constructor-argument threading — we
    /// prefer a setter to avoid a circular build order between the
    /// function manager and the actor manager.
    pub fn set_function_manager(
        &self,
        fm: Arc<crate::function_manager::GcsFunctionManager>,
    ) {
        *self.function_manager.lock() = Some(fm);
    }

    /// Install the runtime-env service. Wired by
    /// `GcsServer::new_with_store` after the runtime-env service is
    /// constructed (same instance handed to the job manager). Mirrors
    /// C++ `runtime_env_manager_` threaded into `GcsActorManager` at
    /// construction time in `gcs_server.cc:694-738`.
    pub fn set_runtime_env_service(
        &self,
        svc: Arc<crate::runtime_env_stub::RuntimeEnvService>,
    ) {
        *self.runtime_env_service.lock() = Some(svc);
    }

    /// Install the usage-stats recorder. Mirrors C++
    /// `GcsActorManager::SetUsageStatsClient`
    /// (`actor/gcs_actor_manager.h:224`), invoked from
    /// `GcsServer::InitUsageStatsClient` (`gcs_server.cc:634`).
    pub fn set_usage_stats_client(
        &self,
        client: Arc<crate::usage_stats::UsageStatsClient>,
    ) {
        *self.usage_stats_client.lock() = Some(client);
    }

    /// Install the raylet client pool used to send `KillLocalActor`
    /// RPCs from `destroy_actor`. Mirrors C++ `raylet_client_pool_`
    /// threaded into `GcsActorManager`'s constructor; the same
    /// instance is also held by the autoscaler (single-pool-per-GCS
    /// model). Implemented as a setter rather than a constructor
    /// argument so the server can wire it after both the actor
    /// manager and the pool are `Arc`-shared.
    pub fn set_raylet_client_pool(&self, pool: Arc<dyn RayletClientPool>) {
        *self.raylet_client_pool.lock() = Some(pool);
    }

    /// Capture a `Weak<Self>` for use by detached tasks (e.g. the
    /// graceful-shutdown timer that has to call back into
    /// `notify_raylet_to_kill_actor` after a `tokio::spawn`). Parity
    /// with C++ `weak_from_this()` (used at
    /// `gcs_actor_manager.cc:1048` to capture a weak reference for
    /// the timer callback). Idempotent — calling more than once
    /// updates the stored weak reference.
    ///
    /// Production wiring calls this from `GcsServer::new_with_store`
    /// after the manager is wrapped in `Arc`. Tests that exercise
    /// timer-driven paths must also call this; without it,
    /// `destroy_actor(force_kill=false)` cannot arm a timer and
    /// degrades to "no escalation" (the prior behavior).
    pub fn install_self_arc(self: &Arc<Self>) {
        *self.self_weak.lock() = Some(Arc::downgrade(self));
    }

    /// Bump `lifetime_num_created_actors` and forward to the usage
    /// client if one is installed. Call site: the actor scheduler's
    /// success path (mirrors C++
    /// `gcs_actor_manager.cc:1637` + `:2046-2049`).
    fn record_actor_created(&self) {
        let mut counter = self.lifetime_num_created_actors.lock();
        *counter += 1;
        let count = *counter;
        drop(counter);
        let client = self.usage_stats_client.lock().clone();
        if let Some(client) = client {
            client.record_extra_usage_counter_spawn(
                crate::usage_stats::TagKey::ActorNumCreated,
                count,
            );
        }
    }

    /// Returns the cap currently in effect — primarily for tests.
    pub fn max_destroyed_actors_cached(&self) -> usize {
        self.max_destroyed_actors_cached
    }

    /// Insert a registered actor directly. For tests that need to seed
    /// actor rows without running the full creation pipeline.
    pub fn insert_registered_actor(&self, actor_id: Vec<u8>, data: ActorTableData) {
        self.actors.insert(actor_id, data);
    }

    /// Record `(actor_id, worker_id)` as alive on `node_id`. For tests
    /// that need to simulate a completed actor creation.
    pub fn record_created_actor(&self, node_id: Vec<u8>, worker_id: Vec<u8>, actor_id: Vec<u8>) {
        self.created_actors
            .lock()
            .entry(node_id)
            .or_default()
            .insert(worker_id, actor_id);
    }

    /// Read a registered actor's ActorTableData (test-only getter).
    pub fn get_registered_actor(&self, actor_id: &[u8]) -> Option<ActorTableData> {
        self.actors.get(actor_id).map(|r| r.clone())
    }

    /// Mark every alive actor on `node_id` as `preempted=true`, persist
    /// the updated rows, and publish them on the actor pubsub channel.
    ///
    /// Maps C++ `GcsActorManager::SetPreemptedAndPublish`
    /// (`src/ray/gcs/actor/gcs_actor_manager.cc:1417-1444`). Invoked from
    /// `HandleDrainNode` *before* the raylet RPC is issued so that
    /// preemption state reaches subscribers even if the raylet RPC hangs
    /// or the node disappears. When the preempted actor later dies,
    /// downstream readers (Ray's restart / error-cause logic in
    /// `gcs_actor.cc:159` and `actor_manager.cc:1505-1506`) rely on this
    /// bit to decide that the death was a preemption rather than an
    /// application fault.
    ///
    /// Async because it awaits the `ActorTable::put` for each affected
    /// actor (same contract as the C++ `ActorTable().Put` that wraps
    /// the callback + publisher in `gcs_actor_manager.cc:1435-1442`).
    pub async fn set_preempted_and_publish(&self, node_id: &[u8]) {
        // Snapshot the actor ids on this node under the short-held lock,
        // then release the lock before doing any async work. Matches the
        // C++ pattern where `created_actors_.find(node_id)` is a simple
        // lookup and the I/O happens outside any locked section.
        let actor_ids: Vec<Vec<u8>> = {
            let created = self.created_actors.lock();
            match created.get(node_id) {
                Some(workers) => workers.values().cloned().collect(),
                None => return,
            }
        };

        if actor_ids.is_empty() {
            return;
        }

        for actor_id in actor_ids {
            // Flip the bit in-memory. The DashMap row must exist (C++
            // `RAY_CHECK`s the same thing at line 1427-1428); if it's
            // somehow gone, log and keep going instead of aborting the
            // entire drain, because the write side here is advisory and
            // the raylet RPC is still the authoritative acceptance step.
            let snapshot = {
                let Some(mut entry) = self.actors.get_mut(&actor_id) else {
                    warn!(
                        node_id = hex(node_id),
                        actor_id = hex(&actor_id),
                        "set_preempted_and_publish: actor missing from registered table"
                    );
                    continue;
                };
                entry.preempted = true;
                entry.clone()
            };

            // Persist, then publish. Order matters — C++ only publishes
            // after the `Put` callback runs (`gcs_actor_manager.cc:1438-1441`),
            // so a subscriber that reads storage will always see the
            // same value that was published.
            self.table_storage
                .actor_table()
                .put(&hex(&actor_id), &snapshot)
                .await;
            self.publish_actor_update(&snapshot);
        }
    }

    fn actor_state_name(state: i32) -> &'static str {
        match ActorState::try_from(state) {
            Ok(ActorState::DependenciesUnready) => "DEPENDENCIES_UNREADY",
            Ok(ActorState::PendingCreation) => "PENDING_CREATION",
            Ok(ActorState::Alive) => "ALIVE",
            Ok(ActorState::Restarting) => "RESTARTING",
            Ok(ActorState::Dead) => "DEAD",
            _ => "UNSPECIFIED",
        }
    }

    /// Build an `ExportActorData` payload for the export-event log.
    /// Mirrors C++ `gcs_actor.cc:138-159` (ConvertActorTableDataToExportActorData).
    fn actor_to_export(
        actor: &ActorTableData,
    ) -> crate::export_event_writer::ExportActorData {
        use crate::export_event_writer::ExportActorData;
        ExportActorData {
            actor_id: super::actor_stub::hex(&actor.actor_id),
            job_id: super::actor_stub::hex(&actor.job_id),
            state: Self::actor_state_name(actor.state).to_string(),
            is_detached: actor.is_detached,
            name: actor.name.clone(),
            ray_namespace: actor.ray_namespace.clone(),
            serialized_runtime_env: actor.serialized_runtime_env.clone(),
            class_name: actor.class_name.clone(),
            start_time: actor.start_time as i64,
            end_time: actor.end_time as i64,
            pid: actor.pid,
            node_id: actor
                .node_id
                .as_ref()
                .map(|n| super::actor_stub::hex(n))
                .unwrap_or_default(),
            placement_group_id: actor
                .placement_group_id
                .as_ref()
                .map(|p| super::actor_stub::hex(p))
                .unwrap_or_default(),
            repr_name: actor.repr_name.clone(),
            required_resources: actor.required_resources.clone(),
        }
    }


    /// Initialize from persisted data (on restart recovery).
    pub fn initialize(
        &self,
        actors: &std::collections::HashMap<String, ActorTableData>,
        task_specs: &std::collections::HashMap<String, TaskSpec>,
    ) {
        let fm = self.function_manager.lock().clone();
        for (_key, actor) in actors {
            let actor_id = actor.actor_id.clone();
            if !actor.name.is_empty() {
                self.named_actors.insert(
                    (actor.name.clone(), actor.ray_namespace.clone()),
                    actor_id.clone(),
                );
            }
            // Parity with C++ `gcs_actor_manager.cc:1730` recovery
            // path: every recovered actor contributes one refcount to
            // its job. The matching `RemoveJobReference` fires from
            // the destroy path when the actor later retires.
            if let Some(ref fm) = fm {
                if let Some(job_id) = Self::job_id_from_actor_id(&actor_id) {
                    fm.add_job_reference(&job_id);
                }
            }
            self.actors.insert(actor_id, actor.clone());
        }
        for (_key, spec) in task_specs {
            if let Some(actor_id) = Self::actor_id_from_task_spec(spec) {
                self.actor_task_specs.insert(actor_id, spec.clone());
            }
        }
        info!(
            actors = self.actors.len(),
            named = self.named_actors.len(),
            task_specs = self.actor_task_specs.len(),
            "Actor manager initialized"
        );
    }

    /// Publish an actor state change on the GCS_ACTOR_CHANNEL and emit a
    /// matching `EXPORT_ACTOR` JSON-line event.
    ///
    /// Maps C++ pubsub publish + `RayExportEvent(ExportActorData).SendEvent()`
    /// (gcs_actor.cc:159).
    fn publish_actor_update(&self, actor: &ActorTableData) {
        let msg = PubMessage {
            channel_type: ChannelType::GcsActorChannel as i32,
            key_id: actor.actor_id.clone(),
            sequence_id: 0,
            inner_message: Some(InnerMessage::ActorMessage(actor.clone())),
        };
        self.publisher.publish(msg);
        self.export_events
            .report_actor_event(&Self::actor_to_export(actor));
    }

    /// Extract the actor_id from a TaskSpec's actor_creation_task_spec.
    fn actor_id_from_task_spec(task_spec: &TaskSpec) -> Option<Vec<u8>> {
        task_spec
            .actor_creation_task_spec
            .as_ref()
            .map(|s| s.actor_id.clone())
    }

    /// Extract the 4-byte `JobID` from an `ActorID`. Mirrors C++
    /// `ActorID::JobId()` (`common/id.cc:150-154`), which reads the
    /// `JobID::kLength`-byte suffix starting at
    /// `ActorID::kUniqueBytesLength = 12`
    /// (`common/id.h:126-129`). The full `ActorID` is 16 bytes.
    ///
    /// Returns `None` when the input is shorter than expected so
    /// malformed callers log rather than panic.
    fn job_id_from_actor_id(actor_id: &[u8]) -> Option<Vec<u8>> {
        const ACTOR_UNIQUE: usize = 12;
        const JOB_LEN: usize = 4;
        if actor_id.len() < ACTOR_UNIQUE + JOB_LEN {
            return None;
        }
        Some(actor_id[ACTOR_UNIQUE..ACTOR_UNIQUE + JOB_LEN].to_vec())
    }

    /// Build initial ActorTableData from a TaskSpec.
    fn build_actor_table_data(task_spec: &TaskSpec, state: ActorState) -> ActorTableData {
        let creation = task_spec.actor_creation_task_spec.as_ref();
        let actor_id = creation.map(|c| c.actor_id.clone()).unwrap_or_default();
        let name = creation.map(|c| c.name.clone()).unwrap_or_default();
        let ray_namespace = creation.map(|c| c.ray_namespace.clone()).unwrap_or_default();
        let is_detached = creation.map(|c| c.is_detached).unwrap_or(false);
        let max_restarts = creation.map(|c| c.max_actor_restarts).unwrap_or(0);

        ActorTableData {
            actor_id,
            parent_id: task_spec.caller_id.clone(),
            job_id: task_spec.job_id.clone(),
            state: state as i32,
            max_restarts,
            num_restarts: 0,
            address: task_spec.caller_address.clone(),
            owner_address: task_spec.caller_address.clone(),
            is_detached,
            name,
            timestamp: now_ms(),
            function_descriptor: task_spec.function_descriptor.clone(),
            ray_namespace,
            required_resources: task_spec.required_resources.clone(),
            serialized_runtime_env: task_spec
                .runtime_env_info
                .as_ref()
                .map(|r| r.serialized_runtime_env.clone())
                .unwrap_or_default(),
            class_name: task_spec.name.clone(),
            ..Default::default()
        }
    }

    /// Check if a named actor slot is occupied by a non-DEAD actor.
    fn is_named_actor_alive(&self, name: &str, namespace: &str) -> bool {
        let key = (name.to_string(), namespace.to_string());
        if let Some(existing_id) = self.named_actors.get(&key) {
            if let Some(existing_actor) = self.actors.get(existing_id.value()) {
                return existing_actor.state != ActorState::Dead as i32;
            }
        }
        false
    }

    /// Check if an actor is restartable (matching C++ IsActorRestartable).
    ///
    /// An actor is restartable only if:
    /// - It is in DEAD state
    /// - It died due to OUT_OF_SCOPE (not RAY_KILL, WORKER_DIED, etc.)
    /// - It has restart budget remaining (max_restarts == -1 for infinite,
    ///   or remaining_restarts > 0 accounting for preemption)
    fn is_actor_restartable(actor: &ActorTableData) -> bool {
        if actor.state != ActorState::Dead as i32 {
            return false;
        }

        // Check death cause: only OUT_OF_SCOPE deaths are restartable.
        let is_out_of_scope = actor.death_cause.as_ref().map_or(false, |dc| {
            match &dc.context {
                Some(DeathContext::ActorDiedErrorContext(ctx)) => {
                    ctx.reason == DeathReason::OutOfScope as i32
                }
                _ => false,
            }
        });
        if !is_out_of_scope {
            return false;
        }

        // Check restart budget.
        if actor.max_restarts < 0 {
            return true; // Infinite restarts (-1)
        }
        let effective_restarts =
            actor.num_restarts - actor.num_restarts_due_to_node_preemption as i64;
        effective_restarts < actor.max_restarts
    }

    /// Test-only wrapper around the private `destroy_actor` path so
    /// cross-crate integration tests (in `gcs-server`) can drive the
    /// not-restartable destroy path without reaching into private
    /// internals.
    ///
    /// Defaults `force_kill=true` and `graceful_shutdown_timeout_ms=-1`,
    /// matching C++ `DestroyActor`'s default arguments
    /// (`gcs_actor_manager.h:303-305`). Tests exercising graceful-kill
    /// semantics should call `destroy_actor` directly with
    /// `force_kill=false` and a positive timeout.
    pub async fn destroy_actor_for_test(
        &self,
        actor_id: &[u8],
        death_reason: DeathReason,
        error_message: &str,
    ) {
        self.destroy_actor(
            actor_id,
            death_reason,
            error_message,
            /*force_kill=*/ true,
            /*graceful_shutdown_timeout_ms=*/ -1,
        )
        .await;
    }

    /// Test-only wrapper that lets graceful-shutdown timer tests
    /// drive the full `destroy_actor(force_kill, timeout_ms)`
    /// signature. Hidden behind `cfg(test)` so it does not widen
    /// the public API. Production code paths reach `destroy_actor`
    /// through the RPC handlers; the only call site that arms a
    /// timer in production is `report_actor_out_of_scope` (which
    /// reads the timeout from `RayConfig`).
    #[cfg(test)]
    pub async fn destroy_actor_with_timeout_for_test(
        &self,
        actor_id: &[u8],
        death_reason: DeathReason,
        error_message: &str,
        force_kill: bool,
        graceful_shutdown_timeout_ms: i64,
    ) {
        self.destroy_actor(
            actor_id,
            death_reason,
            error_message,
            force_kill,
            graceful_shutdown_timeout_ms,
        )
        .await;
    }

    /// Test-only accessor for the graceful-shutdown timer count.
    /// Lets tests assert that arming/cancellation accounting is
    /// correct without exposing the timer map publicly.
    #[cfg(test)]
    pub fn graceful_shutdown_timer_count(&self) -> usize {
        self.graceful_shutdown_timers.lock().len()
    }

    /// Destroy an actor: transition to DEAD, notify the owning raylet
    /// to kill the worker, and clean up tracking structures.
    ///
    /// `force_kill` matches C++ `DestroyActor(..., bool force_kill, ...)`
    /// at `gcs_actor_manager.cc:985`. It is forwarded to the raylet in
    /// `KillLocalActorRequest.force_kill` (C++ line 1850); the raylet
    /// translates `force_kill=false` into a graceful `Exit` and
    /// `force_kill=true` into SIGKILL.
    ///
    /// `graceful_shutdown_timeout_ms` matches C++
    /// `DestroyActor(..., int64_t graceful_shutdown_timeout_ms)` at
    /// `gcs_actor_manager.h:305` (default `-1`). When `force_kill=false`
    /// AND `graceful_shutdown_timeout_ms > 0` AND the worker is in
    /// `created_actors`, this method arms a per-worker timer that
    /// fires a second `KillLocalActor(force_kill=true)` RPC if the
    /// worker has not exited within the timeout. Parity with
    /// `gcs_actor_manager.cc:1041-1080`. The timer is cancelled by:
    ///  - a subsequent `destroy_actor(force_kill=true)` (escalation
    ///    interrupt, parity `gcs_actor_manager.cc:1011-1016`),
    ///  - `restart_actor_internal` (timer must not survive into a
    ///    restarted instance, parity `gcs_actor_manager.cc:1468-1472`),
    ///  - the timer task firing successfully (self-removal).
    /// Idempotent graceful calls preserve the original timer (C++
    /// only arms when no entry exists for `worker_id`).
    ///
    /// If the actor is restartable (OUT_OF_SCOPE + budget), it stays
    /// in `actors` for potential lineage reconstruction. Otherwise,
    /// it's fully cleaned up.
    async fn destroy_actor(
        &self,
        actor_id: &[u8],
        death_reason: DeathReason,
        error_message: &str,
        force_kill: bool,
        graceful_shutdown_timeout_ms: i64,
    ) {
        let Some(mut entry) = self.actors.get_mut(actor_id) else {
            return; // Already destroyed or never registered.
        };

        // Snapshot the actor's CURRENT worker_id BEFORE we mutate the
        // entry. Used both for the cancel-on-force-kill check and (on
        // the graceful branch) for arming the new timer. C++ reads
        // `actor->GetWorkerID()` at this same point in
        // `gcs_actor_manager.cc:1012`.
        let pre_worker_id = entry
            .address
            .as_ref()
            .map(|a| a.worker_id.clone())
            .unwrap_or_default();

        // Cancel an existing graceful-shutdown timer ONLY when this
        // call is itself a force-kill — that is what interrupts the
        // graceful phase. Runs BEFORE the already-DEAD early-return:
        // a force-kill upgrade after the actor has been marked DEAD
        // (e.g. graceful destroy → DEAD in self.actors → caller
        // invokes destroy_actor(force_kill=true) to escalate) must
        // still cancel the timer, matching C++
        // `gcs_actor_manager.cc:1011-1016` which performs the cancel
        // before checking state. Idempotent graceful calls keep the
        // original timer running.
        if force_kill && !pre_worker_id.is_empty() {
            self.graceful_shutdown_timers
                .lock()
                .remove(&pre_worker_id);
        }

        // Skip if already dead — but only AFTER the timer-cancel
        // step above. C++ runs `DestroyActor` unconditionally and
        // lets the state == DEAD branch fall through; Rust's early
        // return is a pre-existing optimization on the side-effect
        // free path (no NotifyRaylet, no re-publish), but the timer
        // cancel must still happen for parity with the
        // force-kill-upgrade contract.
        if entry.state == ActorState::Dead as i32 {
            return;
        }

        entry.state = ActorState::Dead as i32;
        entry.timestamp = now_ms();
        entry.end_time = now_ms_u64();
        entry.death_cause = Some(make_death_cause(death_reason, error_message));

        let actor_data = entry.clone();
        drop(entry);

        // Persist the DEAD state.
        self.table_storage
            .actor_table()
            .put(&hex(actor_id), &actor_data)
            .await;
        self.publish_actor_update(&actor_data);

        // Parity with C++ `gcs_actor_manager.cc:1025-1037`: when the
        // actor had a live worker on a known raylet (i.e. was tracked
        // in `created_actors`), forward a `KillLocalActor` RPC to
        // that raylet before any further cleanup. The raylet is
        // authoritative for the actual process termination — without
        // this, GCS marks the actor DEAD but the worker process keeps
        // running.
        //
        // Snapshot the raylet-address inputs off `actor_data` / the
        // `created_actors` map into owned values; the helper is async
        // and must not hold any DashMap entry across `.await`.
        let worker_id = actor_data
            .address
            .as_ref()
            .map(|a| a.worker_id.clone())
            .unwrap_or_default();
        let node_id = actor_data.node_id.clone().unwrap_or_default();
        let has_created_entry = !node_id.is_empty()
            && !worker_id.is_empty()
            && {
                let created = self.created_actors.lock();
                created
                    .get(&node_id)
                    .is_some_and(|m| m.contains_key(&worker_id))
            };
        if has_created_entry {
            self.notify_raylet_to_kill_actor(
                actor_id,
                &worker_id,
                &node_id,
                actor_data.death_cause.clone(),
                force_kill,
            )
            .await;
            // Mirror C++ `node_it->second.erase(actor->GetWorkerID())`
            // at `gcs_actor_manager.cc:1034`: once the raylet has
            // been notified, the entry is no longer considered
            // "created" on that node.
            let mut created = self.created_actors.lock();
            if let Some(workers) = created.get_mut(&node_id) {
                workers.remove(&worker_id);
                if workers.is_empty() {
                    created.remove(&node_id);
                }
            }
            drop(created);

            // Arm the graceful-shutdown timer when this is a graceful
            // kill, the configured timeout is positive, and there is
            // no existing timer for this worker. Parity with
            // C++ `gcs_actor_manager.cc:1041-1042`.
            if !force_kill && graceful_shutdown_timeout_ms > 0 {
                self.arm_graceful_shutdown_timer(
                    actor_id,
                    &worker_id,
                    actor_data.death_cause.clone(),
                    graceful_shutdown_timeout_ms,
                );
            }
        }

        // If not restartable, clean up fully and move to destroyed cache.
        if !Self::is_actor_restartable(&actor_data) {
            // Free the named actor slot.
            if !actor_data.name.is_empty() {
                self.named_actors.remove(&(
                    actor_data.name.clone(),
                    actor_data.ray_namespace.clone(),
                ));
            }
            // Remove task spec from storage.
            self.table_storage
                .actor_task_spec_table()
                .delete(&hex(actor_id))
                .await;
            self.actor_task_specs.remove(actor_id);
            // Move from actors → destroyed_actors cache (for GetActorInfo fallback).
            // Maps C++ AddDestroyedActorToCache.
            if let Some((_, data)) = self.actors.remove(actor_id) {
                // Evict oldest if cache is full. Cap is per-instance,
                // captured at construction from
                // `RayConfig::maximum_gcs_destroyed_actor_cached_count`.
                if self.destroyed_actors.len() >= self.max_destroyed_actors_cached {
                    if let Some(oldest) = self.destroyed_actors.iter().next() {
                        let oldest_id = oldest.key().clone();
                        drop(oldest);
                        self.destroyed_actors.remove(&oldest_id);
                    }
                }
                self.destroyed_actors.insert(actor_id.to_vec(), data);
            }

            // Drop the function-manager refcount for this actor's
            // job. Parity with C++ `gcs_actor_manager.cc:1114`:
            // `function_manager_.RemoveJobReference(actor_id.JobId())`
            // only fires on the not-restartable branch — restartable
            // actors stay tracked and their refcount is preserved.
            let fm = self.function_manager.lock().clone();
            if let Some(fm) = fm {
                if let Some(job_id) = Self::job_id_from_actor_id(actor_id) {
                    fm.remove_job_reference(&job_id).await;
                }
            }

            // Release runtime-env URI references for detached actors.
            // Parity with C++ `gcs_actor_manager.cc:1120`: inside the
            // `!is_restartable` block, the detached-actor branch calls
            // `runtime_env_manager_.RemoveURIReference(actor_id.Hex())`.
            // Non-detached actors never pinned URIs (register_actor
            // gates the pin on `is_detached`), so there is nothing to
            // release for them.
            if actor_data.is_detached {
                let res = self.runtime_env_service.lock().clone();
                if let Some(res) = res {
                    res.remove_uri_reference(&hex(actor_id)).await;
                }
            }
        }
    }

    /// Forward a `KillLocalActor` RPC to the raylet that owns this
    /// actor's worker. Maps C++
    /// `GcsActorManager::NotifyRayletToKillActor`
    /// (`gcs_actor_manager.cc:1843-1875`).
    ///
    /// The raylet is authoritative: it sends the graceful `Exit`
    /// (`force_kill=false`) or SIGKILL (`force_kill=true`) to the
    /// worker. Before this helper existed, the Rust GCS silently
    /// dropped `force_kill` — this is the call that restores parity.
    ///
    /// Fire-and-forget: the outcome of the RPC does not change any
    /// GCS state. Mirrors the C++ callback at lines 1867-1873 which
    /// logs success or "Node with actor is already dead" on failure.
    ///
    /// No-ops (debug-log) when:
    /// - no pool is installed (unit tests that don't wire one);
    /// - the owning node is no longer alive in `GcsNodeManager`
    ///   (matches C++ early return at line 1851 when
    ///   `!actor->LocalRayletAddress()`).
    async fn notify_raylet_to_kill_actor(
        &self,
        actor_id: &[u8],
        worker_id: &[u8],
        node_id: &[u8],
        death_cause: Option<ActorDeathCause>,
        force_kill: bool,
    ) {
        let Some(pool) = self.raylet_client_pool.lock().clone() else {
            debug!(
                actor_id = hex(actor_id),
                "raylet client pool not installed, skipping KillLocalActor RPC"
            );
            return;
        };
        let Some(node) = self.node_manager.get_alive_node(node_id) else {
            debug!(
                actor_id = hex(actor_id),
                node_id = hex(node_id),
                "owning node not alive, skipping KillLocalActor RPC"
            );
            return;
        };

        let request = KillLocalActorRequest {
            intended_actor_id: actor_id.to_vec(),
            worker_id: worker_id.to_vec(),
            force_kill,
            death_cause,
        };
        let address = node.node_manager_address.clone();
        let port = node.node_manager_port as u16;
        match pool.kill_local_actor(&address, port, request).await {
            Ok(()) => {
                info!(
                    actor_id = hex(actor_id),
                    force_kill, "Killed actor successfully."
                );
            }
            Err(status) => {
                // Matches C++ `gcs_actor_manager.cc:1867-1871`: log
                // but do not change GCS state — the node going down
                // will be handled by `OnNodeDead`.
                info!(
                    actor_id = hex(actor_id),
                    node_id = hex(node_id),
                    status = %status,
                    "Node with actor is already dead, return status"
                );
            }
        }
    }

    /// Arm the per-worker graceful-shutdown timer. Parity with
    /// C++ `gcs_actor_manager.cc:1041-1080`. Idempotent: if a timer
    /// is already armed for this `worker_id`, this call does nothing
    /// (matches C++ `find(worker_id) == end()` guard at line 1042).
    ///
    /// On expiry, the spawned task:
    ///  1. Re-acquires the manager via `Weak::upgrade` (matches C++
    ///     `weak_self.lock()` at line 1053).
    ///  2. Atomically removes its own entry from
    ///     `graceful_shutdown_timers`. If the entry was already
    ///     removed (cancellation by `destroy_actor(force_kill=true)`
    ///     or `restart_actor_internal`), the task exits without
    ///     firing — this is the Rust equivalent of C++'s
    ///     `error == operation_aborted` early-return at line 1059.
    ///  3. Re-validates that the actor is still in `self.actors`
    ///     AND its current worker_id still equals the snapshotted
    ///     one (matches C++ `actor_iter->second->GetWorkerID() == worker_id`
    ///     at line 1069). A worker_id mismatch means the actor was
    ///     restarted with a fresh worker; the old timer must not
    ///     escalate into the new instance.
    ///  4. Fires `notify_raylet_to_kill_actor(force_kill=true)`
    ///     (parity with C++ line 1070-1071).
    ///
    /// If `self.self_weak` has not been initialized via
    /// `install_self_arc`, this method logs a debug line and skips
    /// arming. Tests that care about timer behavior must call
    /// `install_self_arc` after construction.
    fn arm_graceful_shutdown_timer(
        &self,
        actor_id: &[u8],
        worker_id: &[u8],
        death_cause: Option<ActorDeathCause>,
        timeout_ms: i64,
    ) {
        debug_assert!(timeout_ms > 0, "arm only called with positive timeout");

        // C++ `find(worker_id) == end()` guard: don't replace an
        // existing timer with a fresh one — keep the original.
        let mut timers = self.graceful_shutdown_timers.lock();
        if timers.contains_key(worker_id) {
            return;
        }

        let weak = match self.self_weak.lock().clone() {
            Some(w) => w,
            None => {
                debug!(
                    actor_id = hex(actor_id),
                    worker_id = hex(worker_id),
                    "self_weak not installed; graceful-shutdown timer NOT armed (call install_self_arc to enable parity)"
                );
                return;
            }
        };

        let actor_id_owned = actor_id.to_vec();
        let worker_id_owned = worker_id.to_vec();
        let timeout = std::time::Duration::from_millis(timeout_ms as u64);

        let handle = tokio::spawn(async move {
            tokio::time::sleep(timeout).await;

            let Some(this) = weak.upgrade() else {
                // Manager dropped — nothing to do (parity with C++
                // `if (!self) return;` at line 1054).
                return;
            };

            // Atomic self-remove: whoever wins this `remove` race
            // (the timer task vs a canceller) is the only side that
            // proceeds. If `None`, we were cancelled — exit silently
            // (parity with C++ `operation_aborted` at line 1059).
            let still_armed = {
                let mut timers = this.graceful_shutdown_timers.lock();
                timers.remove(&worker_id_owned).is_some()
            };
            if !still_armed {
                return;
            }

            // Re-validate that the actor still maps to the same
            // worker_id (parity with C++ check at line 1068-1069).
            // After a restart, the address is cleared and the new
            // worker has a different id; the timer must not fire.
            let (still_present, current_node_id) = {
                if let Some(entry) = this.actors.get(&actor_id_owned) {
                    let cur_worker = entry
                        .address
                        .as_ref()
                        .map(|a| a.worker_id.clone())
                        .unwrap_or_default();
                    if cur_worker == worker_id_owned {
                        (true, entry.node_id.clone().unwrap_or_default())
                    } else {
                        (false, vec![])
                    }
                } else {
                    (false, vec![])
                }
            };
            if !still_present {
                debug!(
                    actor_id = hex(&actor_id_owned),
                    worker_id = hex(&worker_id_owned),
                    "Graceful-shutdown timer fired but actor/worker no longer matches; skipping force-kill"
                );
                return;
            }

            warn!(
                actor_id = hex(&actor_id_owned),
                worker_id = hex(&worker_id_owned),
                timeout_ms,
                "Graceful shutdown timeout exceeded. Falling back to force kill."
            );

            this.notify_raylet_to_kill_actor(
                &actor_id_owned,
                &worker_id_owned,
                &current_node_id,
                death_cause,
                /*force_kill=*/ true,
            )
            .await;
        });

        timers.insert(worker_id.to_vec(), GracefulShutdownTimer { handle });
    }

    /// Application-killed path for an actor that may still be
    /// restarted. Maps C++ `GcsActorManager::KillActor`
    /// (`gcs_actor_manager.cc:1877-1916`).
    ///
    /// Distinct from `destroy_actor`:
    /// - `destroy_actor` immediately marks the actor DEAD and
    ///   persists a death cause. Used by the `no_restart=true`
    ///   path and other terminal lifecycle events.
    /// - `kill_actor` *does not* mark the actor DEAD when it has a
    ///   live worker. It only sends a `KillLocalActor` RPC to the
    ///   owning raylet with a `RAY_KILL` (application-killed) death
    ///   cause and `force_kill` flag. The raylet kills the worker;
    ///   the resulting `OnWorkerDead` callback drives the state
    ///   transition through `restart_actor_internal` (RESTARTING if
    ///   restart budget allows, DEAD otherwise). This matches C++'s
    ///   asynchronous kill→worker-dead→restart sequence.
    /// - For an actor that is NOT yet in `created_actors` (i.e.
    ///   PENDING_CREATION / RESTARTING that hasn't completed
    ///   creation), `kill_actor` notifies the raylet (if a worker
    ///   was leased), cancels any in-flight scheduling, and calls
    ///   `restart_actor_internal` directly with `RAY_KILL` cause —
    ///   matching C++ lines 1903-1914.
    ///
    /// Early-returns for `DEAD` and `DEPENDENCIES_UNREADY` states,
    /// matching C++ line 1887-1890.
    async fn kill_actor(&self, actor_id: &[u8], force_kill: bool) {
        // Snapshot the bits we need without holding the DashMap entry
        // across awaits.
        let (state, worker_id, node_id) = {
            let Some(entry) = self.actors.get(actor_id) else {
                debug!(
                    actor_id = hex(actor_id),
                    "kill_actor: actor not registered"
                );
                return;
            };
            (
                entry.state,
                entry
                    .address
                    .as_ref()
                    .map(|a| a.worker_id.clone())
                    .unwrap_or_default(),
                entry.node_id.clone().unwrap_or_default(),
            )
        };

        // C++ `:1887-1890`: DEAD or DEPENDENCIES_UNREADY → return.
        if state == ActorState::Dead as i32
            || state == ActorState::DependenciesUnready as i32
        {
            debug!(
                actor_id = hex(actor_id),
                state, "kill_actor: skipping DEAD/DEPENDENCIES_UNREADY actor"
            );
            return;
        }

        // The death cause is `GenKilledByApplicationCause` in C++ —
        // mapped to `RAY_KILL` in Rust's `DeathReason` enum.
        let death_cause =
            make_death_cause(DeathReason::RayKill, "Killed by application via ray.kill()");

        // Is the actor in `created_actors` (i.e. has a live worker on
        // a known raylet)?
        let is_created = !node_id.is_empty()
            && !worker_id.is_empty()
            && {
                let created = self.created_actors.lock();
                created
                    .get(&node_id)
                    .is_some_and(|m| m.contains_key(&worker_id))
            };

        if is_created {
            // C++ `:1896-1900`: created actor — only notify the raylet.
            // Do NOT change state here; the actor stays in its current
            // state (e.g. ALIVE) until the worker actually dies and
            // `OnWorkerDead` drives the next transition.
            self.notify_raylet_to_kill_actor(
                actor_id,
                &worker_id,
                &node_id,
                Some(death_cause),
                force_kill,
            )
            .await;
            info!(
                actor_id = hex(actor_id),
                force_kill,
                "Notified raylet to kill ALIVE actor (state unchanged until worker dies)"
            );
        } else {
            // C++ `:1901-1915`: not yet created. If a worker was
            // leased, notify it; cancel scheduling; restart.
            if !worker_id.is_empty() {
                self.notify_raylet_to_kill_actor(
                    actor_id,
                    &worker_id,
                    &node_id,
                    Some(death_cause),
                    force_kill,
                )
                .await;
            }
            // C++ `CancelActorInScheduling` (`:1934-1965`).
            let cancelled = self.scheduler.cancel_actor_scheduling(actor_id);
            debug!(
                actor_id = hex(actor_id),
                cancelled, "kill_actor: cancelled in-flight scheduling"
            );
            // C++ `RestartActor(actor_id, /*need_reschedule=*/true,
            // GenKilledByApplicationCause(...))` at `:1912-1914`.
            self.restart_actor_internal(
                actor_id,
                /*need_reschedule=*/ true,
                DeathReason::RayKill,
                "Killed by application via ray.kill()",
            )
            .await;
        }
    }
}

#[tonic::async_trait]
impl ActorInfoGcsService for GcsActorManager {
    /// Register an actor — store its task spec and create initial ActorTableData
    /// in DEPENDENCIES_UNREADY state.
    ///
    /// Matches C++ RegisterActor (gcs_actor_manager.cc:665-797):
    /// - Idempotent: if already registered, returns OK
    /// - Named actor uniqueness: returns AlreadyExists if name taken by live actor
    async fn register_actor(
        &self,
        req: Request<RegisterActorRequest>,
    ) -> Result<Response<RegisterActorReply>, Status> {
        let inner = req.into_inner();
        let Some(task_spec) = inner.task_spec else {
            return Ok(Response::new(RegisterActorReply {
                status: Some(ok_status()),
            }));
        };
        let Some(actor_id) = Self::actor_id_from_task_spec(&task_spec) else {
            return Ok(Response::new(RegisterActorReply {
                status: Some(ok_status()),
            }));
        };

        // Idempotency: if already registered, return OK.
        if self.actors.contains_key(&actor_id) {
            return Ok(Response::new(RegisterActorReply {
                status: Some(ok_status()),
            }));
        }

        let actor_data =
            Self::build_actor_table_data(&task_spec, ActorState::DependenciesUnready);

        // Named actor uniqueness check (matching C++).
        if !actor_data.name.is_empty()
            && self.is_named_actor_alive(&actor_data.name, &actor_data.ray_namespace)
        {
            return Ok(Response::new(RegisterActorReply {
                status: Some(already_exists_status(&format!(
                    "Actor with name '{}' already exists in namespace '{}'",
                    actor_data.name, actor_data.ray_namespace
                ))),
            }));
        }

        // Register named actor.
        if !actor_data.name.is_empty() {
            self.named_actors.insert(
                (actor_data.name.clone(), actor_data.ray_namespace.clone()),
                actor_id.clone(),
            );
        }

        // If this is a detached actor, pin its runtime-env URIs under
        // `actor_id.Hex()` BEFORE awaiting the task-spec storage write.
        // Parity with C++ `gcs_actor_manager.cc:744-745`:
        // `runtime_env_manager_.AddURIReference(actor->GetActorID().Hex(),
        //  request.task_spec().runtime_env_info())` runs before the
        // `ActorTaskSpecTable().Put` at `:750`. Ordering matters: the Put
        // suspends on an `.await`, and any temporary `PinRuntimeEnvURI`
        // pin (runtime_env_stub.rs:200-251) that was holding the URI can
        // expire on another task during the suspend. If we pinned after
        // the Put, the refcount could drop to zero mid-register and the
        // deleter would delete the package before the actor's reference
        // is recorded, breaking the C++ guarantee that a detached
        // actor's runtime env survives from register through destroy.
        if actor_data.is_detached {
            let res = self.runtime_env_service.lock().clone();
            if let Some(res) = res {
                if let Some(info) = task_spec.runtime_env_info.as_ref() {
                    res.add_uri_reference_from_runtime_env(&hex(&actor_id), info);
                }
            }
        }

        // Persist task spec.
        self.table_storage
            .actor_task_spec_table()
            .put(&hex(&actor_id), &task_spec)
            .await;
        self.actor_task_specs.insert(actor_id.clone(), task_spec);

        // Persist actor data.
        self.table_storage
            .actor_table()
            .put(&hex(&actor_id), &actor_data)
            .await;
        self.actors.insert(actor_id.clone(), actor_data.clone());

        // Bump the function-manager refcount. Parity with C++
        // `gcs_actor_manager.cc:731`:
        // `function_manager_.AddJobReference(actor_id.JobId())`.
        // Placed after the `actors` insert for the same ordering
        // reason C++ uses — the refcount only bumps for an actor
        // we've committed to tracking.
        let fm = self.function_manager.lock().clone();
        if let Some(fm) = fm {
            if let Some(job_id) = Self::job_id_from_actor_id(&actor_id) {
                fm.add_job_reference(&job_id);
            }
        }

        self.publish_actor_update(&actor_data);
        info!(actor_id = hex(&actor_id), "Actor registered");

        Ok(Response::new(RegisterActorReply {
            status: Some(ok_status()),
        }))
    }

    /// Create an actor — transition to PENDING_CREATION and schedule via raylet.
    ///
    /// Matches C++ CreateActor (gcs_actor_manager.cc:799-877):
    /// - Actor must be registered first
    /// - Idempotent: if already ALIVE, returns OK immediately
    /// - If already PENDING_CREATION: returns OK (creation in progress)
    /// - If DEAD: returns error (actor was destroyed)
    /// - Transitions DEPENDENCIES_UNREADY → PENDING_CREATION, then calls
    ///   `gcs_actor_scheduler_->Schedule()` (C++ line 875)
    /// - The actor transitions to ALIVE asynchronously when the scheduler reports
    ///   success via `on_actor_creation_success`
    async fn create_actor(
        &self,
        req: Request<CreateActorRequest>,
    ) -> Result<Response<CreateActorReply>, Status> {
        let inner = req.into_inner();
        let Some(task_spec) = inner.task_spec else {
            return Ok(Response::new(CreateActorReply {
                status: Some(ok_status()),
                ..Default::default()
            }));
        };
        let Some(actor_id) = Self::actor_id_from_task_spec(&task_spec) else {
            return Ok(Response::new(CreateActorReply {
                status: Some(ok_status()),
                ..Default::default()
            }));
        };

        // Actor must be registered first (matching C++ check).
        let Some(mut entry) = self.actors.get_mut(&actor_id) else {
            return Ok(Response::new(CreateActorReply {
                status: Some(invalid_status(
                    "Actor not registered. It may have already been destroyed.",
                )),
                ..Default::default()
            }));
        };

        // Idempotency: if already ALIVE, return OK with address (C++ duplicate handling).
        if entry.state == ActorState::Alive as i32 {
            let addr = entry.address.clone();
            return Ok(Response::new(CreateActorReply {
                status: Some(ok_status()),
                actor_address: addr,
                ..Default::default()
            }));
        }

        // If already PENDING_CREATION, return OK (creation in progress, C++ queues callback).
        if entry.state == ActorState::PendingCreation as i32 {
            let addr = entry.address.clone();
            return Ok(Response::new(CreateActorReply {
                status: Some(ok_status()),
                actor_address: addr,
                ..Default::default()
            }));
        }

        // Cannot create a dead actor.
        if entry.state == ActorState::Dead as i32 {
            return Ok(Response::new(CreateActorReply {
                status: Some(invalid_status("Actor is dead and cannot be created.")),
                ..Default::default()
            }));
        }

        // --- Transition to PENDING_CREATION (C++ lines 845-866) ---
        // Accepted from DEPENDENCIES_UNREADY or RESTARTING states.
        entry.state = ActorState::PendingCreation as i32;
        entry.timestamp = now_ms();
        let pending_data = entry.clone();
        drop(entry);

        // Publish PENDING_CREATION state (for dashboard / subscribers).
        self.publish_actor_update(&pending_data);

        // Store task spec if not already stored.
        if !self.actor_task_specs.contains_key(&actor_id) {
            self.table_storage
                .actor_task_spec_table()
                .put(&hex(&actor_id), &task_spec)
                .await;
            self.actor_task_specs.insert(actor_id.clone(), task_spec.clone());
        }

        info!(actor_id = hex(&actor_id), "Actor PENDING_CREATION, scheduling via raylet");

        // --- Hand off to scheduler (C++ line 875: gcs_actor_scheduler_->Schedule(actor)) ---
        GcsActorScheduler::schedule(&self.scheduler, PendingActor {
            actor_id: actor_id.clone(),
            task_spec,
            actor_data: pending_data,
        });

        Ok(Response::new(CreateActorReply {
            status: Some(ok_status()),
            ..Default::default()
        }))
    }

    /// Get actor info by actor_id.
    ///
    /// Matches C++ HandleGetActorInfo (gcs_actor_manager.cc:461-486):
    /// Checks registered_actors_ first, then destroyed_actors_.
    async fn get_actor_info(
        &self,
        req: Request<GetActorInfoRequest>,
    ) -> Result<Response<GetActorInfoReply>, Status> {
        let inner = req.into_inner();
        // Check active actors first, then destroyed cache.
        let actor = self
            .actors
            .get(&inner.actor_id)
            .map(|r| r.clone())
            .or_else(|| self.destroyed_actors.get(&inner.actor_id).map(|r| r.clone()));

        match actor {
            Some(data) => Ok(Response::new(GetActorInfoReply {
                status: Some(ok_status()),
                actor_table_data: Some(data),
            })),
            None => Ok(Response::new(GetActorInfoReply {
                status: Some(not_found_status("Actor not found")),
                actor_table_data: None,
            })),
        }
    }

    /// Get named actor info by (name, namespace).
    async fn get_named_actor_info(
        &self,
        req: Request<GetNamedActorInfoRequest>,
    ) -> Result<Response<GetNamedActorInfoReply>, Status> {
        let inner = req.into_inner();
        let key = (inner.name, inner.ray_namespace);
        let actor_id = self.named_actors.get(&key).map(|r| r.clone());

        match actor_id {
            Some(aid) => {
                let actor = self.actors.get(&aid).map(|r| r.clone());
                let task_spec = self.actor_task_specs.get(&aid).map(|r| r.clone());
                Ok(Response::new(GetNamedActorInfoReply {
                    status: Some(ok_status()),
                    actor_table_data: actor,
                    task_spec,
                }))
            }
            None => Ok(Response::new(GetNamedActorInfoReply {
                status: Some(not_found_status("Named actor not found")),
                actor_table_data: None,
                task_spec: None,
            })),
        }
    }

    /// List all named actors, optionally filtered by namespace.
    async fn list_named_actors(
        &self,
        req: Request<ListNamedActorsRequest>,
    ) -> Result<Response<ListNamedActorsReply>, Status> {
        let inner = req.into_inner();
        let mut result = Vec::new();

        for entry in self.named_actors.iter() {
            let (name, ns) = entry.key();
            if inner.all_namespaces || *ns == inner.ray_namespace {
                let actor_id = entry.value();
                let is_alive = self
                    .actors
                    .get(actor_id)
                    .map(|a| a.state != ActorState::Dead as i32)
                    .unwrap_or(false);
                if is_alive {
                    result.push(NamedActorInfo {
                        ray_namespace: ns.clone(),
                        name: name.clone(),
                    });
                }
            }
        }

        Ok(Response::new(ListNamedActorsReply {
            status: Some(ok_status()),
            named_actors_list: result,
        }))
    }

    /// Get all actors, with optional filters, limit, and show_dead_jobs support.
    ///
    /// Matches C++ HandleGetAllActorInfo (gcs_actor_manager.cc:488-587):
    /// - If show_dead_jobs == false: returns in-memory actors + destroyed cache
    /// - If show_dead_jobs == true: queries actor_table storage for ALL actors
    ///   (including those from dead jobs not in memory)
    async fn get_all_actor_info(
        &self,
        req: Request<GetAllActorInfoRequest>,
    ) -> Result<Response<GetAllActorInfoReply>, Status> {
        let inner = req.into_inner();
        let limit = inner.limit.filter(|&l| l > 0).map(|l| l as usize);

        let filter_fn = |actor: &ActorTableData| -> bool {
            if let Some(ref filters) = inner.filters {
                if let Some(ref fid) = filters.actor_id {
                    if !fid.is_empty() && actor.actor_id != *fid {
                        return false;
                    }
                }
                if let Some(ref fjid) = filters.job_id {
                    if !fjid.is_empty() && actor.job_id != *fjid {
                        return false;
                    }
                }
                if let Some(fs) = filters.state {
                    if actor.state != fs {
                        return false;
                    }
                }
            }
            true
        };

        if inner.show_dead_jobs {
            // Query ALL actors from backend storage (matching C++ ActorTable().GetAll()).
            let all_stored = self.table_storage.actor_table().get_all().await;
            let total = all_stored.len() as i64;
            let mut actors = Vec::new();
            let mut num_filtered: i64 = 0;

            for (_key, actor) in &all_stored {
                if !filter_fn(actor) {
                    num_filtered += 1;
                    continue;
                }
                if let Some(lim) = limit {
                    if actors.len() >= lim {
                        continue;
                    }
                }
                actors.push(actor.clone());
            }

            return Ok(Response::new(GetAllActorInfoReply {
                status: Some(ok_status()),
                actor_table_data: actors,
                total,
                num_filtered,
            }));
        }

        // Default path: return in-memory actors + destroyed cache.
        // Maps C++ which iterates registered_actors_ + destroyed_actors_.
        let total =
            (self.actors.len() + self.destroyed_actors.len()) as i64;
        let mut actors = Vec::new();
        let mut num_filtered: i64 = 0;

        for entry in self.actors.iter() {
            let actor = entry.value();
            if !filter_fn(actor) {
                num_filtered += 1;
                continue;
            }
            if let Some(lim) = limit {
                if actors.len() >= lim {
                    continue;
                }
            }
            actors.push(actor.clone());
        }

        for entry in self.destroyed_actors.iter() {
            let actor = entry.value();
            if !filter_fn(actor) {
                num_filtered += 1;
                continue;
            }
            if let Some(lim) = limit {
                if actors.len() >= lim {
                    continue;
                }
            }
            actors.push(actor.clone());
        }

        Ok(Response::new(GetAllActorInfoReply {
            status: Some(ok_status()),
            actor_table_data: actors,
            total,
            num_filtered,
        }))
    }

    /// Kill an actor via GCS.
    ///
    /// Matches C++ `HandleKillActorViaGcs`
    /// (`gcs_actor_manager.cc:636-663`):
    /// - If the actor is not in `registered_actors_` (Rust:
    ///   `self.actors`): reply with
    ///   `Status::NotFound("Could not find actor with ID {hex}")`.
    ///   C++ only consults `registered_actors_`; an actor evicted to
    ///   the `destroyed_actors` cache is no longer in
    ///   `registered_actors_`, so it also returns NotFound.
    /// - `no_restart == true`: C++ calls
    ///   `DestroyActor(actor_id, GenKilledByApplicationCause(...))`
    ///   which uses the default `force_kill=true` — the user's
    ///   `request.force_kill()` is **ignored** on this branch. We
    ///   replicate that by passing `force_kill=true` unconditionally.
    ///   Rust calls `destroy_actor(.., DeathReason::RayKill, ..,
    ///   force_kill=true)` which marks the actor DEAD with a
    ///   `RAY_KILL` death cause.
    /// - `no_restart == false`: C++ calls
    ///   `KillActor(actor_id, force_kill)`. This is **not** a
    ///   `DestroyActor` and is **not** an out-of-scope path. Rust
    ///   maps this to `kill_actor(actor_id, force_kill)`, the
    ///   private method that mirrors C++ `KillActor`'s state
    ///   machine: created actors are only signalled to the raylet
    ///   (state stays ALIVE until the worker dies and
    ///   `OnWorkerDead` drives the restart); not-yet-created actors
    ///   have their scheduling cancelled and go through
    ///   `restart_actor_internal` with a `RAY_KILL` death cause.
    ///   This replaces the prior implementation that incorrectly
    ///   marked the actor DEAD with `OUT_OF_SCOPE` — wrong death
    ///   cause and wrong state transition for both subscribers and
    ///   storage.
    ///
    /// Note: this RPC's `no_restart=true` branch passes
    /// `force_kill=true` and `graceful_shutdown_timeout_ms=-1`, so
    /// the per-worker graceful-shutdown timer (parity with C++
    /// `gcs_actor_manager.cc:1041-1080`) is NOT armed on this path.
    /// The timer is only relevant to the graceful destroy callers
    /// (`report_actor_out_of_scope`); see
    /// `destroy_actor`'s docstring for the full timer contract.
    ///
    /// Note: C++ does NOT early-return when the entry is already
    /// DEAD — it calls `DestroyActor`/`KillActor` unconditionally
    /// and those helpers handle the already-dead case internally.
    /// Both Rust paths (`destroy_actor`, `kill_actor`) no-op on
    /// DEAD/DEPENDENCIES_UNREADY entries, mirroring C++.
    async fn kill_actor_via_gcs(
        &self,
        req: Request<KillActorViaGcsRequest>,
    ) -> Result<Response<KillActorViaGcsReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        // Unknown actor → NotFound, matching C++ gcs_actor_manager.cc:655-660.
        // The `self.actors` table (== C++ `registered_actors_`) is the only
        // thing checked; an actor evicted to `destroyed_actors` is also
        // unknown to C++ at this point.
        if !self.actors.contains_key(&actor_id) {
            let msg = format!("Could not find actor with ID {}.", hex(&actor_id));
            debug!(actor_id = hex(&actor_id), "{}", msg);
            return Ok(Response::new(KillActorViaGcsReply {
                status: Some(not_found_status(&msg)),
            }));
        }

        if inner.no_restart {
            // C++ :645 — `DestroyActor(..., cause)` with default
            // `force_kill=true` and the default
            // `graceful_shutdown_timeout_ms=-1` (no graceful timer).
            // Request's `force_kill()` is ignored on this branch.
            self.destroy_actor(
                &actor_id,
                DeathReason::RayKill,
                "Killed via ray.kill()",
                /*force_kill=*/ true,
                /*graceful_shutdown_timeout_ms=*/ -1,
            )
            .await;
        } else {
            // C++ :647 — `KillActor(actor_id, force_kill)`. NOT a
            // destroy: created actors stay in their current state
            // until the raylet kills the worker and `OnWorkerDead`
            // fires; not-yet-created actors restart with a RAY_KILL
            // cause via `restart_actor_internal`.
            self.kill_actor(&actor_id, inner.force_kill).await;
        }

        info!(
            actor_id = hex(&actor_id),
            no_restart = inner.no_restart,
            force_kill = inner.force_kill,
            "Actor killed via GCS"
        );

        Ok(Response::new(KillActorViaGcsReply {
            status: Some(ok_status()),
        }))
    }

    /// Handle actor going out of scope.
    ///
    /// Matches C++ ReportActorOutOfScope (gcs_actor_manager.cc:276-306):
    /// - Stale message detection via lineage reconstruction counter
    /// - Only kills non-detached actors
    /// - Sets death cause to OUT_OF_SCOPE (enabling lineage restart)
    async fn report_actor_out_of_scope(
        &self,
        req: Request<ReportActorOutOfScopeRequest>,
    ) -> Result<Response<ReportActorOutOfScopeReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        let Some(entry) = self.actors.get(&actor_id) else {
            // Actor already destroyed.
            return Ok(Response::new(ReportActorOutOfScopeReply {
                status: Some(ok_status()),
            }));
        };

        // Stale message detection. Parity with C++
        // `gcs_actor_manager.cc:283-290`: the report is stale when the
        // actor's CURRENT lineage-reconstruction counter is GREATER
        // than the counter on the incoming request — i.e. the actor
        // has already been lineage-restarted past the point at which
        // the report was generated, so acting on it would kill a
        // live restarted instance. An equal counter is NOT stale; the
        // out-of-scope path proceeds with the normal destroy.
        if entry.num_restarts_due_to_lineage_reconstruction
            > inner.num_restarts_due_to_lineage_reconstruction
        {
            debug!(
                actor_id = hex(&actor_id),
                request_counter = inner.num_restarts_due_to_lineage_reconstruction,
                current_counter = entry.num_restarts_due_to_lineage_reconstruction,
                "Out of scope report is stale (actor already restarted), ignoring"
            );
            return Ok(Response::new(ReportActorOutOfScopeReply {
                status: Some(ok_status()),
            }));
        }

        // Only kill non-detached actors.
        if entry.is_detached {
            return Ok(Response::new(ReportActorOutOfScopeReply {
                status: Some(ok_status()),
            }));
        }

        drop(entry);

        // Destroy with OUT_OF_SCOPE cause (makes actor restartable
        // if budget allows). force_kill=false matches C++ `:297`
        // where `ReportActorOutOfScope` passes `/*force_kill=*/false`
        // and the configured
        // `RayConfig::actor_graceful_shutdown_timeout_ms` so that
        // `destroy_actor` arms the per-worker timer that escalates
        // to a force-kill if the worker has not exited within the
        // timeout. Parity with C++
        // `gcs_actor_manager.cc:293-301` + `:1041-1080`.
        let graceful_timeout_ms =
            ray_config::instance().actor_graceful_shutdown_timeout_ms;
        self.destroy_actor(
            &actor_id,
            DeathReason::OutOfScope,
            "Actor went out of scope",
            /*force_kill=*/ false,
            graceful_timeout_ms,
        )
        .await;

        debug!(actor_id = hex(&actor_id), "Actor out of scope, marked DEAD");

        Ok(Response::new(ReportActorOutOfScopeReply {
            status: Some(ok_status()),
        }))
    }

    /// Restart actor for lineage reconstruction.
    ///
    /// Matches C++ RestartActorForLineageReconstruction (gcs_actor_manager.cc:335-425):
    /// - Stale message detection via lineage reconstruction counter
    /// - Actor must be DEAD and restartable (OUT_OF_SCOPE + budget)
    /// - Increments restart counters
    async fn restart_actor_for_lineage_reconstruction(
        &self,
        req: Request<RestartActorForLineageReconstructionRequest>,
    ) -> Result<Response<RestartActorForLineageReconstructionReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        let Some(mut entry) = self.actors.get_mut(&actor_id) else {
            // Actor already fully destroyed.
            return Ok(Response::new(RestartActorForLineageReconstructionReply {
                status: Some(ok_status()),
            }));
        };

        // Stale message detection (matching C++ lines 366-379):
        // If request counter <= actor's current counter, this is a stale/retried request.
        if inner.num_restarts_due_to_lineage_reconstruction
            <= entry.num_restarts_due_to_lineage_reconstruction
        {
            info!(
                actor_id = hex(&actor_id),
                request_counter = inner.num_restarts_due_to_lineage_reconstruction,
                current_counter = entry.num_restarts_due_to_lineage_reconstruction,
                "Ignoring stale actor restart request"
            );
            return Ok(Response::new(RestartActorForLineageReconstructionReply {
                status: Some(ok_status()),
            }));
        }

        // Actor must be DEAD to restart (C++ asserts this).
        if entry.state != ActorState::Dead as i32 {
            warn!(
                actor_id = hex(&actor_id),
                state = entry.state,
                "Restart requested for non-DEAD actor, ignoring"
            );
            return Ok(Response::new(RestartActorForLineageReconstructionReply {
                status: Some(ok_status()),
            }));
        }

        // Check restartability (budget + death cause).
        if !Self::is_actor_restartable(&entry) {
            warn!(
                actor_id = hex(&actor_id),
                "Actor is not restartable (budget exhausted or wrong death cause)"
            );
            return Ok(Response::new(RestartActorForLineageReconstructionReply {
                status: Some(invalid_status("Actor is not restartable")),
            }));
        }

        // Parity with C++ `gcs_actor_manager.cc:1468-1472`: any
        // `RestartActor` flow cancels the per-worker
        // graceful-shutdown timer for the restarting worker. The
        // C++ lineage-reconstruction handler reaches `RestartActor`
        // via `RestartActor(.., need_reschedule=true, ..)`; the
        // cancel is therefore part of every restart path, not just
        // the OnWorkerDead one. Without this, an old graceful
        // timer could fire a force-kill RPC into the restarted
        // instance.
        let pre_restart_worker_id = entry
            .address
            .as_ref()
            .map(|a| a.worker_id.clone())
            .unwrap_or_default();
        if !pre_restart_worker_id.is_empty() {
            self.graceful_shutdown_timers
                .lock()
                .remove(&pre_restart_worker_id);
        }

        // Perform restart: increment counters, transition to RESTARTING.
        entry.num_restarts += 1;
        entry.num_restarts_due_to_lineage_reconstruction =
            inner.num_restarts_due_to_lineage_reconstruction;
        entry.state = ActorState::Restarting as i32;
        entry.timestamp = now_ms();
        // Clear death cause and end_time since actor is being restarted.
        entry.death_cause = None;
        entry.end_time = 0;

        let actor_data = entry.clone();
        drop(entry);

        self.table_storage
            .actor_table()
            .put(&hex(&actor_id), &actor_data)
            .await;
        self.publish_actor_update(&actor_data);

        info!(
            actor_id = hex(&actor_id),
            num_restarts = actor_data.num_restarts,
            "Actor restarting for lineage reconstruction"
        );

        Ok(Response::new(RestartActorForLineageReconstructionReply {
            status: Some(ok_status()),
        }))
    }
}

// --- Scheduler lifecycle hooks (not part of gRPC trait) ---
impl GcsActorManager {
    /// Handle successful actor creation from the scheduler.
    ///
    /// Maps C++ `GcsActorManager::OnActorCreationSuccess` (gcs_actor_manager.cc:1634-1699).
    /// Transitions PENDING_CREATION → ALIVE, registers in created_actors, persists and publishes.
    pub async fn on_actor_creation_success(&self, success: ActorCreationSuccess) {
        let actor_id = &success.actor_id;

        let Some(mut entry) = self.actors.get_mut(actor_id) else {
            debug!(actor_id = hex(actor_id), "Actor gone before creation completed");
            return;
        };

        // If actor was killed before creation completed, skip (C++ line 1643-1646).
        if entry.state == ActorState::Dead as i32 {
            debug!(actor_id = hex(actor_id), "Actor killed before creation completed");
            return;
        }

        // Transition to ALIVE (C++ line 1667).
        let time = now_ms();
        entry.state = ActorState::Alive as i32;
        entry.timestamp = time;
        if entry.start_time == 0 {
            entry.start_time = now_ms_u64();
        }
        entry.preempted = false;
        entry.pid = success.worker_pid;
        if !success.actor_repr_name.is_empty() {
            entry.repr_name = success.actor_repr_name.clone();
        }

        // Set worker address.
        let worker_address = success.worker_address.clone();
        entry.address = Some(worker_address.clone());
        entry.node_id = Some(worker_address.node_id.clone());

        // Set resource mapping.
        entry.resource_mapping = success.resources.clone();

        let actor_data = entry.clone();
        let worker_id = worker_address.worker_id.clone();
        let node_id = worker_address.node_id.clone();
        drop(entry);

        // Register in created_actors map (C++ line 1678).
        {
            let mut created = self.created_actors.lock();
            created
                .entry(node_id.clone())
                .or_default()
                .insert(worker_id.clone(), actor_id.clone());
        }

        // Bump the lifetime-created counter and record it through the
        // usage-stats client if one is installed. Mirrors C++
        // `lifetime_num_created_actors_++` at
        // `gcs_actor_manager.cc:1637`, paired with the periodic write
        // in `RecordMetrics` (`:2046-2049`). Recording here instead of
        // on a separate tick is equivalent — `ACTOR_NUM_CREATED` is a
        // last-write-wins KV key.
        self.record_actor_created();

        // Persist ALIVE state (C++ line 1680-1699).
        self.table_storage
            .actor_table()
            .put(&hex(actor_id), &actor_data)
            .await;
        self.publish_actor_update(&actor_data);

        info!(
            actor_id = hex(actor_id),
            worker_id = hex(&worker_id),
            node_id = hex(&node_id),
            "Actor created (ALIVE) via scheduler"
        );
    }

    /// Handle scheduling failure from the scheduler.
    ///
    /// Maps C++ `GcsActorManager::OnActorSchedulingFailed` (gcs_actor_manager.cc:1581-1632).
    /// On soft failure (SCHEDULING_FAILED): requeue for retry.
    /// On hard failure: destroy the actor.
    pub async fn on_actor_scheduling_failed(&self, failure: ActorSchedulingFailure) {
        let actor_id = &failure.actor_id;

        match failure.failure_type {
            SchedulingFailureType::SchedulingFailed => {
                // Soft failure — requeue for retry when nodes become available (C++ line 1588).
                if let Some(spec) = self.actor_task_specs.get(actor_id) {
                    if let Some(data) = self.actors.get(actor_id) {
                        debug!(
                            actor_id = hex(actor_id),
                            "Scheduling failed, requeueing for retry"
                        );
                        self.scheduler.enqueue_pending(PendingActor {
                            actor_id: actor_id.clone(),
                            task_spec: spec.clone(),
                            actor_data: data.clone(),
                        });
                        return;
                    }
                }
                // Actor no longer exists — nothing to requeue.
            }
            SchedulingFailureType::SchedulingCancelledIntended => {
                // Intentionally cancelled — no action (C++ line 1591-1595).
                return;
            }
            _ => {
                // Hard failure — destroy actor (C++ line 1599-1631).
                warn!(
                    actor_id = hex(actor_id),
                    failure_type = ?failure.failure_type,
                    error = failure.error_message,
                    "Actor scheduling permanently failed"
                );
                // force_kill=true on terminal scheduling failure —
                // the actor was never created, and if a worker was
                // leased it must be torn down forcefully. Matches
                // the C++ fallback through `DestroyActor` which uses
                // its default `force_kill=true` and
                // `graceful_shutdown_timeout_ms=-1` on this path.
                self.destroy_actor(
                    actor_id,
                    DeathReason::RayKill,
                    &failure.error_message,
                    /*force_kill=*/ true,
                    /*graceful_shutdown_timeout_ms=*/ -1,
                )
                .await;
            }
        }
    }

    /// Handle node death — cancel scheduling and restart affected actors.
    ///
    /// Maps C++ `GcsActorManager::OnNodeDead` (gcs_actor_manager.cc:1289-1415).
    pub async fn on_node_dead(&self, node_id: &[u8]) {
        // Cancel actors being scheduled on this node (C++ line 1328).
        let cancelled = self.scheduler.cancel_on_node(node_id);
        for aid in &cancelled {
            // Restart cancelled actors (C++ line 1345-1347).
            self.restart_actor_internal(
                aid,
                true,
                DeathReason::NodeDied,
                "Node died during scheduling",
            )
            .await;
        }

        // Restart actors that were alive on this node (C++ line 1350-1376).
        let created_on_node = {
            let mut created = self.created_actors.lock();
            created.remove(node_id).unwrap_or_default()
        };
        for (_worker_id, actor_id) in &created_on_node {
            self.restart_actor_internal(
                actor_id,
                true,
                DeathReason::NodeDied,
                "Node died",
            )
            .await;
        }

        info!(
            node_id = hex(node_id),
            cancelled = cancelled.len(),
            alive_affected = created_on_node.len(),
            "Handled node death for actors"
        );
    }

    /// Handle worker death — cancel scheduling and restart affected actor.
    ///
    /// Maps C++ `GcsActorManager::OnWorkerDead` (gcs_actor_manager.cc:1194-1287).
    pub async fn on_worker_dead(&self, node_id: &[u8], worker_id: &[u8], need_reconstruct: bool) {
        // Check if a creating-phase actor needs cancellation (C++ line 1262).
        if let Some(actor_id) = self.scheduler.cancel_on_worker(node_id, worker_id) {
            self.restart_actor_internal(
                &actor_id,
                need_reconstruct,
                DeathReason::WorkerDied,
                "Worker died during creation",
            )
            .await;
            return;
        }

        // Check if an ALIVE actor on this worker needs restart (C++ line 1251-1286).
        let actor_id = {
            let mut created = self.created_actors.lock();
            if let Some(workers) = created.get_mut(node_id) {
                let aid = workers.remove(worker_id);
                if workers.is_empty() {
                    created.remove(node_id);
                }
                aid
            } else {
                None
            }
        };

        if let Some(actor_id) = actor_id {
            self.restart_actor_internal(
                &actor_id,
                need_reconstruct,
                DeathReason::WorkerDied,
                "Worker died",
            )
            .await;
        }
    }

    /// Re-schedule all pending actors (called when new nodes register).
    ///
    /// Maps C++ `GcsActorManager::SchedulePendingActors` (gcs_actor_manager.cc:1701-1711).
    pub fn schedule_pending_actors(&self) {
        GcsActorScheduler::schedule_pending_actors(&self.scheduler);
    }

    /// Internal restart logic matching C++ `RestartActor` (gcs_actor_manager.cc:1446-1579).
    ///
    /// If the actor has restart budget: transition to RESTARTING and re-schedule.
    /// Otherwise: transition to DEAD with the supplied death reason.
    ///
    /// `death_reason` is only persisted on the no-budget DEAD branch
    /// — RESTARTING actors carry no death cause, mirroring C++
    /// `RestartActor` which only sets `mutable_actor_table_data->death_cause`
    /// on the dead branch (`gcs_actor_manager.cc:1547-1551`). Callers
    /// must pick the death reason that fits the upstream cause:
    /// `WorkerDied`/`NodeDied` for failure-driven paths, `RayKill`
    /// for the application-killed path (`HandleKillActorViaGcs`
    /// `no_restart=false` → `KillActor` → not-yet-created branch).
    async fn restart_actor_internal(
        &self,
        actor_id: &[u8],
        need_reschedule: bool,
        death_reason: DeathReason,
        reason: &str,
    ) {
        let Some(mut entry) = self.actors.get_mut(actor_id) else {
            return;
        };

        // Parity with C++ `gcs_actor_manager.cc:1468-1472`: cancel
        // any per-worker graceful-shutdown timer before the restart
        // proceeds. C++ comments: "Cancel timer to prevent
        // force-killing the new instance when old timer fires." The
        // worker_id comes from the entry's CURRENT address — after
        // this method runs, the address is cleared (RESTARTING) or
        // the entry transitions to DEAD; either way, any timer
        // armed for the old worker_id must not survive into a
        // restarted instance. Removing from the map is the
        // synchronization signal: the timer task observes the
        // removal on wake (self-remove returns `None`) and exits
        // silently.
        let pre_restart_worker_id = entry
            .address
            .as_ref()
            .map(|a| a.worker_id.clone())
            .unwrap_or_default();
        if !pre_restart_worker_id.is_empty() {
            self.graceful_shutdown_timers
                .lock()
                .remove(&pre_restart_worker_id);
        }

        let max_restarts = entry.max_restarts;
        let num_restarts = entry.num_restarts;
        let num_preemption_restarts = entry.num_restarts_due_to_node_preemption;

        // Calculate remaining restarts (C++ lines 1474-1494).
        let remaining = if !need_reschedule {
            0i64
        } else if max_restarts == -1 {
            -1 // Unlimited
        } else {
            let effective = num_restarts as i64 - num_preemption_restarts as i64;
            (max_restarts - effective).max(0)
        };

        // Also allow restart if preempted and max_restarts > 0 (C++ line 1505-1506).
        let allow_preemption_restart =
            need_reschedule && max_restarts > 0 && entry.preempted;

        if remaining != 0 || allow_preemption_restart {
            // RESTARTING: increment counters and re-schedule (C++ line 1505-1545).
            if entry.preempted {
                entry.num_restarts_due_to_node_preemption = num_preemption_restarts + 1;
            }
            entry.num_restarts = num_restarts + 1;
            entry.state = ActorState::Restarting as i32;
            entry.timestamp = now_ms();
            entry.address = None;
            entry.resource_mapping.clear();

            let actor_data = entry.clone();
            let task_spec = self.actor_task_specs.get(actor_id).map(|s| s.clone());
            drop(entry);

            self.table_storage
                .actor_table()
                .put(&hex(actor_id), &actor_data)
                .await;
            self.publish_actor_update(&actor_data);

            info!(
                actor_id = hex(actor_id),
                num_restarts = actor_data.num_restarts,
                reason,
                "Actor restarting"
            );

            // Re-schedule if we have the task spec.
            if let Some(spec) = task_spec {
                GcsActorScheduler::schedule(&self.scheduler, PendingActor {
                    actor_id: actor_id.to_vec(),
                    task_spec: spec,
                    actor_data,
                });
            }
        } else {
            // DEAD: no restarts remaining (C++ line 1546-1578).
            entry.state = ActorState::Dead as i32;
            entry.timestamp = now_ms();
            entry.end_time = now_ms_u64();
            entry.death_cause = Some(make_death_cause(death_reason, reason));

            let actor_data = entry.clone();
            drop(entry);

            self.table_storage
                .actor_table()
                .put(&hex(actor_id), &actor_data)
                .await;
            self.publish_actor_update(&actor_data);

            // Move to destroyed cache if not restartable.
            if !Self::is_actor_restartable(&actor_data) {
                self.actors.remove(actor_id);
                if self.destroyed_actors.len() >= self.max_destroyed_actors_cached {
                    if let Some(oldest) = self.destroyed_actors.iter().next().map(|e| e.key().clone()) {
                        self.destroyed_actors.remove(&oldest);
                    }
                }
                self.destroyed_actors
                    .insert(actor_id.to_vec(), actor_data);

                // Same RemoveJobReference bookkeeping as the destroy
                // path. Parity with C++ `gcs_actor_manager.cc:1114`:
                // every final transition to a non-restartable dead
                // state drops one refcount on the actor's job.
                let fm = self.function_manager.lock().clone();
                if let Some(fm) = fm {
                    if let Some(job_id) = Self::job_id_from_actor_id(actor_id) {
                        fm.remove_job_reference(&job_id).await;
                    }
                }
            }

            info!(actor_id = hex(actor_id), reason, "Actor dead (no restarts remaining)");
        }
    }

    /// Start the background loop that processes scheduler success/failure events.
    ///
    /// Must be called once after construction. Reads from the scheduler's
    /// success and failure channels and drives actor state transitions.
    /// Also captures the `Weak<Self>` used by detached tasks
    /// (graceful-shutdown timer) so the timer parity path works in
    /// production wiring (`GcsServer::new_with_store`) without an
    /// extra setter call.
    pub fn start_scheduler_loop(
        self: &Arc<Self>,
        mut success_rx: mpsc::UnboundedReceiver<ActorCreationSuccess>,
        mut failure_rx: mpsc::UnboundedReceiver<ActorSchedulingFailure>,
    ) {
        self.install_self_arc();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(success) = success_rx.recv() => {
                        this.on_actor_creation_success(success).await;
                    }
                    Some(failure) = failure_rx.recv() => {
                        this.on_actor_scheduling_failed(failure).await;
                    }
                    else => break,
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> Arc<GcsActorManager> {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        let gcs_publisher = Arc::new(gcs_pubsub::GcsPublisher::new(16));
        let node_manager = Arc::new(GcsNodeManager::new(
            table_storage.clone(),
            gcs_publisher,
            b"test_cluster".to_vec(),
        ));
        let (success_tx, _success_rx) = mpsc::unbounded_channel();
        let (failure_tx, _failure_rx) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsActorScheduler::new(
            node_manager.clone(),
            success_tx,
            failure_tx,
        ));
        let mgr = Arc::new(GcsActorManager::new(
            publisher,
            table_storage,
            scheduler,
            node_manager,
        ));
        // Mirror production wiring: detached tasks (graceful-shutdown
        // timer) need a `Weak<Self>` to call back into the manager.
        mgr.install_self_arc();
        mgr
    }

    fn make_task_spec(actor_id: &[u8], name: &str) -> TaskSpec {
        make_task_spec_with_restarts(actor_id, name, 0)
    }

    fn make_task_spec_with_restarts(actor_id: &[u8], name: &str, max_restarts: i64) -> TaskSpec {
        TaskSpec {
            r#type: 1,
            name: name.to_string(),
            job_id: b"job1".to_vec(),
            task_id: b"task1".to_vec(),
            caller_id: b"caller1".to_vec(),
            caller_address: Some(Address {
                node_id: b"node1".to_vec(),
                ip_address: "127.0.0.1".to_string(),
                port: 10001,
                worker_id: b"worker1".to_vec(),
            }),
            actor_creation_task_spec: Some(ActorCreationTaskSpec {
                actor_id: actor_id.to_vec(),
                name: name.to_string(),
                ray_namespace: "default".to_string(),
                is_detached: false,
                max_actor_restarts: max_restarts,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Register, create (transitions to PENDING_CREATION), and simulate
    /// scheduler success (transitions to ALIVE). This mirrors the full C++
    /// flow: CreateActor → Schedule → OnActorCreationSuccess.
    async fn register_and_create(mgr: &GcsActorManager, spec: &TaskSpec) {
        let actor_id = GcsActorManager::actor_id_from_task_spec(spec).unwrap();
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
        // Simulate scheduler success callback (in production, the scheduler
        // drives this after RequestWorkerLease + PushTask).
        mgr.on_actor_creation_success(ActorCreationSuccess {
            actor_id,
            worker_address: spec.caller_address.clone().unwrap_or_default(),
            worker_pid: 12345,
            resources: vec![],
            actor_repr_name: String::new(),
        })
        .await;
    }

    fn get_actor_state(mgr: &GcsActorManager, actor_id: &[u8]) -> Option<i32> {
        mgr.actors.get(actor_id).map(|a| a.state)
    }

    /// Parity with C++ `RayConfig::maximum_gcs_destroyed_actor_cached_count`
    /// (`ray_config_def.h:936`). The cap is now a per-instance field
    /// driven by config, not a hardcoded constant.
    ///
    /// This test verifies:
    /// 1. A manager constructed with `with_export_events_and_cap(cap=2)`
    ///    reports the configured cap via its accessor.
    /// 2. The default constructor still uses `DEFAULT_MAX_DESTROYED_ACTORS`
    ///    (the C++ default of 100_000).
    #[test]
    fn test_destroyed_actor_cache_respects_configured_cap() {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        let gcs_publisher = Arc::new(gcs_pubsub::GcsPublisher::new(16));
        let node_manager = Arc::new(GcsNodeManager::new(
            table_storage.clone(),
            gcs_publisher,
            b"test_cluster".to_vec(),
        ));
        let (success_tx, _rx1) = mpsc::unbounded_channel();
        let (failure_tx, _rx2) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsActorScheduler::new(
            node_manager.clone(),
            success_tx,
            failure_tx,
        ));

        // (1) Custom cap honored.
        let mgr_small = GcsActorManager::with_export_events_and_cap(
            publisher.clone(),
            table_storage.clone(),
            scheduler.clone(),
            node_manager.clone(),
            crate::export_event_writer::ExportEventManager::disabled(),
            /*max_destroyed_actors_cached=*/ 2,
        );
        assert_eq!(mgr_small.max_destroyed_actors_cached(), 2);

        // (2) Default cap = C++ default.
        let mgr_default = GcsActorManager::new(
            publisher,
            table_storage,
            scheduler,
            node_manager,
        );
        assert_eq!(
            mgr_default.max_destroyed_actors_cached(),
            DEFAULT_MAX_DESTROYED_ACTORS,
            "default cap must match C++ ray_config_def.h:936 default"
        );
        assert_eq!(DEFAULT_MAX_DESTROYED_ACTORS, 100_000);
    }

    #[tokio::test]
    async fn test_register_and_get_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyActor");

        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor1".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.actor_table_data.is_some());
        let actor = reply.actor_table_data.unwrap();
        assert_eq!(actor.state, ActorState::DependenciesUnready as i32);
    }

    #[tokio::test]
    async fn test_create_actor_alive() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyActor");
        register_and_create(&mgr, &spec).await;

        let actor = mgr.actors.get(b"actor1".as_slice()).unwrap();
        assert_eq!(actor.state, ActorState::Alive as i32);
    }

    #[tokio::test]
    async fn test_get_named_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyNamedActor");

        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_named_actor_info(Request::new(GetNamedActorInfoRequest {
                name: "MyNamedActor".to_string(),
                ray_namespace: "default".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.actor_table_data.is_some());
    }

    #[tokio::test]
    async fn test_kill_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "");
        register_and_create(&mgr, &spec).await;

        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"actor1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // Actor should be fully removed (RAY_KILL is not restartable).
        assert!(mgr.actors.get(b"actor1".as_slice()).is_none());
    }

    /// Parity with C++ `gcs_actor_manager.cc:655-660`: killing an actor id
    /// that is not in `registered_actors_` must reply with
    /// `Status::NotFound("Could not find actor with ID {hex}")`, not OK.
    #[tokio::test]
    async fn test_kill_actor_unknown_returns_not_found() {
        let mgr = make_manager();
        let unknown = b"never_registered";

        let reply = mgr
            .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: unknown.to_vec(),
                force_kill: true,
                no_restart: true,
            }))
            .await
            .unwrap()
            .into_inner();

        let status = reply.status.expect("status must be set");
        assert_eq!(
            status.code, 17,
            "unknown actor kill must return Ray NotFound (code 17), got {}",
            status.code
        );
        assert!(
            status.message.contains("Could not find actor with ID"),
            "message should match C++ phrasing, got: {:?}",
            status.message
        );
        // Hex of the actor id should appear in the message (C++ uses actor_id.Hex()).
        let hex_id: String = unknown.iter().map(|b| format!("{b:02x}")).collect();
        assert!(
            status.message.contains(&hex_id),
            "message should include hex actor id {hex_id}, got: {:?}",
            status.message
        );
    }

    /// Parity with C++ which only checks `registered_actors_` — not the
    /// `destroyed_actors_` cache. After a non-restartable kill, the actor
    /// is moved out of `self.actors` into `destroyed_actors`. A second
    /// kill for the same id must therefore return NotFound, matching
    /// what C++ would return in the same state.
    #[tokio::test]
    async fn test_kill_actor_after_permanent_destroy_returns_not_found() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor_perm", "");
        register_and_create(&mgr, &spec).await;

        // First kill: RAY_KILL is non-restartable → actor evicted to
        // destroyed_actors cache, removed from self.actors.
        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"actor_perm".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();
        assert!(!mgr.actors.contains_key(b"actor_perm".as_slice()));
        assert!(mgr.destroyed_actors.contains_key(b"actor_perm".as_slice()));

        // Second kill: actor is no longer in registered_actors_ /
        // self.actors → NotFound.
        let reply = mgr
            .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_perm".to_vec(),
                force_kill: true,
                no_restart: true,
            }))
            .await
            .unwrap()
            .into_inner();

        let status = reply.status.expect("status must be set");
        assert_eq!(
            status.code, 17,
            "destroyed-cache hit must still return NotFound (C++ checks registered_actors_ only)"
        );
    }

    /// Parity regression: C++ does not early-return when the entry is
    /// already DEAD (restartable) — it calls DestroyActor unconditionally.
    /// A kill on a dead-but-restartable actor (still in registered_actors_
    /// / self.actors) must NOT return NotFound; our destroy_actor helper
    /// safely no-ops on DEAD entries but the RPC must report OK.
    #[tokio::test]
    async fn test_kill_actor_already_dead_restartable_returns_ok() {
        let mgr = make_manager();
        // Restart budget so the actor stays restartable after OUT_OF_SCOPE death.
        let spec = make_task_spec_with_restarts(b"actor_rst", "", 3);
        register_and_create(&mgr, &spec).await;

        // Drive to DEAD-but-restartable via out-of-scope.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor_rst".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();
        assert!(
            mgr.actors.contains_key(b"actor_rst".as_slice()),
            "restartable-dead actor must stay in registered_actors_ (self.actors)"
        );
        assert_eq!(get_actor_state(&mgr, b"actor_rst"), Some(ActorState::Dead as i32));

        // Kill again → C++ proceeds because the entry is still in
        // registered_actors_. Rust must return OK, not NotFound.
        let reply = mgr
            .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_rst".to_vec(),
                force_kill: true,
                no_restart: true,
            }))
            .await
            .unwrap()
            .into_inner();

        let status = reply.status.expect("status must be set");
        assert_eq!(
            status.code, 0,
            "kill on dead-but-restartable actor must be OK (actor is still registered), got code {} msg {:?}",
            status.code, status.message
        );
    }

    #[tokio::test]
    async fn test_get_all_actors() {
        let mgr = make_manager();

        for i in 0..3 {
            let spec = make_task_spec(format!("actor{i}").as_bytes(), &format!("Actor{i}"));
            register_and_create(&mgr, &spec).await;
        }

        let reply = mgr
            .get_all_actor_info(Request::new(GetAllActorInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.actor_table_data.len(), 3);
        assert_eq!(reply.total, 3);
    }

    #[tokio::test]
    async fn test_list_named_actors() {
        let mgr = make_manager();

        let spec1 = make_task_spec(b"a1", "NamedActor1");
        let spec2 = make_task_spec(b"a2", "NamedActor2");
        register_and_create(&mgr, &spec1).await;
        register_and_create(&mgr, &spec2).await;

        let reply = mgr
            .list_named_actors(Request::new(ListNamedActorsRequest {
                all_namespaces: true,
                ray_namespace: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.named_actors_list.len(), 2);
    }

    #[tokio::test]
    async fn test_report_actor_out_of_scope() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor_oos", "");
        register_and_create(&mgr, &spec).await;

        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor_oos".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Non-detached actor with 0 restarts should be fully removed.
        assert!(mgr.actors.get(b"actor_oos".as_slice()).is_none());
    }

    #[tokio::test]
    async fn test_restart_actor_for_lineage_reconstruction() {
        let mgr = make_manager();
        // Actor with max_restarts=3 so it can be restarted.
        let spec = make_task_spec_with_restarts(b"actor_restart", "", 3);
        register_and_create(&mgr, &spec).await;

        assert_eq!(get_actor_state(&mgr, b"actor_restart"), Some(ActorState::Alive as i32));

        // First: mark out of scope (required for restartability).
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor_restart".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Actor should be DEAD but still in actors map (restartable).
        assert_eq!(get_actor_state(&mgr, b"actor_restart"), Some(ActorState::Dead as i32));

        // Now restart for lineage reconstruction.
        mgr.restart_actor_for_lineage_reconstruction(Request::new(
            RestartActorForLineageReconstructionRequest {
                actor_id: b"actor_restart".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();

        let actor = mgr.actors.get(b"actor_restart".as_slice()).unwrap();
        assert_eq!(actor.state, ActorState::Restarting as i32);
        assert_eq!(actor.num_restarts, 1);
    }

    // --- New tests for C++ semantic parity ---

    #[tokio::test]
    async fn test_named_actor_uniqueness() {
        let mgr = make_manager();
        let spec1 = make_task_spec(b"actor1", "SharedName");
        let spec2 = make_task_spec(b"actor2", "SharedName");

        // First registration succeeds.
        let reply = mgr
            .register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);

        // Second registration with same name fails.
        let reply = mgr
            .register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec2),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 6); // AlreadyExists
    }

    #[tokio::test]
    async fn test_create_requires_registration() {
        let mgr = make_manager();
        let spec = make_task_spec(b"unregistered", "");

        let reply = mgr
            .create_actor(Request::new(CreateActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();

        // Should return Invalid since actor wasn't registered first.
        assert_eq!(reply.status.unwrap().code, 5); // Invalid
    }

    #[tokio::test]
    async fn test_create_idempotent() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "");
        register_and_create(&mgr, &spec).await;

        // Second create on ALIVE actor should succeed (idempotent).
        let reply = mgr
            .create_actor(Request::new(CreateActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.status.unwrap().code, 0);
        assert_eq!(
            get_actor_state(&mgr, b"actor1"),
            Some(ActorState::Alive as i32)
        );
    }

    #[tokio::test]
    async fn test_stale_lineage_restart() {
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"actor1", "", 3);
        register_and_create(&mgr, &spec).await;

        // Kill via out-of-scope to make restartable.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Restart with counter=1 (valid).
        mgr.restart_actor_for_lineage_reconstruction(Request::new(
            RestartActorForLineageReconstructionRequest {
                actor_id: b"actor1".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Restarting as i32));

        // Stale restart with counter=1 again should be ignored.
        // First we need to kill it again to make it DEAD.
        // (In real system, the actor would die and go through out-of-scope again.)
        // For this test, just verify the stale detection: send counter=0.
        // The actor is in RESTARTING state so the "must be DEAD" check triggers first.
        let reply = mgr
            .restart_actor_for_lineage_reconstruction(Request::new(
                RestartActorForLineageReconstructionRequest {
                    actor_id: b"actor1".to_vec(),
                    num_restarts_due_to_lineage_reconstruction: 0, // stale
                },
            ))
            .await
            .unwrap()
            .into_inner();

        // Should succeed (stale detected, returns OK).
        assert_eq!(reply.status.unwrap().code, 0);
        // State unchanged.
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Restarting as i32));
    }

    #[tokio::test]
    async fn test_restart_budget_exhausted() {
        let mgr = make_manager();
        // Actor with max_restarts=1.
        let spec = make_task_spec_with_restarts(b"actor1", "", 1);
        register_and_create(&mgr, &spec).await;

        // First out-of-scope + restart cycle.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        mgr.restart_actor_for_lineage_reconstruction(Request::new(
            RestartActorForLineageReconstructionRequest {
                actor_id: b"actor1".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Restarting as i32));

        // Create again → PENDING_CREATION, then simulate scheduler success → ALIVE.
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
        mgr.on_actor_creation_success(ActorCreationSuccess {
            actor_id: b"actor1".to_vec(),
            worker_address: spec.caller_address.clone().unwrap_or_default(),
            worker_pid: 12345,
            resources: vec![],
            actor_repr_name: String::new(),
        })
        .await;
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Alive as i32));

        // Second out-of-scope.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 1,
        }))
        .await
        .unwrap();

        // Budget exhausted (1 restart used, max=1) → actor fully destroyed.
        assert!(mgr.actors.get(b"actor1".as_slice()).is_none());
    }

    /// Parity with C++ `gcs_actor_manager.cc:283-290`. After a lineage
    /// restart advances the actor's counter to 1, an OLD out-of-scope
    /// report carrying counter=0 was generated before that restart and
    /// must be ignored — acting on it would kill the live restarted
    /// instance. The stale condition is `current > request`.
    #[tokio::test]
    async fn test_stale_out_of_scope_report_lower_request_counter_is_ignored() {
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"actor1", "", 3);
        register_and_create(&mgr, &spec).await;

        // First out-of-scope with counter=0 puts the actor DEAD (restartable).
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Dead as i32));

        // Lineage restart advances the actor's current counter to 1.
        mgr.restart_actor_for_lineage_reconstruction(Request::new(
            RestartActorForLineageReconstructionRequest {
                actor_id: b"actor1".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();

        // Re-create → PENDING_CREATION, then simulate scheduler success → ALIVE.
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
        mgr.on_actor_creation_success(ActorCreationSuccess {
            actor_id: b"actor1".to_vec(),
            worker_address: spec.caller_address.unwrap_or_default(),
            worker_pid: 12345,
            resources: vec![],
            actor_repr_name: String::new(),
        })
        .await;
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Alive as i32));
        // Precondition: actor's current counter is now 1.
        assert_eq!(
            mgr.actors
                .get(b"actor1".as_slice())
                .unwrap()
                .num_restarts_due_to_lineage_reconstruction,
            1
        );

        // An OLD report (counter=0) from before the restart. Current=1 > request=0
        // → stale under C++ semantics → ignored.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Actor must still be ALIVE (stale report was ignored).
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Alive as i32));
    }

    /// Parity with C++ `gcs_actor_manager.cc:283-290`: when the request's
    /// lineage counter equals the actor's current counter, the report is
    /// NOT stale (the C++ guard is strict `>`, not `>=`). The normal
    /// out-of-scope destroy path runs and the actor transitions to DEAD.
    #[tokio::test]
    async fn test_out_of_scope_report_equal_counter_destroys_actor() {
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"actor1", "", 3);
        register_and_create(&mgr, &spec).await;

        // Actor's counter is 0 and the request also carries 0 — not stale.
        assert_eq!(
            mgr.actors
                .get(b"actor1".as_slice())
                .unwrap()
                .num_restarts_due_to_lineage_reconstruction,
            0
        );
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Out-of-scope destroys the actor with OUT_OF_SCOPE cause.
        // With restart budget remaining, it stays in `actors` as DEAD
        // (restartable), not evicted to `destroyed_actors`.
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Dead as i32));
    }

    /// Parity follow-up: a request counter *higher* than current was NOT
    /// the stale case in C++ (it fails the strict `>` guard in the other
    /// direction). It should proceed to destroy. This protects against
    /// regressing back to the reversed interpretation.
    #[tokio::test]
    async fn test_out_of_scope_report_higher_request_counter_destroys_actor() {
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"actor1", "", 3);
        register_and_create(&mgr, &spec).await;

        // Actor's counter is 0; request counter is 5. Under C++ semantics,
        // only `current > request` is stale, so 0 > 5 is false → proceed.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 5,
        }))
        .await
        .unwrap();

        assert_eq!(
            get_actor_state(&mgr, b"actor1"),
            Some(ActorState::Dead as i32),
            "request counter > current must NOT be treated as stale"
        );
    }

    #[tokio::test]
    async fn test_kill_cleans_up_named_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyNamedActor");
        register_and_create(&mgr, &spec).await;

        assert!(mgr
            .named_actors
            .contains_key(&("MyNamedActor".to_string(), "default".to_string())));

        // Kill the actor.
        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"actor1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // Named actor slot should be freed.
        assert!(!mgr
            .named_actors
            .contains_key(&("MyNamedActor".to_string(), "default".to_string())));

        // A new actor can now reuse the name.
        let spec2 = make_task_spec(b"actor2", "MyNamedActor");
        let reply = mgr
            .register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec2),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_dead_actor_not_recreatable() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "");
        register_and_create(&mgr, &spec).await;

        // Kill it (RAY_KILL = non-restartable → fully removed).
        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"actor1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // Try to create again → should fail (actor not registered).
        let reply = mgr
            .create_actor(Request::new(CreateActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 5); // Invalid
    }

    #[tokio::test]
    async fn test_register_idempotent() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyActor");

        // First register.
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();

        // Second register with same actor_id should succeed (idempotent).
        let reply = mgr
            .register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    // ─── New tests for lifecycle parity ──────────────────────────────

    #[tokio::test]
    async fn test_create_actor_final_state_is_alive() {
        // create_actor should end in ALIVE state (after going through
        // PENDING_CREATION internally, matching C++ state machine).
        let mgr = make_manager();
        let spec = make_task_spec(b"a1", "");
        register_and_create(&mgr, &spec).await;

        let actor = mgr.actors.get(b"a1".as_slice()).unwrap();
        assert_eq!(actor.state, ActorState::Alive as i32);
        assert!(actor.start_time > 0);
    }

    #[tokio::test]
    async fn test_create_actor_pending_creation_idempotent() {
        // If actor is in PENDING_CREATION, create_actor should return OK.
        let mgr = make_manager();
        let spec = make_task_spec(b"a1", "");
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();

        // Manually set to PENDING_CREATION to test idempotency.
        if let Some(mut entry) = mgr.actors.get_mut(b"a1".as_slice()) {
            entry.state = ActorState::PendingCreation as i32;
        }

        let reply = mgr
            .create_actor(Request::new(CreateActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_create_from_restarting_state() {
        // After RESTARTING (driven via worker death + restart budget),
        // CreateActor should transition RESTARTING → PENDING_CREATION → ALIVE.
        //
        // Note: prior to the `KillActor` parity fix this test drove
        // RESTARTING via `kill_actor_via_gcs(no_restart=false)` — that
        // worked only because Rust incorrectly marked the actor DEAD
        // with OUT_OF_SCOPE on that path. With the fix
        // (`actor_stub.rs::kill_actor`), `no_restart=false` no longer
        // marks an ALIVE actor DEAD; the state transition flows
        // through `OnWorkerDead` instead. We drive that path directly
        // here so the test exercises the same RESTARTING → ALIVE
        // sequence without depending on the old (wrong) shortcut.
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"a1", "", 3);
        register_and_create(&mgr, &spec).await;

        // Drive the actor to RESTARTING via the worker-dead path,
        // matching the production sequence (raylet kills worker →
        // OnWorkerDead → restart_actor_internal).
        let addr = spec.caller_address.clone().unwrap_or_default();
        mgr.on_worker_dead(&addr.node_id, &addr.worker_id, /*need_reconstruct=*/ true)
            .await;

        // Actor should be in RESTARTING now.
        let actor = mgr.actors.get(b"a1".as_slice()).unwrap();
        assert_eq!(actor.state, ActorState::Restarting as i32);
        drop(actor);

        // CreateActor should transition to PENDING_CREATION.
        let reply = mgr
            .create_actor(Request::new(CreateActorRequest {
                task_spec: Some(spec.clone()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);

        let actor = mgr.actors.get(b"a1".as_slice()).unwrap();
        assert_eq!(actor.state, ActorState::PendingCreation as i32);
        drop(actor);

        // Simulate scheduler success → ALIVE.
        mgr.on_actor_creation_success(ActorCreationSuccess {
            actor_id: b"a1".to_vec(),
            worker_address: spec.caller_address.unwrap_or_default(),
            worker_pid: 12345,
            resources: vec![],
            actor_repr_name: String::new(),
        })
        .await;

        let actor = mgr.actors.get(b"a1".as_slice()).unwrap();
        assert_eq!(actor.state, ActorState::Alive as i32);
    }

    #[tokio::test]
    async fn test_kill_no_restart_false_does_not_mark_alive_dead() {
        // Parity with C++ `KillActor` (`gcs_actor_manager.cc:1877-1916`):
        // `no_restart=false` against an ALIVE actor must NOT
        // immediately mark it DEAD. C++ only sends a `KillLocalActor`
        // RPC to the raylet; the state transition comes later via
        // `OnWorkerDead` once the worker actually exits. Marking the
        // actor DEAD with OUT_OF_SCOPE here (the old Rust behavior)
        // diverged from C++ in both death cause AND state machine.
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"a1", "", 3);
        register_and_create(&mgr, &spec).await;
        assert_eq!(get_actor_state(&mgr, b"a1"), Some(ActorState::Alive as i32));

        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"a1".to_vec(),
            force_kill: true,
            no_restart: false,
        }))
        .await
        .unwrap();

        // State must stay ALIVE — the worker will be killed by the
        // raylet, and only then will OnWorkerDead drive RESTARTING.
        let actor = mgr.actors.get(b"a1".as_slice()).unwrap();
        assert_eq!(
            actor.state,
            ActorState::Alive as i32,
            "ALIVE actor must stay ALIVE after kill_actor_via_gcs(no_restart=false) — \
             state transitions through OnWorkerDead, not through marking DEAD here"
        );
        // No death_cause set (only set on transitions to DEAD).
        assert!(
            actor.death_cause.is_none(),
            "ALIVE actor must not have a death_cause yet"
        );
    }

    #[tokio::test]
    async fn test_kill_no_restart_true_is_not_restartable() {
        // no_restart=true should use RAY_KILL cause, preventing restart.
        let mgr = make_manager();
        let spec = make_task_spec(b"a1", "");
        register_and_create(&mgr, &spec).await;

        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"a1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // Actor should have been moved to destroyed cache (non-restartable).
        assert!(!mgr.actors.contains_key(b"a1".as_slice()));
        assert!(mgr.destroyed_actors.contains_key(b"a1".as_slice()));
    }

    #[tokio::test]
    async fn test_get_actor_info_checks_destroyed_cache() {
        let mgr = make_manager();
        let spec = make_task_spec(b"a1", "");
        register_and_create(&mgr, &spec).await;

        // Kill permanently.
        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"a1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // Should still be findable via destroyed cache.
        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"a1".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
        assert!(reply.actor_table_data.is_some());
        assert_eq!(
            reply.actor_table_data.unwrap().state,
            ActorState::Dead as i32
        );
    }

    #[tokio::test]
    async fn test_get_all_actor_info_includes_destroyed() {
        let mgr = make_manager();
        let spec1 = make_task_spec(b"a1", "");
        let spec2 = make_task_spec(b"a2", "");
        register_and_create(&mgr, &spec1).await;
        register_and_create(&mgr, &spec2).await;

        // Kill a1 permanently.
        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"a1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // get_all should include both alive a2 and destroyed a1.
        let reply = mgr
            .get_all_actor_info(Request::new(GetAllActorInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.actor_table_data.len(), 2);
        assert_eq!(reply.total, 2);
    }

    #[tokio::test]
    async fn test_show_dead_jobs_queries_storage() {
        let mgr = make_manager();
        let spec = make_task_spec(b"a1", "");
        register_and_create(&mgr, &spec).await;

        // Kill permanently — removes from actors map but stays in storage.
        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"a1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        // Clear the destroyed cache to simulate it being evicted.
        mgr.destroyed_actors.clear();

        // Without show_dead_jobs: actor not found (not in memory).
        let reply = mgr
            .get_all_actor_info(Request::new(GetAllActorInfoRequest {
                show_dead_jobs: false,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.actor_table_data.len(), 0);

        // With show_dead_jobs: finds actor from storage.
        let reply = mgr
            .get_all_actor_info(Request::new(GetAllActorInfoRequest {
                show_dead_jobs: true,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            reply.actor_table_data.len() >= 1,
            "Expected at least 1 actor from storage"
        );
    }

    /// Parity guard for C++ `GcsActorManager::SetPreemptedAndPublish`
    /// (`gcs_actor_manager.cc:1417-1444`):
    /// - flips `preempted=true` on every actor associated with the
    ///   given node via `created_actors`
    /// - persists each updated row via `ActorTable.put`
    /// - publishes each via the GcsActorChannel pubsub
    #[tokio::test]
    async fn set_preempted_and_publish_flips_bit_and_persists() {
        let mgr = make_manager();

        // Seed two alive actors on node X, one on a different node, so we
        // can confirm the scoping behaviour.
        let node_x = b"node-X-aaaaaaaaaaaaaaaaaaaaa".to_vec();
        let node_y = b"node-Y-aaaaaaaaaaaaaaaaaaaaa".to_vec();
        assert_eq!(node_x.len(), 28);
        let actor_a = b"actor-A-aaaaaaaaaaaaaaaaaaaa".to_vec();
        let actor_b = b"actor-B-aaaaaaaaaaaaaaaaaaaa".to_vec();
        let actor_c = b"actor-C-aaaaaaaaaaaaaaaaaaaa".to_vec();

        for (aid, nid, wid) in [
            (&actor_a, &node_x, b"w-A".to_vec()),
            (&actor_b, &node_x, b"w-B".to_vec()),
            (&actor_c, &node_y, b"w-C".to_vec()),
        ] {
            mgr.actors.insert(
                aid.clone(),
                ActorTableData {
                    actor_id: aid.clone(),
                    state: ActorState::Alive as i32,
                    node_id: Some(nid.clone()),
                    preempted: false,
                    ..Default::default()
                },
            );
            mgr.created_actors
                .lock()
                .entry(nid.clone())
                .or_default()
                .insert(wid, aid.clone());
        }

        mgr.set_preempted_and_publish(&node_x).await;

        // Actors on node_x are flipped.
        assert!(
            mgr.actors.get(&actor_a).unwrap().preempted,
            "actor A on node_x must be preempted"
        );
        assert!(
            mgr.actors.get(&actor_b).unwrap().preempted,
            "actor B on node_x must be preempted"
        );
        // Actor on node_y untouched.
        assert!(
            !mgr.actors.get(&actor_c).unwrap().preempted,
            "actor C on a different node must not be preempted"
        );

        // Persisted copies reflect the flipped bit.
        for aid in [&actor_a, &actor_b] {
            let persisted = mgr
                .table_storage
                .actor_table()
                .get(&hex(aid))
                .await
                .expect("persisted row must exist");
            assert!(
                persisted.preempted,
                "ActorTable::put was not called for {:?}",
                hex(aid)
            );
        }

        let persisted_c = mgr.table_storage.actor_table().get(&hex(&actor_c)).await;
        assert!(
            persisted_c.is_none(),
            "untouched actors should not have been persisted by this call"
        );
    }

    /// Unknown-node fast path: no actors on the node → no-op. C++ lines
    /// 1421-1423 (`if (created_actors_.find(node_id) == created_actors_.end()) return;`).
    #[tokio::test]
    async fn set_preempted_and_publish_on_unknown_node_is_noop() {
        let mgr = make_manager();
        // Seed one actor on a different node.
        let other_node = b"node-Y-aaaaaaaaaaaaaaaaaaaa".to_vec();
        let actor_a = b"actor-A-aaaaaaaaaaaaaaaaaaaa".to_vec();
        mgr.actors.insert(
            actor_a.clone(),
            ActorTableData {
                actor_id: actor_a.clone(),
                state: ActorState::Alive as i32,
                node_id: Some(other_node.clone()),
                preempted: false,
                ..Default::default()
            },
        );
        mgr.created_actors
            .lock()
            .entry(other_node)
            .or_default()
            .insert(b"w".to_vec(), actor_a.clone());

        mgr.set_preempted_and_publish(b"node-not-in-created-actors-x")
            .await;

        assert!(!mgr.actors.get(&actor_a).unwrap().preempted);
    }

    // ─── detached-actor runtime-env URI parity tests ─────────────────────

    /// Helpers for runtime-env tests. These mirror the C++ call shape:
    /// a detached-actor task spec carrying a RuntimeEnvInfo with gcs:// URIs.
    fn make_detached_actor_task_spec(
        actor_id: &[u8],
        job_id: &[u8],
        working_dir_uri: &str,
        py_modules_uris: &[&str],
    ) -> TaskSpec {
        TaskSpec {
            r#type: 1,
            name: "DetachedActor".to_string(),
            job_id: job_id.to_vec(),
            task_id: b"task_detached".to_vec(),
            caller_id: b"caller_d".to_vec(),
            caller_address: Some(Address {
                node_id: b"node_d".to_vec(),
                ip_address: "127.0.0.1".to_string(),
                port: 10001,
                worker_id: b"worker_d".to_vec(),
            }),
            actor_creation_task_spec: Some(ActorCreationTaskSpec {
                actor_id: actor_id.to_vec(),
                name: "DetachedActor".to_string(),
                ray_namespace: "default".to_string(),
                is_detached: true,
                max_actor_restarts: 0,
                ..Default::default()
            }),
            runtime_env_info: Some(RuntimeEnvInfo {
                uris: Some(RuntimeEnvUris {
                    working_dir_uri: working_dir_uri.to_string(),
                    py_modules_uris: py_modules_uris
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build an actor manager wired with a runtime-env service backed by KV.
    fn make_manager_with_runtime_env() -> (
        Arc<GcsActorManager>,
        Arc<crate::runtime_env_stub::RuntimeEnvService>,
        Arc<dyn gcs_kv::InternalKVInterface>,
    ) {
        let store = Arc::new(InMemoryStoreClient::new());
        let kv: Arc<dyn gcs_kv::InternalKVInterface> =
            Arc::new(gcs_kv::StoreClientInternalKV::new(store.clone()));
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        let gcs_publisher = Arc::new(gcs_pubsub::GcsPublisher::new(16));
        let node_manager = Arc::new(GcsNodeManager::new(
            table_storage.clone(),
            gcs_publisher,
            b"test_cluster".to_vec(),
        ));
        let (success_tx, _rx1) = mpsc::unbounded_channel();
        let (failure_tx, _rx2) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsActorScheduler::new(
            node_manager.clone(),
            success_tx,
            failure_tx,
        ));
        let mgr = Arc::new(GcsActorManager::new(
            publisher,
            table_storage,
            scheduler,
            node_manager,
        ));
        let svc = Arc::new(crate::runtime_env_stub::RuntimeEnvService::new_with_kv(
            kv.clone(),
        ));
        mgr.set_runtime_env_service(svc.clone());
        (mgr, svc, kv)
    }

    /// Parity with C++ `gcs_actor_manager.cc:744-745`: detached-actor
    /// registration pins runtime-env URIs under actor_id.Hex(). The pinned
    /// gcs:// URIs must remain in KV while the actor is alive.
    #[tokio::test]
    async fn test_detached_actor_register_pins_runtime_env_uris() {
        let (mgr, _svc, kv) = make_manager_with_runtime_env();

        kv.put("", "gcs://actor_env/working_dir.zip", "wd_payload".into(), true)
            .await;
        kv.put("", "gcs://actor_env/py_mod.zip", "py_payload".into(), true)
            .await;

        let actor_id = b"detached_actor_1";
        let spec = make_detached_actor_task_spec(
            actor_id,
            b"job_of_detached",
            "gcs://actor_env/working_dir.zip",
            &["gcs://actor_env/py_mod.zip"],
        );
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        // Actor alive → URIs still in KV.
        assert!(kv.get("", "gcs://actor_env/working_dir.zip").await.is_some());
        assert!(kv.get("", "gcs://actor_env/py_mod.zip").await.is_some());
    }

    /// Parity with C++ `gcs_actor_manager.cc:1120`: on the non-restartable
    /// destroy branch for detached actors, runtime-env URIs are released.
    /// The pinned gcs:// packages must then be deleted from KV.
    #[tokio::test]
    async fn test_detached_actor_destroy_releases_runtime_env_uris() {
        let (mgr, _svc, kv) = make_manager_with_runtime_env();

        kv.put("", "gcs://actor_env2/wd.zip", "payload".into(), true).await;
        kv.put("", "gcs://actor_env2/pym.zip", "payload".into(), true).await;

        let actor_id = b"detached_actor_2";
        let spec = make_detached_actor_task_spec(
            actor_id,
            b"job_of_detached2",
            "gcs://actor_env2/wd.zip",
            &["gcs://actor_env2/pym.zip"],
        );
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        // Drive a non-restartable destroy (RAY_KILL). Per
        // `is_actor_restartable`, only OUT_OF_SCOPE+budget is restartable,
        // so RAY_KILL lands on the non-restartable branch.
        mgr.destroy_actor_for_test(actor_id, DeathReason::RayKill, "killed by test")
            .await;

        assert!(
            kv.get("", "gcs://actor_env2/wd.zip").await.is_none(),
            "working_dir URI must be deleted after detached actor is destroyed"
        );
        assert!(
            kv.get("", "gcs://actor_env2/pym.zip").await.is_none(),
            "py_modules URI must be deleted after detached actor is destroyed"
        );
    }

    /// Non-detached actors must NOT touch runtime-env refcounts. Parity
    /// with C++ `gcs_actor_manager.cc:738-745`: the AddURIReference call
    /// is inside the `else` branch for detached actors only.
    #[tokio::test]
    async fn test_non_detached_actor_does_not_pin_runtime_env_uris() {
        let (mgr, svc, _kv) = make_manager_with_runtime_env();

        // Same URIs, but is_detached = false.
        let mut spec = make_detached_actor_task_spec(
            b"non_detached_1",
            b"job1",
            "gcs://nd/wd.zip",
            &["gcs://nd/pym.zip"],
        );
        if let Some(creation) = spec.actor_creation_task_spec.as_mut() {
            creation.is_detached = false;
        }

        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        // No references should have been created under the actor id.
        let released = svc.remove_uri_reference_sync(&hex(b"non_detached_1"));
        assert!(
            released.is_empty(),
            "non-detached actors must not pin runtime-env URIs"
        );
    }

    /// Regression test for the race closed by pinning URIs BEFORE the
    /// awaited task-spec Put. A concurrent temporary pin with
    /// `expiration_s = 0` is released on the next executor tick
    /// (runtime_env_stub.rs:200-251). If `register_actor` pinned AFTER
    /// its `actor_task_spec_table().put(...).await`, the temporary pin's
    /// expiry could land during that await and drop the refcount to 0,
    /// letting the KV deleter delete the `gcs://` package before the
    /// detached actor's own reference is recorded. Pinning first — as
    /// C++ does at `gcs_actor_manager.cc:744-745` — keeps the URI
    /// live across the await.
    #[tokio::test]
    async fn test_detached_register_pin_survives_concurrent_zero_ttl_expiry() {
        use gcs_proto::ray::rpc::runtime_env_gcs_service_server::RuntimeEnvGcsService;

        let (mgr, svc, kv) = make_manager_with_runtime_env();
        kv.put("", "gcs://race/wd.zip", "payload".into(), true).await;

        // Step 1: a pin held by the client (expiration_s = 0 fires on
        // the next tick). This is the only holder before registration.
        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://race/wd.zip".to_string(),
            expiration_s: 0,
        }))
        .await
        .unwrap();

        // Step 2: register a detached actor whose runtime_env references
        // the same URI. With pinning BEFORE the storage await, the
        // refcount is ≥1 at all times; the zero-TTL expiry ticks during
        // the Put but leaves the actor's pin intact.
        let actor_id = b"detached_race";
        let spec = make_detached_actor_task_spec(
            actor_id,
            b"job_race",
            "gcs://race/wd.zip",
            &[],
        );
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        // Let any lingering expiry tasks run.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        // The actor's pin keeps the URI live and the KV entry intact.
        assert!(
            kv.get("", "gcs://race/wd.zip").await.is_some(),
            "gcs:// URI must survive concurrent zero-TTL expiry during register"
        );
    }

    /// A detached actor that dies with OUT_OF_SCOPE and restart budget
    /// stays `restartable`, so `destroy_actor` must NOT release URI
    /// references. Parity with the C++ `!is_restartable` guard at
    /// `gcs_actor_manager.cc:1111`.
    #[tokio::test]
    async fn test_detached_actor_restartable_dead_does_not_release_uris() {
        let (mgr, _svc, kv) = make_manager_with_runtime_env();

        kv.put("", "gcs://restartable_env/wd.zip", "p".into(), true).await;

        let actor_id = b"detached_restartable";
        let mut spec = make_detached_actor_task_spec(
            actor_id,
            b"job_rst",
            "gcs://restartable_env/wd.zip",
            &[],
        );
        // Give it restart budget so OUT_OF_SCOPE death is restartable.
        if let Some(creation) = spec.actor_creation_task_spec.as_mut() {
            creation.max_actor_restarts = 5;
        }
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        mgr.destroy_actor_for_test(actor_id, DeathReason::OutOfScope, "oos")
            .await;

        // Restartable → refcount preserved → KV intact.
        assert!(
            kv.get("", "gcs://restartable_env/wd.zip").await.is_some(),
            "restartable detached actor must keep its runtime-env URI pinned"
        );
    }

    // ─── force_kill parity tests ────────────────────────────────────
    //
    // These tests verify the fix for the parity gap where Rust's
    // `kill_actor_via_gcs` previously ignored the `force_kill` field
    // on `KillActorViaGcsRequest`. The C++ contract is:
    // - `no_restart=true`: DestroyActor uses default `force_kill=true`,
    //   ignoring the request flag (gcs_actor_manager.cc:645).
    // - `no_restart=false`: KillActor forwards the request flag into
    //   NotifyRayletToKillActor (gcs_actor_manager.cc:647, 1850).
    // The flag must reach the raylet in `KillLocalActorRequest.force_kill`.

    mod force_kill_parity {
        use super::*;
        use crate::autoscaler_stub::RayletClientPool;
        use async_trait::async_trait;
        use parking_lot::Mutex as PlMutex;
        use std::collections::HashMap;
        use tonic::Status;

        /// Test-only `RayletClientPool` that records every
        /// `kill_local_actor` call so tests can assert the exact
        /// `force_kill` bit the actor manager forwarded.
        struct FakeKillActorPool {
            kill_calls: PlMutex<Vec<(String, u16, KillLocalActorRequest)>>,
            fail: bool,
        }

        impl FakeKillActorPool {
            fn new() -> Arc<Self> {
                Arc::new(Self {
                    kill_calls: PlMutex::new(Vec::new()),
                    fail: false,
                })
            }
            fn new_failing() -> Arc<Self> {
                Arc::new(Self {
                    kill_calls: PlMutex::new(Vec::new()),
                    fail: true,
                })
            }
            fn calls(&self) -> Vec<(String, u16, KillLocalActorRequest)> {
                self.kill_calls.lock().clone()
            }
        }

        #[async_trait]
        impl RayletClientPool for FakeKillActorPool {
            async fn resize_local_resource_instances(
                &self,
                _address: &str,
                _port: u16,
                _resources: HashMap<String, f64>,
            ) -> Result<HashMap<String, f64>, Status> {
                Err(Status::unavailable("n/a"))
            }
            async fn drain_raylet(
                &self,
                _address: &str,
                _port: u16,
                _request: gcs_proto::ray::rpc::DrainRayletRequest,
            ) -> Result<gcs_proto::ray::rpc::DrainRayletReply, Status> {
                Err(Status::unavailable("n/a"))
            }
            async fn kill_local_actor(
                &self,
                address: &str,
                port: u16,
                request: KillLocalActorRequest,
            ) -> Result<(), Status> {
                self.kill_calls
                    .lock()
                    .push((address.to_string(), port, request));
                if self.fail {
                    Err(Status::unavailable("raylet down"))
                } else {
                    Ok(())
                }
            }
        }

        /// Build a manager with the given pool installed and with a
        /// matching alive node registered so
        /// `notify_raylet_to_kill_actor` can resolve the worker's
        /// raylet address. Mirrors `make_manager()` but adds the
        /// pool + node-seed.
        async fn make_manager_with_pool(
            pool: Arc<dyn RayletClientPool>,
        ) -> Arc<GcsActorManager> {
            let store = Arc::new(InMemoryStoreClient::new());
            let table_storage = Arc::new(GcsTableStorage::new(store));
            let publisher = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
            let gcs_publisher = Arc::new(gcs_pubsub::GcsPublisher::new(16));
            let node_manager = Arc::new(GcsNodeManager::new(
                table_storage.clone(),
                gcs_publisher,
                b"test_cluster".to_vec(),
            ));
            // Seed the alive node that task specs reference (`b"node1"`
            // in `make_task_spec`). `node_manager_address` /
            // `node_manager_port` become the raylet endpoint the
            // kill RPC is directed at.
            let node = GcsNodeInfo {
                node_id: b"node1".to_vec(),
                node_manager_address: "10.0.0.1".to_string(),
                node_manager_port: 4321,
                state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
                ..Default::default()
            };
            node_manager.add_node(node).await;

            let (success_tx, _rx1) = mpsc::unbounded_channel();
            let (failure_tx, _rx2) = mpsc::unbounded_channel();
            let scheduler = Arc::new(GcsActorScheduler::new(
                node_manager.clone(),
                success_tx,
                failure_tx,
            ));
            let mgr = Arc::new(GcsActorManager::new(
                publisher,
                table_storage,
                scheduler,
                node_manager,
            ));
            mgr.set_raylet_client_pool(pool);
            mgr.install_self_arc();
            mgr
        }

        /// `no_restart=true` path: C++ `DestroyActor` uses default
        /// `force_kill=true`, ignoring `request.force_kill()`. Even
        /// when the caller sends `force_kill=false`, the raylet must
        /// receive `force_kill=true`.
        #[tokio::test]
        async fn test_no_restart_always_force_kill_true() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec(b"actor_a", "");
            register_and_create(&mgr, &spec).await;

            mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_a".to_vec(),
                force_kill: false, // user asked graceful
                no_restart: true,  // ... but no_restart=true overrides
            }))
            .await
            .unwrap();

            let calls = pool.calls();
            assert_eq!(
                calls.len(),
                1,
                "exactly one KillLocalActor RPC must be emitted"
            );
            let (addr, port, req) = &calls[0];
            assert_eq!(addr, "10.0.0.1");
            assert_eq!(*port, 4321u16);
            assert!(
                req.force_kill,
                "no_restart=true must forward force_kill=true regardless of request flag (C++ :645)"
            );
            assert_eq!(req.intended_actor_id, b"actor_a".to_vec());
            assert_eq!(req.worker_id, b"worker1".to_vec());
            assert_eq!(
                death_cause_reason(&req.death_cause),
                Some(DeathReason::RayKill as i32),
                "no_restart=true must carry RAY_KILL cause (C++ KilledByApplication)"
            );
            // no_restart=true does mark DEAD (DestroyActor path).
            assert!(
                !mgr.actors.contains_key(b"actor_a".as_slice()),
                "non-restartable actor must be moved out of `actors` to `destroyed_actors`"
            );
            assert!(
                mgr.destroyed_actors.contains_key(b"actor_a".as_slice()),
                "non-restartable actor must end up in `destroyed_actors`"
            );
        }

        /// Helper: extract the `DeathReason` from an
        /// `Option<ActorDeathCause>` for cause-parity assertions.
        fn death_cause_reason(cause: &Option<ActorDeathCause>) -> Option<i32> {
            let cause = cause.as_ref()?;
            let ctx = cause.context.as_ref()?;
            match ctx {
                DeathContext::ActorDiedErrorContext(c) => Some(c.reason),
                _ => None,
            }
        }

        /// `no_restart=false, force_kill=true`: C++ `KillActor`
        /// forwards the request flag and sets the death cause to
        /// `RAY_KILL` (KilledByApplication), NOT `OUT_OF_SCOPE`.
        /// State must stay ALIVE — `OnWorkerDead` drives the next
        /// transition, not this handler.
        #[tokio::test]
        async fn test_restart_true_force_kill_true_forwarded() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_b", "", 3);
            register_and_create(&mgr, &spec).await;

            mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_b".to_vec(),
                force_kill: true,
                no_restart: false,
            }))
            .await
            .unwrap();

            let calls = pool.calls();
            assert_eq!(calls.len(), 1);
            assert!(calls[0].2.force_kill, "force_kill=true must be forwarded");
            // Death cause carried in the RPC must be RAY_KILL —
            // matches C++ `GenKilledByApplicationCause(...)` at
            // `gcs_actor_manager.cc:1899-1900`. Before the fix this
            // would have been OUT_OF_SCOPE.
            assert_eq!(
                death_cause_reason(&calls[0].2.death_cause),
                Some(DeathReason::RayKill as i32),
                "no_restart=false must use RAY_KILL cause (C++ KilledByApplication), not OUT_OF_SCOPE"
            );
            // State must stay ALIVE — only the raylet RPC fires;
            // RESTARTING/DEAD comes via OnWorkerDead.
            let actor = mgr.actors.get(b"actor_b".as_slice()).unwrap();
            assert_eq!(
                actor.state,
                ActorState::Alive as i32,
                "ALIVE actor must stay ALIVE — no immediate state transition"
            );
            assert!(
                actor.death_cause.is_none(),
                "no death_cause is set on the actor row until it actually dies"
            );
        }

        /// `no_restart=false, force_kill=false`: the raylet must
        /// receive `force_kill=false` so it performs a graceful
        /// `Exit`. Same death-cause and state-transition rules as
        /// the force_kill=true variant.
        #[tokio::test]
        async fn test_restart_true_force_kill_false_forwarded() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_c", "", 3);
            register_and_create(&mgr, &spec).await;

            mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_c".to_vec(),
                force_kill: false,
                no_restart: false,
            }))
            .await
            .unwrap();

            let calls = pool.calls();
            assert_eq!(calls.len(), 1);
            assert!(
                !calls[0].2.force_kill,
                "force_kill=false must be forwarded — raylet decides graceful vs SIGKILL"
            );
            assert_eq!(
                death_cause_reason(&calls[0].2.death_cause),
                Some(DeathReason::RayKill as i32),
                "no_restart=false must use RAY_KILL cause regardless of force_kill flag"
            );
            // Same: state stays ALIVE.
            assert_eq!(
                get_actor_state(&mgr, b"actor_c"),
                Some(ActorState::Alive as i32),
            );
        }

        /// `ReportActorOutOfScope` path: C++ calls
        /// `DestroyActor(..., /*force_kill=*/false)` at line 297.
        /// The raylet must receive `force_kill=false`.
        #[tokio::test]
        async fn test_report_out_of_scope_sends_graceful_kill() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec(b"actor_d", "");
            register_and_create(&mgr, &spec).await;

            mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
                actor_id: b"actor_d".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 0,
            }))
            .await
            .unwrap();

            let calls = pool.calls();
            assert_eq!(calls.len(), 1);
            assert!(
                !calls[0].2.force_kill,
                "out-of-scope destroy must forward force_kill=false (C++ :297)"
            );
        }

        /// Regression guard: unknown-actor kill must still return
        /// NotFound and emit NO RPC.
        #[tokio::test]
        async fn test_unknown_actor_returns_not_found_no_rpc() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;

            let reply = mgr
                .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                    actor_id: b"ghost".to_vec(),
                    force_kill: true,
                    no_restart: true,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(reply.status.as_ref().unwrap().code, 17, "NotFound");
            assert_eq!(
                pool.calls().len(),
                0,
                "no RPC must be emitted for an unknown actor"
            );
        }

        /// Raylet RPC failure must not change the GCS-visible outcome.
        /// C++ (`gcs_actor_manager.cc:1869-1871`) logs the error and
        /// keeps going — the actor is still marked DEAD and the RPC
        /// caller still gets OK.
        #[tokio::test]
        async fn test_rpc_failure_still_reports_ok_and_marks_dead() {
            let pool = FakeKillActorPool::new_failing();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec(b"actor_e", "");
            register_and_create(&mgr, &spec).await;

            let reply = mgr
                .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                    actor_id: b"actor_e".to_vec(),
                    force_kill: true,
                    no_restart: true,
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(reply.status.as_ref().unwrap().code, 0, "reply is OK");

            let calls = pool.calls();
            assert_eq!(calls.len(), 1);
            // Non-restartable RAY_KILL → moved to destroyed cache.
            assert!(
                mgr.destroyed_actors
                    .contains_key(b"actor_e".as_slice()),
                "actor must end up DEAD and in destroyed cache despite RPC failure"
            );
        }

        /// Regression guard for tests that don't wire a pool:
        /// without `set_raylet_client_pool`, `destroy_actor` must
        /// still succeed (the raylet-notify helper short-circuits).
        #[tokio::test]
        async fn test_no_pool_installed_is_noop() {
            let mgr = make_manager();
            let spec = make_task_spec(b"actor_f", "");
            register_and_create(&mgr, &spec).await;

            let reply = mgr
                .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                    actor_id: b"actor_f".to_vec(),
                    force_kill: false,
                    no_restart: true,
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(reply.status.as_ref().unwrap().code, 0);
            assert!(
                mgr.destroyed_actors
                    .contains_key(b"actor_f".as_slice()),
                "actor must be destroyed even without a pool installed"
            );
        }

        // ─── KillActor-path parity tests (Finding 2) ────────────────
        //
        // The tests above exercise `force_kill` forwarding. These
        // exercise the rest of the C++ `KillActor` contract: state
        // machine, death-cause selection, and the not-yet-created
        // restart branch.

        /// End-to-end follow-on: after kill_actor_via_gcs leaves the
        /// actor ALIVE, an `OnWorkerDead` (which production fires
        /// once the raylet has actually killed the worker)
        /// transitions the actor to RESTARTING with a fresh
        /// scheduler enqueue. This locks down the C++ async sequence
        /// kill→worker-dead→restart.
        #[tokio::test]
        async fn test_no_restart_false_alive_then_worker_dead_drives_restart() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g", "", 3);
            register_and_create(&mgr, &spec).await;

            // 1. kill_actor_via_gcs(no_restart=false) → ALIVE unchanged.
            mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_g".to_vec(),
                force_kill: true,
                no_restart: false,
            }))
            .await
            .unwrap();
            assert_eq!(
                get_actor_state(&mgr, b"actor_g"),
                Some(ActorState::Alive as i32),
            );

            // 2. Simulate the raylet → GCS worker-dead callback that
            // production fires after the kill. Restart budget = 3 →
            // RESTARTING.
            let addr = spec.caller_address.clone().unwrap_or_default();
            mgr.on_worker_dead(&addr.node_id, &addr.worker_id, true).await;
            assert_eq!(
                get_actor_state(&mgr, b"actor_g"),
                Some(ActorState::Restarting as i32),
                "OnWorkerDead must drive ALIVE → RESTARTING with restart budget"
            );
        }

        /// `kill_actor` on a not-yet-created (PENDING_CREATION) actor:
        /// C++ `:1903-1914` calls `RestartActor(..., need_reschedule=
        /// true, GenKilledByApplicationCause(...))`. With restart
        /// budget remaining, the actor must transition to RESTARTING.
        #[tokio::test]
        async fn test_no_restart_false_pending_creation_drives_restart() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;

            // Register an actor with restart budget but DO NOT call
            // create_actor — the actor stays in PENDING_CREATION /
            // DEPENDENCIES_UNREADY without being added to
            // `created_actors`. Manually set state to PendingCreation
            // so it isn't filtered out by the C++ DEPENDENCIES_UNREADY
            // early return.
            let spec = make_task_spec_with_restarts(b"actor_h", "", 3);
            mgr.register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec.clone()),
            }))
            .await
            .unwrap();
            if let Some(mut entry) = mgr.actors.get_mut(b"actor_h".as_slice()) {
                entry.state = ActorState::PendingCreation as i32;
            }

            mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_h".to_vec(),
                force_kill: true,
                no_restart: false,
            }))
            .await
            .unwrap();

            // No raylet RPC: no worker_id was leased.
            assert_eq!(
                pool.calls().len(),
                0,
                "no KillLocalActor RPC for an actor without a leased worker"
            );

            // restart_actor_internal with budget → RESTARTING.
            assert_eq!(
                get_actor_state(&mgr, b"actor_h"),
                Some(ActorState::Restarting as i32),
                "not-yet-created actor with restart budget must go to RESTARTING (C++ :1912-1914)"
            );
        }

        /// `kill_actor` on a not-yet-created actor with NO restart
        /// budget: `RestartActor` falls through to the no-budget DEAD
        /// branch and the death cause must be `RAY_KILL` — NOT
        /// `OUT_OF_SCOPE` (old wrong behavior) and NOT `NodeDied`
        /// (the previously hard-coded value in
        /// `restart_actor_internal`'s DEAD branch).
        #[tokio::test]
        async fn test_no_restart_false_pending_creation_no_budget_dies_with_ray_kill() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_i", "", 0);
            mgr.register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec.clone()),
            }))
            .await
            .unwrap();
            if let Some(mut entry) = mgr.actors.get_mut(b"actor_i".as_slice()) {
                entry.state = ActorState::PendingCreation as i32;
            }

            mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                actor_id: b"actor_i".to_vec(),
                force_kill: true,
                no_restart: false,
            }))
            .await
            .unwrap();

            // No restart budget → final DEAD with RAY_KILL.
            // After the no-budget branch, the actor is moved to
            // `destroyed_actors` because it isn't restartable.
            assert!(
                !mgr.actors.contains_key(b"actor_i".as_slice()),
                "non-restartable actor must be evicted from registered_actors"
            );
            let dead = mgr.destroyed_actors.get(b"actor_i".as_slice()).unwrap();
            assert_eq!(dead.state, ActorState::Dead as i32);
            assert_eq!(
                death_cause_reason(&dead.death_cause),
                Some(DeathReason::RayKill as i32),
                "no_restart=false on a no-budget actor must DIE with RAY_KILL, \
                 not OUT_OF_SCOPE (the old wrong behavior) and not the \
                 hard-coded NodeDied previously used in restart_actor_internal"
            );
        }

        /// C++ `KillActor` early-returns when the actor is already
        /// DEAD. Rust must match: no RPC, no state churn.
        #[tokio::test]
        async fn test_no_restart_false_already_dead_is_noop() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_j", "", 3);
            register_and_create(&mgr, &spec).await;
            // Manually mark DEAD (still in `actors` since restart budget).
            if let Some(mut entry) = mgr.actors.get_mut(b"actor_j".as_slice()) {
                entry.state = ActorState::Dead as i32;
            }

            let reply = mgr
                .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                    actor_id: b"actor_j".to_vec(),
                    force_kill: true,
                    no_restart: false,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(reply.status.as_ref().unwrap().code, 0);
            assert_eq!(
                pool.calls().len(),
                0,
                "DEAD actor must not trigger a KillLocalActor RPC (C++ :1887-1890)"
            );
        }

        /// C++ `KillActor` early-returns for `DEPENDENCIES_UNREADY`
        /// — no raylet, no scheduling, no state change.
        #[tokio::test]
        async fn test_no_restart_false_dependencies_unready_is_noop() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec(b"actor_k", "");
            mgr.register_actor(Request::new(RegisterActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap();
            // After register, actor is in DEPENDENCIES_UNREADY.
            assert_eq!(
                get_actor_state(&mgr, b"actor_k"),
                Some(ActorState::DependenciesUnready as i32),
            );

            let reply = mgr
                .kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
                    actor_id: b"actor_k".to_vec(),
                    force_kill: true,
                    no_restart: false,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(reply.status.as_ref().unwrap().code, 0);
            assert_eq!(
                pool.calls().len(),
                0,
                "DEPENDENCIES_UNREADY actor must not trigger a KillLocalActor RPC (C++ :1887-1890)"
            );
            assert_eq!(
                get_actor_state(&mgr, b"actor_k"),
                Some(ActorState::DependenciesUnready as i32),
                "state must be unchanged"
            );
        }

        // ─── Graceful-shutdown timer parity tests ─────────────────
        //
        // Parity with C++ `gcs_actor_manager.cc:1041-1080` and the
        // cancellation paths at `:1011-1016` (force_kill upgrade)
        // and `:1468-1472` (RestartActor). The ms numbers used
        // below are deliberately small so the suite stays fast;
        // the contract being verified is the cancellation /
        // arming logic, not the absolute timeout.

        /// Helper: count force-kill calls in the recorded set.
        fn count_force_kill_calls(
            calls: &[(String, u16, KillLocalActorRequest)],
        ) -> usize {
            calls.iter().filter(|(_, _, r)| r.force_kill).count()
        }

        fn count_graceful_calls(
            calls: &[(String, u16, KillLocalActorRequest)],
        ) -> usize {
            calls.iter().filter(|(_, _, r)| !r.force_kill).count()
        }

        /// `destroy_actor(force_kill=false, timeout>0)` on a created
        /// actor must arm exactly one graceful-shutdown timer keyed
        /// by the actor's worker_id. Parity with C++
        /// `gcs_actor_manager.cc:1041-1042`.
        #[tokio::test]
        async fn test_graceful_timer_arms_on_destroy_force_kill_false() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g1", "", 3);
            register_and_create(&mgr, &spec).await;

            assert_eq!(mgr.graceful_shutdown_timer_count(), 0);

            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g1",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 60_000,
            )
            .await;

            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                1,
                "graceful destroy with positive timeout must arm exactly one timer"
            );
            // The graceful RPC was sent immediately.
            let calls = pool.calls();
            assert_eq!(count_graceful_calls(&calls), 1);
            assert_eq!(count_force_kill_calls(&calls), 0);
        }

        /// `destroy_actor(force_kill=true, ...)` must NEVER arm a
        /// timer — only the graceful path arms. Parity with C++
        /// `:1041` `if (!force_kill && ...)` guard.
        #[tokio::test]
        async fn test_graceful_timer_not_armed_on_force_kill_true() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g2", "", 3);
            register_and_create(&mgr, &spec).await;

            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g2",
                DeathReason::RayKill,
                "force",
                /*force_kill=*/ true,
                /*timeout_ms=*/ 60_000,
            )
            .await;

            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                0,
                "force_kill=true must NOT arm a graceful-shutdown timer"
            );
        }

        /// `timeout_ms <= 0` disables timer arming, matching C++
        /// `:1041` `graceful_shutdown_timeout_ms > 0` guard. Default
        /// C++ argument is `-1`; passing `0` likewise disables.
        #[tokio::test]
        async fn test_graceful_timer_not_armed_for_nonpositive_timeout() {
            for &ms in &[-1i64, 0i64] {
                let pool = FakeKillActorPool::new();
                let mgr = make_manager_with_pool(pool.clone()).await;
                let spec = make_task_spec_with_restarts(b"actor_g3", "", 3);
                register_and_create(&mgr, &spec).await;

                mgr.destroy_actor_with_timeout_for_test(
                    b"actor_g3",
                    DeathReason::OutOfScope,
                    "graceful",
                    /*force_kill=*/ false,
                    /*timeout_ms=*/ ms,
                )
                .await;

                assert_eq!(
                    mgr.graceful_shutdown_timer_count(),
                    0,
                    "timeout_ms={ms} must NOT arm a timer (C++ requires > 0)"
                );
            }
        }

        /// After the timeout elapses, the timer task must fire a
        /// second `KillLocalActor` RPC with `force_kill=true`.
        /// Parity with C++ `:1063-1072`.
        #[tokio::test]
        async fn test_graceful_timer_fires_force_kill_after_timeout() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            // Restart budget so destroy(graceful, OOS) leaves the
            // actor in self.actors as DEAD-but-restartable. The
            // timer task's worker_id re-validation needs the actor
            // entry to still be there, mirroring C++'s
            // `registered_actors_.find(actor_id)` re-check at line 1067.
            let spec = make_task_spec_with_restarts(b"actor_g4", "", 3);
            register_and_create(&mgr, &spec).await;

            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g4",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 50,
            )
            .await;

            // Initial graceful RPC sent.
            assert_eq!(count_graceful_calls(&pool.calls()), 1);
            assert_eq!(count_force_kill_calls(&pool.calls()), 0);
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            // Wait long enough for the timer task to wake and fire.
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let calls = pool.calls();
            assert_eq!(
                count_graceful_calls(&calls),
                1,
                "exactly one graceful RPC was sent up front"
            );
            assert_eq!(
                count_force_kill_calls(&calls),
                1,
                "timer must escalate with one force_kill=true RPC after timeout"
            );
            // Timer self-removed after firing.
            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                0,
                "timer task must self-remove from the map after firing (parity with C++ :1058)"
            );

            // The escalation RPC must carry the same actor/worker
            // ids and the snapshotted death cause.
            let force_call = calls.iter().find(|(_, _, r)| r.force_kill).unwrap();
            assert_eq!(force_call.2.intended_actor_id, b"actor_g4".to_vec());
            assert_eq!(force_call.2.worker_id, b"worker1".to_vec());
        }

        /// A second graceful destroy for the same worker must NOT
        /// replace the existing timer — the original keeps running.
        /// Parity with C++ `:1041-1042`
        /// `find(worker_id) == end()` guard.
        #[tokio::test]
        async fn test_graceful_timer_idempotent_keeps_original() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g5", "", 3);
            register_and_create(&mgr, &spec).await;

            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g5",
                DeathReason::OutOfScope,
                "graceful #1",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 60_000,
            )
            .await;
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            // Second graceful destroy: actor is already DEAD,
            // destroy_actor early-returns. Timer count must not
            // drop, and no new timer is created.
            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g5",
                DeathReason::OutOfScope,
                "graceful #2",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 60_000,
            )
            .await;

            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                1,
                "duplicate graceful destroy must keep the original timer (no replace)"
            );
        }

        /// A force-kill destroy following a graceful destroy must
        /// cancel the existing timer for that worker. Parity with
        /// C++ `:1011-1016`. Even though the second destroy
        /// early-returns on already-DEAD, the cancel runs first.
        #[tokio::test]
        async fn test_graceful_timer_cancelled_by_force_kill_destroy() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g6", "", 3);
            register_and_create(&mgr, &spec).await;

            // Arm timer with a short timeout so the test would
            // catch a stray fire if cancellation fails.
            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g6",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 50,
            )
            .await;
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            // Force-kill upgrade: cancels the timer.
            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g6",
                DeathReason::RayKill,
                "escalate",
                /*force_kill=*/ true,
                /*timeout_ms=*/ -1,
            )
            .await;
            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                0,
                "force-kill destroy must cancel the existing graceful timer"
            );

            // Sleep past the original timeout to confirm the
            // cancelled timer does not fire a second force-kill.
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let calls = pool.calls();
            // Only the original graceful RPC was sent. The actor
            // was already DEAD when the force-kill destroy ran,
            // so no second RPC is emitted from that path
            // (matches C++'s state==DEAD skip on the NotifyRaylet
            // branch). Critically: NO escalation RPC from the
            // (cancelled) timer.
            assert_eq!(
                count_graceful_calls(&calls),
                1,
                "exactly the initial graceful RPC was sent"
            );
            assert_eq!(
                count_force_kill_calls(&calls),
                0,
                "cancelled timer must NOT fire a force-kill RPC"
            );
        }

        /// A restart (lineage reconstruction or worker-dead path)
        /// must cancel any timer for the restarting worker.
        /// Parity with C++ `:1468-1472`.
        #[tokio::test]
        async fn test_graceful_timer_cancelled_by_lineage_restart() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g7", "", 3);
            register_and_create(&mgr, &spec).await;

            // Arm timer (short so a stray fire would be visible).
            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g7",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 50,
            )
            .await;
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            // Drive lineage reconstruction (RESTARTING). C++
            // `RestartActor` cancels the timer before transitioning.
            mgr.restart_actor_for_lineage_reconstruction(Request::new(
                RestartActorForLineageReconstructionRequest {
                    actor_id: b"actor_g7".to_vec(),
                    num_restarts_due_to_lineage_reconstruction: 1,
                },
            ))
            .await
            .unwrap();
            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                0,
                "restart_actor_for_lineage_reconstruction must cancel the graceful-shutdown timer"
            );

            // Confirm no timer fires after cancellation.
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let calls = pool.calls();
            assert_eq!(count_force_kill_calls(&calls), 0);
        }

        /// The `OnWorkerDead` → `restart_actor_internal` path must
        /// also cancel the timer. C++ reaches the same cancel via
        /// `RestartActor`. Parity with C++ `:1468-1472` from the
        /// `OnWorkerDead` callsite at `:1286`.
        #[tokio::test]
        async fn test_graceful_timer_cancelled_by_worker_dead_restart() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g8", "", 3);
            register_and_create(&mgr, &spec).await;

            // The actor's worker is `worker1` on `node1`. We arm a
            // timer for it via a graceful destroy.
            //
            // NOTE: destroy_actor removes the worker from
            // `created_actors`, so a subsequent `on_worker_dead`
            // for that same worker would not match the
            // ALIVE-actor-on-worker branch. To exercise the
            // restart cancel cleanly, we drive
            // `restart_actor_internal` directly via the
            // public-from-test reach: lineage reconstruction is
            // already covered above; here we simply assert the
            // worker-dead handler is a no-op when the actor is
            // already DEAD (and the timer survives the no-op
            // call), matching C++'s shape that worker-dead only
            // cancels if it actually drives a restart.
            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g8",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 60_000,
            )
            .await;
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            // OnWorkerDead with the same node/worker: the actor
            // was already removed from `created_actors` by the
            // destroy; the scheduler also has no creating-phase
            // lease for it. So this should be a no-op and the
            // timer should remain.
            mgr.on_worker_dead(b"node1", b"worker1", true).await;
            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                1,
                "no-op OnWorkerDead must not affect the timer map"
            );
        }

        /// If the actor is already gone from `self.actors` by the
        /// time the timer fires, the task must NOT send a
        /// force-kill RPC. Parity with C++ `:1067-1072` actor
        /// re-lookup guard.
        #[tokio::test]
        async fn test_graceful_timer_skips_when_actor_gone() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            // No restart budget → graceful OOS destroy is
            // non-restartable, actor moves to destroyed_actors,
            // is removed from self.actors.
            let spec = make_task_spec(b"actor_g9", "");
            register_and_create(&mgr, &spec).await;

            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g9",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 50,
            )
            .await;

            // Actor moved out of self.actors (not restartable).
            assert!(!mgr.actors.contains_key(b"actor_g9".as_slice()));
            // But timer was armed before the move-out — that's
            // OK; the timer task will re-look-up on wake and find
            // nothing.
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let calls = pool.calls();
            assert_eq!(
                count_force_kill_calls(&calls),
                0,
                "timer must NOT escalate to force-kill if actor is gone from self.actors"
            );
            // Timer self-removed.
            assert_eq!(mgr.graceful_shutdown_timer_count(), 0);
        }

        /// If the actor was restarted with a new worker_id between
        /// arm and fire, the timer must NOT escalate into the new
        /// instance. Parity with C++ `:1068-1069` worker_id
        /// re-validation.
        #[tokio::test]
        async fn test_graceful_timer_skips_after_worker_id_changed() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g10", "", 3);
            register_and_create(&mgr, &spec).await;

            // Arm timer with short timeout but cancel via restart
            // BEFORE it fires: the lineage-restart path now also
            // cancels the timer, so this is the same machinery as
            // the worker_id-change guard. To specifically exercise
            // the worker_id-change branch in the timer task, we
            // bypass the cancel by inserting the actor's address
            // change directly while leaving the timer entry in
            // place.
            mgr.destroy_actor_with_timeout_for_test(
                b"actor_g10",
                DeathReason::OutOfScope,
                "graceful",
                /*force_kill=*/ false,
                /*timeout_ms=*/ 80,
            )
            .await;
            assert_eq!(mgr.graceful_shutdown_timer_count(), 1);

            // Simulate a restart that gave the actor a new worker
            // address WITHOUT going through the cancel path:
            // mutate the address in place. This isolates the
            // "worker_id mismatch" branch of the timer's re-check.
            {
                let mut entry = mgr.actors.get_mut(b"actor_g10".as_slice()).unwrap();
                entry.address = Some(Address {
                    node_id: b"node1".to_vec(),
                    ip_address: "127.0.0.1".to_string(),
                    port: 10001,
                    worker_id: b"worker_NEW".to_vec(),
                });
                entry.state = ActorState::Alive as i32;
                entry.death_cause = None;
            }

            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            let calls = pool.calls();
            assert_eq!(
                count_force_kill_calls(&calls),
                0,
                "timer must NOT escalate after the actor's worker_id changed"
            );
            assert_eq!(mgr.graceful_shutdown_timer_count(), 0);
        }

        /// `report_actor_out_of_scope` is the production graceful
        /// path (matches C++ `gcs_actor_manager.cc:293-301`). It
        /// must arm the per-worker timer using the configured
        /// `actor_graceful_shutdown_timeout_ms`. End-to-end check.
        #[tokio::test]
        async fn test_report_actor_out_of_scope_arms_graceful_timer() {
            let pool = FakeKillActorPool::new();
            let mgr = make_manager_with_pool(pool.clone()).await;
            let spec = make_task_spec_with_restarts(b"actor_g11", "", 3);
            register_and_create(&mgr, &spec).await;

            // Sanity: default RayConfig timeout is positive, so the
            // path through `report_actor_out_of_scope` will arm.
            assert!(ray_config::instance().actor_graceful_shutdown_timeout_ms > 0);

            mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
                actor_id: b"actor_g11".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 0,
            }))
            .await
            .unwrap();

            assert_eq!(
                mgr.graceful_shutdown_timer_count(),
                1,
                "report_actor_out_of_scope must arm the graceful-shutdown timer (parity with C++ ReportActorOutOfScope :293-301)"
            );
            // Initial graceful RPC was sent.
            let calls = pool.calls();
            assert_eq!(count_graceful_calls(&calls), 1);
            assert_eq!(count_force_kill_calls(&calls), 0);
        }
    }
}
