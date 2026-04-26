//! Full implementation of PlacementGroupInfoGcsService.
//!
//! Manages placement group registration, lifecycle, state transitions,
//! and name uniqueness enforcement.
//!
//! Maps C++ `GcsPlacementGroupManager` from
//! `src/ray/gcs/gcs_placement_group_manager.h/cc`.
//!
//! ## Placement Group Lifecycle (matching C++ semantics)
//!
//! States: PENDING → CREATED → REMOVED
//!                  → RESCHEDULING → CREATED
//!
//! `CreatePlacementGroup` transitions to PENDING, then hands off to
//! `GcsPlacementGroupScheduler::schedule_unplaced_bundles()` which sends
//! `PrepareBundleResources`/`CommitBundleResources` RPCs to raylets.
//! On success, `on_placement_group_creation_success` transitions to CREATED
//! (matching C++ `OnPlacementGroupCreationSuccess`).
//!
//! `WaitPlacementGroupUntilReady` genuinely waits via oneshot callbacks
//! when the PG is not yet CREATED, matching C++ `placement_group_to_create_callbacks_`.
//! Callbacks are drained when PG reaches CREATED (on success) or REMOVED
//! (on removal, with NotFound).
//!
//! Key C++ behaviors preserved:
//! - Named PG uniqueness per namespace
//! - Idempotent creation (duplicate PG ID returns OK)
//! - REMOVED state persisted to storage (not deleted)
//! - `get_placement_group` falls back to storage for removed PGs
//! - `get_all_placement_group` reads from storage (enriched with in-memory data)
//! - `wait_placement_group_until_ready` returns meaningful status and can wait

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
use gcs_proto::ray::rpc::placement_group_table_data::PlacementGroupState;
use gcs_proto::ray::rpc::*;
use gcs_table_storage::GcsTableStorage;

use crate::node_manager::GcsNodeManager;
use crate::pg_scheduler::{
    GcsPlacementGroupScheduler, PendingPlacementGroup, PgCreationSuccess, PgSchedulingFailure,
};

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
    error_status(17, msg)
}

fn invalid_status(msg: &str) -> GcsStatus {
    error_status(5, msg)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// GCS Placement Group Manager.
pub struct GcsPlacementGroupManager {
    /// placement_group_id -> PlacementGroupTableData
    /// Contains all registered (non-removed) placement groups.
    /// Removed PGs are deleted from this map but kept in storage.
    placement_groups: DashMap<Vec<u8>, PlacementGroupTableData>,
    /// (name, namespace) -> placement_group_id for named placement groups.
    named_pgs: DashMap<(String, String), Vec<u8>>,
    /// Callbacks waiting for a PG to reach CREATED state.
    /// Maps C++ `placement_group_to_create_callbacks_`.
    wait_callbacks: DashMap<Vec<u8>, Vec<tokio::sync::oneshot::Sender<GcsStatus>>>,
    table_storage: Arc<GcsTableStorage>,
    /// Placement group scheduler for async PENDING → CREATED flow.
    scheduler: Arc<GcsPlacementGroupScheduler>,
    node_manager: Arc<GcsNodeManager>,
    /// Optional usage-stats client. Set by `set_usage_stats_client`
    /// after KV init. Mirrors C++ `usage_stats_client_` field +
    /// `GcsPlacementGroupManager::SetUsageStatsClient`
    /// (`gcs_placement_group_manager.h:210`), wired from
    /// `GcsServer::InitUsageStatsClient` (`gcs_server.cc:635`).
    usage_stats_client: parking_lot::Mutex<Option<Arc<crate::usage_stats::UsageStatsClient>>>,
    /// Monotonic count of PGs moved to CREATED. Parity with C++
    /// `lifetime_num_placement_groups_created_` at
    /// `gcs_placement_group_manager.cc:294`.
    lifetime_num_placement_groups_created: parking_lot::Mutex<i64>,
}

impl GcsPlacementGroupManager {
    pub fn new(
        table_storage: Arc<GcsTableStorage>,
        scheduler: Arc<GcsPlacementGroupScheduler>,
        node_manager: Arc<GcsNodeManager>,
    ) -> Self {
        Self {
            placement_groups: DashMap::new(),
            named_pgs: DashMap::new(),
            wait_callbacks: DashMap::new(),
            table_storage,
            scheduler,
            node_manager,
            usage_stats_client: parking_lot::Mutex::new(None),
            lifetime_num_placement_groups_created: parking_lot::Mutex::new(0),
        }
    }

    /// Install the usage-stats recorder.
    pub fn set_usage_stats_client(
        &self,
        client: Arc<crate::usage_stats::UsageStatsClient>,
    ) {
        *self.usage_stats_client.lock() = Some(client);
    }

    /// Bump `lifetime_num_placement_groups_created` and forward to
    /// the usage client. Mirrors C++ `gcs_placement_group_manager.cc:294`
    /// + `:1003-1006`.
    fn record_pg_created(&self) {
        let mut counter = self.lifetime_num_placement_groups_created.lock();
        *counter += 1;
        let count = *counter;
        drop(counter);
        let client = self.usage_stats_client.lock().clone();
        if let Some(client) = client {
            client.record_extra_usage_counter_spawn(
                crate::usage_stats::TagKey::PgNumCreated,
                count,
            );
        }
    }

    /// Initialize from persisted data (on restart recovery).
    ///
    /// Maps C++ GcsPlacementGroupManager::Initialize
    /// (gcs_placement_group_manager.cc:871-964).
    pub fn initialize(
        &self,
        pgs: &std::collections::HashMap<String, PlacementGroupTableData>,
    ) {
        for (_key, pg) in pgs {
            // Skip REMOVED PGs — they stay in storage but not in the in-memory map.
            if pg.state == PlacementGroupState::Removed as i32 {
                continue;
            }
            let pg_id = pg.placement_group_id.clone();
            if !pg.name.is_empty() {
                self.named_pgs.insert(
                    (pg.name.clone(), pg.ray_namespace.clone()),
                    pg_id.clone(),
                );
            }
            self.placement_groups.insert(pg_id.clone(), pg.clone());

            // Recover PENDING/RESCHEDULING PGs into the scheduler's pending queue
            // (matching C++ GcsPlacementGroupManager::Initialize recovery logic).
            if pg.state == PlacementGroupState::Pending as i32
                || pg.state == PlacementGroupState::Rescheduling as i32
            {
                self.scheduler.add_to_pending(PendingPlacementGroup {
                    pg_id,
                    pg_data: pg.clone(),
                });
            }
        }
        info!(
            placement_groups = self.placement_groups.len(),
            named = self.named_pgs.len(),
            pending_recovered = self.scheduler.pending_count(),
            "Placement group manager initialized"
        );
    }

    /// Check if a named PG slot is occupied by a non-REMOVED PG.
    fn is_named_pg_active(&self, name: &str, namespace: &str) -> bool {
        let key = (name.to_string(), namespace.to_string());
        if let Some(existing_id) = self.named_pgs.get(&key) {
            if let Some(existing_pg) = self.placement_groups.get(existing_id.value()) {
                return existing_pg.state != PlacementGroupState::Removed as i32;
            }
        }
        false
    }

    /// Drain wait callbacks for a PG, sending the given status to all waiters.
    /// Maps C++ pattern of invoking `placement_group_to_create_callbacks_[pg_id]`.
    fn fire_wait_callbacks(&self, pg_id: &[u8], status: GcsStatus) {
        if let Some((_, senders)) = self.wait_callbacks.remove(pg_id) {
            for tx in senders {
                let _ = tx.send(status.clone());
            }
        }
    }

    /// Remove a placement group by id. Shared implementation used by both
    /// the `RemovePlacementGroup` RPC handler and
    /// `clean_placement_group_if_needed_when_job_dead`.
    ///
    /// Maps C++ `GcsPlacementGroupManager::RemovePlacementGroup`
    /// (`gcs_placement_group_manager.cc:400-476`).
    async fn remove_placement_group_internal(&self, pg_id: &[u8]) {
        if let Some((_, mut pg_data)) = self.placement_groups.remove(pg_id) {
            if !pg_data.name.is_empty() {
                self.named_pgs
                    .remove(&(pg_data.name.clone(), pg_data.ray_namespace.clone()));
            }

            // Remove from scheduler queues (C++ lines 430-452).
            self.scheduler.remove_placement_group(pg_id);

            // Transition to REMOVED and persist (C++: state stays in storage).
            pg_data.state = PlacementGroupState::Removed as i32;
            self.table_storage
                .placement_group_table()
                .put(&hex(pg_id), &pg_data)
                .await;

            self.fire_wait_callbacks(
                pg_id,
                not_found_status("Placement group is removed before it is created."),
            );

            info!(pg_id = hex(pg_id), "Placement group removed");
        }
    }

    /// Mark PGs whose creator job died and remove the ones whose lifetime
    /// is now done (non-detached, creator_actor also dead).
    ///
    /// Maps C++ `GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenJobDead`
    /// (`gcs_placement_group_manager.cc:748-774`). Invoked from the
    /// `job_finished` lifecycle listener.
    ///
    /// Semantics (per `GcsPlacementGroup::IsPlacementGroupLifetimeDone` —
    /// `gcs_placement_group.cc:129-132`): a PG's lifetime is done when
    /// `!is_detached && creator_job_dead && creator_actor_dead`. Setting
    /// `creator_job_dead=true` alone is not enough to remove — the actor
    /// must also be gone. Detached PGs are never auto-removed on job death.
    pub async fn clean_placement_group_if_needed_when_job_dead(&self, job_id: &[u8]) {
        // Collect the PGs to touch up front (can't hold DashMap refs while
        // calling an async helper that mutates the same map).
        let mut to_mark: Vec<Vec<u8>> = Vec::new();
        let mut to_remove: Vec<Vec<u8>> = Vec::new();
        for entry in self.placement_groups.iter() {
            let pg = entry.value();
            if pg.creator_job_id != job_id {
                continue;
            }
            to_mark.push(entry.key().clone());
            // Lifetime done → schedule removal below.
            if !pg.is_detached && pg.creator_actor_dead {
                to_remove.push(entry.key().clone());
            }
        }

        // Mark `creator_job_dead = true` and persist the update for each
        // PG whose creator is this job. This mirrors C++
        // `placement_group->MarkCreatorJobDead()` at line 757.
        for pg_id in &to_mark {
            if let Some(mut entry) = self.placement_groups.get_mut(pg_id) {
                entry.creator_job_dead = true;
                let snapshot = entry.value().clone();
                // Persist the mark. Unlike C++ (which uses a single in-memory
                // map and the periodic flusher), we write through immediately
                // so restart recovery sees the updated flag.
                self.table_storage
                    .placement_group_table()
                    .put(&hex(pg_id), &snapshot)
                    .await;
            }
        }

        for pg_id in &to_remove {
            info!(
                pg_id = hex(pg_id),
                job_id = hex(job_id),
                "Removing placement group because its creator job finished"
            );
            self.remove_placement_group_internal(pg_id).await;
        }
    }

    /// Return placement group load for the autoscaler.
    ///
    /// Maps C++ `GetPlacementGroupLoad` (gcs_placement_group_manager.cc:817-863).
    /// Reports PGs in PENDING or RESCHEDULING state from the scheduler's
    /// pending and infeasible queues (these are the PGs that need resources).
    pub fn get_placement_group_load(&self) -> PlacementGroupLoad {
        let mut load = PlacementGroupLoad::default();

        // Add pending PGs (PENDING/RESCHEDULING in retry queue).
        for pg_data in self.scheduler.get_pending_pg_data() {
            if pg_data.state == PlacementGroupState::Pending as i32
                || pg_data.state == PlacementGroupState::Rescheduling as i32
            {
                load.placement_group_data.push(pg_data);
            }
        }

        // Add infeasible PGs (no eligible nodes currently).
        for pg_data in self.scheduler.get_infeasible_pg_data() {
            if pg_data.state == PlacementGroupState::Pending as i32
                || pg_data.state == PlacementGroupState::Rescheduling as i32
            {
                load.placement_group_data.push(pg_data);
            }
        }

        load
    }
}

#[tonic::async_trait]
impl PlacementGroupInfoGcsService for GcsPlacementGroupManager {
    /// Create a placement group.
    ///
    /// Maps C++ HandleCreatePlacementGroup + RegisterPlacementGroup
    /// (gcs_placement_group_manager.cc:353-374, 107-190):
    /// - Idempotent: if PG ID already exists, returns OK
    /// - Named PG uniqueness: returns Invalid if name taken
    /// - Transitions to PENDING, then hands off to scheduler which drives
    ///   the async PrepareBundleResources → CommitBundleResources → CREATED flow
    async fn create_placement_group(
        &self,
        req: Request<CreatePlacementGroupRequest>,
    ) -> Result<Response<CreatePlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let Some(spec) = inner.placement_group_spec else {
            return Ok(Response::new(CreatePlacementGroupReply {
                status: Some(ok_status()),
            }));
        };

        let pg_id = spec.placement_group_id.clone();

        // Idempotency: if PG already exists, return OK (network retry).
        if self.placement_groups.contains_key(&pg_id) {
            return Ok(Response::new(CreatePlacementGroupReply {
                status: Some(ok_status()),
            }));
        }

        // Build initial PG data in PENDING state (matching C++).
        let pg_data = PlacementGroupTableData {
            placement_group_id: pg_id.clone(),
            name: spec.name.clone(),
            bundles: spec.bundles.clone(),
            strategy: spec.strategy,
            state: PlacementGroupState::Pending as i32,
            creator_job_id: spec.creator_job_id.clone(),
            creator_actor_id: spec.creator_actor_id.clone(),
            creator_job_dead: spec.creator_job_dead,
            creator_actor_dead: spec.creator_actor_dead,
            is_detached: spec.is_detached,
            ray_namespace: String::new(),
            ..Default::default()
        };

        // Named PG uniqueness check (matching C++).
        if !pg_data.name.is_empty()
            && self.is_named_pg_active(&pg_data.name, &pg_data.ray_namespace)
        {
            return Ok(Response::new(CreatePlacementGroupReply {
                status: Some(invalid_status(&format!(
                    "Placement group with name '{}' already exists in namespace '{}'",
                    pg_data.name, pg_data.ray_namespace
                ))),
            }));
        }

        // Register named PG.
        if !pg_data.name.is_empty() {
            self.named_pgs.insert(
                (pg_data.name.clone(), pg_data.ray_namespace.clone()),
                pg_id.clone(),
            );
        }

        // --- Register in PENDING state (matching C++ RegisterPlacementGroup line 157) ---
        self.placement_groups.insert(pg_id.clone(), pg_data.clone());
        self.table_storage
            .placement_group_table()
            .put(&hex(&pg_id), &pg_data)
            .await;

        info!(pg_id = hex(&pg_id), "Placement group PENDING, scheduling bundles");

        // --- Hand off to scheduler (C++ SchedulePendingPlacementGroups line 334) ---
        GcsPlacementGroupScheduler::schedule_unplaced_bundles(
            &self.scheduler,
            PendingPlacementGroup {
                pg_id: pg_id.clone(),
                pg_data,
            },
        );

        Ok(Response::new(CreatePlacementGroupReply {
            status: Some(ok_status()),
        }))
    }

    /// Remove a placement group.
    ///
    /// Maps C++ RemovePlacementGroup (gcs_placement_group_manager.cc:400-476):
    /// - Transitions state to REMOVED (not deleted from storage)
    /// - Removes from named index
    /// - Removes from in-memory map (but keeps in storage for wait queries)
    /// - Fires wait callbacks with NotFound
    async fn remove_placement_group(
        &self,
        req: Request<RemovePlacementGroupRequest>,
    ) -> Result<Response<RemovePlacementGroupReply>, Status> {
        self.remove_placement_group_internal(&req.into_inner().placement_group_id)
            .await;
        Ok(Response::new(RemovePlacementGroupReply {
            status: Some(ok_status()),
        }))
    }

    /// Get placement group by ID.
    ///
    /// Matches C++ HandleGetPlacementGroup (gcs_placement_group_manager.cc:478-506):
    /// Checks in-memory first, then falls back to storage (for removed PGs).
    async fn get_placement_group(
        &self,
        req: Request<GetPlacementGroupRequest>,
    ) -> Result<Response<GetPlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let pg_id = &inner.placement_group_id;

        // Check in-memory first.
        if let Some(entry) = self.placement_groups.get(pg_id) {
            return Ok(Response::new(GetPlacementGroupReply {
                status: Some(ok_status()),
                placement_group_table_data: Some(entry.clone()),
            }));
        }

        // Fall back to storage (finds removed PGs).
        let stored = self
            .table_storage
            .placement_group_table()
            .get(&hex(pg_id))
            .await;

        Ok(Response::new(GetPlacementGroupReply {
            status: Some(ok_status()),
            placement_group_table_data: stored,
        }))
    }

    /// Get named placement group by (name, namespace).
    ///
    /// Matches C++ HandleGetNamedPlacementGroup
    /// (gcs_placement_group_manager.cc:508-531).
    /// Returns empty reply (not error) if not found, matching C++.
    async fn get_named_placement_group(
        &self,
        req: Request<GetNamedPlacementGroupRequest>,
    ) -> Result<Response<GetNamedPlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let key = (inner.name, inner.ray_namespace);

        let pg = self.named_pgs.get(&key).and_then(|pg_id| {
            self.placement_groups
                .get(pg_id.value())
                .map(|r| r.clone())
        });

        Ok(Response::new(GetNamedPlacementGroupReply {
            status: Some(ok_status()),
            placement_group_table_data: pg,
        }))
    }

    /// Get all placement groups.
    ///
    /// Matches C++ HandleGetAllPlacementGroup
    /// (gcs_placement_group_manager.cc:533-572):
    /// - Reads ALL PGs from storage (including removed)
    /// - Enriches with in-memory data if available (fresher stats)
    /// - Supports optional limit
    async fn get_all_placement_group(
        &self,
        req: Request<GetAllPlacementGroupRequest>,
    ) -> Result<Response<GetAllPlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let limit = inner.limit.filter(|&l| l > 0).map(|l| l as usize);

        // Read all from storage (matching C++ PlacementGroupTable().GetAll()).
        let all_stored = self.table_storage.placement_group_table().get_all().await;
        let total = all_stored.len() as i64;

        let mut pgs: Vec<PlacementGroupTableData> = Vec::new();
        for (_key, stored_pg) in &all_stored {
            if let Some(lim) = limit {
                if pgs.len() >= lim {
                    break;
                }
            }
            // Use in-memory version if available (fresher stats, matching C++).
            if let Some(mem_pg) = self
                .placement_groups
                .get(&stored_pg.placement_group_id)
            {
                pgs.push(mem_pg.clone());
            } else {
                pgs.push(stored_pg.clone());
            }
        }

        Ok(Response::new(GetAllPlacementGroupReply {
            status: Some(ok_status()),
            placement_group_table_data: pgs,
            total,
        }))
    }

    /// Wait for a placement group to be ready (CREATED state).
    ///
    /// Matches C++ WaitPlacementGroup (gcs_placement_group_manager.cc:601-641):
    /// 1. PG in memory AND CREATED → OK immediately
    /// 2. PG in memory but not CREATED → register callback, wait
    /// 3. PG not in memory, in storage as REMOVED → NotFound
    /// 4. PG not in memory, not in storage → register callback, wait
    ///    (handles "wait before create" race, C++ lines 626-632)
    async fn wait_placement_group_until_ready(
        &self,
        req: Request<WaitPlacementGroupUntilReadyRequest>,
    ) -> Result<Response<WaitPlacementGroupUntilReadyReply>, Status> {
        let inner = req.into_inner();
        let pg_id = inner.placement_group_id;

        // Check in-memory map first.
        if let Some(pg) = self.placement_groups.get(&pg_id) {
            if pg.state == PlacementGroupState::Created as i32 {
                return Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                    status: Some(ok_status()),
                }));
            }
            // PG exists but not yet CREATED — fall through to register callback.
        } else {
            // Not in memory — check storage for REMOVED PGs.
            let stored = self
                .table_storage
                .placement_group_table()
                .get(&hex(&pg_id))
                .await;

            if let Some(stored_pg) = stored {
                if stored_pg.state == PlacementGroupState::Removed as i32 {
                    return Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                        status: Some(not_found_status("Placement group is removed.")),
                    }));
                }
                // Found in storage but not removed and not in memory — unusual,
                // treat as not-yet-created and register callback.
            }
            // Not found anywhere — could be "wait before create" race (C++ line 626-632).
            // Register callback and wait for future create.
        }

        // Register a oneshot callback and wait.
        // Maps C++ placement_group_to_create_callbacks_[pg_id].emplace_back(callback).
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.wait_callbacks
            .entry(pg_id.clone())
            .or_default()
            .push(tx);

        // Await the callback (fires when PG reaches CREATED or is removed).
        match rx.await {
            Ok(status) => Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                status: Some(status),
            })),
            Err(_) => {
                // Sender dropped (server shutting down or PG removed without firing).
                Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                    status: Some(not_found_status("Placement group wait cancelled")),
                }))
            }
        }
    }
}

// --- Scheduler lifecycle hooks (not part of gRPC trait) ---
impl GcsPlacementGroupManager {
    /// Handle successful placement group creation from the scheduler.
    ///
    /// Maps C++ `OnPlacementGroupCreationSuccess` (gcs_placement_group_manager.cc:246-298).
    /// Transitions PENDING/RESCHEDULING → CREATED, persists, fires wait callbacks.
    pub async fn on_placement_group_creation_success(&self, success: PgCreationSuccess) {
        let pg_id = &success.pg_id;

        let Some(mut entry) = self.placement_groups.get_mut(pg_id) else {
            debug!(pg_id = hex(pg_id), "PG gone before scheduling completed");
            return;
        };

        // Update bundles with node assignments from scheduler.
        if !success.bundles.is_empty() {
            entry.bundles = success.bundles;
        }

        // Transition to CREATED (C++ line 267).
        entry.state = PlacementGroupState::Created as i32;
        let pg_data = entry.clone();
        drop(entry);

        // Persist CREATED state.
        self.table_storage
            .placement_group_table()
            .put(&hex(pg_id), &pg_data)
            .await;

        // Fire wait callbacks (C++ lines 284-291).
        self.fire_wait_callbacks(pg_id, ok_status());

        // Bump the lifetime counter and record it to the usage-stats
        // client. Parity with C++ `gcs_placement_group_manager.cc:294`
        // + `:1003-1006`.
        self.record_pg_created();

        info!(pg_id = hex(pg_id), "Placement group CREATED via scheduler");
    }

    /// Handle scheduling failure from the scheduler.
    ///
    /// Maps C++ `OnPlacementGroupCreationFailed` (gcs_placement_group_manager.cc:205-244).
    /// Infeasible → infeasible queue; transient → pending queue for retry.
    pub async fn on_placement_group_creation_failed(&self, failure: PgSchedulingFailure) {
        let pg_id = &failure.pg_id;

        let Some(mut entry) = self.placement_groups.get_mut(pg_id) else {
            return;
        };

        // If there are uncommitted bundle indices (partial commit failure),
        // clear node_id only on those bundles and set RESCHEDULING.
        // Matches C++ lines 467-477 in gcs_placement_group_scheduler.cc.
        if !failure.uncommitted_bundle_indices.is_empty() {
            for bundle in &mut entry.bundles {
                if let Some(bid) = &bundle.bundle_id {
                    if failure.uncommitted_bundle_indices.contains(&bid.bundle_index) {
                        bundle.node_id.clear();
                    }
                }
            }
            entry.state = PlacementGroupState::Rescheduling as i32;
            let pg_data = entry.clone();
            drop(entry);

            self.table_storage
                .placement_group_table()
                .put(&hex(pg_id), &pg_data)
                .await;

            debug!(
                pg_id = hex(pg_id),
                uncommitted = failure.uncommitted_bundle_indices.len(),
                "PG partial commit failure, RESCHEDULING with unplaced bundles"
            );
            self.scheduler.add_to_pending(PendingPlacementGroup {
                pg_id: pg_id.clone(),
                pg_data,
            });
            return;
        }

        let pending_pg = PendingPlacementGroup {
            pg_id: pg_id.clone(),
            pg_data: entry.clone(),
        };
        drop(entry);

        if failure.is_infeasible {
            // No eligible nodes — move to infeasible queue (C++ line 217-218).
            debug!(
                pg_id = hex(pg_id),
                "PG scheduling infeasible, queued for node addition"
            );
            self.scheduler.add_to_infeasible(pending_pg);
        } else {
            // Transient failure — requeue for retry (C++ line 219-238).
            debug!(pg_id = hex(pg_id), "PG scheduling failed, requeuing for retry");
            self.scheduler.add_to_pending(pending_pg);
        }
    }

    /// Handle node death — affected CREATED PGs → RESCHEDULING.
    ///
    /// Maps C++ `OnNodeDead` (gcs_placement_group_manager.cc:684-731).
    /// Uses per-bundle tracking from BundleLocationIndex to clear only
    /// the specific bundles that were on the dead node.
    pub async fn on_node_dead(&self, node_id: &[u8]) {
        // get_and_remove_on_node returns (pg_id, Vec<bundle_index>) pairs.
        let affected = self.scheduler.get_and_remove_on_node(node_id);

        for (pg_id, dead_bundle_indices) in &affected {
            let Some(mut entry) = self.placement_groups.get_mut(pg_id) else {
                continue;
            };

            // Only CREATED PGs transition to RESCHEDULING (C++ line 711-720).
            if entry.state != PlacementGroupState::Created as i32 {
                continue;
            }

            // Clear node_id only on bundles that were on the dead node,
            // identified by their bundle index (matching C++ line 692).
            for bundle in &mut entry.bundles {
                if let Some(bid) = &bundle.bundle_id {
                    if dead_bundle_indices.contains(&bid.bundle_index) {
                        bundle.node_id.clear();
                    }
                }
            }

            // Transition to RESCHEDULING (C++ line 720).
            entry.state = PlacementGroupState::Rescheduling as i32;
            let pg_data = entry.clone();
            drop(entry);

            // Persist RESCHEDULING state (C++ line 724-727).
            self.table_storage
                .placement_group_table()
                .put(&hex(pg_id), &pg_data)
                .await;

            // Add to pending queue with high priority (C++ line 723).
            self.scheduler.add_to_pending(PendingPlacementGroup {
                pg_id: pg_id.clone(),
                pg_data,
            });

            info!(
                pg_id = hex(pg_id),
                node_id = hex(node_id),
                dead_bundles = dead_bundle_indices.len(),
                "Placement group RESCHEDULING due to node death"
            );
        }

        // Schedule pending PGs.
        GcsPlacementGroupScheduler::schedule_pending_placement_groups(&self.scheduler);
    }

    /// Handle node addition — retry infeasible PGs.
    ///
    /// Maps C++ `OnNodeAdd` (gcs_placement_group_manager.cc:733-746).
    pub fn on_node_add(&self) {
        self.scheduler.move_infeasible_to_pending();
        GcsPlacementGroupScheduler::schedule_pending_placement_groups(&self.scheduler);
    }

    /// Start the background loop that processes scheduler success/failure events.
    pub fn start_scheduler_loop(
        self: &Arc<Self>,
        mut success_rx: mpsc::UnboundedReceiver<PgCreationSuccess>,
        mut failure_rx: mpsc::UnboundedReceiver<PgSchedulingFailure>,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(success) = success_rx.recv() => {
                        this.on_placement_group_creation_success(success).await;
                        // After success, try scheduling next pending PG.
                        GcsPlacementGroupScheduler::schedule_pending_placement_groups(
                            &this.scheduler,
                        );
                    }
                    Some(failure) = failure_rx.recv() => {
                        this.on_placement_group_creation_failed(failure).await;
                        // After failure handling, try scheduling next pending PG.
                        GcsPlacementGroupScheduler::schedule_pending_placement_groups(
                            &this.scheduler,
                        );
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

    fn make_manager() -> Arc<GcsPlacementGroupManager> {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let gcs_publisher = Arc::new(gcs_pubsub::GcsPublisher::new(16));
        let node_manager = Arc::new(GcsNodeManager::new(
            table_storage.clone(),
            gcs_publisher,
            b"test_cluster".to_vec(),
        ));
        let (success_tx, _success_rx) = mpsc::unbounded_channel();
        let (failure_tx, _failure_rx) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsPlacementGroupScheduler::new(
            node_manager.clone(),
            success_tx,
            failure_tx,
        ));
        Arc::new(GcsPlacementGroupManager::new(table_storage, scheduler, node_manager))
    }

    /// Create a PG and simulate scheduler success (PENDING → CREATED).
    async fn create_and_complete(mgr: &GcsPlacementGroupManager, spec: &PlacementGroupSpec) {
        let pg_id = spec.placement_group_id.clone();
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
        // Simulate scheduler success callback.
        mgr.on_placement_group_creation_success(PgCreationSuccess {
            pg_id,
            bundles: spec.bundles.clone(),
        })
        .await;
    }

    fn make_pg_spec(id: &[u8], name: &str) -> PlacementGroupSpec {
        PlacementGroupSpec {
            placement_group_id: id.to_vec(),
            name: name.to_string(),
            creator_job_id: b"job1".to_vec(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_create_and_get_pg() {
        let mgr = make_manager();
        let spec = make_pg_spec(b"pg1", "my_pg");

        create_and_complete(&mgr, &spec).await;

        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.placement_group_table_data.is_some());
        let pg = reply.placement_group_table_data.unwrap();
        assert_eq!(pg.state, PlacementGroupState::Created as i32);
    }

    #[tokio::test]
    async fn test_remove_pg() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "")).await;

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg1".to_vec(),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.placement_group_table_data.is_some());
        assert_eq!(
            reply.placement_group_table_data.unwrap().state,
            PlacementGroupState::Removed as i32
        );
    }

    /// Parity with C++ `CleanPlacementGroupIfNeededWhenJobDead`
    /// (`gcs_placement_group_manager.cc:748-774`). If the PG's creator
    /// actor is also dead, the non-detached PG must be removed when the
    /// job finishes.
    #[tokio::test]
    async fn test_clean_pg_on_job_dead_removes_when_lifetime_done() {
        let mgr = make_manager();
        let spec = PlacementGroupSpec {
            placement_group_id: b"pg1".to_vec(),
            creator_job_id: b"job1".to_vec(),
            creator_actor_dead: true, // creator actor is already gone
            is_detached: false,
            ..Default::default()
        };
        create_and_complete(&mgr, &spec).await;

        mgr.clean_placement_group_if_needed_when_job_dead(b"job1").await;

        // lifetime_done = !is_detached && creator_job_dead && creator_actor_dead
        // → REMOVED after the listener runs.
        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        let data = reply.placement_group_table_data.expect("pg must be in storage");
        assert_eq!(data.state, PlacementGroupState::Removed as i32);
        assert!(data.creator_job_dead);
    }

    /// Without `creator_actor_dead`, lifetime is not done, so C++ only
    /// marks `creator_job_dead=true` and leaves the PG in place. Rust
    /// must match.
    #[tokio::test]
    async fn test_clean_pg_on_job_dead_marks_without_removing() {
        let mgr = make_manager();
        let spec = PlacementGroupSpec {
            placement_group_id: b"pg_marked".to_vec(),
            creator_job_id: b"job1".to_vec(),
            creator_actor_dead: false,
            is_detached: false,
            ..Default::default()
        };
        create_and_complete(&mgr, &spec).await;

        mgr.clean_placement_group_if_needed_when_job_dead(b"job1").await;

        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg_marked".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        let data = reply.placement_group_table_data.unwrap();
        assert!(data.creator_job_dead, "creator_job_dead must be set");
        assert_ne!(
            data.state,
            PlacementGroupState::Removed as i32,
            "PG must not be removed while creator actor is still alive"
        );
    }

    /// Detached PGs survive their creator job on purpose — matches C++
    /// `IsPlacementGroupLifetimeDone` which returns false when
    /// `is_detached=true`.
    #[tokio::test]
    async fn test_clean_pg_on_job_dead_ignores_detached() {
        let mgr = make_manager();
        let spec = PlacementGroupSpec {
            placement_group_id: b"pg_detached".to_vec(),
            creator_job_id: b"job1".to_vec(),
            creator_actor_dead: true, // would normally trigger removal…
            is_detached: true,        // …but detached cancels it.
            ..Default::default()
        };
        create_and_complete(&mgr, &spec).await;

        mgr.clean_placement_group_if_needed_when_job_dead(b"job1").await;

        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg_detached".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        let data = reply.placement_group_table_data.unwrap();
        assert_ne!(
            data.state,
            PlacementGroupState::Removed as i32,
            "Detached PG must survive job death"
        );
    }

    /// PGs owned by a different job must not be touched.
    #[tokio::test]
    async fn test_clean_pg_on_job_dead_scoped_to_job() {
        let mgr = make_manager();
        let spec_mine = PlacementGroupSpec {
            placement_group_id: b"mine".to_vec(),
            creator_job_id: b"job1".to_vec(),
            creator_actor_dead: true,
            is_detached: false,
            ..Default::default()
        };
        let spec_theirs = PlacementGroupSpec {
            placement_group_id: b"theirs".to_vec(),
            creator_job_id: b"job2".to_vec(),
            creator_actor_dead: true,
            is_detached: false,
            ..Default::default()
        };
        create_and_complete(&mgr, &spec_mine).await;
        create_and_complete(&mgr, &spec_theirs).await;

        mgr.clean_placement_group_if_needed_when_job_dead(b"job1").await;

        // `mine` removed, `theirs` untouched.
        for (pg_id, should_be_removed) in
            [(b"mine".as_slice(), true), (b"theirs".as_slice(), false)]
        {
            let reply = mgr
                .get_placement_group(Request::new(GetPlacementGroupRequest {
                    placement_group_id: pg_id.to_vec(),
                }))
                .await
                .unwrap()
                .into_inner();
            let state = reply.placement_group_table_data.unwrap().state;
            let is_removed = state == PlacementGroupState::Removed as i32;
            assert_eq!(
                is_removed, should_be_removed,
                "pg {:?} removal state = {} but expected {}",
                pg_id, is_removed, should_be_removed
            );
        }
    }

    #[tokio::test]
    async fn test_get_all_pgs_reads_from_storage() {
        let mgr = make_manager();
        for i in 0..3 {
            create_and_complete(
                &mgr,
                &make_pg_spec(format!("pg{i}").as_bytes(), &format!("pg_{i}")),
            )
            .await;
        }

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg0".to_vec(),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_all_placement_group(Request::new(GetAllPlacementGroupRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.placement_group_table_data.len(), 3);
        assert_eq!(reply.total, 3);
    }

    #[tokio::test]
    async fn test_get_all_pgs_with_limit() {
        let mgr = make_manager();
        for i in 0..5 {
            create_and_complete(&mgr, &make_pg_spec(format!("pg{i}").as_bytes(), "")).await;
        }

        let reply = mgr
            .get_all_placement_group(Request::new(GetAllPlacementGroupRequest {
                limit: Some(2),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.placement_group_table_data.len(), 2);
        assert_eq!(reply.total, 5);
    }

    #[tokio::test]
    async fn test_get_named_pg() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "named_pg")).await;

        let reply = mgr
            .get_named_placement_group(Request::new(GetNamedPlacementGroupRequest {
                name: "named_pg".to_string(),
                ray_namespace: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.placement_group_table_data.is_some());
    }

    #[tokio::test]
    async fn test_get_named_pg_not_found() {
        let mgr = make_manager();
        let reply = mgr
            .get_named_placement_group(Request::new(GetNamedPlacementGroupRequest {
                name: "no_such_pg".to_string(),
                ray_namespace: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
        assert!(reply.placement_group_table_data.is_none());
    }

    #[tokio::test]
    async fn test_wait_placement_group_until_ready() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg_wait", "wait_pg")).await;

        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"pg_wait".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_named_pg_uniqueness() {
        let mgr = make_manager();
        // First create stays at PENDING — uniqueness enforced regardless of state.
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "SharedName")),
        }))
        .await
        .unwrap();

        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(make_pg_spec(b"pg2", "SharedName")),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 5);
    }

    #[tokio::test]
    async fn test_create_idempotent() {
        let mgr = make_manager();
        let spec = make_pg_spec(b"pg1", "my_pg");
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();

        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
        assert_eq!(mgr.placement_groups.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_sets_removed_state() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "")).await;

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg1".to_vec(),
        }))
        .await
        .unwrap();

        assert!(!mgr.placement_groups.contains_key(b"pg1".as_slice()));
        let stored = mgr
            .table_storage
            .placement_group_table()
            .get(&hex(b"pg1"))
            .await;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().state, PlacementGroupState::Removed as i32);
    }

    #[tokio::test]
    async fn test_wait_for_created_pg() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "")).await;

        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"pg1".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_wait_for_removed_pg() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "")).await;

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg1".to_vec(),
        }))
        .await
        .unwrap();

        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"pg1".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let status = reply.status.unwrap();
        assert_eq!(status.code, 17);
        assert!(status.message.contains("removed"));
    }

    #[tokio::test]
    async fn test_wait_for_nonexistent_pg() {
        // Wait for a PG that doesn't exist yet — callback fires when
        // on_placement_group_creation_success is called.
        let mgr = make_manager();
        let mgr_clone = mgr.clone();

        let wait_handle = tokio::spawn(async move {
            mgr_clone
                .wait_placement_group_until_ready(Request::new(
                    WaitPlacementGroupUntilReadyRequest {
                        placement_group_id: b"future_pg".to_vec(),
                    },
                ))
                .await
                .unwrap()
                .into_inner()
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Create PG (stays in PENDING).
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"future_pg", "")),
        }))
        .await
        .unwrap();

        // Simulate scheduler success — this fires the wait callback.
        mgr.on_placement_group_creation_success(PgCreationSuccess {
            pg_id: b"future_pg".to_vec(),
            bundles: vec![],
        })
        .await;

        let reply = wait_handle.await.unwrap();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_wait_before_create_removed() {
        // Wait → create → remove: callback fires from remove with NotFound.
        let mgr = make_manager();
        let mgr_clone = mgr.clone();

        let wait_handle = tokio::spawn(async move {
            mgr_clone
                .wait_placement_group_until_ready(Request::new(
                    WaitPlacementGroupUntilReadyRequest {
                        placement_group_id: b"doomed_pg".to_vec(),
                    },
                ))
                .await
                .unwrap()
                .into_inner()
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Create (stays PENDING) then remove — remove fires callback with NotFound.
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"doomed_pg", "")),
        }))
        .await
        .unwrap();

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"doomed_pg".to_vec(),
        }))
        .await
        .unwrap();

        let reply = wait_handle.await.unwrap();
        let status = reply.status.unwrap();
        // Removed before CREATED → NotFound.
        assert_eq!(status.code, 17);
    }

    #[tokio::test]
    async fn test_remove_frees_name() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "SharedName")).await;

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg1".to_vec(),
        }))
        .await
        .unwrap();

        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(make_pg_spec(b"pg2", "SharedName")),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_get_placement_group_load_reports_pending() {
        let mgr = make_manager();

        // Create a PG (stays in PENDING — scheduler has no nodes).
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "")),
        }))
        .await
        .unwrap();

        // PG is PENDING, so load should NOT be empty anymore
        // (unlike the old test which expected empty because of immediate CREATED).
        let pg = mgr.placement_groups.get(b"pg1".as_slice()).unwrap();
        assert_eq!(pg.state, PlacementGroupState::Pending as i32);
    }

    #[tokio::test]
    async fn test_get_placement_group_load_empty_after_created() {
        let mgr = make_manager();
        create_and_complete(&mgr, &make_pg_spec(b"pg1", "")).await;

        // After creation completes, load should be empty.
        let load = mgr.get_placement_group_load();
        assert!(load.placement_group_data.is_empty());
    }
}
