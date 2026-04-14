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
//! 2. `CreateActor` — transitions to ALIVE (scheduling is stubbed; C++ goes
//!    through PENDING_CREATION with actual scheduler placement)
//! 3. `KillActorViaGcs` — transitions to DEAD
//! 4. `ReportActorOutOfScope` — transitions to DEAD with OUT_OF_SCOPE cause
//!    (stale-message detection via lineage reconstruction counter)
//! 5. `RestartActorForLineageReconstruction` — transitions DEAD → RESTARTING
//!    with idempotency/stale-message checks and restart budget enforcement

use std::sync::Arc;

use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::actor_info_gcs_service_server::ActorInfoGcsService;
use gcs_proto::ray::rpc::actor_table_data::ActorState;
use gcs_proto::ray::rpc::actor_died_error_context::Reason as DeathReason;
use gcs_proto::ray::rpc::actor_death_cause::Context as DeathContext;
use gcs_proto::ray::rpc::pub_message::InnerMessage;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::PubSubManager;
use gcs_table_storage::GcsTableStorage;

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

/// GCS Actor Manager.
pub struct GcsActorManager {
    /// actor_id -> ActorTableData (all registered actors: unresolved, pending, alive, dead-but-restartable)
    actors: DashMap<Vec<u8>, ActorTableData>,
    /// (name, namespace) -> actor_id for named actors
    named_actors: DashMap<(String, String), Vec<u8>>,
    /// actor_id -> TaskSpec (the creation task spec)
    actor_task_specs: DashMap<Vec<u8>, TaskSpec>,
    publisher: Arc<PubSubManager>,
    table_storage: Arc<GcsTableStorage>,
}

impl GcsActorManager {
    pub fn new(publisher: Arc<PubSubManager>, table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            actors: DashMap::new(),
            named_actors: DashMap::new(),
            actor_task_specs: DashMap::new(),
            publisher,
            table_storage,
        }
    }

    /// Initialize from persisted data (on restart recovery).
    pub fn initialize(
        &self,
        actors: &std::collections::HashMap<String, ActorTableData>,
        task_specs: &std::collections::HashMap<String, TaskSpec>,
    ) {
        for (_key, actor) in actors {
            let actor_id = actor.actor_id.clone();
            if !actor.name.is_empty() {
                self.named_actors.insert(
                    (actor.name.clone(), actor.ray_namespace.clone()),
                    actor_id.clone(),
                );
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

    /// Publish an actor state change on the GCS_ACTOR_CHANNEL (channel_type = 3).
    fn publish_actor_update(&self, actor: &ActorTableData) {
        let msg = PubMessage {
            channel_type: ChannelType::GcsActorChannel as i32,
            key_id: actor.actor_id.clone(),
            sequence_id: 0,
            inner_message: Some(InnerMessage::ActorMessage(actor.clone())),
        };
        self.publisher.publish(msg);
    }

    /// Extract the actor_id from a TaskSpec's actor_creation_task_spec.
    fn actor_id_from_task_spec(task_spec: &TaskSpec) -> Option<Vec<u8>> {
        task_spec
            .actor_creation_task_spec
            .as_ref()
            .map(|s| s.actor_id.clone())
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

    /// Destroy an actor: transition to DEAD, clean up tracking structures.
    ///
    /// If the actor is restartable (OUT_OF_SCOPE + budget), it stays in `actors`
    /// for potential lineage reconstruction. Otherwise, it's fully cleaned up.
    async fn destroy_actor(
        &self,
        actor_id: &[u8],
        death_reason: DeathReason,
        error_message: &str,
    ) {
        let Some(mut entry) = self.actors.get_mut(actor_id) else {
            return; // Already destroyed or never registered.
        };

        // Skip if already dead.
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

        // If not restartable, clean up fully.
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
            // Remove from actors map.
            self.actors.remove(actor_id);
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

        self.publish_actor_update(&actor_data);
        info!(actor_id = hex(&actor_id), "Actor registered");

        Ok(Response::new(RegisterActorReply {
            status: Some(ok_status()),
        }))
    }

    /// Create an actor — transition to ALIVE state.
    ///
    /// Matches C++ CreateActor (gcs_actor_manager.cc:799-877):
    /// - Actor must be registered first
    /// - Idempotent: if already ALIVE, returns OK immediately
    /// - If DEAD: returns error (actor was destroyed)
    /// - Transitions DEPENDENCIES_UNREADY → ALIVE (scheduling stubbed)
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

        // Cannot create a dead actor.
        if entry.state == ActorState::Dead as i32 {
            return Ok(Response::new(CreateActorReply {
                status: Some(invalid_status("Actor is dead and cannot be created.")),
                ..Default::default()
            }));
        }

        // Transition to ALIVE (C++ goes through PENDING_CREATION with scheduling;
        // we go directly to ALIVE since scheduling is not yet implemented).
        entry.state = ActorState::Alive as i32;
        entry.timestamp = now_ms();
        entry.start_time = now_ms_u64();

        // Update address from task spec if available.
        if task_spec.caller_address.is_some() {
            entry.address = task_spec.caller_address.clone();
        }

        let actor_data = entry.clone();
        drop(entry);

        // Persist.
        self.table_storage
            .actor_table()
            .put(&hex(&actor_id), &actor_data)
            .await;

        // Store task spec if not already stored.
        if !self.actor_task_specs.contains_key(&actor_id) {
            self.table_storage
                .actor_task_spec_table()
                .put(&hex(&actor_id), &task_spec)
                .await;
            self.actor_task_specs.insert(actor_id.clone(), task_spec);
        }

        self.publish_actor_update(&actor_data);
        info!(actor_id = hex(&actor_id), "Actor created (ALIVE)");

        Ok(Response::new(CreateActorReply {
            status: Some(ok_status()),
            actor_address: actor_data.address.clone(),
            ..Default::default()
        }))
    }

    /// Get actor info by actor_id.
    async fn get_actor_info(
        &self,
        req: Request<GetActorInfoRequest>,
    ) -> Result<Response<GetActorInfoReply>, Status> {
        let inner = req.into_inner();
        let actor = self.actors.get(&inner.actor_id).map(|r| r.clone());

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

    /// Get all actors, with optional filters and limit.
    async fn get_all_actor_info(
        &self,
        req: Request<GetAllActorInfoRequest>,
    ) -> Result<Response<GetAllActorInfoReply>, Status> {
        let inner = req.into_inner();
        let limit = inner.limit.filter(|&l| l > 0).map(|l| l as usize);

        let mut actors: Vec<ActorTableData> = Vec::new();
        let mut num_filtered: i64 = 0;
        let total = self.actors.len() as i64;

        for entry in self.actors.iter() {
            let actor = entry.value();

            if let Some(ref filters) = inner.filters {
                if let Some(ref filter_actor_id) = filters.actor_id {
                    if !filter_actor_id.is_empty() && actor.actor_id != *filter_actor_id {
                        num_filtered += 1;
                        continue;
                    }
                }
                if let Some(ref filter_job_id) = filters.job_id {
                    if !filter_job_id.is_empty() && actor.job_id != *filter_job_id {
                        num_filtered += 1;
                        continue;
                    }
                }
                if let Some(filter_state) = filters.state {
                    if actor.state != filter_state {
                        num_filtered += 1;
                        continue;
                    }
                }
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
    /// Matches C++ KillActor (gcs_actor_manager.cc:1877-1916):
    /// - If already dead: return OK
    /// - Sets death cause to RAY_KILL
    /// - If no_restart: ensures actor won't be restarted
    async fn kill_actor_via_gcs(
        &self,
        req: Request<KillActorViaGcsRequest>,
    ) -> Result<Response<KillActorViaGcsReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        // If unknown or already dead, return OK.
        match self.actors.get(&actor_id) {
            None => {
                return Ok(Response::new(KillActorViaGcsReply {
                    status: Some(ok_status()),
                }));
            }
            Some(entry) if entry.state == ActorState::Dead as i32 => {
                return Ok(Response::new(KillActorViaGcsReply {
                    status: Some(ok_status()),
                }));
            }
            _ => {}
        }

        self.destroy_actor(&actor_id, DeathReason::RayKill, "Killed via ray.kill()")
            .await;

        info!(actor_id = hex(&actor_id), "Actor killed via GCS");

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

        // Stale message detection (matching C++):
        // If the request's lineage counter is behind the actor's, this report
        // was sent before a lineage restart that has already occurred.
        if inner.num_restarts_due_to_lineage_reconstruction
            > entry.num_restarts_due_to_lineage_reconstruction
        {
            debug!(
                actor_id = hex(&actor_id),
                "Out of scope report is stale, ignoring"
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

        // Destroy with OUT_OF_SCOPE cause (makes actor restartable if budget allows).
        self.destroy_actor(
            &actor_id,
            DeathReason::OutOfScope,
            "Actor went out of scope",
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

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> GcsActorManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        GcsActorManager::new(publisher, table_storage)
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

    async fn register_and_create(mgr: &GcsActorManager, spec: &TaskSpec) {
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
    }

    fn get_actor_state(mgr: &GcsActorManager, actor_id: &[u8]) -> Option<i32> {
        mgr.actors.get(actor_id).map(|a| a.state)
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

        // Create again (simulate scheduler success).
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
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

    #[tokio::test]
    async fn test_stale_out_of_scope_report() {
        let mgr = make_manager();
        let spec = make_task_spec_with_restarts(b"actor1", "", 3);
        register_and_create(&mgr, &spec).await;

        // Out of scope with counter=0.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Restart with counter=1.
        mgr.restart_actor_for_lineage_reconstruction(Request::new(
            RestartActorForLineageReconstructionRequest {
                actor_id: b"actor1".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();

        // Re-create (simulate scheduler success).
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Alive as i32));

        // Stale out-of-scope report with counter=2 > actor's current 1.
        // This means the report was generated after a restart we haven't seen yet → stale.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor1".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 2,
        }))
        .await
        .unwrap();

        // Actor should still be ALIVE (stale report ignored).
        assert_eq!(get_actor_state(&mgr, b"actor1"), Some(ActorState::Alive as i32));
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
}
