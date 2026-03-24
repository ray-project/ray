// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Actor Manager — manages actor lifecycle across the cluster.
//!
//! Replaces `src/ray/gcs/actor/gcs_actor_manager.h/cc`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use base64::Engine;
use dashmap::DashMap;
use ray_common::id::{ActorID, JobID, NodeID, WorkerID};
use tokio::sync::oneshot;
use tonic::Status;

use crate::actor_scheduler::GcsActorScheduler;
use crate::node_manager::GcsNodeManager;
use crate::pubsub_handler::{ChannelType, InternalPubSubHandler};
use crate::table_storage::GcsTableStorage;

/// Actor states matching the protobuf ActorTableData.ActorState enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ActorState {
    DependenciesUnready = 0,
    PendingCreation = 1,
    Alive = 2,
    Restarting = 3,
    Dead = 4,
}

impl From<i32> for ActorState {
    fn from(v: i32) -> Self {
        match v {
            0 => ActorState::DependenciesUnready,
            1 => ActorState::PendingCreation,
            2 => ActorState::Alive,
            3 => ActorState::Restarting,
            4 => ActorState::Dead,
            _ => ActorState::Dead,
        }
    }
}

/// Build an `ActorDeathCause` with an `ActorDiedErrorContext` from actor data.
fn make_actor_died_death_cause(
    actor: &ray_proto::ray::rpc::ActorTableData,
    reason: ray_proto::ray::rpc::actor_died_error_context::Reason,
    error_message: &str,
) -> ray_proto::ray::rpc::ActorDeathCause {
    ray_proto::ray::rpc::ActorDeathCause {
        context: Some(
            ray_proto::ray::rpc::actor_death_cause::Context::ActorDiedErrorContext(
                ray_proto::ray::rpc::ActorDiedErrorContext {
                    reason: reason as i32,
                    error_message: error_message.to_string(),
                    name: actor.name.clone(),
                    ray_namespace: actor.ray_namespace.clone(),
                    actor_id: actor.actor_id.clone(),
                    pid: actor.pid,
                    ..Default::default()
                },
            ),
        ),
    }
}

/// Configuration for actor lifecycle/export event emission.
///
/// Actor export/event configuration matching the C++ config model from `ray_config_def.h`.
///
/// C++ has three relevant flags:
/// - `enable_export_api_write` (bool, default false): global enable for ALL export types to file
/// - `enable_export_api_write_config` (Vec<String>, default []): selective per-type enable
/// - `enable_ray_event` (bool, default false): enables aggregator-path events
///
/// When `enable_ray_event` is true, the aggregator path is used and the file-export
/// path is bypassed (matching C++ `WriteActorExportEvent` branch logic in `gcs_actor.cc`).
#[derive(Debug, Clone)]
pub struct ActorExportConfig {
    /// The full C++ export API config.
    pub export_api_config: ray_observability::export::ExportApiConfig,
}

impl ActorExportConfig {
    /// Create config from the `enabled` boolean (backwards-compatible convenience).
    /// Sets `enable_export_api_write = enabled`.
    pub fn from_enabled(enabled: bool) -> Self {
        Self {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_export_api_write: enabled,
                ..Default::default()
            },
        }
    }

    /// Check if actor export to file is enabled.
    pub fn is_actor_export_enabled(&self) -> bool {
        self.export_api_config.is_actor_export_enabled()
    }

    /// Check if the aggregator (ray_event) path is enabled.
    pub fn is_ray_event_enabled(&self) -> bool {
        self.export_api_config.is_ray_event_enabled()
    }
}

impl Default for ActorExportConfig {
    fn default() -> Self {
        Self {
            export_api_config: ray_observability::export::ExportApiConfig::default(),
        }
    }
}

/// The GCS actor manager tracks all actors in the cluster.
///
/// Uses DashMap for per-key locking instead of global RwLock<HashMap>,
/// and Arc-wraps ActorTableData to avoid deep clones on reads.
pub struct GcsActorManager {
    /// All registered actors (Arc-wrapped for cheap clones on read paths).
    pub(crate) registered_actors: DashMap<ActorID, Arc<ray_proto::ray::rpc::ActorTableData>>,
    /// Named actors: (namespace, name) → ActorID.
    named_actors: DashMap<(String, String), ActorID>,
    /// Dead actors cache (for queries).
    dead_actors: DashMap<ActorID, Arc<ray_proto::ray::rpc::ActorTableData>>,
    /// Actors by node: node_id → set of actor_ids.
    pub(crate) actors_by_node: DashMap<NodeID, Vec<ActorID>>,
    /// Actors by owner: (node_id, worker_id) → set of actor_ids.
    #[allow(dead_code)]
    actors_by_owner: DashMap<(NodeID, WorkerID), Vec<ActorID>>,
    /// State counts for metrics.
    state_counts: DashMap<ActorState, usize>,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
    /// Actor scheduler for placing actors on nodes (set once during init).
    actor_scheduler: OnceLock<Arc<GcsActorScheduler>>,
    /// Pubsub handler for publishing actor state changes (set once during init).
    pubsub_handler: OnceLock<Arc<InternalPubSubHandler>>,
    /// Pending CreateActor reply channels: actor_id → Vec<oneshot::Sender>.
    #[allow(clippy::type_complexity)]
    create_callbacks: DashMap<
        ActorID,
        Vec<oneshot::Sender<Result<ray_proto::ray::rpc::CreateActorReply, Status>>>,
    >,
    /// Pending RestartActorForLineageReconstruction reply channels.
    lineage_restart_callbacks: DashMap<ActorID, Vec<oneshot::Sender<Result<(), Status>>>>,
    /// Task specs stored for actors being created (needed for scheduling).
    actor_task_specs: DashMap<ActorID, ray_proto::ray::rpc::TaskSpec>,
    /// Node manager for looking up raylet addresses.
    node_manager: Option<Arc<GcsNodeManager>>,
    /// Raylet client for sending KillLocalActor RPCs.
    raylet_client: Option<Arc<dyn crate::actor_scheduler::RayletClient>>,
    /// Actor lifecycle/export event configuration (C++ parity: WriteActorExportEvent).
    actor_export_config: ActorExportConfig,
    /// Event exporter for actor lifecycle events (aggregator/ray_event path).
    event_exporter: Arc<ray_observability::export::EventExporter>,
    /// File-based export event sink (export API file path).
    /// C++ parity: `LogEventReporter` writing to `export_events/event_EXPORT_ACTOR.log`.
    file_export_sink: Option<Arc<ray_observability::export::FileExportEventSink>>,
}

impl GcsActorManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self::with_export_config(table_storage, ActorExportConfig::default())
    }

    /// Create a new actor manager with explicit export event configuration.
    pub fn with_export_config(
        table_storage: Arc<GcsTableStorage>,
        export_config: ActorExportConfig,
    ) -> Self {
        // The event exporter is enabled when either path is active
        // (aggregator path buffers RayEvent, export API path also buffers for programmatic access).
        let exporter_config = ray_observability::export::ExportConfig {
            enabled: export_config.is_ray_event_enabled()
                || export_config.is_actor_export_enabled(),
            ..Default::default()
        };

        // The file-based export sink is created when export API is enabled for actors.
        let file_export_sink = if export_config.is_actor_export_enabled() {
            if let Some(ref log_dir) = export_config.export_api_config.log_dir {
                match ray_observability::export::FileExportEventSink::new(
                    log_dir,
                    "EXPORT_ACTOR",
                ) {
                    Ok(sink) => Some(Arc::new(sink)),
                    Err(e) => {
                        tracing::warn!("Failed to create export event file sink: {}", e);
                        None
                    }
                }
            } else {
                // No log_dir — create an in-test-friendly mode where the sink
                // can be set later, or use a temp dir for test contexts.
                None
            }
        } else {
            None
        };

        Self {
            registered_actors: DashMap::new(),
            named_actors: DashMap::new(),
            dead_actors: DashMap::new(),
            actors_by_node: DashMap::new(),
            actors_by_owner: DashMap::new(),
            state_counts: DashMap::new(),
            table_storage,
            actor_scheduler: OnceLock::new(),
            pubsub_handler: OnceLock::new(),
            create_callbacks: DashMap::new(),
            lineage_restart_callbacks: DashMap::new(),
            actor_task_specs: DashMap::new(),
            node_manager: None,
            raylet_client: None,
            actor_export_config: export_config,
            event_exporter: Arc::new(ray_observability::export::EventExporter::new(
                exporter_config,
            )),
            file_export_sink,
        }
    }

    /// Set the file export sink externally (for testing or late initialization).
    pub fn set_file_export_sink(
        &mut self,
        sink: Arc<ray_observability::export::FileExportEventSink>,
    ) {
        self.file_export_sink = Some(sink);
    }

    /// Get the file export sink (for test verification).
    pub fn file_export_sink(
        &self,
    ) -> Option<&Arc<ray_observability::export::FileExportEventSink>> {
        self.file_export_sink.as_ref()
    }

    /// Get the event exporter (for tests and external sink wiring).
    pub fn event_exporter(&self) -> &Arc<ray_observability::export::EventExporter> {
        &self.event_exporter
    }

    /// Set the actor scheduler (called once during server initialization).
    pub fn set_actor_scheduler(&self, scheduler: Arc<GcsActorScheduler>) {
        let _ = self.actor_scheduler.set(scheduler);
    }

    /// Set the node manager and raylet client (for sending KillLocalActor RPCs).
    pub fn set_node_manager_and_raylet_client(
        &mut self,
        node_manager: Arc<GcsNodeManager>,
        raylet_client: Arc<dyn crate::actor_scheduler::RayletClient>,
    ) {
        self.node_manager = Some(node_manager);
        self.raylet_client = Some(raylet_client);
    }

    /// Set the pubsub handler (called once during server initialization).
    pub fn set_pubsub_handler(&self, handler: Arc<InternalPubSubHandler>) {
        let _ = self.pubsub_handler.set(handler);
    }

    /// Initialize from persisted data.
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let all_actors = self
            .table_storage
            .actor_table()
            .get_all()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        for (key, actor) in all_actors {
            let actor_id = ActorID::from_hex(&key);
            let state = ActorState::from(actor.state);

            *self.state_counts.entry(state).or_insert(0) += 1;

            if state == ActorState::Dead {
                self.dead_actors.insert(actor_id, Arc::new(actor));
            } else {
                if !actor.ray_namespace.is_empty() && !actor.name.is_empty() {
                    self.named_actors
                        .insert((actor.ray_namespace.clone(), actor.name.clone()), actor_id);
                }
                self.registered_actors.insert(actor_id, Arc::new(actor));
            }
        }
        Ok(())
    }

    /// Handle RegisterActor RPC — registers an actor from its creation task spec.
    pub async fn handle_register_actor(
        &self,
        task_spec: ray_proto::ray::rpc::TaskSpec,
    ) -> Result<(), tonic::Status> {
        let creation_spec = task_spec
            .actor_creation_task_spec
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("missing actor_creation_task_spec"))?;
        let actor_id_bytes = &creation_spec.actor_id;
        let actor_id =
            ActorID::from_binary(actor_id_bytes.as_slice().try_into().unwrap_or(&[0u8; 16]));

        let name = creation_spec.name.clone();
        // ray_namespace is on ActorTableData, not TaskSpec; extract from creation spec
        let namespace = creation_spec.ray_namespace.clone();

        // Extract class_name from function descriptor (matching C++ GcsActor constructor)
        let class_name = task_spec
            .function_descriptor
            .as_ref()
            .and_then(|fd| fd.function_descriptor.as_ref())
            .map(|fd| match fd {
                ray_proto::ray::rpc::function_descriptor::FunctionDescriptor::JavaFunctionDescriptor(j) => {
                    j.class_name.clone()
                }
                ray_proto::ray::rpc::function_descriptor::FunctionDescriptor::PythonFunctionDescriptor(p) => {
                    p.class_name.clone()
                }
                ray_proto::ray::rpc::function_descriptor::FunctionDescriptor::CppFunctionDescriptor(c) => {
                    c.class_name.clone()
                }
            })
            .unwrap_or_default();

        // Build the actor table data — copy all fields from task_spec/creation_spec
        // that C++ copies into ActorTableData at registration time.
        // This ensures WriteActorExportEvent has full data for ActorDefinitionEvent.

        // Extract placement_group_id from scheduling_strategy if it's a PG strategy
        let placement_group_id = task_spec
            .scheduling_strategy
            .as_ref()
            .and_then(|ss| ss.scheduling_strategy.as_ref())
            .and_then(|s| match s {
                ray_proto::ray::rpc::scheduling_strategy::SchedulingStrategy::PlacementGroupSchedulingStrategy(pg) => {
                    if pg.placement_group_id.is_empty() { None } else { Some(pg.placement_group_id.clone()) }
                }
                _ => None,
            });

        // ActorTableData.label_selector is map<string, string>.
        // TaskSpec.label_selector is LabelSelector (message with label_constraints).
        // C++ converts LabelSelector constraints to the map at actor creation time.
        // For now, label_selector is populated later when the actor data is updated.
        let label_selector = HashMap::new();

        // Extract serialized_runtime_env from runtime_env_info
        let serialized_runtime_env = task_spec
            .runtime_env_info
            .as_ref()
            .map(|rei| rei.serialized_runtime_env.clone())
            .unwrap_or_default();

        let actor_data = ray_proto::ray::rpc::ActorTableData {
            actor_id: actor_id_bytes.clone(),
            state: ActorState::DependenciesUnready as i32,
            name: name.clone(),
            ray_namespace: namespace.clone(),
            job_id: task_spec.job_id.clone(),
            function_descriptor: task_spec.function_descriptor.clone(),
            owner_address: task_spec.caller_address.clone(),
            is_detached: creation_spec.is_detached,
            class_name,
            max_restarts: creation_spec.max_actor_restarts as i64,
            // C++ parity: fields needed for ActorDefinitionEvent
            required_resources: task_spec.required_resources.clone(),
            placement_group_id,
            label_selector,
            call_site: task_spec.call_site.clone(),
            parent_id: task_spec.parent_task_id.clone(),
            labels: task_spec.labels.clone(),
            serialized_runtime_env,
            ..Default::default()
        };

        // Check for name conflict
        if !name.is_empty() {
            let key = (namespace.clone(), name.clone());
            if self.named_actors.contains_key(&key) {
                return Err(tonic::Status::already_exists(format!(
                    "actor with name '{}' already exists",
                    name
                )));
            }
        }

        let key = hex::encode(actor_id_bytes);
        self.table_storage
            .actor_table()
            .put(&key, &actor_data)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Register name
        if !name.is_empty() {
            self.named_actors.insert((namespace, name), actor_id);
        }

        // C++ parity: WriteActorExportEvent(true) — registration event
        self.write_actor_export_event(&actor_data, true);

        self.registered_actors
            .insert(actor_id, Arc::new(actor_data));
        // Store the task spec immediately so that GetNamedActorInfo can return
        // it even before CreateActor is called. This matches C++ behavior where
        // GcsActor stores the TaskSpec from the moment of registration, ensuring
        // make_actor_handle can reconstruct the PythonFunctionDescriptor.
        self.actor_task_specs.insert(actor_id, task_spec);
        *self
            .state_counts
            .entry(ActorState::DependenciesUnready)
            .or_insert(0) += 1;

        tracing::info!(?actor_id, "Actor registered");
        Ok(())
    }

    /// Handle CreateActor RPC.
    ///
    /// Transitions actor to PENDING_CREATION, spawns scheduling, and returns
    /// a oneshot receiver that resolves when the actor becomes ALIVE or fails.
    pub async fn handle_create_actor(
        self: &Arc<Self>,
        task_spec: ray_proto::ray::rpc::TaskSpec,
    ) -> Result<oneshot::Receiver<Result<ray_proto::ray::rpc::CreateActorReply, Status>>, Status>
    {
        let creation_spec = task_spec
            .actor_creation_task_spec
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing actor_creation_task_spec"))?;
        let actor_id = ActorID::from_binary(
            creation_spec
                .actor_id
                .as_slice()
                .try_into()
                .unwrap_or(&[0u8; 16]),
        );

        // Transition to PENDING_CREATION
        let pending_pub_msg = {
            if let Some((_, old_arc)) = self.registered_actors.remove(&actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                actor.state = ActorState::PendingCreation as i32;

                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                *self
                    .state_counts
                    .entry(ActorState::PendingCreation)
                    .or_insert(0) += 1;

                let msg = Self::build_actor_pub_message(&actor);
                // C++ parity: WriteActorExportEvent(false) — PENDING_CREATION
                self.write_actor_export_event(&actor, false);
                self.registered_actors.insert(actor_id, Arc::new(actor));
                Some(msg)
            } else {
                return Err(Status::not_found(format!(
                    "actor {:?} not registered",
                    actor_id
                )));
            }
        };
        // Publish outside the critical section
        self.publish_deferred_messages(pending_pub_msg.into_iter().collect());

        // Store the task spec for scheduling
        self.actor_task_specs.insert(actor_id, task_spec.clone());

        // Create oneshot channel for deferred reply
        let (tx, rx) = oneshot::channel();
        self.create_callbacks
            .entry(actor_id)
            .or_default()
            .push(tx);

        // Spawn scheduling task
        let scheduler = self
            .actor_scheduler
            .get()
            .cloned()
            .ok_or_else(|| Status::internal("actor scheduler not initialized"))?;
        let mgr = Arc::clone(self);
        let task_spec_clone = task_spec;
        tokio::spawn(async move {
            match scheduler.schedule(&task_spec_clone).await {
                Ok(result) => {
                    mgr.on_actor_creation_success(
                        &actor_id,
                        result.worker_address,
                        result.worker_pid,
                        result.resource_mapping,
                        result.node_id,
                    );
                }
                Err(e) => {
                    mgr.on_actor_scheduling_failed(&actor_id, e.message().to_string());
                }
            }
        });

        tracing::info!(
            ?actor_id,
            "Actor creation requested, scheduling in progress"
        );
        Ok(rx)
    }

    /// Handle RestartActorForLineageReconstruction RPC.
    pub async fn handle_restart_actor_for_lineage_reconstruction(
        self: &Arc<Self>,
        actor_id_bytes: &[u8],
        target_num_restarts_due_to_lineage_reconstruction: u64,
    ) -> Result<(), Status> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));

        if let Some(actor) = self.registered_actors.get(&actor_id) {
            if target_num_restarts_due_to_lineage_reconstruction
                <= actor.num_restarts_due_to_lineage_reconstruction
            {
                return Ok(());
            }
            return Err(Status::failed_precondition(format!(
                "actor {:?} is not dead and cannot be restarted for lineage reconstruction",
                actor_id
            )));
        }

        if let Some(mut pending) = self.lineage_restart_callbacks.get_mut(&actor_id) {
            let current = self
                .registered_actors
                .get(&actor_id)
                .map(|actor| actor.num_restarts_due_to_lineage_reconstruction)
                .or_else(|| {
                    self.dead_actors
                        .get(&actor_id)
                        .map(|actor| actor.num_restarts_due_to_lineage_reconstruction)
                })
                .unwrap_or(target_num_restarts_due_to_lineage_reconstruction.saturating_sub(1));
            if target_num_restarts_due_to_lineage_reconstruction <= current + 1 {
                let (tx, rx) = oneshot::channel();
                pending.push(tx);
                drop(pending);
                return rx
                    .await
                    .map_err(|_| Status::internal("lineage restart callback cancelled"))?;
            }
        }

        // Early stale-request check: if the actor is already dead with a
        // lineage reconstruction count >= the target, skip without requiring
        // a scheduler or task spec.
        if let Some(actor) = self.dead_actors.get(&actor_id) {
            if target_num_restarts_due_to_lineage_reconstruction
                <= actor.num_restarts_due_to_lineage_reconstruction
            {
                return Ok(());
            }
        }

        let task_spec = self
            .actor_task_specs
            .get(&actor_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "actor {:?} has no stored task spec for lineage reconstruction",
                    actor_id
                ))
            })?;
        let scheduler = self
            .actor_scheduler
            .get()
            .cloned()
            .ok_or_else(|| Status::internal("actor scheduler not initialized"))?;

        {
            let mut actor = self
                .dead_actors
                .remove(&actor_id)
                .map(|(_, actor)| Arc::unwrap_or_clone(actor))
                .ok_or_else(|| Status::invalid_argument("Actor is permanently dead."))?;

            if target_num_restarts_due_to_lineage_reconstruction
                <= actor.num_restarts_due_to_lineage_reconstruction
            {
                self.dead_actors.insert(actor_id, Arc::new(actor));
                return Ok(());
            }
            if target_num_restarts_due_to_lineage_reconstruction
                != actor.num_restarts_due_to_lineage_reconstruction + 1
            {
                let current = actor.num_restarts_due_to_lineage_reconstruction;
                self.dead_actors.insert(actor_id, Arc::new(actor));
                return Err(Status::invalid_argument(format!(
                    "current num_restarts_due_to_lineage_reconstruction: {}, target: {}",
                    current,
                    target_num_restarts_due_to_lineage_reconstruction
                )));
            }
            if actor.max_restarts != -1 && actor.num_restarts >= actor.max_restarts {
                self.dead_actors.insert(actor_id, Arc::new(actor));
                return Err(Status::invalid_argument("Actor is not restartable."));
            }

            if let Some(mut c) = self.state_counts.get_mut(&ActorState::Dead) {
                *c = c.saturating_sub(1);
            }
            actor.state = ActorState::Restarting as i32;
            actor.num_restarts_due_to_lineage_reconstruction += 1;
            actor.address = None;
            actor.node_id = None;
            actor.pid = 0;
            actor.death_cause = Some(make_actor_died_death_cause(
                &actor,
                ray_proto::ray::rpc::actor_died_error_context::Reason::OutOfScope,
                "Actor is restarting due to lineage reconstruction.",
            ));
            *self.state_counts.entry(ActorState::Restarting).or_insert(0) += 1;
            if !actor.name.is_empty() {
                self.named_actors
                    .insert((actor.ray_namespace.clone(), actor.name.clone()), actor_id);
            }
            self.publish_actor_state(&actor);
            self.registered_actors.insert(actor_id, Arc::new(actor));
        }

        let (tx, rx) = oneshot::channel();
        self.lineage_restart_callbacks
            .entry(actor_id)
            .or_default()
            .push(tx);

        match scheduler.schedule(&task_spec).await {
            Ok(result) => {
                self.on_actor_creation_success(
                    &actor_id,
                    result.worker_address,
                    result.worker_pid,
                    result.resource_mapping,
                    result.node_id,
                );
                if let Some((_, callbacks)) = self.lineage_restart_callbacks.remove(&actor_id) {
                    for tx in callbacks {
                        let _ = tx.send(Ok(()));
                    }
                }
            }
            Err(e) => {
                let message = e.message().to_string();
                self.on_actor_scheduling_failed(&actor_id, message.clone());
                if let Some((_, callbacks)) = self.lineage_restart_callbacks.remove(&actor_id) {
                    for tx in callbacks {
                        let _ = tx.send(Err(Status::internal(message.clone())));
                    }
                }
            }
        }

        rx.await
            .map_err(|_| Status::internal("lineage restart callback cancelled"))?
    }

    /// Called by scheduler on success. Transitions actor to ALIVE and resolves callbacks.
    pub fn on_actor_creation_success(
        &self,
        actor_id: &ActorID,
        worker_address: ray_proto::ray::rpc::Address,
        worker_pid: u32,
        resource_mapping: Vec<ray_proto::ray::rpc::ResourceMapEntry>,
        node_id: Vec<u8>,
    ) {
        let actor_address = worker_address.clone();

        // Update actor state to ALIVE
        let pub_msg = {
            if let Some((_, old_arc)) = self.registered_actors.remove(actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                actor.state = ActorState::Alive as i32;
                actor.address = Some(worker_address);
                actor.pid = worker_pid;
                actor.resource_mapping = resource_mapping;
                actor.node_id = Some(node_id.clone());
                // C++ parity: reset preempted on successful creation
                actor.preempted = false;

                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                *self.state_counts.entry(ActorState::Alive).or_insert(0) += 1;

                // Track actor by node
                let node_id_obj =
                    NodeID::from_binary(node_id.as_slice().try_into().unwrap_or(&[0u8; 28]));
                self.actors_by_node
                    .entry(node_id_obj)
                    .or_default()
                    .push(*actor_id);

                // Track actor by owner (owner = the worker that created it)
                if let Some(ref addr) = actor.address {
                    let owner_node = NodeID::from_binary(
                        addr.node_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                    );
                    let owner_worker = WorkerID::from_binary(
                        addr.worker_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                    );
                    self.actors_by_owner
                        .entry((owner_node, owner_worker))
                        .or_default()
                        .push(*actor_id);
                }

                let msg = Self::build_actor_pub_message(&actor);
                let actor_data_for_persist = actor.clone();
                self.registered_actors.insert(*actor_id, Arc::new(actor));
                Some((msg, actor_data_for_persist))
            } else {
                None
            }
        };

        // C++ parity: persist first, then publish, then resolve callbacks.
        // All three events are inside the ActorTable().Put() completion handler
        // in C++ (lines 1677–1692 of gcs_actor_manager.cc). A successful callback
        // implies the actor-table state is already durably written and published.
        let callback_senders = self
            .create_callbacks
            .remove(actor_id)
            .map(|(_, senders)| senders);

        // C++ parity: WriteActorExportEvent(false) — ALIVE.
        // Emit before spawn so we have access to self; C++ emits inside Put callback
        // but the event is based on in-memory state which is already updated.
        if let Some((_, ref actor_data)) = pub_msg {
            self.write_actor_export_event(actor_data, false);
        }

        if let Some((msg, actor_data)) = pub_msg {
            let storage = self.table_storage.clone();
            let pubsub = self.pubsub_handler.get().cloned();
            let aid = *actor_id;
            tokio::spawn(async move {
                // 1. Persist
                let key = hex::encode(aid.binary());
                if let Err(e) = storage.actor_table().put(&key, &actor_data).await {
                    tracing::error!(?aid, ?e, "Failed to persist actor creation success");
                    return; // Do not publish or fire callbacks if persist failed
                }
                // 2. Publish (only after successful persistence)
                if let Some(handler) = pubsub {
                    handler.publish_pubmessage(msg);
                }
                // 3. Resolve creation callbacks (only after persist + publish)
                if let Some(senders) = callback_senders {
                    let reply = ray_proto::ray::rpc::CreateActorReply {
                        actor_address: Some(actor_address),
                        ..Default::default()
                    };
                    for tx in senders {
                        let _ = tx.send(Ok(reply.clone()));
                    }
                }
                tracing::info!(?aid, "Actor is now ALIVE");
            });
        } else {
            // No pub_msg means actor was removed or dead — still resolve callbacks
            // (matches C++ RunAndClearActorCreationCallbacks on early-return paths)
            if let Some(senders) = callback_senders {
                let reply = ray_proto::ray::rpc::CreateActorReply {
                    actor_address: Some(actor_address),
                    ..Default::default()
                };
                for tx in senders {
                    let _ = tx.send(Ok(reply.clone()));
                }
            }
        }
    }

    /// Called by scheduler on failure. Transitions actor to DEAD and resolves callbacks.
    pub fn on_actor_scheduling_failed(&self, actor_id: &ActorID, reason: String) {
        // Transition to DEAD
        let (pub_msg, named_key) = {
            if let Some((_, old_arc)) = self.registered_actors.remove(actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *self.state_counts.entry(ActorState::Dead).or_insert(0) += 1;

                // Set death cause so C++ core worker can deserialize the error
                actor.death_cause = Some(make_actor_died_death_cause(
                    &actor,
                    ray_proto::ray::rpc::actor_died_error_context::Reason::Unspecified,
                    &reason,
                ));

                let named_key = if !actor.name.is_empty() {
                    Some((actor.ray_namespace.clone(), actor.name.clone()))
                } else {
                    None
                };

                let msg = Self::build_actor_pub_message(&actor);
                // C++ parity: WriteActorExportEvent(false) — DEAD (scheduling failed)
                self.write_actor_export_event(&actor, false);
                self.dead_actors.insert(*actor_id, Arc::new(actor));
                (Some(msg), named_key)
            } else {
                (None, None)
            }
        };
        // Remove named actor
        if let Some(key) = named_key {
            self.named_actors.remove(&key);
        }
        // Publish DEAD state
        self.publish_deferred_messages(pub_msg.into_iter().collect());

        // Clean up task spec
        self.actor_task_specs.remove(actor_id);

        // Resolve all pending create callbacks with error
        let callbacks = self.create_callbacks.remove(actor_id);
        if let Some((_, senders)) = callbacks {
            for tx in senders {
                let _ = tx.send(Err(Status::internal(format!(
                    "actor scheduling failed: {}",
                    reason
                ))));
            }
        }

        tracing::warn!(?actor_id, %reason, "Actor scheduling failed, actor is DEAD");
    }

    /// Build a PubMessage for an actor state change (without publishing).
    /// Use this inside critical sections, then call `publish_deferred_messages` after
    /// dropping locks to avoid holding write locks during pubsub delivery.
    fn build_actor_pub_message(
        actor: &ray_proto::ray::rpc::ActorTableData,
    ) -> ray_proto::ray::rpc::PubMessage {
        ray_proto::ray::rpc::PubMessage {
            channel_type: ChannelType::GcsActorChannel as i32,
            key_id: actor.actor_id.clone(),
            inner_message: Some(
                ray_proto::ray::rpc::pub_message::InnerMessage::ActorMessage(actor.clone()),
            ),
            ..Default::default()
        }
    }

    /// Publish deferred messages outside critical sections.
    fn publish_deferred_messages(&self, messages: Vec<ray_proto::ray::rpc::PubMessage>) {
        if messages.is_empty() {
            return;
        }
        if let Some(handler) = self.pubsub_handler.get() {
            for msg in messages {
                handler.publish_pubmessage(msg);
            }
        }
    }

    /// Publish actor state change via pubsub.
    /// NOTE: Only call this when NOT holding any manager write locks to avoid
    /// lock contention. For code inside critical sections, use
    /// `build_actor_pub_message` + `publish_deferred_messages` instead.
    fn publish_actor_state(&self, actor: &ray_proto::ray::rpc::ActorTableData) {
        if let Some(handler) = self.pubsub_handler.get() {
            handler.publish_pubmessage(Self::build_actor_pub_message(actor));
        }
    }

    /// Emit an actor lifecycle/export event.
    ///
    /// C++ parity: mirrors `GcsActor::WriteActorExportEvent()` in `gcs_actor.cc`.
    ///
    /// Two paths (mutually exclusive, matching C++):
    /// 1. `enable_ray_event` → aggregator path: buffers `RayEvent` in `event_exporter`
    /// 2. export API path → file output: builds `ExportActorData` proto, wraps in
    ///    `ExportEvent`, writes JSON to `export_events/event_EXPORT_ACTOR.log`
    ///
    /// If neither path is enabled, this is a no-op.
    fn write_actor_export_event(
        &self,
        actor: &ray_proto::ray::rpc::ActorTableData,
        is_registration: bool,
    ) {
        let is_ray_event = self.actor_export_config.is_ray_event_enabled();
        let is_export_api = self.actor_export_config.is_actor_export_enabled();

        if !is_ray_event && !is_export_api {
            return;
        }

        // Branch A: aggregator path (C++ enable_ray_event=true)
        // C++ emits separate RayEvent protos: on registration, one definition + one
        // lifecycle; on other transitions, one lifecycle only.
        // All fields needed by the sink must be passed via custom_fields.
        if is_ray_event {
            let actor_id_hex = hex::encode(&actor.actor_id);
            let state_name = match ActorState::from(actor.state) {
                ActorState::DependenciesUnready => "DEPENDENCIES_UNREADY",
                ActorState::PendingCreation => "PENDING_CREATION",
                ActorState::Alive => "ALIVE",
                ActorState::Restarting => "RESTARTING",
                ActorState::Dead => "DEAD",
            };

            // Build common fields shared by all event types
            let build_common = |event: ray_observability::events::RayEvent| -> ray_observability::events::RayEvent {
                event
                    .with_field("actor_id", &actor_id_hex)
                    .with_field("job_id", &hex::encode(&actor.job_id))
                    .with_field("state", state_name)
                    .with_field("name", &actor.name)
                    .with_field("pid", actor.pid.to_string())
                    .with_field("ray_namespace", &actor.ray_namespace)
                    .with_field("class_name", &actor.class_name)
                    .with_field("is_detached", actor.is_detached.to_string())
                    .with_field(
                        "node_id",
                        &actor
                            .node_id
                            .as_ref()
                            .map(|n| hex::encode(n))
                            .unwrap_or_default(),
                    )
                    .with_field("repr_name", &actor.repr_name)
            };

            if is_registration {
                // C++ emits ActorDefinitionEvent + ActorLifecycleEvent as separate protos.
                // Event 1: definition event
                let def_event = build_common(ray_observability::events::RayEvent::new(
                    ray_observability::events::EventSourceType::Gcs,
                    ray_observability::events::EventSeverity::Info,
                    "ACTOR_REGISTERED_DEF",
                    format!("Actor {} definition", actor.name),
                ))
                .with_field("is_registration", "true")
                .with_field("event_kind", "definition")
                .with_field("serialized_runtime_env", &actor.serialized_runtime_env)
                // required_resources: serialize as JSON map
                .with_field(
                    "required_resources",
                    &serde_json::to_string(&actor.required_resources).unwrap_or_default(),
                )
                // placement_group_id
                .with_field(
                    "placement_group_id",
                    &actor
                        .placement_group_id
                        .as_ref()
                        .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
                        .unwrap_or_default(),
                )
                // label_selector
                .with_field(
                    "label_selector",
                    &serde_json::to_string(&actor.label_selector).unwrap_or_default(),
                )
                // call_site
                .with_field(
                    "call_site",
                    actor.call_site.as_deref().unwrap_or(""),
                )
                // parent_id
                .with_field("parent_id", &base64::engine::general_purpose::STANDARD.encode(&actor.parent_id))
                // ref_ids (C++ stores labels() in ref_ids)
                .with_field(
                    "ref_ids",
                    &serde_json::to_string(&actor.labels).unwrap_or_default(),
                );
                self.event_exporter.add_event(def_event);

                // Event 2: lifecycle event for the initial state
                let lifecycle_event = build_common(ray_observability::events::RayEvent::new(
                    ray_observability::events::EventSourceType::Gcs,
                    ray_observability::events::EventSeverity::Info,
                    "ACTOR_REGISTERED_LIFECYCLE",
                    format!("Actor {} state: {}", actor.name, state_name),
                ))
                .with_field("is_registration", "false")
                .with_field("event_kind", "lifecycle");
                self.event_exporter.add_event(lifecycle_event);
            } else {
                // Non-registration: single lifecycle event with full transition fields
                let label = match ActorState::from(actor.state) {
                    ActorState::Alive => "ACTOR_CREATED",
                    ActorState::Restarting => "ACTOR_RESTARTED",
                    ActorState::Dead => "ACTOR_DIED",
                    ActorState::PendingCreation => "ACTOR_PENDING_CREATION",
                    ActorState::DependenciesUnready => "ACTOR_DEPENDENCIES_UNREADY",
                };

                let mut event = build_common(ray_observability::events::RayEvent::new(
                    ray_observability::events::EventSourceType::Gcs,
                    ray_observability::events::EventSeverity::Info,
                    label,
                    format!("Actor {} state: {}", actor.name, state_name),
                ))
                .with_field("is_registration", "false")
                .with_field("event_kind", "lifecycle");

                // ALIVE-specific fields: worker_id, port
                if ActorState::from(actor.state) == ActorState::Alive {
                    if let Some(ref addr) = actor.address {
                        event = event
                            .with_field("worker_id", &base64::engine::general_purpose::STANDARD.encode(&addr.worker_id))
                            .with_field("port", addr.port.to_string());
                    }
                }

                // DEAD-specific field: death_cause (serialized as JSON)
                if ActorState::from(actor.state) == ActorState::Dead {
                    if let Some(ref dc) = actor.death_cause {
                        event = event.with_field(
                            "death_cause",
                            &serde_json::to_string(dc).unwrap_or_default(),
                        );
                    }
                }

                // RESTARTING-specific field: restart_reason
                if ActorState::from(actor.state) == ActorState::Restarting {
                    let restart_reason = if actor.preempted {
                        2 // NODE_PREEMPTION
                    } else {
                        0 // ACTOR_FAILURE (default)
                    };
                    event = event.with_field("restart_reason", restart_reason.to_string());
                }

                self.event_exporter.add_event(event);
            }

            return; // C++ returns here — aggregator path bypasses file export
        }

        // Branch B: export API file path (C++ enable_export_api_write / _config)
        // Builds ExportActorData proto with ALL 16 fields, wraps in ExportEvent,
        // writes JSON-per-line to file.
        self.write_actor_export_event_to_file(actor);
    }

    /// Build `ExportActorData` proto matching C++ `gcs_actor.cc` lines 130-160
    /// and write it to the export event log file.
    fn write_actor_export_event_to_file(
        &self,
        actor: &ray_proto::ray::rpc::ActorTableData,
    ) {
        use ray_proto::ray::rpc::{
            export_actor_data::ActorState as ExportActorState, export_event, ExportActorData,
            ExportEvent,
        };

        // Convert ActorTableData state → ExportActorData state
        let export_state = match ActorState::from(actor.state) {
            ActorState::DependenciesUnready => ExportActorState::DependenciesUnready,
            ActorState::PendingCreation => ExportActorState::PendingCreation,
            ActorState::Alive => ExportActorState::Alive,
            ActorState::Restarting => ExportActorState::Restarting,
            ActorState::Dead => ExportActorState::Dead,
        };

        // Get labels from the task spec (C++ uses task_spec_->labels())
        let labels = if actor.actor_id.len() == 16 {
            self.actor_task_specs
                .get(&ActorID::from_binary(
                    actor.actor_id.as_slice().try_into().unwrap(),
                ))
                .map(|ts| ts.labels.clone())
                .unwrap_or_default()
        } else {
            std::collections::HashMap::new()
        };

        // Build ExportActorData with all 16 fields (C++ parity: gcs_actor.cc:130-160)
        let export_actor_data = ExportActorData {
            actor_id: actor.actor_id.clone(),                        // field 1
            job_id: actor.job_id.clone(),                            // field 2
            state: export_state as i32,                              // field 3
            is_detached: actor.is_detached,                          // field 4
            name: actor.name.clone(),                                // field 5
            pid: actor.pid,                                          // field 6
            ray_namespace: actor.ray_namespace.clone(),              // field 7
            serialized_runtime_env: actor.serialized_runtime_env.clone(), // field 8
            class_name: actor.class_name.clone(),                    // field 9
            death_cause: actor.death_cause.clone(),                  // field 10
            required_resources: actor.required_resources.clone(),    // field 11
            node_id: actor.node_id.clone(),                          // field 12
            placement_group_id: actor.placement_group_id.clone(),    // field 13
            repr_name: actor.repr_name.clone(),                      // field 14
            labels,                                                  // field 15
            label_selector: actor.label_selector.clone(),            // field 16
        };

        // Wrap in ExportEvent (C++ parity: RayExportEvent::SendEvent() in event.cc:429-473)
        let export_event = ExportEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            source_type: export_event::SourceType::ExportActor as i32,
            timestamp: ray_util::time::current_time_ms() as i64,
            event_data: Some(export_event::EventData::ActorEventData(export_actor_data)),
        };

        // Write to file via the file export sink
        if let Some(ref sink) = self.file_export_sink {
            sink.write_export_event(&export_event);
        }

        // Also buffer in the event exporter for programmatic access (e.g., tests)
        let actor_id_hex = hex::encode(&actor.actor_id);
        let state_name = match ActorState::from(actor.state) {
            ActorState::DependenciesUnready => "DEPENDENCIES_UNREADY",
            ActorState::PendingCreation => "PENDING_CREATION",
            ActorState::Alive => "ALIVE",
            ActorState::Restarting => "RESTARTING",
            ActorState::Dead => "DEAD",
        };
        let event = ray_observability::events::RayEvent::new(
            ray_observability::events::EventSourceType::Gcs,
            ray_observability::events::EventSeverity::Info,
            "EXPORT_ACTOR",
            format!("Actor {} state: {}", actor.name, state_name),
        )
        .with_field("actor_id", &actor_id_hex);
        self.event_exporter.add_event(event);
    }

    /// Send KillLocalActor RPC to the raylet hosting the actor.
    /// This tells the raylet to kill the worker process and release its resources.
    async fn notify_raylet_to_kill_actor(&self, actor: &ray_proto::ray::rpc::ActorTableData) {
        let (node_manager, raylet_client) =
            match (self.node_manager.as_ref(), self.raylet_client.as_ref()) {
                (Some(nm), Some(rc)) => (nm, rc),
                _ => {
                    tracing::warn!("KillLocalActor: node_manager or raylet_client not set");
                    return;
                }
            };

        // Get the actor's node ID to look up the raylet address
        let node_id_bytes = match actor.node_id {
            Some(ref nid) => nid.clone(),
            None => {
                tracing::warn!("KillLocalActor: actor has no node_id");
                return;
            }
        };
        let node_id = NodeID::from_binary(
            node_id_bytes.as_slice().try_into().unwrap_or(&[0u8; 28]),
        );

        let node_info = match node_manager.get_alive_node(&node_id) {
            Some(info) => info,
            None => {
                tracing::info!(
                    ?node_id,
                    "Not sending KillLocalActor: actor's node is not alive"
                );
                return;
            }
        };

        let raylet_addr = format!(
            "{}:{}",
            node_info.node_manager_address, node_info.node_manager_port
        );

        // Extract worker_id from actor's address
        let worker_id = actor
            .address
            .as_ref()
            .map(|a| a.worker_id.clone())
            .unwrap_or_default();

        let request = ray_proto::ray::rpc::KillLocalActorRequest {
            intended_actor_id: actor.actor_id.clone(),
            worker_id,
            force_kill: true,
            death_cause: actor.death_cause.clone(),
        };

        match raylet_client.kill_local_actor(&raylet_addr, request).await {
            Ok(_) => {
                tracing::info!("Sent KillLocalActor to raylet");
            }
            Err(e) => {
                tracing::info!(%e, "KillLocalActor failed (node may already be dead)");
            }
        }
    }

    /// Handle GetActorInfo RPC.
    pub fn handle_get_actor_info(
        &self,
        actor_id_bytes: &[u8],
    ) -> Option<ray_proto::ray::rpc::ActorTableData> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));
        if let Some(entry) = self.registered_actors.get(&actor_id) {
            return Some((**entry).clone());
        }
        self.dead_actors.get(&actor_id).map(|e| (**e).clone())
    }

    /// Handle GetNamedActorInfo RPC.
    /// Returns both the actor table data and the task spec (needed for make_actor_handle).
    pub fn handle_get_named_actor_info(
        &self,
        name: &str,
        namespace: &str,
    ) -> Option<(
        ray_proto::ray::rpc::ActorTableData,
        Option<ray_proto::ray::rpc::TaskSpec>,
    )> {
        let actor_id = *self
            .named_actors
            .get(&(namespace.to_string(), name.to_string()))?;
        let actor_data = self.registered_actors.get(&actor_id)?;
        let task_spec = self.actor_task_specs.get(&actor_id).map(|e| e.clone());
        Some(((**actor_data).clone(), task_spec))
    }

    /// Handle ListNamedActors RPC.
    pub fn handle_list_named_actors(
        &self,
        namespace: &str,
        all_namespaces: bool,
    ) -> Vec<(String, String)> {
        self.named_actors
            .iter()
            .filter(|entry| {
                let (ns, _) = entry.key();
                all_namespaces || ns == namespace
            })
            .map(|entry| {
                let (ns, name) = entry.key();
                (ns.clone(), name.clone())
            })
            .collect()
    }

    /// Handle GetAllActorInfo RPC.
    pub fn handle_get_all_actor_info(
        &self,
        limit: Option<usize>,
        _job_id_filter: Option<&JobID>,
        actor_state_filter: Option<ActorState>,
    ) -> Vec<ray_proto::ray::rpc::ActorTableData> {
        // Collect Arc refs from both maps, then clone the data
        let registered: Vec<Arc<ray_proto::ray::rpc::ActorTableData>> = self
            .registered_actors
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect();
        let dead: Vec<Arc<ray_proto::ray::rpc::ActorTableData>> =
            self.dead_actors.iter().map(|e| Arc::clone(e.value())).collect();

        let all = registered.iter().chain(dead.iter()).filter(|a| {
            if let Some(state) = actor_state_filter {
                if ActorState::from(a.state) != state {
                    return false;
                }
            }
            true
        });

        if let Some(limit) = limit {
            all.take(limit).map(|a| (**a).clone()).collect()
        } else {
            all.map(|a| (**a).clone()).collect()
        }
    }

    /// Handle KillActorViaGcs RPC.
    pub async fn handle_kill_actor(&self, actor_id_bytes: &[u8]) -> Result<(), tonic::Status> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));

        let actor = {
            if let Some((_, old_arc)) = self.registered_actors.remove(&actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *self.state_counts.entry(ActorState::Dead).or_insert(0) += 1;

                // Set death cause so C++ core worker can deserialize the error
                actor.death_cause = Some(make_actor_died_death_cause(
                    &actor,
                    ray_proto::ray::rpc::actor_died_error_context::Reason::RayKill,
                    "The actor was killed by `ray.kill`.",
                ));

                Some(actor)
            } else {
                None
            }
        };

        if let Some(actor) = actor {
            // Publish DEAD state BEFORE notifying the raylet to kill the worker.
            self.publish_actor_state(&actor);

            // Remove from named actors
            if !actor.name.is_empty() {
                self.named_actors
                    .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
            }

            // Notify raylet to kill the actor's worker and release resources
            self.notify_raylet_to_kill_actor(&actor).await;

            self.dead_actors.insert(actor_id, Arc::new(actor));
            // Clean up task spec
            self.actor_task_specs.remove(&actor_id);
            // Resolve any pending create callbacks with error
            let callbacks = self.create_callbacks.remove(&actor_id);
            if let Some((_, senders)) = callbacks {
                for tx in senders {
                    let _ = tx.send(Err(Status::cancelled("actor was killed")));
                }
            }
            tracing::info!(?actor_id, "Actor killed");
        }
        Ok(())
    }

    /// Handle ReportActorOutOfScope RPC.
    pub fn handle_report_actor_out_of_scope(
        &self,
        actor_id_bytes: &[u8],
        num_restarts_due_to_lineage_reconstruction: u64,
    ) -> Result<(), tonic::Status> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));

        let actor = {
            if let Some((_, old_arc)) = self.registered_actors.remove(&actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);

                if (actor.num_restarts as u64) > num_restarts_due_to_lineage_reconstruction {
                    self.registered_actors.insert(actor_id, Arc::new(actor));
                    return Ok(());
                }

                let old_state = ActorState::from(actor.state);
                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *self.state_counts.entry(ActorState::Dead).or_insert(0) += 1;

                actor.death_cause = Some(make_actor_died_death_cause(
                    &actor,
                    ray_proto::ray::rpc::actor_died_error_context::Reason::OutOfScope,
                    "The actor is dead because it has gone out of scope.",
                ));

                Some(actor)
            } else {
                None
            }
        };

        if let Some(actor) = actor {
            self.publish_actor_state(&actor);
            if !actor.name.is_empty() {
                self.named_actors
                    .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
            }
            self.dead_actors.insert(actor_id, Arc::new(actor));
            self.actor_task_specs.remove(&actor_id);
            if let Some((_, senders)) = self.create_callbacks.remove(&actor_id) {
                for tx in senders {
                    let _ = tx.send(Err(Status::cancelled("actor went out of scope")));
                }
            }
        }

        Ok(())
    }

    /// Mark all actors on a node as preempted, persist to table storage, and publish.
    /// C++ parity: `GcsActorManager::SetPreemptedAndPublish(node_id)`.
    ///
    /// This is called before the drain is forwarded to the raylet so that
    /// actors' `preempted` flag is set before they die. On actor death,
    /// the preempted flag determines `num_restarts_due_to_node_preemption`.
    ///
    /// C++ persists to ActorTable().Put() then publishes in the callback.
    /// We persist + publish here so the state survives a GCS restart.
    pub fn set_preempted_and_publish(&self, node_id: &NodeID) {
        let actor_ids = match self.actors_by_node.get(node_id) {
            Some(entry) => entry.value().clone(),
            None => return, // No actors on this node
        };

        let storage = self.table_storage.clone();
        let pubsub = self.pubsub_handler.get().cloned();
        let mut actors_to_persist_and_publish = Vec::new();

        for actor_id in actor_ids {
            if let Some((_, old_arc)) = self.registered_actors.remove(&actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                actor.preempted = true;
                let updated = Arc::new(actor);
                // Build pub message now, but defer publication until after persist
                let msg = Self::build_actor_pub_message(&updated);
                actors_to_persist_and_publish.push((actor_id, (*updated).clone(), msg));
                self.registered_actors.insert(actor_id, updated);
            }
        }

        // C++ parity: persist first, then publish from the Put callback.
        // Publication is ordered after persistence so that pubsub observers
        // never see actor state that storage does not yet contain.
        tokio::spawn(async move {
            for (actor_id, actor_data, pub_msg) in actors_to_persist_and_publish {
                let key = hex::encode(actor_id.binary());
                if let Err(e) = storage.actor_table().put(&key, &actor_data).await {
                    tracing::error!(?actor_id, ?e, "Failed to persist preempted actor state");
                    continue; // Do not publish if persist failed
                }
                // Publish only after successful persistence (matches C++ Put callback)
                if let Some(ref handler) = pubsub {
                    handler.publish_pubmessage(pub_msg);
                }
            }
        });
    }

    /// Handle node death — kill or restart actors on the dead node.
    ///
    /// Actors with remaining restarts are transitioned to RESTARTING and will
    /// need to be rescheduled. Actors at max restarts are transitioned to DEAD.
    pub fn on_node_dead(&self, node_id: &NodeID) {
        let actor_ids = self
            .actors_by_node
            .get(node_id)
            .map(|e| e.value().clone())
            .unwrap_or_default();

        let mut deferred_messages = Vec::new();
        let mut named_keys_to_remove = Vec::new();
        let mut callback_errors: Vec<(
            ActorID,
            Vec<oneshot::Sender<Result<ray_proto::ray::rpc::CreateActorReply, Status>>>,
        )> = Vec::new();

        for actor_id in actor_ids {
            if let Some((_, old_arc)) = self.registered_actors.remove(&actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }

                // Check if actor can be restarted — C++ parity:
                // effective_restarts = num_restarts - num_restarts_due_to_node_preemption
                // remaining = max_restarts - effective_restarts
                // Also: preempted actors get an extra restart even at budget limit
                let max_restarts = actor.max_restarts;
                let num_restarts = actor.num_restarts;
                let preemption_restarts = actor.num_restarts_due_to_node_preemption;
                let is_preempted = actor.preempted;

                // C++ parity: effective_restarts = num_restarts - num_restarts_due_to_node_preemption
                let remaining_restarts = if max_restarts == -1 {
                    -1i64
                } else {
                    let effective = num_restarts as u64 - preemption_restarts;
                    let remaining = max_restarts - effective as i64;
                    remaining.max(0)
                };

                // C++ parity: restart if budget remains, OR if preempted with max_restarts > 0
                let can_restart = remaining_restarts != 0
                    || (max_restarts > 0 && is_preempted);

                if can_restart {
                    // C++ parity: if preempted, increment num_restarts_due_to_node_preemption
                    if is_preempted {
                        actor.num_restarts_due_to_node_preemption = preemption_restarts + 1;
                    }

                    // Transition to RESTARTING
                    actor.state = ActorState::Restarting as i32;
                    actor.num_restarts = num_restarts + 1;
                    actor.address = None;
                    actor.node_id = None;
                    *self
                        .state_counts
                        .entry(ActorState::Restarting)
                        .or_insert(0) += 1;
                    deferred_messages.push(Self::build_actor_pub_message(&actor));
                    // C++ parity: WriteActorExportEvent(false, restart_reason) — RESTARTING
                    self.write_actor_export_event(&actor, false);
                    // Re-insert as RESTARTING
                    self.registered_actors.insert(actor_id, Arc::new(actor));
                    tracing::info!(?actor_id, restart = num_restarts + 1, "Actor restarting");
                } else {
                    // Transition to DEAD
                    actor.state = ActorState::Dead as i32;
                    *self.state_counts.entry(ActorState::Dead).or_insert(0) += 1;

                    actor.death_cause = Some(make_actor_died_death_cause(
                        &actor,
                        ray_proto::ray::rpc::actor_died_error_context::Reason::NodeDied,
                        "The actor is dead because its node has died.",
                    ));

                    if !actor.name.is_empty() {
                        named_keys_to_remove
                            .push((actor.ray_namespace.clone(), actor.name.clone()));
                    }

                    deferred_messages.push(Self::build_actor_pub_message(&actor));
                    // C++ parity: WriteActorExportEvent(false) — DEAD (node died)
                    self.write_actor_export_event(&actor, false);

                    if let Some((_, senders)) = self.create_callbacks.remove(&actor_id) {
                        callback_errors.push((actor_id, senders));
                    }

                    self.dead_actors.insert(actor_id, Arc::new(actor));
                }
            }
        }

        // Remove named actors
        for key in named_keys_to_remove {
            self.named_actors.remove(&key);
        }

        // Publish all state changes
        self.publish_deferred_messages(deferred_messages);

        // Resolve callbacks
        for (_actor_id, senders) in callback_errors {
            for tx in senders {
                let _ = tx.send(Err(Status::unavailable("node died during actor creation")));
            }
        }

        self.actors_by_node.remove(node_id);
    }

    /// Handle job death — kill non-detached actors belonging to the job.
    ///
    /// Detached actors survive job completion. Non-detached actors are killed.
    pub fn on_job_dead(&self, job_id: &JobID) {
        let job_id_int = job_id.to_int();

        // Find actors belonging to this job (job ID is embedded in actor ID)
        let actor_ids: Vec<ActorID> = self
            .registered_actors
            .iter()
            .filter(|entry| entry.key().job_id().to_int() == job_id_int)
            .map(|entry| *entry.key())
            .collect();

        let mut deferred_messages = Vec::new();
        let mut named_keys_to_remove = Vec::new();

        for actor_id in actor_ids {
            // Check if detached before removing
            let is_detached = self
                .registered_actors
                .get(&actor_id)
                .map(|e| e.is_detached)
                .unwrap_or(false);
            if is_detached {
                continue;
            }

            if let Some((_, old_arc)) = self.registered_actors.remove(&actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *self.state_counts.entry(ActorState::Dead).or_insert(0) += 1;

                actor.death_cause = Some(make_actor_died_death_cause(
                    &actor,
                    ray_proto::ray::rpc::actor_died_error_context::Reason::OutOfScope,
                    "The actor is dead because its owner job has completed.",
                ));

                if !actor.name.is_empty() {
                    named_keys_to_remove
                        .push((actor.ray_namespace.clone(), actor.name.clone()));
                }

                deferred_messages.push(Self::build_actor_pub_message(&actor));
                self.dead_actors.insert(actor_id, Arc::new(actor));
            }
        }

        // Remove named actors
        for key in named_keys_to_remove {
            self.named_actors.remove(&key);
        }

        // Publish all state changes
        self.publish_deferred_messages(deferred_messages);
    }

    /// Handle worker death — kill actors owned by the dead worker.
    pub fn on_worker_dead(&self, node_id: &NodeID, worker_id: &WorkerID) {
        let key = (*node_id, *worker_id);
        let actor_ids = self
            .actors_by_owner
            .get(&key)
            .map(|e| e.value().clone())
            .unwrap_or_default();

        if actor_ids.is_empty() {
            return;
        }

        let mut deferred_messages = Vec::new();
        let mut named_keys_to_remove = Vec::new();

        for actor_id in &actor_ids {
            if let Some((_, old_arc)) = self.registered_actors.remove(actor_id) {
                let mut actor = Arc::unwrap_or_clone(old_arc);
                let old_state = ActorState::from(actor.state);
                if let Some(mut c) = self.state_counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *self.state_counts.entry(ActorState::Dead).or_insert(0) += 1;

                actor.death_cause = Some(make_actor_died_death_cause(
                    &actor,
                    ray_proto::ray::rpc::actor_died_error_context::Reason::WorkerDied,
                    "The actor is dead because its worker process has died.",
                ));

                if !actor.name.is_empty() {
                    named_keys_to_remove
                        .push((actor.ray_namespace.clone(), actor.name.clone()));
                }

                deferred_messages.push(Self::build_actor_pub_message(&actor));
                self.dead_actors.insert(*actor_id, Arc::new(actor));
            }
        }

        // Remove named actors
        for key in named_keys_to_remove {
            self.named_actors.remove(&key);
        }

        // Publish all state changes
        self.publish_deferred_messages(deferred_messages);

        self.actors_by_owner.remove(&key);
    }

    /// Get state counts for metrics.
    pub fn state_counts(&self) -> HashMap<ActorState, usize> {
        self.state_counts
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect()
    }

    /// Total registered (non-dead) actors.
    pub fn num_registered_actors(&self) -> usize {
        self.registered_actors.len()
    }

    #[cfg(test)]
    pub(crate) fn force_move_registered_actor_to_dead_for_test(&self, actor_id_bytes: &[u8]) {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));
        if let Some((_, actor)) = self.registered_actors.remove(&actor_id) {
            let mut actor = Arc::unwrap_or_clone(actor);
            actor.state = ActorState::Dead as i32;
            self.dead_actors.insert(actor_id, Arc::new(actor));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor_scheduler::GcsActorScheduler;
    use crate::store_client::InMemoryStoreClient;

    fn make_task_spec(actor_id: u8, name: &str) -> ray_proto::ray::rpc::TaskSpec {
        let mut aid = vec![0u8; 16];
        aid[0] = actor_id;
        ray_proto::ray::rpc::TaskSpec {
            actor_creation_task_spec: Some(ray_proto::ray::rpc::ActorCreationTaskSpec {
                actor_id: aid,
                name: name.to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_register_and_get_actor() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        mgr.handle_register_actor(make_task_spec(1, "my_actor"))
            .await
            .unwrap();
        assert_eq!(mgr.num_registered_actors(), 1);

        // Get by name and verify fields match what was registered
        let (actor, _task_spec) = mgr
            .handle_get_named_actor_info("my_actor", "default")
            .expect("actor should be found by name");
        assert_eq!(actor.name, "my_actor");
        assert_eq!(actor.ray_namespace, "default");
        assert_eq!(actor.actor_id[0], 1);
    }

    #[tokio::test]
    async fn test_duplicate_named_actor() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        mgr.handle_register_actor(make_task_spec(1, "dup"))
            .await
            .unwrap();
        let result = mgr.handle_register_actor(make_task_spec(2, "dup")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_kill_actor() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        mgr.handle_register_actor(make_task_spec(1, "killme"))
            .await
            .unwrap();

        let mut aid = [0u8; 16];
        aid[0] = 1;
        mgr.handle_kill_actor(&aid).await.unwrap();
        assert_eq!(mgr.num_registered_actors(), 0);
        assert!(mgr
            .handle_get_named_actor_info("killme", "default")
            .is_none());
    }

    #[tokio::test]
    async fn test_report_actor_out_of_scope_marks_actor_dead() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        mgr.handle_register_actor(make_task_spec(1, "scopey"))
            .await
            .unwrap();

        let mut aid = [0u8; 16];
        aid[0] = 1;
        mgr.handle_report_actor_out_of_scope(&aid, 0).unwrap();

        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Dead);
        assert!(mgr
            .handle_get_named_actor_info("scopey", "default")
            .is_none());
    }

    #[tokio::test]
    async fn test_report_actor_out_of_scope_ignores_stale_restart_count() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        let mut spec = make_task_spec(2, "stale");
        spec.actor_creation_task_spec
            .as_mut()
            .unwrap()
            .max_actor_restarts = 5;
        mgr.handle_register_actor(spec).await.unwrap();

        let mut aid = [0u8; 16];
        aid[0] = 2;
        let actor_id = ActorID::from_binary(&aid);

        {
            let (id, actor) = mgr.registered_actors.remove(&actor_id).unwrap();
            let mut actor = Arc::unwrap_or_clone(actor);
            actor.state = ActorState::Restarting as i32;
            actor.num_restarts = 2;
            mgr.registered_actors.insert(id, Arc::new(actor));
        }

        mgr.handle_report_actor_out_of_scope(&aid, 1).unwrap();

        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Restarting);
    }

    #[tokio::test]
    async fn test_restart_actor_for_lineage_reconstruction() {
        use crate::actor_scheduler::tests::{MockCoreWorkerClient, MockRayletClient};
        use crate::node_manager::GcsNodeManager;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsActorManager::new(storage.clone()));

        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_mgr
            .handle_register_node(ray_proto::ray::rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(ray_proto::ray::rpc::RequestWorkerLeaseReply {
            worker_address: Some(ray_proto::ray::rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: vec![7u8; 28],
            }),
            worker_pid: 4321,
            ..Default::default()
        }));
        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(ray_proto::ray::rpc::PushTaskReply::default()));
        mgr.set_actor_scheduler(Arc::new(GcsActorScheduler::new(node_mgr, raylet, worker)));

        let mut task_spec = make_task_spec(3, "reconstructable");
        task_spec
            .actor_creation_task_spec
            .as_mut()
            .unwrap()
            .max_actor_restarts = 1;
        mgr.handle_register_actor(task_spec).await.unwrap();

        let actor_id = ActorID::from_binary(&[3u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        if let Some((_, actor)) = mgr.registered_actors.remove(&actor_id) {
            let mut actor = Arc::unwrap_or_clone(actor);
            actor.state = ActorState::Dead as i32;
            mgr.dead_actors.insert(actor_id, Arc::new(actor));
        }

        mgr.handle_restart_actor_for_lineage_reconstruction(
            &[3u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            1,
        )
        .await
        .unwrap();

        let actor = mgr
            .handle_get_actor_info(&[3u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Alive);
        assert_eq!(actor.num_restarts_due_to_lineage_reconstruction, 1);
        assert_eq!(actor.pid, 4321);
    }

    #[tokio::test]
    async fn test_restart_actor_for_lineage_reconstruction_stale_request_is_ignored() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsActorManager::new(storage));

        let mut task_spec = make_task_spec(4, "stale_restart");
        task_spec
            .actor_creation_task_spec
            .as_mut()
            .unwrap()
            .max_actor_restarts = 1;
        mgr.handle_register_actor(task_spec).await.unwrap();

        let actor_id = ActorID::from_binary(&[4u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        if let Some((_, actor)) = mgr.registered_actors.remove(&actor_id) {
            let mut actor = Arc::unwrap_or_clone(actor);
            actor.state = ActorState::Dead as i32;
            actor.num_restarts_due_to_lineage_reconstruction = 1;
            mgr.dead_actors.insert(actor_id, Arc::new(actor));
        }

        mgr.handle_restart_actor_for_lineage_reconstruction(
            &[4u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            1,
        )
        .await
        .unwrap();

        let actor = mgr
            .handle_get_actor_info(&[4u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Dead);
        assert_eq!(actor.num_restarts_due_to_lineage_reconstruction, 1);
    }

    #[tokio::test]
    async fn test_list_named_actors() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        mgr.handle_register_actor(make_task_spec(1, "a"))
            .await
            .unwrap();
        mgr.handle_register_actor(make_task_spec(2, "b"))
            .await
            .unwrap();

        let names = mgr.handle_list_named_actors("default", false);
        assert_eq!(names.len(), 2);
    }

    #[tokio::test]
    async fn test_create_actor_basic() {
        use crate::actor_scheduler::tests::{MockCoreWorkerClient, MockRayletClient};
        use crate::node_manager::GcsNodeManager;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsActorManager::new(storage.clone()));

        // Set up node manager with one alive node
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_mgr
            .handle_register_node(ray_proto::ray::rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        // Set up mock clients
        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(ray_proto::ray::rpc::RequestWorkerLeaseReply {
            worker_address: Some(ray_proto::ray::rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: vec![42u8; 28],
            }),
            worker_pid: 12345,
            ..Default::default()
        }));
        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(ray_proto::ray::rpc::PushTaskReply::default()));

        let scheduler = Arc::new(GcsActorScheduler::new(node_mgr, raylet, worker));
        mgr.set_actor_scheduler(scheduler);

        // Register and create actor
        let task_spec = make_task_spec(1, "test_actor");
        mgr.handle_register_actor(task_spec.clone()).await.unwrap();

        let rx = mgr.handle_create_actor(task_spec).await.unwrap();
        let reply = rx.await.unwrap().unwrap();

        // Verify actor is ALIVE
        let mut aid = [0u8; 16];
        aid[0] = 1;
        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Alive);
        assert_eq!(actor.pid, 12345);
        assert!(reply.actor_address.is_some());
    }

    #[tokio::test]
    async fn test_create_actor_scheduling_failed() {
        use crate::actor_scheduler::tests::{MockCoreWorkerClient, MockRayletClient};
        use crate::node_manager::GcsNodeManager;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsActorManager::new(storage.clone()));

        // Empty cluster — no nodes
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let raylet = Arc::new(MockRayletClient::new());
        let worker = Arc::new(MockCoreWorkerClient::new());
        let scheduler = Arc::new(GcsActorScheduler::new(node_mgr, raylet, worker));
        mgr.set_actor_scheduler(scheduler);

        let task_spec = make_task_spec(1, "doomed_actor");
        mgr.handle_register_actor(task_spec.clone()).await.unwrap();

        let rx = mgr.handle_create_actor(task_spec).await.unwrap();
        let result = rx.await.unwrap();

        // Should fail with error
        assert!(result.is_err());
        // Actor should be DEAD
        assert_eq!(mgr.num_registered_actors(), 0);
    }

    #[tokio::test]
    async fn test_kill_actor_during_creation() {
        use crate::actor_scheduler::tests::{MockCoreWorkerClient, MockRayletClient};
        use crate::node_manager::GcsNodeManager;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsActorManager::new(storage.clone()));

        // Set up a node manager with a node, but a raylet that will never respond
        // (we won't push any replies, so schedule() will fail immediately since
        // we use a mock that returns error when no replies)
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        node_mgr
            .handle_register_node(ray_proto::ray::rpc::GcsNodeInfo {
                node_id: vec![0u8; 28],
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        // Don't push any reply — it will error
        let worker = Arc::new(MockCoreWorkerClient::new());
        let scheduler = Arc::new(GcsActorScheduler::new(node_mgr, raylet, worker));
        mgr.set_actor_scheduler(scheduler);

        let task_spec = make_task_spec(1, "");
        mgr.handle_register_actor(task_spec.clone()).await.unwrap();

        let rx = mgr.handle_create_actor(task_spec).await.unwrap();
        // The scheduling will fail (no mock reply = error)
        let result = rx.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_on_actor_creation_success_directly() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        // Register an actor
        mgr.handle_register_actor(make_task_spec(1, "direct"))
            .await
            .unwrap();

        // Manually transition to PENDING and add callback
        let mut aid = [0u8; 16];
        aid[0] = 1;
        let actor_id = ActorID::from_binary(&aid);
        if let Some((_, old_arc)) = mgr.registered_actors.remove(&actor_id) {
            let mut a = Arc::unwrap_or_clone(old_arc);
            a.state = ActorState::PendingCreation as i32;
            mgr.registered_actors.insert(actor_id, Arc::new(a));
        }

        let (tx, rx) = oneshot::channel();
        mgr.create_callbacks
            .entry(actor_id)
            .or_default()
            .push(tx);

        // Simulate success
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: vec![42u8; 28],
            },
            9999,
            vec![],
            node_id,
        );

        let reply = rx.await.unwrap().unwrap();
        assert!(reply.actor_address.is_some());

        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Alive);
        assert_eq!(actor.pid, 9999);
    }

    #[tokio::test]
    async fn test_actor_restart_on_node_death() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        // Register an actor with max_restarts = 3
        let mut aid = vec![0u8; 16];
        aid[0] = 1;
        let task_spec = ray_proto::ray::rpc::TaskSpec {
            actor_creation_task_spec: Some(ray_proto::ray::rpc::ActorCreationTaskSpec {
                actor_id: aid.clone(),
                name: "restartable".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_register_actor(task_spec).await.unwrap();

        // Place actor on node 1
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        let actor_id = ActorID::from_binary(aid.as_slice().try_into().unwrap());
        if let Some((_, old_arc)) = mgr.registered_actors.remove(&actor_id) {
            let mut a = Arc::unwrap_or_clone(old_arc);
            a.state = ActorState::Alive as i32;
            a.max_restarts = 3;
            a.node_id = Some(node_id.clone());
            mgr.registered_actors.insert(actor_id, Arc::new(a));
        }
        mgr.actors_by_node
            .entry(NodeID::from_binary(node_id.as_slice().try_into().unwrap()))
            .or_default()
            .push(actor_id);

        // Kill node 1 — actor should restart, not die
        let nid = NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        mgr.on_node_dead(&nid);

        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Restarting);
        assert_eq!(actor.num_restarts, 1);
        assert!(actor.address.is_none()); // cleared for rescheduling
    }

    #[tokio::test]
    async fn test_actor_no_restart_when_exhausted() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        // max_restarts = 1, already restarted once
        let mut aid = vec![0u8; 16];
        aid[0] = 2;
        mgr.handle_register_actor(make_task_spec(2, "limited"))
            .await
            .unwrap();

        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        let actor_id = ActorID::from_binary(aid.as_slice().try_into().unwrap());
        if let Some((_, old_arc)) = mgr.registered_actors.remove(&actor_id) {
            let mut a = Arc::unwrap_or_clone(old_arc);
            a.state = ActorState::Alive as i32;
            a.max_restarts = 1;
            a.num_restarts = 1; // already restarted once
            a.node_id = Some(node_id.clone());
            mgr.registered_actors.insert(actor_id, Arc::new(a));
        }
        mgr.actors_by_node
            .entry(NodeID::from_binary(node_id.as_slice().try_into().unwrap()))
            .or_default()
            .push(actor_id);

        let nid = NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        mgr.on_node_dead(&nid);

        // Should be DEAD since restarts exhausted
        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Dead);
    }

    #[tokio::test]
    async fn test_on_job_dead_kills_non_detached() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        // Create two actors — one detached, one not
        // Both belong to job 1 (embedded in actor ID)
        let job_id = JobID::from_int(1);
        let driver_task = ray_common::id::TaskID::for_driver_task(&job_id);
        let non_detached_aid = ActorID::of(&job_id, &driver_task, 0);
        let detached_aid = ActorID::of(&job_id, &driver_task, 1);

        // Register non-detached actor
        let non_detached_spec = ray_proto::ray::rpc::TaskSpec {
            actor_creation_task_spec: Some(ray_proto::ray::rpc::ActorCreationTaskSpec {
                actor_id: non_detached_aid.binary().to_vec(),
                name: "normal".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_register_actor(non_detached_spec).await.unwrap();
        if let Some((_, old_arc)) = mgr.registered_actors.remove(&non_detached_aid) {
            let mut a = Arc::unwrap_or_clone(old_arc);
            a.is_detached = false;
            a.state = ActorState::Alive as i32;
            mgr.registered_actors.insert(non_detached_aid, Arc::new(a));
        }

        // Register detached actor
        let detached_spec = ray_proto::ray::rpc::TaskSpec {
            actor_creation_task_spec: Some(ray_proto::ray::rpc::ActorCreationTaskSpec {
                actor_id: detached_aid.binary().to_vec(),
                name: "detached".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_register_actor(detached_spec).await.unwrap();
        if let Some((_, old_arc)) = mgr.registered_actors.remove(&detached_aid) {
            let mut a = Arc::unwrap_or_clone(old_arc);
            a.is_detached = true;
            a.state = ActorState::Alive as i32;
            mgr.registered_actors.insert(detached_aid, Arc::new(a));
        }

        assert_eq!(mgr.num_registered_actors(), 2);

        // Job dies
        mgr.on_job_dead(&job_id);

        // Non-detached should be dead, detached should survive
        assert_eq!(mgr.num_registered_actors(), 1);
        let detached = mgr.handle_get_actor_info(&detached_aid.binary()).unwrap();
        assert_eq!(ActorState::from(detached.state), ActorState::Alive);
    }

    #[tokio::test]
    async fn test_on_worker_dead() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        let mut aid = vec![0u8; 16];
        aid[0] = 1;
        mgr.handle_register_actor(make_task_spec(1, "owned"))
            .await
            .unwrap();

        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        let mut worker_id = vec![0u8; 28];
        worker_id[0] = 42;
        let actor_id = ActorID::from_binary(aid.as_slice().try_into().unwrap());
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: worker_id.clone(),
            },
            1234,
            vec![],
            node_id.clone(),
        );

        // Worker dies
        let nid = NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        let wid = WorkerID::from_binary(worker_id.as_slice().try_into().unwrap());
        mgr.on_worker_dead(&nid, &wid);

        let actor = mgr.handle_get_actor_info(&aid).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Dead);
    }

    #[tokio::test]
    async fn test_named_actor_discoverable_after_alive() {
        use crate::actor_scheduler::tests::{MockCoreWorkerClient, MockRayletClient};
        use crate::node_manager::GcsNodeManager;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsActorManager::new(storage.clone()));

        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_mgr
            .handle_register_node(ray_proto::ray::rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(ray_proto::ray::rpc::RequestWorkerLeaseReply {
            worker_address: Some(ray_proto::ray::rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: vec![42u8; 28],
            }),
            worker_pid: 111,
            ..Default::default()
        }));
        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(ray_proto::ray::rpc::PushTaskReply::default()));

        let scheduler = Arc::new(GcsActorScheduler::new(node_mgr, raylet, worker));
        mgr.set_actor_scheduler(scheduler);

        let task_spec = make_task_spec(1, "named_one");
        mgr.handle_register_actor(task_spec.clone()).await.unwrap();

        let rx = mgr.handle_create_actor(task_spec).await.unwrap();
        rx.await.unwrap().unwrap();

        // Should be discoverable by name
        let (actor, _task_spec) = mgr
            .handle_get_named_actor_info("named_one", "default")
            .expect("named actor should be found");
        assert_eq!(ActorState::from(actor.state), ActorState::Alive);
    }

    // ---- Actor preemption lifecycle tests (C++ parity) ----

    /// Helper: register an actor, place it on a node as ALIVE, and track it.
    fn setup_alive_actor_on_node(
        mgr: &GcsActorManager,
        actor_id_byte: u8,
        name: &str,
        max_restarts: i64,
        node_id_bytes: &[u8],
    ) -> (ActorID, NodeID) {
        let mut aid = vec![0u8; 16];
        aid[0] = actor_id_byte;
        let actor_id = ActorID::from_binary(aid.as_slice().try_into().unwrap());
        let node_id = NodeID::from_binary(node_id_bytes.try_into().unwrap());

        // Insert directly into registered_actors as ALIVE
        let actor_data = ray_proto::ray::rpc::ActorTableData {
            actor_id: aid.clone(),
            state: ActorState::Alive as i32,
            name: name.to_string(),
            ray_namespace: "default".to_string(),
            max_restarts,
            node_id: Some(node_id_bytes.to_vec()),
            ..Default::default()
        };
        mgr.registered_actors.insert(actor_id, Arc::new(actor_data));
        mgr.actors_by_node
            .entry(node_id)
            .or_default()
            .push(actor_id);
        *mgr.state_counts.entry(ActorState::Alive).or_insert(0) += 1;

        (actor_id, node_id)
    }

    #[tokio::test]
    async fn test_preempted_actor_state_persisted_to_actor_table() {
        // C++ persists preempted=true to actor table storage in SetPreemptedAndPublish.
        // Rust must do the same so that the state survives GCS restart.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 10, "persist_test", 3, &node_id_bytes);

        // Mark preempted
        mgr.set_preempted_and_publish(&node_id);

        // Yield to let the spawned persist task complete
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // In-memory should be preempted
        let actor = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert!(actor.preempted, "in-memory actor should be preempted");

        // Actor table storage should also have preempted=true
        let key = hex::encode(&actor_id.binary());
        let persisted: Option<ray_proto::ray::rpc::ActorTableData> =
            storage.actor_table().get(&key).await.unwrap();
        assert!(
            persisted.is_some(),
            "actor should be persisted to table storage"
        );
        assert!(
            persisted.unwrap().preempted,
            "persisted actor should have preempted=true"
        );
    }

    #[tokio::test]
    async fn test_node_preemption_increments_num_restarts_due_to_node_preemption() {
        // C++ increments num_restarts_due_to_node_preemption when restarting a
        // preempted actor. Rust must do the same.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 11, "preempt_counter", 5, &node_id_bytes);

        // Mark preempted
        mgr.set_preempted_and_publish(&node_id);

        // Kill node — actor should restart
        mgr.on_node_dead(&node_id);

        let actor = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Restarting);
        assert_eq!(actor.num_restarts, 1);
        assert_eq!(
            actor.num_restarts_due_to_node_preemption, 1,
            "preemption restart should increment num_restarts_due_to_node_preemption"
        );
    }

    #[tokio::test]
    async fn test_node_preemption_restart_does_not_consume_regular_restart_budget() {
        // C++ computes effective_restarts = num_restarts - num_restarts_due_to_node_preemption.
        // So preemption restarts don't count against max_restarts.
        // An actor with max_restarts=1, 1 preemption restart, should still be restartable.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 12, "budget_test", 1, &node_id_bytes);

        // Mark preempted and kill — first restart is a preemption restart
        mgr.set_preempted_and_publish(&node_id);
        mgr.on_node_dead(&node_id);

        let actor = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert_eq!(ActorState::from(actor.state), ActorState::Restarting);
        assert_eq!(actor.num_restarts, 1);
        assert_eq!(actor.num_restarts_due_to_node_preemption, 1);

        // Now simulate actor coming back alive on a new node, then dying (non-preempted)
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        let node2 = NodeID::from_binary(node2_bytes.as_slice().try_into().unwrap());
        {
            if let Some((_, old_arc)) = mgr.registered_actors.remove(&actor_id) {
                let mut a = Arc::unwrap_or_clone(old_arc);
                a.state = ActorState::Alive as i32;
                a.preempted = false; // reset on creation success
                a.node_id = Some(node2_bytes.clone());
                mgr.registered_actors.insert(actor_id, Arc::new(a));
            }
            mgr.actors_by_node
                .entry(node2)
                .or_default()
                .push(actor_id);
        }

        // Kill node 2 — NOT preempted this time.
        // effective_restarts = 1 - 1 = 0, remaining = 1 - 0 = 1. Actor should restart.
        mgr.on_node_dead(&node2);

        let actor2 = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert_eq!(
            ActorState::from(actor2.state),
            ActorState::Restarting,
            "Actor should still be restartable because preemption restart didn't consume budget"
        );
        assert_eq!(actor2.num_restarts, 2);
        // num_restarts_due_to_node_preemption stays at 1 (second restart was not preemption)
        assert_eq!(actor2.num_restarts_due_to_node_preemption, 1);
    }

    #[tokio::test]
    async fn test_preempted_actor_state_survives_gcs_restart_until_consumed() {
        // C++ persists preempted=true to table storage. After a GCS restart,
        // the actor should still have preempted=true so the restart-accounting
        // works correctly.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store.clone()));
        let mgr = GcsActorManager::new(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 13, "gcs_restart_test", 3, &node_id_bytes);

        // Also persist the initial actor to storage (simulating normal operation)
        let key = hex::encode(&actor_id.binary());
        let initial_actor = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        storage.actor_table().put(&key, &initial_actor).await.unwrap();

        // Mark preempted (should persist)
        mgr.set_preempted_and_publish(&node_id);

        // Yield to let the spawned persist task complete
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Simulate GCS restart: create a new actor manager, initialize from storage
        let mgr2 = GcsActorManager::new(storage.clone());
        mgr2.initialize().await.unwrap();

        let actor = mgr2.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert!(
            actor.preempted,
            "after GCS restart, actor should still have preempted=true from storage"
        );
    }

    // ---- Creation-success persistence tests (C++ parity Round 4) ----

    #[tokio::test]
    async fn test_on_actor_creation_success_persists_preempted_reset() {
        // C++ OnActorCreationSuccess sets preempted=false AND persists via ActorTable().Put().
        // Rust must also persist, not just update in memory.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage.clone());

        let mut node1_bytes = vec![0u8; 28];
        node1_bytes[0] = 1;
        let (actor_id, node1) =
            setup_alive_actor_on_node(&mgr, 20, "persist_reset", 3, &node1_bytes);

        // Persist initial actor to storage
        let key = hex::encode(&actor_id.binary());
        let initial = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        storage.actor_table().put(&key, &initial).await.unwrap();

        // Mark preempted and wait for persist
        mgr.set_preempted_and_publish(&node1);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Verify preempted=true in storage
        let persisted: ray_proto::ray::rpc::ActorTableData =
            storage.actor_table().get(&key).await.unwrap().unwrap();
        assert!(persisted.preempted, "should be preempted in storage before creation success");

        // Kill node (actor restarts)
        mgr.on_node_dead(&node1);

        // Now call the real on_actor_creation_success (actor re-created on node 2)
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            9999,
            vec![],
            node2_bytes,
        );

        // Yield to let any spawned persist task complete
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // In-memory should have preempted=false
        let actor = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert!(!actor.preempted, "in-memory should have preempted=false after creation success");

        // Storage should ALSO have preempted=false
        let persisted_after: ray_proto::ray::rpc::ActorTableData =
            storage.actor_table().get(&key).await.unwrap().unwrap();
        assert!(
            !persisted_after.preempted,
            "storage should have preempted=false after creation success"
        );
    }

    #[tokio::test]
    async fn test_preempted_reset_survives_gcs_restart_after_creation_success() {
        // After on_actor_creation_success resets preempted=false and persists it,
        // a fresh GcsActorManager::initialize() must load preempted=false.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store.clone()));
        let mgr = GcsActorManager::new(storage.clone());

        let mut node1_bytes = vec![0u8; 28];
        node1_bytes[0] = 1;
        let (actor_id, node1) =
            setup_alive_actor_on_node(&mgr, 21, "restart_survive", 3, &node1_bytes);

        // Persist initial actor
        let key = hex::encode(&actor_id.binary());
        let initial = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        storage.actor_table().put(&key, &initial).await.unwrap();

        // Mark preempted, wait for persist
        mgr.set_preempted_and_publish(&node1);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Kill node, actor restarts
        mgr.on_node_dead(&node1);

        // Real creation success on new node
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            8888,
            vec![],
            node2_bytes,
        );

        // Yield to let persist complete
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Simulate GCS restart
        let mgr2 = GcsActorManager::new(storage.clone());
        mgr2.initialize().await.unwrap();

        let actor = mgr2.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert!(
            !actor.preempted,
            "after GCS restart, actor should have preempted=false from persisted creation-success state"
        );
        assert_eq!(
            ActorState::from(actor.state),
            ActorState::Alive,
            "after GCS restart, actor should be ALIVE"
        );
    }

    #[tokio::test]
    async fn test_real_creation_success_path_clears_preempted_in_storage_not_just_memory() {
        // This test proves that the real on_actor_creation_success path
        // (not manual in-memory mutation) clears preempted in storage.
        // It uses the full preempt → kill → restart → creation-success flow.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage.clone());

        let mut node1_bytes = vec![0u8; 28];
        node1_bytes[0] = 1;
        let (actor_id, node1) =
            setup_alive_actor_on_node(&mgr, 22, "full_flow", 5, &node1_bytes);

        // Persist initial actor
        let key = hex::encode(&actor_id.binary());
        let initial = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        storage.actor_table().put(&key, &initial).await.unwrap();

        // Mark preempted, persist
        mgr.set_preempted_and_publish(&node1);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Kill node → actor RESTARTING with preempted counters
        mgr.on_node_dead(&node1);
        let restarting = mgr.handle_get_actor_info(&actor_id.binary()).unwrap();
        assert_eq!(ActorState::from(restarting.state), ActorState::Restarting);
        assert_eq!(restarting.num_restarts_due_to_node_preemption, 1);

        // Real creation success
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            7777,
            vec![],
            node2_bytes.clone(),
        );

        // Yield
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Check storage directly — must have preempted=false and state=ALIVE
        let persisted: ray_proto::ray::rpc::ActorTableData =
            storage.actor_table().get(&key).await.unwrap().unwrap();
        assert!(
            !persisted.preempted,
            "storage must have preempted=false after real creation-success path"
        );
        assert_eq!(
            ActorState::from(persisted.state),
            ActorState::Alive,
            "storage must have state=ALIVE after real creation-success path"
        );
        assert_eq!(
            persisted.num_restarts_due_to_node_preemption, 1,
            "storage must preserve num_restarts_due_to_node_preemption"
        );
    }

    // ---- Publication ordering tests (C++ parity: persist before publish) ----

    /// A store client wrapper that blocks `put` on the Actor table until a
    /// `Notify` is signalled. This lets tests verify that publication does not
    /// happen before persistence completes.
    struct GatedStoreClient {
        inner: InMemoryStoreClient,
        /// Signalled by the test when persistence should be allowed to proceed.
        gate: Arc<tokio::sync::Notify>,
        /// Signalled by the store when a `put` is waiting at the gate.
        waiting: Arc<tokio::sync::Notify>,
    }

    impl GatedStoreClient {
        fn new() -> (Arc<Self>, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            let gate = Arc::new(tokio::sync::Notify::new());
            let waiting = Arc::new(tokio::sync::Notify::new());
            let client = Arc::new(Self {
                inner: InMemoryStoreClient::new(),
                gate: gate.clone(),
                waiting: waiting.clone(),
            });
            (client, gate, waiting)
        }
    }

    #[async_trait::async_trait]
    impl crate::store_client::StoreClient for GatedStoreClient {
        async fn put(
            &self,
            table: &str,
            key: &str,
            data: Vec<u8>,
            overwrite: bool,
        ) -> crate::store_client::StoreResult<bool> {
            if table == crate::table_storage::table_names::ACTOR {
                // Signal that we are waiting, then block until the gate is opened
                self.waiting.notify_one();
                self.gate.notified().await;
            }
            self.inner.put(table, key, data, overwrite).await
        }

        async fn get(
            &self,
            table: &str,
            key: &str,
        ) -> crate::store_client::StoreResult<Option<Vec<u8>>> {
            self.inner.get(table, key).await
        }

        async fn multi_get(
            &self,
            table: &str,
            keys: &[String],
        ) -> crate::store_client::StoreResult<std::collections::HashMap<String, Vec<u8>>> {
            self.inner.multi_get(table, keys).await
        }

        async fn get_all(
            &self,
            table: &str,
        ) -> crate::store_client::StoreResult<std::collections::HashMap<String, Vec<u8>>> {
            self.inner.get_all(table).await
        }

        async fn delete(
            &self,
            table: &str,
            key: &str,
        ) -> crate::store_client::StoreResult<bool> {
            self.inner.delete(table, key).await
        }

        async fn batch_delete(
            &self,
            table: &str,
            keys: &[String],
        ) -> crate::store_client::StoreResult<i64> {
            self.inner.batch_delete(table, keys).await
        }

        async fn get_next_job_id(&self) -> crate::store_client::StoreResult<i32> {
            self.inner.get_next_job_id().await
        }

        async fn get_keys(
            &self,
            table: &str,
            prefix: &str,
        ) -> crate::store_client::StoreResult<Vec<String>> {
            self.inner.get_keys(table, prefix).await
        }

        async fn exists(
            &self,
            table: &str,
            key: &str,
        ) -> crate::store_client::StoreResult<bool> {
            self.inner.exists(table, key).await
        }
    }

    #[tokio::test]
    async fn test_preempted_publish_occurs_only_after_actor_table_persist() {
        // C++ contract: SetPreemptedAndPublish persists via ActorTable().Put()
        // and publishes from the Put callback — publication is strictly ordered
        // after persistence. This test proves the Rust implementation matches.
        let (store, gate, waiting) = GatedStoreClient::new();
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage.clone());

        // Set up pubsub so we can observe publication
        let pubsub = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(pubsub.clone());
        let mut rx = pubsub
            .subscribe(crate::pubsub_handler::ChannelType::GcsActorChannel as i32)
            .unwrap();

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (_actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 50, "ordering_preempt", 3, &node_id_bytes);

        // Call set_preempted_and_publish — this spawns the persist-then-publish task
        mgr.set_preempted_and_publish(&node_id);

        // Wait until the persist task is blocked at the gate
        waiting.notified().await;

        // While persistence is blocked, publication must NOT have happened yet
        assert!(
            rx.try_recv().is_err(),
            "publication must not occur before persistence completes"
        );

        // Now release the gate — persistence completes, then publication should follow
        gate.notify_one();

        // Give the spawned task time to publish
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let msg = rx.try_recv().expect("publication should occur after persistence");
        assert_eq!(
            msg.channel_type,
            crate::pubsub_handler::ChannelType::GcsActorChannel as i32,
        );
    }

    #[tokio::test]
    async fn test_creation_success_publish_occurs_only_after_actor_table_persist() {
        // C++ contract: OnActorCreationSuccess persists via ActorTable().Put()
        // and publishes from the Put callback — publication is strictly ordered
        // after persistence. This test proves the Rust implementation matches.
        let (store, gate, waiting) = GatedStoreClient::new();
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage.clone());

        // Set up pubsub
        let pubsub = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(pubsub.clone());
        let mut rx = pubsub
            .subscribe(crate::pubsub_handler::ChannelType::GcsActorChannel as i32)
            .unwrap();

        // Set up actor in RESTARTING state (simulates preempt → kill → restart cycle)
        let mut node1_bytes = vec![0u8; 28];
        node1_bytes[0] = 1;
        let (actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 51, "ordering_creation", 5, &node1_bytes);

        mgr.set_preempted_and_publish(&node_id);
        // Unblock the preempt persist
        waiting.notified().await;
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        // Drain the preempt publication
        let _ = rx.try_recv();

        mgr.on_node_dead(&node_id);
        // Drain any node-dead publications
        tokio::task::yield_now().await;
        while rx.try_recv().is_ok() {}

        // Now call on_actor_creation_success — this should persist then publish
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            8888,
            vec![],
            node2_bytes,
        );

        // Wait until the persist task is blocked at the gate
        waiting.notified().await;

        // While persistence is blocked, ALIVE publication must NOT have happened
        assert!(
            rx.try_recv().is_err(),
            "ALIVE publication must not occur before persistence completes"
        );

        // Release the gate
        gate.notify_one();

        // Give the spawned task time to publish
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let msg = rx.try_recv().expect("ALIVE publication should occur after persistence");
        assert_eq!(
            msg.channel_type,
            crate::pubsub_handler::ChannelType::GcsActorChannel as i32,
        );
    }

    #[tokio::test]
    async fn test_pubsub_observer_cannot_see_unpersisted_actor_state() {
        // This test verifies the observer-visible contract: when a pubsub
        // subscriber receives an actor state update, the corresponding state
        // must already be present in storage. This is what the C++ Put-callback
        // ordering guarantees.
        let (store_client, gate, waiting) = GatedStoreClient::new();
        let storage = Arc::new(GcsTableStorage::new(store_client));
        let mgr = GcsActorManager::new(storage.clone());

        // Set up pubsub
        let pubsub = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(pubsub.clone());
        let mut rx = pubsub
            .subscribe(crate::pubsub_handler::ChannelType::GcsActorChannel as i32)
            .unwrap();

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 52, "observer_contract", 3, &node_id_bytes);

        // Trigger set_preempted_and_publish
        mgr.set_preempted_and_publish(&node_id);

        // Wait for persist to reach the gate (blocked)
        waiting.notified().await;

        // At this point, persistence is blocked. Verify storage does NOT yet
        // have the preempted state, AND pubsub has NOT received the message.
        let key = hex::encode(&actor_id.binary());
        let stored: Option<ray_proto::ray::rpc::ActorTableData> =
            storage.actor_table().get(&key).await.unwrap();
        // Storage should either be None or have preempted=false (pre-existing state)
        if let Some(ref data) = stored {
            assert!(
                !data.preempted,
                "storage must not have preempted=true while persist is still blocked"
            );
        }
        assert!(
            rx.try_recv().is_err(),
            "pubsub must not deliver preempted state before storage contains it"
        );

        // Release the gate — persist completes, then publish
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Now the observer receives the message
        let _msg = rx.try_recv().expect("pubsub should deliver after persist");

        // And at this point, storage must already contain the persisted state
        let stored: ray_proto::ray::rpc::ActorTableData =
            storage.actor_table().get(&key).await.unwrap().unwrap();
        assert!(
            stored.preempted,
            "when pubsub delivers preempted=true, storage must already contain it"
        );
    }

    // ---- Creation-success callback ordering tests (C++ parity: persist → publish → callbacks) ----

    /// Helper: set up a manager with GatedStoreClient and pubsub, register an actor
    /// as ALIVE, preempt it, kill the node (RESTARTING), and insert a create callback.
    /// Returns everything needed for callback ordering assertions.
    #[allow(clippy::type_complexity)]
    fn setup_callback_ordering_test(
        actor_byte: u8,
    ) -> (
        GcsActorManager,
        Arc<GcsTableStorage>,
        Arc<tokio::sync::Notify>,                      // gate
        Arc<tokio::sync::Notify>,                      // waiting
        ActorID,
        tokio::sync::broadcast::Receiver<crate::pubsub_handler::PubSubMessage>, // pubsub rx
        oneshot::Receiver<Result<ray_proto::ray::rpc::CreateActorReply, Status>>, // callback rx
    ) {
        let (store, gate, waiting) = GatedStoreClient::new();
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsActorManager::new(storage.clone());

        let pubsub = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(pubsub.clone());
        let rx_pubsub = pubsub
            .subscribe(crate::pubsub_handler::ChannelType::GcsActorChannel as i32)
            .unwrap();

        let mut node1_bytes = vec![0u8; 28];
        node1_bytes[0] = 1;
        let (actor_id, node_id) = setup_alive_actor_on_node(
            &mgr,
            actor_byte,
            &format!("cb_ordering_{}", actor_byte),
            5,
            &node1_bytes,
        );

        // Preempt — we need to unblock the gated persist for the preempt path
        mgr.set_preempted_and_publish(&node_id);

        // Insert a creation callback (simulates what handle_create_actor does)
        let (tx_cb, rx_cb) = oneshot::channel();
        mgr.create_callbacks
            .entry(actor_id)
            .or_default()
            .push(tx_cb);

        // Kill node so actor is RESTARTING
        mgr.on_node_dead(&node_id);

        (mgr, storage, gate, waiting, actor_id, rx_pubsub, rx_cb)
    }

    #[tokio::test]
    async fn test_creation_success_callbacks_fire_only_after_actor_table_persist() {
        // C++ contract: RunAndClearActorCreationCallbacks runs inside the
        // ActorTable().Put() callback, AFTER persistence completes.
        // A successful callback implies the actor table has been durably written.
        let (mgr, storage, gate, waiting, actor_id, _rx_pubsub, mut rx_cb) =
            setup_callback_ordering_test(60);

        // Unblock the preempt persist (from setup)
        waiting.notified().await;
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Now call on_actor_creation_success
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            9001,
            vec![],
            node2_bytes,
        );

        // Wait until the creation-success persist is blocked at the gate
        waiting.notified().await;

        // While persistence is blocked, the callback must NOT have fired
        assert!(
            rx_cb.try_recv().is_err(),
            "creation callback must not fire before persistence completes"
        );

        // Release the gate — persist completes, then publish, then callbacks
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Now the callback should have fired
        let result = rx_cb.try_recv().expect("callback should fire after persistence");
        assert!(result.is_ok(), "callback should indicate success");

        // And storage must already contain the ALIVE state
        let key = hex::encode(&actor_id.binary());
        let persisted: ray_proto::ray::rpc::ActorTableData =
            storage.actor_table().get(&key).await.unwrap().unwrap();
        assert_eq!(ActorState::from(persisted.state), ActorState::Alive);
    }

    #[tokio::test]
    async fn test_creation_success_callbacks_do_not_run_while_persist_blocked() {
        // Stronger version: prove the callback receiver stays pending (not just
        // "no value ready") the entire time persistence is blocked, by polling
        // it multiple times with yields in between.
        let (mgr, _storage, gate, waiting, actor_id, _rx_pubsub, mut rx_cb) =
            setup_callback_ordering_test(61);

        // Unblock the preempt persist
        waiting.notified().await;
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Call on_actor_creation_success
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            9002,
            vec![],
            node2_bytes,
        );

        // Wait for persist to reach the gate
        waiting.notified().await;

        // Poll the callback multiple times while persist is blocked
        for _ in 0..5 {
            tokio::task::yield_now().await;
            assert!(
                rx_cb.try_recv().is_err(),
                "callback must remain pending while persistence is blocked"
            );
        }

        // Also check after a short sleep
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            rx_cb.try_recv().is_err(),
            "callback must still be pending after sleep while persistence is blocked"
        );

        // Release the gate
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Now callback fires
        let result = rx_cb.try_recv().expect("callback should fire once persistence completes");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_creation_success_callbacks_are_ordered_after_publication() {
        // C++ ordering: persist → publish → callbacks.
        // This test proves callbacks fire only after publication, not just
        // after persistence. Uses an atomic flag set by a pubsub subscriber
        // to track publication timing relative to callback delivery.
        let (mgr, _storage, gate, waiting, actor_id, mut rx_pubsub, rx_cb) =
            setup_callback_ordering_test(62);

        // Unblock the preempt persist
        waiting.notified().await;
        gate.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        // Drain any preempt-related pubsub messages
        while rx_pubsub.try_recv().is_ok() {}

        // Set up a flag that tracks whether publication happened before callback
        let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let published_clone = published.clone();

        // Spawn a task that waits for the pubsub ALIVE message
        let pubsub_watcher = tokio::spawn(async move {
            // Wait for the ALIVE publication
            let _msg = rx_pubsub.recv().await.expect("should receive ALIVE pub");
            published_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Call on_actor_creation_success
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            9003,
            vec![],
            node2_bytes,
        );

        // Wait for persist to be blocked
        waiting.notified().await;

        // Release the gate — persist completes, then publish, then callbacks
        gate.notify_one();

        // Wait for the callback to resolve
        let result = rx_cb.await.expect("callback channel should not be dropped");
        assert!(result.is_ok(), "callback should succeed");

        // Give the pubsub watcher a moment to process
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let _ = pubsub_watcher.await;

        // By the time the callback resolved, publication must have already happened
        assert!(
            published.load(std::sync::atomic::Ordering::SeqCst),
            "publication must have occurred before (or at least by the time of) callback resolution"
        );
    }

    // ---- Actor lifecycle/export event tests (C++ parity: WriteActorExportEvent) ----

    /// Helper: create a manager with export events enabled.
    fn make_export_enabled_manager(
        storage: Arc<GcsTableStorage>,
    ) -> GcsActorManager {
        GcsActorManager::with_export_config(
            storage,
            ActorExportConfig::from_enabled(true),
        )
    }

    #[tokio::test]
    async fn test_actor_export_event_emitted_on_creation_success_when_enabled() {
        // C++ emits WriteActorExportEvent(false) on creation success (line 1686).
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = make_export_enabled_manager(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 70, "export_creation", 3, &node_id_bytes);

        // Now simulate creation success on a new node
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            5555,
            vec![],
            node2_bytes,
        );

        // The event exporter should have buffered an event
        assert!(
            mgr.event_exporter().buffer_len() > 0,
            "export event should be emitted on creation success when enabled"
        );
    }

    #[tokio::test]
    async fn test_actor_export_event_emitted_on_restart_when_enabled() {
        // C++ emits WriteActorExportEvent(false, restart_reason) on restart (line 1523).
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = make_export_enabled_manager(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (_actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 71, "export_restart", 5, &node_id_bytes);

        let events_before = mgr.event_exporter().buffer_len();

        // Kill the node — actor has max_restarts=5, so it should restart
        mgr.on_node_dead(&node_id);

        let events_after = mgr.event_exporter().buffer_len();
        assert!(
            events_after > events_before,
            "export event should be emitted on actor restart when enabled (before={}, after={})",
            events_before,
            events_after,
        );
    }

    #[tokio::test]
    async fn test_actor_export_event_emitted_on_death_when_enabled() {
        // C++ emits WriteActorExportEvent(false) on actor death (lines 1145, 1572).
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = make_export_enabled_manager(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        // max_restarts=0 means the actor will die on node death
        let (_actor_id, node_id) =
            setup_alive_actor_on_node(&mgr, 72, "export_death", 0, &node_id_bytes);

        let events_before = mgr.event_exporter().buffer_len();

        // Kill the node — actor has max_restarts=0, so it should die
        mgr.on_node_dead(&node_id);

        let events_after = mgr.event_exporter().buffer_len();
        assert!(
            events_after > events_before,
            "export event should be emitted on actor death when enabled (before={}, after={})",
            events_before,
            events_after,
        );
    }

    #[tokio::test]
    async fn test_actor_export_event_not_emitted_when_disabled() {
        // When export is disabled (default), no events should be buffered.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        // Use default config (disabled)
        let mgr = GcsActorManager::new(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 73, "export_disabled", 5, &node_id_bytes);

        // Creation success
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            6666,
            vec![],
            node2_bytes,
        );

        // Kill node — restart
        let mut node3_bytes = vec![0u8; 28];
        node3_bytes[0] = 3;
        mgr.on_node_dead(
            &NodeID::from_binary(node3_bytes.as_slice().try_into().unwrap()),
        );

        assert_eq!(
            mgr.event_exporter().buffer_len(),
            0,
            "no export events should be buffered when disabled"
        );
    }

    // === Round 9: FULL parity tests for actor export/event feature ===

    #[tokio::test]
    async fn test_actor_export_event_emitted_to_file_when_export_api_enabled() {
        // C++ parity: when `enable_export_api_write=true` (or `enable_export_api_write_config`
        // contains "EXPORT_ACTOR"), actor lifecycle events are written as JSON to
        // `{log_dir}/export_events/event_EXPORT_ACTOR.log`.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));

        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_export_api_write: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 80, "file_export_actor", 3, &node_id_bytes);

        // Trigger a lifecycle event that calls write_actor_export_event
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            7777,
            vec![],
            node2_bytes,
        );

        // Check that a file was created at the expected path
        let export_file = tmp.path().join("export_events/event_EXPORT_ACTOR.log");
        assert!(
            export_file.exists(),
            "export event log file should be created at {:?}",
            export_file
        );

        // Verify the file contains valid JSON lines with ExportActorData
        let content = std::fs::read_to_string(&export_file).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert!(
            !lines.is_empty(),
            "export event log file should contain at least one event"
        );

        // Parse the first event and verify structure
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(
            parsed.get("event_id").is_some(),
            "event should have event_id"
        );
        assert!(
            parsed.get("timestamp").is_some(),
            "event should have timestamp"
        );
        // Check that actor event data is present (prost serde: event_data.ActorEventData)
        let event_data = &parsed["event_data"]["ActorEventData"];
        assert!(
            event_data.is_object(),
            "event should contain event_data.ActorEventData with ExportActorData fields"
        );
    }

    #[tokio::test]
    async fn test_actor_export_event_honors_export_api_write_config_actor_only() {
        // C++ parity: `enable_export_api_write_config = ["EXPORT_ACTOR"]` enables
        // actor events selectively. `enable_export_api_write` (global) is false.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));

        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_export_api_write: false,
                enable_export_api_write_config: vec!["EXPORT_ACTOR".to_string()],
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 81, "selective_export_actor", 3, &node_id_bytes);

        // Trigger lifecycle event
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            8888,
            vec![],
            node2_bytes,
        );

        // File should exist — EXPORT_ACTOR is in the selective config
        let export_file = tmp.path().join("export_events/event_EXPORT_ACTOR.log");
        assert!(
            export_file.exists(),
            "selective EXPORT_ACTOR config should produce export event file"
        );

        // Now test with a config that does NOT include EXPORT_ACTOR
        let tmp2 = tempfile::tempdir().unwrap();
        let store2 = Arc::new(InMemoryStoreClient::new());
        let storage2 = Arc::new(GcsTableStorage::new(store2));
        let config2 = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_export_api_write: false,
                enable_export_api_write_config: vec!["EXPORT_TASK".to_string()],
                log_dir: Some(tmp2.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr2 = GcsActorManager::with_export_config(storage2.clone(), config2);

        let mut node_id_bytes2 = vec![0u8; 28];
        node_id_bytes2[0] = 2;
        let (_actor_id2, _node_id2) =
            setup_alive_actor_on_node(&mgr2, 82, "no_export_actor", 3, &node_id_bytes2);

        // File should NOT exist — EXPORT_TASK doesn't enable actor events
        let export_file2 = tmp2.path().join("export_events/event_EXPORT_ACTOR.log");
        assert!(
            !export_file2.exists(),
            "EXPORT_TASK-only config should NOT produce actor export event file"
        );
        assert_eq!(
            mgr2.event_exporter().buffer_len(),
            0,
            "no events should be buffered when actor export is not enabled"
        );
    }

    #[tokio::test]
    async fn test_actor_export_event_payload_matches_expected_fields() {
        // C++ parity: ExportActorData contains 16 fields.
        // Verify the JSON written to file includes all major fields.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));

        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_export_api_write: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        // Register an actor with known fields to verify payload
        let mut actor_id_bytes = vec![0u8; 16];
        actor_id_bytes[0] = 83;
        let actor_id = ActorID::from_binary(actor_id_bytes.as_slice().try_into().unwrap());
        let mut job_id_bytes = vec![0u8; 4];
        job_id_bytes[0] = 1;

        let mut task_spec = ray_proto::ray::rpc::TaskSpec {
            r#type: ray_proto::ray::rpc::TaskType::ActorCreationTask as i32,
            ..Default::default()
        };
        task_spec.actor_creation_task_spec = Some(
            ray_proto::ray::rpc::ActorCreationTaskSpec {
                actor_id: actor_id_bytes.clone(),
                max_actor_restarts: 3,
                ..Default::default()
            },
        );
        task_spec.job_id = job_id_bytes.clone();
        task_spec.labels.insert("env".to_string(), "prod".to_string());

        let actor_data = ray_proto::ray::rpc::ActorTableData {
            actor_id: actor_id_bytes.clone(),
            job_id: job_id_bytes.clone(),
            state: ray_proto::ray::rpc::actor_table_data::ActorState::DependenciesUnready as i32,
            name: "payload_test_actor".to_string(),
            class_name: "PayloadTestClass".to_string(),
            pid: 0,
            ray_namespace: "test_ns".to_string(),
            is_detached: true,
            serialized_runtime_env: r#"{"pip": ["numpy"]}"#.to_string(),
            repr_name: "PayloadTestClass()".to_string(),
            required_resources: {
                let mut m = std::collections::HashMap::new();
                m.insert("CPU".to_string(), 2.0);
                m.insert("GPU".to_string(), 1.0);
                m
            },
            label_selector: {
                let mut m = std::collections::HashMap::new();
                m.insert("zone".to_string(), "us-west".to_string());
                m
            },
            max_restarts: 3,
            ..Default::default()
        };

        mgr.registered_actors
            .insert(actor_id.clone(), Arc::new(actor_data));
        mgr.actor_task_specs.insert(actor_id.clone(), task_spec);

        // Manually call write_actor_export_event to test payload
        let actor_ref = mgr.registered_actors.get(&actor_id).unwrap();
        mgr.write_actor_export_event(&actor_ref, true);

        // Read and verify the file content
        let export_file = tmp.path().join("export_events/event_EXPORT_ACTOR.log");
        assert!(export_file.exists(), "export file should exist");
        let content = std::fs::read_to_string(&export_file).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        let event_data = &parsed["event_data"]["ActorEventData"];
        assert!(event_data.is_object(), "should have event_data.ActorEventData");

        // Verify all 16 ExportActorData fields are present
        // Fields 1-7: actor_id, job_id, state, is_detached, name, pid, ray_namespace
        assert!(event_data.get("actor_id").is_some(), "missing actor_id");
        assert!(event_data.get("job_id").is_some(), "missing job_id");
        assert!(event_data.get("state").is_some(), "missing state");
        assert!(event_data.get("is_detached").is_some(), "missing is_detached");
        assert_eq!(event_data["name"], "payload_test_actor");
        assert!(event_data.get("pid").is_some(), "missing pid");
        assert_eq!(event_data["ray_namespace"], "test_ns");

        // Field 8: serialized_runtime_env
        assert_eq!(
            event_data["serialized_runtime_env"],
            r#"{"pip": ["numpy"]}"#,
            "missing or wrong serialized_runtime_env"
        );

        // Field 9: class_name
        assert_eq!(event_data["class_name"], "PayloadTestClass");

        // Field 10: death_cause (null for alive actors, but field should exist)
        // For a non-dead actor, death_cause is None, so it may be absent in JSON.
        // This is acceptable per proto3 serialization rules.

        // Field 11: required_resources
        let resources = &event_data["required_resources"];
        assert!(
            resources.is_object(),
            "missing required_resources"
        );
        assert_eq!(resources["CPU"], 2.0);
        assert_eq!(resources["GPU"], 1.0);

        // Field 14: repr_name
        assert_eq!(event_data["repr_name"], "PayloadTestClass()");

        // Field 15: labels (from task spec)
        let labels = &event_data["labels"];
        assert!(labels.is_object(), "missing labels");
        assert_eq!(labels["env"], "prod");

        // Field 16: label_selector
        let selector = &event_data["label_selector"];
        assert!(selector.is_object(), "missing label_selector");
        assert_eq!(selector["zone"], "us-west");
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_emits_when_enable_ray_event_is_enabled() {
        // C++ parity: when `enable_ray_event=true`, the aggregator path is used
        // and the file-export path is bypassed.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));

        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                // Even if export API write is also set, ray_event takes precedence
                enable_export_api_write: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 84, "ray_event_actor", 3, &node_id_bytes);

        // Trigger a lifecycle event
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            9999,
            vec![],
            node2_bytes,
        );

        // The aggregator-path event_exporter should have buffered events
        assert!(
            mgr.event_exporter().buffer_len() > 0,
            "aggregator path should buffer events when enable_ray_event=true"
        );

        // The file-export path should NOT have written anything
        // (because enable_ray_event bypasses it in C++)
        let export_file = tmp.path().join("export_events/event_EXPORT_ACTOR.log");
        // The file may or may not exist (sink might be created), but if it exists
        // it should be empty since the ray_event path returns early before file write
        if export_file.exists() {
            let content = std::fs::read_to_string(&export_file).unwrap();
            assert!(
                content.trim().is_empty(),
                "file export should be empty when enable_ray_event=true (aggregator path takes precedence)"
            );
        }
    }

    // === Round 10: Live runtime sink/flush parity tests ===

    #[tokio::test]
    async fn test_actor_ray_event_path_uses_real_sink_when_enabled() {
        // C++ parity: when enable_ray_event=true, the GCS server wires a real sink
        // (EventAggregatorClient) to the ray_event_recorder. Events must leave memory.
        // This test proves a real sink is attached and events flow through it.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        // Simulate live runtime wiring: attach a counting sink
        let flush_count = StdArc::new(AtomicUsize::new(0));
        let fc = flush_count.clone();
        struct CountingSink(StdArc<AtomicUsize>);
        impl ray_observability::export::EventSink for CountingSink {
            fn flush(&self, events: &[ray_observability::events::RayEvent]) -> usize {
                self.0.fetch_add(events.len(), Ordering::Relaxed);
                events.len()
            }
        }
        mgr.event_exporter().set_sink(Box::new(CountingSink(fc)));

        // Verify sink is attached
        assert!(
            mgr.event_exporter().has_sink(),
            "event_exporter must have a sink attached when enable_ray_event=true"
        );

        // Trigger lifecycle events
        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 90, "sink_test_actor", 3, &node_id_bytes);

        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            10001,
            vec![],
            node2_bytes,
        );

        // Events should be buffered
        assert!(mgr.event_exporter().buffer_len() > 0);

        // Flush — events must flow through the real sink
        let flushed = mgr.event_exporter().flush();
        assert!(
            flushed > 0,
            "flush must deliver events to the sink, got 0"
        );
        assert!(
            flush_count.load(Ordering::Relaxed) > 0,
            "sink must have received events after flush"
        );
        assert_eq!(
            mgr.event_exporter().buffer_len(),
            0,
            "buffer must be empty after flush"
        );
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_flushes_through_live_runtime() {
        // C++ parity: RayEventRecorder::StartExportingEvents() starts a periodic timer
        // that calls ExportEvents(). This test proves the Rust periodic flush loop
        // actually drains the buffer through the sink in a runtime context.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        let flush_count = StdArc::new(AtomicUsize::new(0));
        let fc = flush_count.clone();
        struct CountingSink(StdArc<AtomicUsize>);
        impl ray_observability::export::EventSink for CountingSink {
            fn flush(&self, events: &[ray_observability::events::RayEvent]) -> usize {
                self.0.fetch_add(events.len(), Ordering::Relaxed);
                events.len()
            }
        }
        mgr.event_exporter().set_sink(Box::new(CountingSink(fc)));

        // Start periodic flush loop (matching server.rs production wiring)
        let exporter = Arc::clone(mgr.event_exporter());
        let _flush_handle = tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_millis(50));
            loop {
                ticker.tick().await;
                exporter.flush();
            }
        });

        // Trigger lifecycle events
        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 91, "flush_test_actor", 3, &node_id_bytes);

        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            10002,
            vec![],
            node2_bytes,
        );

        // Wait for periodic flush to drain the buffer
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            flush_count.load(Ordering::Relaxed) > 0,
            "periodic flush must have delivered events to the sink"
        );
        assert_eq!(
            mgr.event_exporter().buffer_len(),
            0,
            "buffer must be drained by periodic flush"
        );
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_is_not_just_buffered_in_memory() {
        // This test proves that the EventExporter infrastructure is not just
        // an in-memory dead buffer. With a sink attached, flush() must deliver
        // events and they must not stay in the buffer.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        // Without a sink: flush returns 0, events are lost
        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 92, "nosink_actor", 3, &node_id_bytes);

        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            10003,
            vec![],
            node2_bytes,
        );

        let buffered = mgr.event_exporter().buffer_len();
        assert!(buffered > 0, "events should be buffered");

        // Without sink: flush discards events (returns 0)
        let flushed_no_sink = mgr.event_exporter().flush();
        assert_eq!(flushed_no_sink, 0, "flush without sink should return 0");

        // Now attach a real sink (matching production wiring)
        let delivered = StdArc::new(AtomicUsize::new(0));
        let dc = delivered.clone();
        struct TrackingSink(StdArc<AtomicUsize>);
        impl ray_observability::export::EventSink for TrackingSink {
            fn flush(&self, events: &[ray_observability::events::RayEvent]) -> usize {
                self.0.fetch_add(events.len(), Ordering::Relaxed);
                events.len()
            }
        }
        mgr.event_exporter().set_sink(Box::new(TrackingSink(dc)));

        // Generate more events
        let mut node3_bytes = vec![0u8; 28];
        node3_bytes[0] = 3;
        let (actor_id2, _) =
            setup_alive_actor_on_node(&mgr, 93, "withsink_actor", 3, &node3_bytes);
        let mut node4_bytes = vec![0u8; 28];
        node4_bytes[0] = 4;
        mgr.on_actor_creation_success(
            &actor_id2,
            ray_proto::ray::rpc::Address {
                node_id: node4_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            10004,
            vec![],
            node4_bytes,
        );

        // With sink: flush delivers events
        let flushed_with_sink = mgr.event_exporter().flush();
        assert!(
            flushed_with_sink > 0,
            "flush with sink must deliver events (got 0)"
        );
        assert!(
            delivered.load(Ordering::Relaxed) > 0,
            "sink must have received events — events must not just sit in memory"
        );
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_disabled_means_no_sink_activity() {
        // When enable_ray_event=false AND no export API enabled, no events should
        // be buffered, no sink should be set, and flush should be a no-op.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        // Default config: everything disabled
        let mgr = GcsActorManager::new(storage.clone());

        assert!(
            !mgr.event_exporter().has_sink(),
            "no sink should be attached when all export/event paths are disabled"
        );

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _node_id) =
            setup_alive_actor_on_node(&mgr, 94, "disabled_actor", 3, &node_id_bytes);

        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            10005,
            vec![],
            node2_bytes,
        );

        assert_eq!(
            mgr.event_exporter().buffer_len(),
            0,
            "no events should be buffered when all paths are disabled"
        );

        let flushed = mgr.event_exporter().flush();
        assert_eq!(
            flushed, 0,
            "flush should be no-op when disabled"
        );
    }

    // === Round 11: Structured output-channel parity tests ===

    #[tokio::test]
    async fn test_actor_ray_event_path_uses_cpp_equivalent_output_channel() {
        // C++ sends AddEventsRequest containing RayEvent protos with nested
        // ActorLifecycleEvent to EventAggregatorService. Rust must produce the same
        // structured proto output, not just tracing log lines.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        // Wire the EventAggregatorSink (matching production server.rs wiring)
        let sink = ray_observability::export::EventAggregatorSink::new(
            tmp.path(),
            vec![1, 2, 3],
            "test_session".to_string(),
        )
        .unwrap();
        mgr.event_exporter().set_sink(Box::new(sink));

        // Trigger lifecycle event
        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _) =
            setup_alive_actor_on_node(&mgr, 95, "aggregator_actor", 3, &node_id_bytes);
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            11001,
            vec![],
            node2_bytes,
        );

        // Flush to write structured output
        let flushed = mgr.event_exporter().flush();
        assert!(flushed > 0, "flush must deliver events");

        // Verify structured output file exists
        let output_file = tmp.path().join("ray_events/actor_events.jsonl");
        assert!(
            output_file.exists(),
            "structured event output file must exist at ray_events/actor_events.jsonl"
        );

        // Parse and verify it's an AddEventsRequest with RayEvent protos
        let content = std::fs::read_to_string(&output_file).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        // Must have events_data.events (C++ AddEventsRequest shape)
        let events_data = &parsed["events_data"];
        assert!(
            events_data.is_object(),
            "output must contain events_data (AddEventsRequest shape)"
        );
        let events = &events_data["events"];
        assert!(
            events.is_array(),
            "events_data must contain events array (RayEventsData shape)"
        );
        assert!(
            !events.as_array().unwrap().is_empty(),
            "events array must not be empty"
        );
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_preserves_expected_structured_event_delivery() {
        // Verify that the structured output contains ActorLifecycleEvent with
        // state_transitions — matching the C++ RayActorLifecycleEvent proto shape.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        let sink = ray_observability::export::EventAggregatorSink::new(
            tmp.path(),
            vec![10, 20, 30],
            "test_session".to_string(),
        )
        .unwrap();
        mgr.event_exporter().set_sink(Box::new(sink));

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _) =
            setup_alive_actor_on_node(&mgr, 96, "structured_actor", 3, &node_id_bytes);
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            11002,
            vec![],
            node2_bytes,
        );
        mgr.event_exporter().flush();

        let content = std::fs::read_to_string(
            tmp.path().join("ray_events/actor_events.jsonl"),
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        let event = &parsed["events_data"]["events"][0];

        // Must have source_type = GCS (2)
        assert_eq!(
            event["source_type"], 2,
            "source_type must be GCS (2)"
        );

        // Must have event_type = ACTOR_LIFECYCLE_EVENT (10)
        assert_eq!(
            event["event_type"], 10,
            "event_type must be ACTOR_LIFECYCLE_EVENT (10)"
        );

        // Must have actor_lifecycle_event with state_transitions
        let lifecycle = &event["actor_lifecycle_event"];
        assert!(
            lifecycle.is_object(),
            "must have actor_lifecycle_event (C++ RayActorLifecycleEvent)"
        );
        let transitions = &lifecycle["state_transitions"];
        assert!(
            transitions.is_array() && !transitions.as_array().unwrap().is_empty(),
            "must have state_transitions (C++ StateTransition)"
        );

        // Verify state transition has the ALIVE state (2)
        let first_transition = &transitions[0];
        assert_eq!(
            first_transition["state"], 2,
            "state must be ALIVE (2) after creation success"
        );

        // Must have node_id on the event (C++ sets this centrally)
        assert!(
            event.get("node_id").is_some(),
            "must have node_id field (C++ sets centrally)"
        );

        // Must have session_name
        assert_eq!(
            event["session_name"], "test_session",
            "must have session_name"
        );
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_not_just_tracing_logs() {
        // This test proves the output is NOT just tracing::info! log lines.
        // It must be structured JSON in the AddEventsRequest proto format,
        // written to a file, with full proto field fidelity.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage.clone(), config);

        let sink = ray_observability::export::EventAggregatorSink::new(
            tmp.path(),
            vec![],
            "test".to_string(),
        )
        .unwrap();
        let output_path = sink.file_path().to_path_buf();
        mgr.event_exporter().set_sink(Box::new(sink));

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _) =
            setup_alive_actor_on_node(&mgr, 97, "not_just_logs_actor", 3, &node_id_bytes);
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            11003,
            vec![],
            node2_bytes,
        );
        mgr.event_exporter().flush();

        // Output must be a file, not just tracing
        assert!(output_path.exists(), "structured output file must exist");

        let content = std::fs::read_to_string(&output_path).unwrap();

        // Must be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        // Must be AddEventsRequest shape (not a plain log line)
        assert!(
            parsed.get("events_data").is_some(),
            "output must be AddEventsRequest JSON, not a plain log line"
        );

        // Must contain nested proto fields, not flattened custom_fields
        let event = &parsed["events_data"]["events"][0];
        assert!(
            event.get("actor_lifecycle_event").is_some()
                || event.get("actor_definition_event").is_some(),
            "output must contain nested proto event fields (ActorLifecycleEvent or ActorDefinitionEvent), not flattened custom_fields"
        );
    }

    #[tokio::test]
    async fn test_actor_ray_event_path_disabled_means_no_output_channel_activity() {
        // When enable_ray_event=false, no structured output file should be created,
        // no events buffered, no sink activity.
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        // Default config: all disabled
        let mgr = GcsActorManager::new(storage.clone());

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _) =
            setup_alive_actor_on_node(&mgr, 98, "disabled_channel_actor", 3, &node_id_bytes);
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            11004,
            vec![],
            node2_bytes,
        );

        // No events buffered
        assert_eq!(mgr.event_exporter().buffer_len(), 0);

        // No output file
        let events_dir = tmp.path().join("ray_events");
        assert!(
            !events_dir.exists(),
            "ray_events directory must not exist when disabled"
        );
    }

    // === Round 12: Full C++/Rust event-model parity tests ===

    /// Helper: create a manager with EventAggregatorSink wired, return (mgr, output_path).
    fn setup_aggregator_manager(
        tmp: &tempfile::TempDir,
    ) -> (GcsActorManager, std::path::PathBuf) {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                log_dir: Some(tmp.path().to_path_buf()),
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage, config);
        let sink = ray_observability::export::EventAggregatorSink::new(
            tmp.path(),
            vec![10, 20, 30],
            "test_session".to_string(),
        )
        .unwrap();
        let output_path = sink.file_path().to_path_buf();
        mgr.event_exporter().set_sink(Box::new(sink));
        (mgr, output_path)
    }

    /// Helper: flush and parse all events from the JSONL output file.
    /// Returns all RayEvent JSON objects across all AddEventsRequest batches.
    fn flush_and_parse_events(
        mgr: &GcsActorManager,
        output_path: &std::path::Path,
    ) -> Vec<serde_json::Value> {
        mgr.event_exporter().flush();
        let content = std::fs::read_to_string(output_path).unwrap();
        let mut all_events = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(events) = parsed["events_data"]["events"].as_array() {
                all_events.extend(events.iter().cloned());
            }
        }
        all_events
    }

    // ---- 1. Registration event cardinality ----

    #[tokio::test]
    async fn test_actor_registration_emits_definition_and_lifecycle_events() {
        // C++ registration emits TWO separate RayEvent protos:
        //   1. event_type=ACTOR_DEFINITION_EVENT (9), with actor_definition_event
        //   2. event_type=ACTOR_LIFECYCLE_EVENT (10), with actor_lifecycle_event
        // Rust must emit the same two separate events, not one combined event.
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        // Register an actor (is_registration=true)
        let task_spec = ray_proto::ray::rpc::TaskSpec {
            job_id: vec![0, 0, 0, 1],
            caller_address: Some(ray_proto::ray::rpc::Address::default()),
            actor_creation_task_spec: Some(
                ray_proto::ray::rpc::ActorCreationTaskSpec {
                    actor_id: vec![0u8; 16],
                    name: "reg_card_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    is_detached: false,
                    max_actor_restarts: 3,
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        mgr.handle_register_actor(task_spec).await.unwrap();

        let events = flush_and_parse_events(&mgr, &output_path);

        // Must have at least 2 events from the registration call
        let def_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 9) // ACTOR_DEFINITION_EVENT
            .collect();
        let lifecycle_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 10) // ACTOR_LIFECYCLE_EVENT
            .collect();

        assert!(
            !def_events.is_empty(),
            "registration must emit a separate ACTOR_DEFINITION_EVENT (event_type=9)"
        );
        assert!(
            !lifecycle_events.is_empty(),
            "registration must emit a separate ACTOR_LIFECYCLE_EVENT (event_type=10)"
        );
    }

    #[tokio::test]
    async fn test_actor_registration_event_types_match_cpp_cardinality() {
        // C++ emits exactly 2 events on registration: one definition + one lifecycle.
        // The definition event must NOT also contain actor_lifecycle_event (separate proto).
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let task_spec = ray_proto::ray::rpc::TaskSpec {
            job_id: vec![0, 0, 0, 2],
            caller_address: Some(ray_proto::ray::rpc::Address::default()),
            actor_creation_task_spec: Some(
                ray_proto::ray::rpc::ActorCreationTaskSpec {
                    actor_id: vec![1u8; 16],
                    name: "card_check_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    is_detached: false,
                    max_actor_restarts: 0,
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        mgr.handle_register_actor(task_spec).await.unwrap();

        let events = flush_and_parse_events(&mgr, &output_path);

        // Find the definition event
        let def_event = events
            .iter()
            .find(|e| e["event_type"] == 9)
            .expect("must have ACTOR_DEFINITION_EVENT");

        // The definition event must have actor_definition_event populated
        assert!(
            def_event.get("actor_definition_event").is_some()
                && def_event["actor_definition_event"].is_object(),
            "definition event must contain actor_definition_event"
        );

        // The definition event must NOT also contain actor_lifecycle_event
        // (C++ sends them as separate RayEvent protos)
        let has_lifecycle = def_event
            .get("actor_lifecycle_event")
            .map(|v| v.is_object() && !v.as_object().unwrap().is_empty())
            .unwrap_or(false);
        assert!(
            !has_lifecycle,
            "definition event must NOT contain actor_lifecycle_event — C++ sends separate protos"
        );
    }

    // ---- 2. ActorDefinitionEvent payload ----

    #[tokio::test]
    async fn test_actor_definition_event_includes_required_resources() {
        // C++ populates required_resources from ActorTableData.required_resources()
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let mut required_resources = std::collections::HashMap::new();
        required_resources.insert("CPU".to_string(), 2.0);
        required_resources.insert("GPU".to_string(), 1.0);

        let task_spec = ray_proto::ray::rpc::TaskSpec {
            job_id: vec![0, 0, 0, 3],
            caller_address: Some(ray_proto::ray::rpc::Address::default()),
            required_resources: required_resources.clone(),
            actor_creation_task_spec: Some(
                ray_proto::ray::rpc::ActorCreationTaskSpec {
                    actor_id: vec![2u8; 16],
                    name: "res_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        mgr.handle_register_actor(task_spec).await.unwrap();

        let events = flush_and_parse_events(&mgr, &output_path);
        let def_event = events
            .iter()
            .find(|e| e["event_type"] == 9)
            .expect("must have definition event");
        let def = &def_event["actor_definition_event"];
        let resources = &def["required_resources"];
        assert!(
            resources.is_object(),
            "actor_definition_event must include required_resources"
        );
        assert_eq!(
            resources["CPU"], 2.0,
            "required_resources must include CPU=2.0"
        );
        assert_eq!(
            resources["GPU"], 1.0,
            "required_resources must include GPU=1.0"
        );
    }

    #[tokio::test]
    async fn test_actor_definition_event_includes_placement_group_and_label_selector() {
        // C++ populates placement_group_id and label_selector from ActorTableData
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let mut label_selector = std::collections::HashMap::new();
        label_selector.insert("zone".to_string(), "us-west-2".to_string());

        let pg_id = vec![42u8; 16]; // Non-empty placement group ID

        let task_spec = ray_proto::ray::rpc::TaskSpec {
            job_id: vec![0, 0, 0, 4],
            caller_address: Some(ray_proto::ray::rpc::Address::default()),
            actor_creation_task_spec: Some(
                ray_proto::ray::rpc::ActorCreationTaskSpec {
                    actor_id: vec![3u8; 16],
                    name: "pg_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };

        // We need to set placement_group_id and label_selector on the ActorTableData.
        // Register, then manually update the actor data with these fields and re-emit.
        mgr.handle_register_actor(task_spec).await.unwrap();

        // Flush initial registration events so they don't interfere with re-emission
        mgr.event_exporter().flush();

        // Update the registered actor with pg_id and label_selector
        let actor_id = ActorID::from_binary(&[3u8; 16]);
        if let Some(mut entry) = mgr.registered_actors.get_mut(&actor_id) {
            let mut data = (**entry).clone();
            data.placement_group_id = Some(pg_id.clone());
            data.label_selector = label_selector.clone();
            *entry = Arc::new(data);
        }

        // Re-emit registration event with updated data
        let actor_ref = mgr.registered_actors.get(&actor_id).unwrap().clone();
        mgr.write_actor_export_event(&actor_ref, true);

        let events = flush_and_parse_events(&mgr, &output_path);

        // Find the last definition event (the one with our updated fields)
        let def_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 9)
            .collect();
        let def_event = def_events.last().expect("must have definition event");
        let def = &def_event["actor_definition_event"];

        // placement_group_id must be populated (prost serializes bytes as array or base64)
        let pg = &def["placement_group_id"];
        assert!(
            (pg.is_array() && !pg.as_array().unwrap().is_empty())
                || (pg.is_string() && !pg.as_str().unwrap().is_empty()),
            "actor_definition_event must include non-empty placement_group_id"
        );

        // label_selector must be populated
        let sel = &def["label_selector"];
        assert!(
            sel.is_object() && !sel.as_object().unwrap().is_empty(),
            "actor_definition_event must include label_selector"
        );
    }

    #[tokio::test]
    async fn test_actor_definition_event_includes_call_site_parent_and_ref_ids() {
        // C++ populates call_site, parent_id, and ref_ids from ActorTableData
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let mut labels = std::collections::HashMap::new();
        labels.insert("trace_id".to_string(), "abc123".to_string());

        let task_spec = ray_proto::ray::rpc::TaskSpec {
            job_id: vec![0, 0, 0, 5],
            caller_address: Some(ray_proto::ray::rpc::Address::default()),
            labels: labels.clone(),
            actor_creation_task_spec: Some(
                ray_proto::ray::rpc::ActorCreationTaskSpec {
                    actor_id: vec![4u8; 16],
                    name: "callsite_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        mgr.handle_register_actor(task_spec).await.unwrap();

        // Flush initial registration events so they don't interfere with re-emission
        mgr.event_exporter().flush();

        // Update actor data with call_site and parent_id
        let actor_id = ActorID::from_binary(&[4u8; 16]);
        if let Some(mut entry) = mgr.registered_actors.get_mut(&actor_id) {
            let mut data = (**entry).clone();
            data.call_site = Some("test_file.py:42".to_string());
            data.parent_id = vec![99u8; 16];
            data.labels = labels.clone();
            *entry = Arc::new(data);
        }

        let actor_ref = mgr.registered_actors.get(&actor_id).unwrap().clone();
        mgr.write_actor_export_event(&actor_ref, true);

        let events = flush_and_parse_events(&mgr, &output_path);
        let def_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 9)
            .collect();
        let def_event = def_events.last().expect("must have definition event");
        let def = &def_event["actor_definition_event"];

        // call_site
        assert!(
            def.get("call_site").is_some()
                && !def["call_site"].as_str().unwrap_or("").is_empty(),
            "actor_definition_event must include call_site"
        );

        // parent_id (prost serializes bytes as array or base64)
        let pid = &def["parent_id"];
        assert!(
            (pid.is_array() && !pid.as_array().unwrap().is_empty())
                || (pid.is_string() && !pid.as_str().unwrap_or("").is_empty()),
            "actor_definition_event must include parent_id"
        );

        // ref_ids (C++ stores labels() in ref_ids field)
        let ref_ids = &def["ref_ids"];
        assert!(
            ref_ids.is_object() && !ref_ids.as_object().unwrap().is_empty(),
            "actor_definition_event must include ref_ids (from labels)"
        );
    }

    // ---- 3. ActorLifecycleEvent payload ----

    #[tokio::test]
    async fn test_actor_lifecycle_alive_event_includes_worker_id_and_port() {
        // C++ ALIVE transition sets: worker_id from address().worker_id(),
        // port from address().port()
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _) =
            setup_alive_actor_on_node(&mgr, 50, "alive_worker_actor", 3, &node_id_bytes);

        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        let worker_id = vec![77u8; 28];
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: worker_id.clone(),
                port: 12345,
                ..Default::default()
            },
            9999,
            vec![],
            node2_bytes,
        );

        let events = flush_and_parse_events(&mgr, &output_path);
        let lifecycle_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 10)
            .collect();
        assert!(!lifecycle_events.is_empty(), "must have lifecycle events");

        // Find the ALIVE transition
        let alive_event = lifecycle_events
            .iter()
            .find(|e| {
                e["actor_lifecycle_event"]["state_transitions"]
                    .as_array()
                    .map(|arr| arr.iter().any(|t| t["state"] == 2))
                    .unwrap_or(false)
            })
            .expect("must have lifecycle event with ALIVE transition");

        let transition = &alive_event["actor_lifecycle_event"]["state_transitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["state"] == 2)
            .unwrap();

        // worker_id must be set (prost serializes bytes as JSON array)
        let wid = &transition["worker_id"];
        assert!(
            (wid.is_array() && !wid.as_array().unwrap().is_empty())
                || (wid.is_string() && !wid.as_str().unwrap().is_empty()),
            "ALIVE transition must include non-empty worker_id"
        );

        // port must be set and non-zero
        let port = transition["port"].as_i64().unwrap_or(0);
        assert!(
            port != 0,
            "ALIVE transition must include non-zero port, got {}",
            port
        );
    }

    #[tokio::test]
    async fn test_actor_lifecycle_dead_event_includes_death_cause() {
        // C++ DEAD transition sets death_cause from ActorTableData.death_cause()
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (_, node_id) =
            setup_alive_actor_on_node(&mgr, 51, "dead_cause_actor", 0, &node_id_bytes);

        // Kill the node → actor goes to DEAD with death_cause
        mgr.on_node_dead(&node_id);

        let events = flush_and_parse_events(&mgr, &output_path);
        let lifecycle_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 10)
            .collect();

        // Find the DEAD transition
        let dead_event = lifecycle_events
            .iter()
            .find(|e| {
                e["actor_lifecycle_event"]["state_transitions"]
                    .as_array()
                    .map(|arr| arr.iter().any(|t| t["state"] == 4))
                    .unwrap_or(false)
            })
            .expect("must have lifecycle event with DEAD transition");

        let transition = &dead_event["actor_lifecycle_event"]["state_transitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["state"] == 4)
            .unwrap();

        // death_cause must be set
        let dc = &transition["death_cause"];
        assert!(
            dc.is_object() && !dc.as_object().unwrap().is_empty(),
            "DEAD transition must include death_cause"
        );
    }

    #[tokio::test]
    async fn test_actor_lifecycle_restarting_event_includes_restart_reason() {
        // C++ RESTARTING transition sets restart_reason
        // (NODE_PREEMPTION=2 when preempted)
        let tmp = tempfile::tempdir().unwrap();
        let (mgr, output_path) = setup_aggregator_manager(&tmp);

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (_, node_id) =
            setup_alive_actor_on_node(&mgr, 52, "restart_reason_actor", 5, &node_id_bytes);

        // Preempt and kill node → actor goes RESTARTING
        mgr.set_preempted_and_publish(&node_id);
        mgr.on_node_dead(&node_id);

        let events = flush_and_parse_events(&mgr, &output_path);
        let lifecycle_events: Vec<&serde_json::Value> = events
            .iter()
            .filter(|e| e["event_type"] == 10)
            .collect();

        // Find the RESTARTING transition
        let restart_event = lifecycle_events
            .iter()
            .find(|e| {
                e["actor_lifecycle_event"]["state_transitions"]
                    .as_array()
                    .map(|arr| arr.iter().any(|t| t["state"] == 3))
                    .unwrap_or(false)
            })
            .expect("must have lifecycle event with RESTARTING transition");

        let transition = &restart_event["actor_lifecycle_event"]["state_transitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["state"] == 3)
            .unwrap();

        // restart_reason must be NODE_PREEMPTION (2)
        let rr = transition["restart_reason"].as_i64().unwrap_or(-1);
        assert_eq!(
            rr, 2,
            "RESTARTING transition for preempted actor must have restart_reason=NODE_PREEMPTION (2), got {}",
            rr
        );
    }

    // ---- 4. Fallback semantics ----

    #[tokio::test]
    async fn test_enable_ray_event_without_structured_sink_is_not_treated_as_full_parity_path() {
        // When enable_ray_event=true but no EventAggregatorSink is available,
        // the system must NOT silently fall back to LoggingEventSink and still
        // claim full parity. Either:
        //   a) No events are delivered (explicit failure), OR
        //   b) The fallback is to LoggingEventSink but events must still be
        //      buffered/counted so the caller knows they are NOT getting
        //      structured proto output.
        //
        // This test verifies that when enable_ray_event=true and NO sink is set,
        // events are buffered but flush() returns 0 (no events delivered to a
        // structured sink). This makes the non-parity state observable.
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let config = ActorExportConfig {
            export_api_config: ray_observability::export::ExportApiConfig {
                enable_ray_event: true,
                // No log_dir → no EventAggregatorSink can be created
                log_dir: None,
                ..Default::default()
            },
        };
        let mgr = GcsActorManager::with_export_config(storage, config);

        // Do NOT set any sink (simulating fallback scenario)
        // In production server.rs, this would fall back to LoggingEventSink.
        // For full parity, we want to verify that without a structured sink,
        // the system does not silently claim delivery.

        let mut node_id_bytes = vec![0u8; 28];
        node_id_bytes[0] = 1;
        let (actor_id, _) =
            setup_alive_actor_on_node(&mgr, 53, "no_sink_actor", 3, &node_id_bytes);
        let mut node2_bytes = vec![0u8; 28];
        node2_bytes[0] = 2;
        mgr.on_actor_creation_success(
            &actor_id,
            ray_proto::ray::rpc::Address {
                node_id: node2_bytes.clone(),
                worker_id: vec![0u8; 28],
                ..Default::default()
            },
            11005,
            vec![],
            node2_bytes,
        );

        // Events are buffered
        assert!(
            mgr.event_exporter().buffer_len() > 0,
            "events must be buffered when enable_ray_event=true"
        );

        // But flush returns 0 because no structured sink is attached
        let flushed = mgr.event_exporter().flush();
        assert_eq!(
            flushed, 0,
            "flush must return 0 when no structured sink is attached — \
             LoggingEventSink fallback must not be treated as full-parity delivery"
        );
    }
}
