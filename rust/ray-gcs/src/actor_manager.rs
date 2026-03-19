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

/// The GCS actor manager tracks all actors in the cluster.
///
/// Uses DashMap for per-key locking instead of global RwLock<HashMap>,
/// and Arc-wraps ActorTableData to avoid deep clones on reads.
pub struct GcsActorManager {
    /// All registered actors (Arc-wrapped for cheap clones on read paths).
    registered_actors: DashMap<ActorID, Arc<ray_proto::ray::rpc::ActorTableData>>,
    /// Named actors: (namespace, name) → ActorID.
    named_actors: DashMap<(String, String), ActorID>,
    /// Dead actors cache (for queries).
    dead_actors: DashMap<ActorID, Arc<ray_proto::ray::rpc::ActorTableData>>,
    /// Actors by node: node_id → set of actor_ids.
    actors_by_node: DashMap<NodeID, Vec<ActorID>>,
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
}

impl GcsActorManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
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
        }
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

        // Build the actor table data — copy critical fields from task_spec
        // so that GetNamedActorInfo returns enough data for make_actor_handle.
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
                self.registered_actors.insert(*actor_id, Arc::new(actor));
                Some(msg)
            } else {
                None
            }
        };
        // Publish ALIVE state outside critical section
        self.publish_deferred_messages(pub_msg.into_iter().collect());

        // Keep task spec alive — it's needed for GetNamedActorInfo responses.
        // Only cleaned up when the actor transitions to DEAD.

        // Resolve all pending create callbacks
        let callbacks = self.create_callbacks.remove(actor_id);
        if let Some((_, senders)) = callbacks {
            let reply = ray_proto::ray::rpc::CreateActorReply {
                actor_address: Some(actor_address),
                ..Default::default()
            };
            for tx in senders {
                let _ = tx.send(Ok(reply.clone()));
            }
        }

        tracing::info!(?actor_id, "Actor is now ALIVE");
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

                // Check if actor can be restarted
                let max_restarts = actor.max_restarts;
                let num_restarts = actor.num_restarts;
                let can_restart = max_restarts == -1 || num_restarts < max_restarts;

                if can_restart {
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
}
