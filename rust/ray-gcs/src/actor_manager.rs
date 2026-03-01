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
use std::sync::Arc;

use parking_lot::RwLock;
use ray_common::id::{ActorID, JobID, NodeID, WorkerID};
use tokio::sync::oneshot;
use tonic::Status;

use crate::actor_scheduler::GcsActorScheduler;
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

/// The GCS actor manager tracks all actors in the cluster.
pub struct GcsActorManager {
    /// All registered actors.
    registered_actors: RwLock<HashMap<ActorID, ray_proto::ray::rpc::ActorTableData>>,
    /// Named actors: (namespace, name) → ActorID.
    named_actors: RwLock<HashMap<(String, String), ActorID>>,
    /// Dead actors cache (for queries).
    dead_actors: RwLock<HashMap<ActorID, ray_proto::ray::rpc::ActorTableData>>,
    /// Actors by node: node_id → set of actor_ids.
    actors_by_node: RwLock<HashMap<NodeID, Vec<ActorID>>>,
    /// Actors by owner: (node_id, worker_id) → set of actor_ids.
    #[allow(dead_code)]
    actors_by_owner: RwLock<HashMap<(NodeID, WorkerID), Vec<ActorID>>>,
    /// State counts for metrics.
    state_counts: RwLock<HashMap<ActorState, usize>>,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
    /// Actor scheduler for placing actors on nodes.
    actor_scheduler: RwLock<Option<Arc<GcsActorScheduler>>>,
    /// Pubsub handler for publishing actor state changes.
    pubsub_handler: RwLock<Option<Arc<InternalPubSubHandler>>>,
    /// Pending CreateActor reply channels: actor_id → Vec<oneshot::Sender>.
    #[allow(clippy::type_complexity)]
    create_callbacks:
        RwLock<HashMap<ActorID, Vec<oneshot::Sender<Result<ray_proto::ray::rpc::CreateActorReply, Status>>>>>,
    /// Task specs stored for actors being created (needed for scheduling).
    actor_task_specs: RwLock<HashMap<ActorID, ray_proto::ray::rpc::TaskSpec>>,
}

impl GcsActorManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            registered_actors: RwLock::new(HashMap::new()),
            named_actors: RwLock::new(HashMap::new()),
            dead_actors: RwLock::new(HashMap::new()),
            actors_by_node: RwLock::new(HashMap::new()),
            actors_by_owner: RwLock::new(HashMap::new()),
            state_counts: RwLock::new(HashMap::new()),
            table_storage,
            actor_scheduler: RwLock::new(None),
            pubsub_handler: RwLock::new(None),
            create_callbacks: RwLock::new(HashMap::new()),
            actor_task_specs: RwLock::new(HashMap::new()),
        }
    }

    /// Set the actor scheduler (called during server initialization).
    pub fn set_actor_scheduler(&self, scheduler: Arc<GcsActorScheduler>) {
        *self.actor_scheduler.write() = Some(scheduler);
    }

    /// Set the pubsub handler (called during server initialization).
    pub fn set_pubsub_handler(&self, handler: Arc<InternalPubSubHandler>) {
        *self.pubsub_handler.write() = Some(handler);
    }

    /// Initialize from persisted data.
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let all_actors = self
            .table_storage
            .actor_table()
            .get_all()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let mut registered = self.registered_actors.write();
        let mut named = self.named_actors.write();
        let mut dead = self.dead_actors.write();
        let mut counts = self.state_counts.write();

        for (key, actor) in all_actors {
            let actor_id = ActorID::from_hex(&key);
            let state = ActorState::from(actor.state);

            *counts.entry(state).or_insert(0) += 1;

            if state == ActorState::Dead {
                dead.insert(actor_id, actor);
            } else {
                // Register named actor (ray_namespace and name are direct fields on ActorTableData)
                if !actor.ray_namespace.is_empty() && !actor.name.is_empty() {
                    named.insert((actor.ray_namespace.clone(), actor.name.clone()), actor_id);
                }
                registered.insert(actor_id, actor);
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

        // Build the actor table data
        let actor_data = ray_proto::ray::rpc::ActorTableData {
            actor_id: actor_id_bytes.clone(),
            state: ActorState::DependenciesUnready as i32,
            name: name.clone(),
            ray_namespace: namespace.clone(),
            ..Default::default()
        };

        // Check for name conflict
        if !name.is_empty() {
            let key = (namespace.clone(), name.clone());
            let named = self.named_actors.read();
            if named.contains_key(&key) {
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
            self.named_actors
                .write()
                .insert((namespace, name), actor_id);
        }

        self.registered_actors.write().insert(actor_id, actor_data);
        *self
            .state_counts
            .write()
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
            creation_spec.actor_id.as_slice().try_into().unwrap_or(&[0u8; 16]),
        );

        // Transition to PENDING_CREATION
        {
            let mut registered = self.registered_actors.write();
            let mut counts = self.state_counts.write();

            if let Some(actor) = registered.get_mut(&actor_id) {
                let old_state = ActorState::from(actor.state);
                actor.state = ActorState::PendingCreation as i32;

                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                *counts.entry(ActorState::PendingCreation).or_insert(0) += 1;

                // Publish PENDING_CREATION state
                self.publish_actor_state(actor);
            } else {
                return Err(Status::not_found(format!(
                    "actor {:?} not registered",
                    actor_id
                )));
            }
        }

        // Store the task spec for scheduling
        self.actor_task_specs
            .write()
            .insert(actor_id, task_spec.clone());

        // Create oneshot channel for deferred reply
        let (tx, rx) = oneshot::channel();
        self.create_callbacks
            .write()
            .entry(actor_id)
            .or_default()
            .push(tx);

        // Spawn scheduling task
        let scheduler = self
            .actor_scheduler
            .read()
            .clone()
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

        tracing::info!(?actor_id, "Actor creation requested, scheduling in progress");
        Ok(rx)
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
        {
            let mut registered = self.registered_actors.write();
            let mut counts = self.state_counts.write();

            if let Some(actor) = registered.get_mut(actor_id) {
                let old_state = ActorState::from(actor.state);
                actor.state = ActorState::Alive as i32;
                actor.address = Some(worker_address);
                actor.pid = worker_pid;
                actor.resource_mapping = resource_mapping;
                actor.node_id = Some(node_id.clone());

                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                *counts.entry(ActorState::Alive).or_insert(0) += 1;

                // Track actor by node
                let node_id_obj = NodeID::from_binary(
                    node_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                );
                self.actors_by_node
                    .write()
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
                        .write()
                        .entry((owner_node, owner_worker))
                        .or_default()
                        .push(*actor_id);
                }

                // Publish ALIVE state
                self.publish_actor_state(actor);
            }
        }

        // Clean up task spec
        self.actor_task_specs.write().remove(actor_id);

        // Resolve all pending create callbacks
        let callbacks = self.create_callbacks.write().remove(actor_id);
        if let Some(senders) = callbacks {
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
        {
            let mut registered = self.registered_actors.write();
            let mut dead = self.dead_actors.write();
            let mut counts = self.state_counts.write();

            if let Some(mut actor) = registered.remove(actor_id) {
                let old_state = ActorState::from(actor.state);
                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *counts.entry(ActorState::Dead).or_insert(0) += 1;

                // Remove from named actors
                if !actor.name.is_empty() {
                    self.named_actors
                        .write()
                        .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
                }

                // Publish DEAD state
                self.publish_actor_state(&actor);

                dead.insert(*actor_id, actor);
            }
        }

        // Clean up task spec
        self.actor_task_specs.write().remove(actor_id);

        // Resolve all pending create callbacks with error
        let callbacks = self.create_callbacks.write().remove(actor_id);
        if let Some(senders) = callbacks {
            for tx in senders {
                let _ = tx.send(Err(Status::internal(format!(
                    "actor scheduling failed: {}",
                    reason
                ))));
            }
        }

        tracing::warn!(?actor_id, %reason, "Actor scheduling failed, actor is DEAD");
    }

    /// Publish actor state change via pubsub.
    fn publish_actor_state(&self, actor: &ray_proto::ray::rpc::ActorTableData) {
        let pubsub = self.pubsub_handler.read();
        if let Some(ref handler) = *pubsub {
            let pub_msg = ray_proto::ray::rpc::PubMessage {
                channel_type: ChannelType::GcsActorChannel as i32,
                key_id: actor.actor_id.clone(),
                inner_message: Some(
                    ray_proto::ray::rpc::pub_message::InnerMessage::ActorMessage(
                        actor.clone(),
                    ),
                ),
                ..Default::default()
            };
            handler.publish_pubmessage(pub_msg);
        }
    }

    /// Handle GetActorInfo RPC.
    pub fn handle_get_actor_info(
        &self,
        actor_id_bytes: &[u8],
    ) -> Option<ray_proto::ray::rpc::ActorTableData> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));
        let registered = self.registered_actors.read();
        if let Some(actor) = registered.get(&actor_id) {
            return Some(actor.clone());
        }
        let dead = self.dead_actors.read();
        dead.get(&actor_id).cloned()
    }

    /// Handle GetNamedActorInfo RPC.
    pub fn handle_get_named_actor_info(
        &self,
        name: &str,
        namespace: &str,
    ) -> Option<ray_proto::ray::rpc::ActorTableData> {
        let named = self.named_actors.read();
        let actor_id = named.get(&(namespace.to_string(), name.to_string()))?;
        let registered = self.registered_actors.read();
        registered.get(actor_id).cloned()
    }

    /// Handle ListNamedActors RPC.
    pub fn handle_list_named_actors(
        &self,
        namespace: &str,
        all_namespaces: bool,
    ) -> Vec<(String, String)> {
        let named = self.named_actors.read();
        named
            .keys()
            .filter(|(ns, _)| all_namespaces || ns == namespace)
            .map(|(ns, name)| (ns.clone(), name.clone()))
            .collect()
    }

    /// Handle GetAllActorInfo RPC.
    pub fn handle_get_all_actor_info(
        &self,
        limit: Option<usize>,
        _job_id_filter: Option<&JobID>,
        actor_state_filter: Option<ActorState>,
    ) -> Vec<ray_proto::ray::rpc::ActorTableData> {
        let registered = self.registered_actors.read();
        let dead = self.dead_actors.read();

        let all = registered.values().chain(dead.values()).filter(|a| {
            if let Some(state) = actor_state_filter {
                if ActorState::from(a.state) != state {
                    return false;
                }
            }
            true
        });

        if let Some(limit) = limit {
            all.take(limit).cloned().collect()
        } else {
            all.cloned().collect()
        }
    }

    /// Handle KillActorViaGcs RPC.
    pub async fn handle_kill_actor(&self, actor_id_bytes: &[u8]) -> Result<(), tonic::Status> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));

        let actor = {
            let mut registered = self.registered_actors.write();
            let mut counts = self.state_counts.write();

            if let Some(mut actor) = registered.remove(&actor_id) {
                let old_state = ActorState::from(actor.state);
                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *counts.entry(ActorState::Dead).or_insert(0) += 1;
                Some(actor)
            } else {
                None
            }
        };

        if let Some(actor) = actor {
            // Remove from named actors
            if !actor.name.is_empty() {
                self.named_actors
                    .write()
                    .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
            }
            // Publish DEAD state
            self.publish_actor_state(&actor);
            self.dead_actors.write().insert(actor_id, actor);
            // Clean up task spec
            self.actor_task_specs.write().remove(&actor_id);
            // Resolve any pending create callbacks with error
            let callbacks = self.create_callbacks.write().remove(&actor_id);
            if let Some(senders) = callbacks {
                for tx in senders {
                    let _ = tx.send(Err(Status::cancelled("actor was killed")));
                }
            }
            tracing::info!(?actor_id, "Actor killed");
        }
        Ok(())
    }

    /// Handle node death — kill or restart actors on the dead node.
    ///
    /// Actors with remaining restarts are transitioned to RESTARTING and will
    /// need to be rescheduled. Actors at max restarts are transitioned to DEAD.
    pub fn on_node_dead(&self, node_id: &NodeID) {
        let actor_ids = {
            let by_node = self.actors_by_node.read();
            by_node.get(node_id).cloned().unwrap_or_default()
        };

        let mut registered = self.registered_actors.write();
        let mut dead = self.dead_actors.write();
        let mut counts = self.state_counts.write();
        let mut callbacks = self.create_callbacks.write();

        for actor_id in actor_ids {
            if let Some(actor) = registered.get_mut(&actor_id) {
                let old_state = ActorState::from(actor.state);
                if let Some(c) = counts.get_mut(&old_state) {
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
                    // Clear node assignment so it can be rescheduled
                    actor.address = None;
                    actor.node_id = None;
                    *counts.entry(ActorState::Restarting).or_insert(0) += 1;
                    self.publish_actor_state(actor);
                    tracing::info!(?actor_id, restart = num_restarts + 1, "Actor restarting");
                } else {
                    // Transition to DEAD
                    let mut actor = registered.remove(&actor_id).unwrap();
                    actor.state = ActorState::Dead as i32;
                    *counts.entry(ActorState::Dead).or_insert(0) += 1;

                    // Remove from named actors
                    if !actor.name.is_empty() {
                        self.named_actors
                            .write()
                            .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
                    }

                    self.publish_actor_state(&actor);

                    // Resolve any pending create callbacks with error
                    if let Some(senders) = callbacks.remove(&actor_id) {
                        for tx in senders {
                            let _ = tx.send(Err(Status::unavailable(
                                "node died during actor creation",
                            )));
                        }
                    }

                    dead.insert(actor_id, actor);
                }
            }
        }

        self.actors_by_node.write().remove(node_id);
    }

    /// Handle job death — kill non-detached actors belonging to the job.
    ///
    /// Detached actors survive job completion. Non-detached actors are killed.
    pub fn on_job_dead(&self, job_id: &JobID) {
        let job_id_int = job_id.to_int();

        // Find actors belonging to this job (job ID is embedded in actor ID)
        let actor_ids: Vec<ActorID> = self
            .registered_actors
            .read()
            .keys()
            .filter(|aid| aid.job_id().to_int() == job_id_int)
            .copied()
            .collect();

        let mut registered = self.registered_actors.write();
        let mut dead = self.dead_actors.write();
        let mut counts = self.state_counts.write();

        for actor_id in actor_ids {
            if let Some(actor) = registered.get(&actor_id) {
                if actor.is_detached {
                    continue; // Detached actors survive job death
                }
            }

            if let Some(mut actor) = registered.remove(&actor_id) {
                let old_state = ActorState::from(actor.state);
                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *counts.entry(ActorState::Dead).or_insert(0) += 1;

                if !actor.name.is_empty() {
                    self.named_actors
                        .write()
                        .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
                }

                self.publish_actor_state(&actor);
                dead.insert(actor_id, actor);
            }
        }
    }

    /// Handle worker death — kill actors owned by the dead worker.
    pub fn on_worker_dead(&self, node_id: &NodeID, worker_id: &WorkerID) {
        let key = (*node_id, *worker_id);
        let actor_ids = {
            let by_owner = self.actors_by_owner.read();
            by_owner.get(&key).cloned().unwrap_or_default()
        };

        if actor_ids.is_empty() {
            return;
        }

        let mut registered = self.registered_actors.write();
        let mut dead = self.dead_actors.write();
        let mut counts = self.state_counts.write();

        for actor_id in &actor_ids {
            if let Some(mut actor) = registered.remove(actor_id) {
                let old_state = ActorState::from(actor.state);
                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *counts.entry(ActorState::Dead).or_insert(0) += 1;

                if !actor.name.is_empty() {
                    self.named_actors
                        .write()
                        .remove(&(actor.ray_namespace.clone(), actor.name.clone()));
                }

                self.publish_actor_state(&actor);
                dead.insert(*actor_id, actor);
            }
        }

        self.actors_by_owner.write().remove(&key);
    }

    /// Get state counts for metrics.
    pub fn state_counts(&self) -> HashMap<ActorState, usize> {
        self.state_counts.read().clone()
    }

    /// Total registered (non-dead) actors.
    pub fn num_registered_actors(&self) -> usize {
        self.registered_actors.read().len()
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
        let actor = mgr
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
        {
            let mut reg = mgr.registered_actors.write();
            if let Some(a) = reg.get_mut(&actor_id) {
                a.state = ActorState::PendingCreation as i32;
            }
        }

        let (tx, rx) = oneshot::channel();
        mgr.create_callbacks
            .write()
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
        {
            let mut reg = mgr.registered_actors.write();
            if let Some(a) = reg.get_mut(&actor_id) {
                a.state = ActorState::Alive as i32;
                a.max_restarts = 3;
                a.node_id = Some(node_id.clone());
            }
        }
        mgr.actors_by_node
            .write()
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
        {
            let mut reg = mgr.registered_actors.write();
            if let Some(a) = reg.get_mut(&actor_id) {
                a.state = ActorState::Alive as i32;
                a.max_restarts = 1;
                a.num_restarts = 1; // already restarted once
                a.node_id = Some(node_id.clone());
            }
        }
        mgr.actors_by_node
            .write()
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
        mgr.handle_register_actor(non_detached_spec)
            .await
            .unwrap();
        {
            let mut reg = mgr.registered_actors.write();
            if let Some(a) = reg.get_mut(&non_detached_aid) {
                a.is_detached = false;
                a.state = ActorState::Alive as i32;
            }
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
        mgr.handle_register_actor(detached_spec)
            .await
            .unwrap();
        {
            let mut reg = mgr.registered_actors.write();
            if let Some(a) = reg.get_mut(&detached_aid) {
                a.is_detached = true;
                a.state = ActorState::Alive as i32;
            }
        }

        assert_eq!(mgr.num_registered_actors(), 2);

        // Job dies
        mgr.on_job_dead(&job_id);

        // Non-detached should be dead, detached should survive
        assert_eq!(mgr.num_registered_actors(), 1);
        let detached = mgr
            .handle_get_actor_info(&detached_aid.binary())
            .unwrap();
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
        let actor = mgr
            .handle_get_named_actor_info("named_one", "default")
            .expect("named actor should be found");
        assert_eq!(ActorState::from(actor.state), ActorState::Alive);
    }
}
