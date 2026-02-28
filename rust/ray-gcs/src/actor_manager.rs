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
    #[allow(dead_code)]
    actors_by_node: RwLock<HashMap<NodeID, Vec<ActorID>>>,
    /// Actors by owner: (node_id, worker_id) → set of actor_ids.
    #[allow(dead_code)]
    actors_by_owner: RwLock<HashMap<(NodeID, WorkerID), Vec<ActorID>>>,
    /// State counts for metrics.
    state_counts: RwLock<HashMap<ActorState, usize>>,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
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
        }
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
    pub async fn handle_create_actor(&self, actor_id_bytes: &[u8]) -> Result<(), tonic::Status> {
        let actor_id = ActorID::from_binary(actor_id_bytes.try_into().unwrap_or(&[0u8; 16]));

        let mut registered = self.registered_actors.write();
        let mut counts = self.state_counts.write();

        if let Some(actor) = registered.get_mut(&actor_id) {
            let old_state = ActorState::from(actor.state);
            actor.state = ActorState::PendingCreation as i32;

            if let Some(c) = counts.get_mut(&old_state) {
                *c = c.saturating_sub(1);
            }
            *counts.entry(ActorState::PendingCreation).or_insert(0) += 1;
        }

        tracing::info!(?actor_id, "Actor creation requested");
        Ok(())
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
            self.dead_actors.write().insert(actor_id, actor);
            tracing::info!(?actor_id, "Actor killed");
        }
        Ok(())
    }

    /// Handle node death — restart or kill actors on the dead node.
    pub fn on_node_dead(&self, node_id: &NodeID) {
        let actor_ids = {
            let by_node = self.actors_by_node.read();
            by_node.get(node_id).cloned().unwrap_or_default()
        };

        let mut registered = self.registered_actors.write();
        let mut dead = self.dead_actors.write();
        let mut counts = self.state_counts.write();

        for actor_id in actor_ids {
            if let Some(mut actor) = registered.remove(&actor_id) {
                let old_state = ActorState::from(actor.state);
                if let Some(c) = counts.get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
                actor.state = ActorState::Dead as i32;
                *counts.entry(ActorState::Dead).or_insert(0) += 1;
                dead.insert(actor_id, actor);
            }
        }

        self.actors_by_node.write().remove(node_id);
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
}
