// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Actor handle registry and named actor resolution.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use ray_common::id::ActorID;

use crate::actor_handle::ActorHandle;

/// Manages actor handles and named actor lookups.
pub struct ActorManager {
    /// Registry of actor handles by actor ID.
    handles: RwLock<HashMap<ActorID, Arc<ActorHandle>>>,
    /// Named actor index: (namespace, name) → ActorID.
    named_actors: RwLock<HashMap<(String, String), ActorID>>,
}

impl ActorManager {
    pub fn new() -> Self {
        Self {
            handles: RwLock::new(HashMap::new()),
            named_actors: RwLock::new(HashMap::new()),
        }
    }

    /// Register an actor handle.
    pub fn register_actor_handle(&self, actor_id: ActorID, handle: Arc<ActorHandle>) {
        self.handles.write().insert(actor_id, handle);
    }

    /// Get an actor handle by ID.
    pub fn get_actor_handle(&self, actor_id: &ActorID) -> Option<Arc<ActorHandle>> {
        self.handles.read().get(actor_id).cloned()
    }

    /// Remove an actor handle.
    pub fn remove_actor_handle(&self, actor_id: &ActorID) -> Option<Arc<ActorHandle>> {
        self.handles.write().remove(actor_id)
    }

    /// Register a named actor mapping.
    pub fn register_named_actor(
        &self,
        namespace: String,
        name: String,
        actor_id: ActorID,
    ) {
        self.named_actors
            .write()
            .insert((namespace, name), actor_id);
    }

    /// Look up a named actor.
    pub fn get_named_actor(&self, namespace: &str, name: &str) -> Option<ActorID> {
        self.named_actors
            .read()
            .get(&(namespace.to_string(), name.to_string()))
            .copied()
    }

    /// Get all named actors, optionally filtered by namespace.
    pub fn get_all_named_actors(&self, namespace: Option<&str>) -> Vec<(String, String, ActorID)> {
        self.named_actors
            .read()
            .iter()
            .filter(|((ns, _), _)| namespace.is_none() || namespace == Some(ns.as_str()))
            .map(|((ns, name), id)| (ns.clone(), name.clone(), *id))
            .collect()
    }

    /// Number of registered actor handles.
    pub fn num_handles(&self) -> usize {
        self.handles.read().len()
    }
}

impl Default for ActorManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_proto::ray::rpc;

    fn make_handle(name: &str) -> Arc<ActorHandle> {
        let aid = ActorID::from_random();
        Arc::new(ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: name.to_string(),
            ..Default::default()
        }))
    }

    #[test]
    fn test_register_and_get_handle() {
        let mgr = ActorManager::new();
        let handle = make_handle("actor1");
        let aid = handle.actor_id();
        mgr.register_actor_handle(aid, handle.clone());
        let got = mgr.get_actor_handle(&aid).unwrap();
        assert_eq!(got.name(), "actor1");
        assert_eq!(mgr.num_handles(), 1);
        mgr.remove_actor_handle(&aid);
        assert!(mgr.get_actor_handle(&aid).is_none());
    }

    #[test]
    fn test_named_actor_lookup() {
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();
        mgr.register_named_actor("default".into(), "my_actor".into(), aid);
        assert_eq!(mgr.get_named_actor("default", "my_actor"), Some(aid));
        assert_eq!(mgr.get_named_actor("default", "missing"), None);
        assert_eq!(mgr.get_named_actor("other_ns", "my_actor"), None);
    }

    #[test]
    fn test_get_all_named_actors() {
        let mgr = ActorManager::new();
        let aid1 = ActorID::from_random();
        let aid2 = ActorID::from_random();
        mgr.register_named_actor("ns1".into(), "a".into(), aid1);
        mgr.register_named_actor("ns2".into(), "b".into(), aid2);

        let all = mgr.get_all_named_actors(None);
        assert_eq!(all.len(), 2);

        let ns1_only = mgr.get_all_named_actors(Some("ns1"));
        assert_eq!(ns1_only.len(), 1);
        assert_eq!(ns1_only[0].2, aid1);
    }
}
