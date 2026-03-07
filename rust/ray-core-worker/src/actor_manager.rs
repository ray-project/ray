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
    pub fn register_named_actor(&self, namespace: String, name: String, actor_id: ActorID) {
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

    // ─── Ported from C++ actor_manager_test.cc ───────────────────────

    #[test]
    fn test_check_actor_handle_doesnt_exist() {
        // Port of TestCheckActorHandleDoesntExists.
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();
        assert!(mgr.get_actor_handle(&aid).is_none());
        assert_eq!(mgr.num_handles(), 0);
    }

    #[test]
    fn test_register_and_remove_handle() {
        // Port of TestAddAndGetActorHandleEndToEnd (register, get, remove).
        let mgr = ActorManager::new();
        let handle = make_handle("test_actor");
        let aid = handle.actor_id();

        // Register handle.
        mgr.register_actor_handle(aid, handle.clone());
        assert_eq!(mgr.num_handles(), 1);

        // Get handle and verify.
        let got = mgr.get_actor_handle(&aid).unwrap();
        assert_eq!(got.actor_id(), aid);
        assert_eq!(got.name(), "test_actor");

        // Remove handle.
        let removed = mgr.remove_actor_handle(&aid).unwrap();
        assert_eq!(removed.actor_id(), aid);
        assert!(mgr.get_actor_handle(&aid).is_none());
        assert_eq!(mgr.num_handles(), 0);
    }

    #[test]
    fn test_remove_nonexistent_handle() {
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();
        assert!(mgr.remove_actor_handle(&aid).is_none());
    }

    #[test]
    fn test_duplicate_register_overwrites() {
        // Port of RegisterActorHandles: re-registering overwrites handle.
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();

        let proto1 = rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "version1".to_string(),
            ..Default::default()
        };
        let handle1 = Arc::new(ActorHandle::from_proto(proto1));
        mgr.register_actor_handle(aid, handle1);
        assert_eq!(mgr.get_actor_handle(&aid).unwrap().name(), "version1");

        let proto2 = rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "version2".to_string(),
            ..Default::default()
        };
        let handle2 = Arc::new(ActorHandle::from_proto(proto2));
        mgr.register_actor_handle(aid, handle2);
        assert_eq!(mgr.get_actor_handle(&aid).unwrap().name(), "version2");
        assert_eq!(mgr.num_handles(), 1);
    }

    #[test]
    fn test_named_actor_overwrite() {
        // Registering a named actor with the same name/namespace overwrites.
        let mgr = ActorManager::new();
        let aid1 = ActorID::from_random();
        let aid2 = ActorID::from_random();
        mgr.register_named_actor("ns".into(), "name".into(), aid1);
        assert_eq!(mgr.get_named_actor("ns", "name"), Some(aid1));

        mgr.register_named_actor("ns".into(), "name".into(), aid2);
        assert_eq!(mgr.get_named_actor("ns", "name"), Some(aid2));
    }

    #[test]
    fn test_multiple_actors_independent() {
        // Multiple actors registered independently.
        let mgr = ActorManager::new();
        let mut aids = Vec::new();
        for i in 0..5 {
            let h = make_handle(&format!("actor_{}", i));
            let aid = h.actor_id();
            mgr.register_actor_handle(aid, h);
            aids.push(aid);
        }
        assert_eq!(mgr.num_handles(), 5);

        // Remove middle one.
        mgr.remove_actor_handle(&aids[2]);
        assert_eq!(mgr.num_handles(), 4);
        assert!(mgr.get_actor_handle(&aids[2]).is_none());
        // Others still present.
        assert!(mgr.get_actor_handle(&aids[0]).is_some());
        assert!(mgr.get_actor_handle(&aids[4]).is_some());
    }

    #[test]
    fn test_default_impl() {
        let mgr = ActorManager::default();
        assert_eq!(mgr.num_handles(), 0);
    }

    #[test]
    fn test_named_actors_different_namespaces_same_name() {
        let mgr = ActorManager::new();
        let aid1 = ActorID::from_random();
        let aid2 = ActorID::from_random();
        mgr.register_named_actor("ns_a".into(), "shared_name".into(), aid1);
        mgr.register_named_actor("ns_b".into(), "shared_name".into(), aid2);

        assert_eq!(mgr.get_named_actor("ns_a", "shared_name"), Some(aid1));
        assert_eq!(mgr.get_named_actor("ns_b", "shared_name"), Some(aid2));
        assert_ne!(aid1, aid2);

        let all = mgr.get_all_named_actors(None);
        assert_eq!(all.len(), 2);
    }

    // ── Additional tests ported from actor_manager_test.cc ──────────

    /// Port of TestActorStateNotificationPending: verify handle state
    /// after registration (actor is initially accessible).
    #[test]
    fn test_actor_state_after_registration() {
        let mgr = ActorManager::new();
        let handle = make_handle("pending_actor");
        let aid = handle.actor_id();

        mgr.register_actor_handle(aid, handle.clone());
        let got = mgr.get_actor_handle(&aid).unwrap();
        assert_eq!(got.name(), "pending_actor");
        assert_eq!(mgr.num_handles(), 1);
    }

    /// Port of TestActorStateNotificationRestarting: register,
    /// remove, and re-register simulates restart.
    #[test]
    fn test_actor_restart_via_reregister() {
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();

        let handle1 = Arc::new(ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "v1".to_string(),
            ..Default::default()
        }));
        mgr.register_actor_handle(aid, handle1);
        assert_eq!(mgr.get_actor_handle(&aid).unwrap().name(), "v1");

        // Simulate restart: register new handle.
        let handle2 = Arc::new(ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "v2_restarted".to_string(),
            ..Default::default()
        }));
        mgr.register_actor_handle(aid, handle2);
        assert_eq!(mgr.get_actor_handle(&aid).unwrap().name(), "v2_restarted");
        assert_eq!(mgr.num_handles(), 1); // Still just one handle.
    }

    /// Port of TestActorStateNotificationDead: removing a handle
    /// simulates actor death.
    #[test]
    fn test_actor_death_removes_handle() {
        let mgr = ActorManager::new();
        let handle = make_handle("dying_actor");
        let aid = handle.actor_id();

        mgr.register_actor_handle(aid, handle);
        assert_eq!(mgr.num_handles(), 1);

        // Simulate death.
        mgr.remove_actor_handle(&aid);
        assert!(mgr.get_actor_handle(&aid).is_none());
        assert_eq!(mgr.num_handles(), 0);
    }

    /// Port of TestActorStateNotificationAlive: named actor lookup
    /// returns the correct actor ID.
    #[test]
    fn test_named_actor_alive_lookup() {
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();
        let handle = Arc::new(ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "alive_named".to_string(),
            ..Default::default()
        }));

        mgr.register_actor_handle(aid, handle);
        mgr.register_named_actor("default".into(), "alive_named".into(), aid);

        let found_id = mgr.get_named_actor("default", "alive_named").unwrap();
        assert_eq!(found_id, aid);

        // Handle is also accessible.
        let found_handle = mgr.get_actor_handle(&found_id).unwrap();
        assert_eq!(found_handle.name(), "alive_named");
    }

    /// Port of TestActorStateIsOnlySubscribedOnce concept: registering
    /// a named actor multiple times should just overwrite.
    #[test]
    fn test_named_actor_re_registration() {
        let mgr = ActorManager::new();
        let aid1 = ActorID::from_random();
        let aid2 = ActorID::from_random();

        mgr.register_named_actor("ns".into(), "actor".into(), aid1);
        mgr.register_named_actor("ns".into(), "actor".into(), aid2);

        // Should point to the latest registration.
        assert_eq!(mgr.get_named_actor("ns", "actor"), Some(aid2));

        // Only one entry in named_actors.
        let all = mgr.get_all_named_actors(Some("ns"));
        assert_eq!(all.len(), 1);
    }

    /// Port of TestNamedActorIsKilledAfterSubscribeFinished concept:
    /// removing a handle for a named actor should not affect the
    /// named actor registry (they are independent).
    #[test]
    fn test_remove_handle_doesnt_affect_named_registry() {
        let mgr = ActorManager::new();
        let aid = ActorID::from_random();
        let handle = Arc::new(ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "named_then_killed".to_string(),
            ..Default::default()
        }));

        mgr.register_actor_handle(aid, handle);
        mgr.register_named_actor("ns".into(), "my_actor".into(), aid);

        // Remove the handle (actor "dies").
        mgr.remove_actor_handle(&aid);
        assert!(mgr.get_actor_handle(&aid).is_none());

        // Named actor registry still has the mapping (stale, but present).
        assert_eq!(mgr.get_named_actor("ns", "my_actor"), Some(aid));
    }

    /// Port of bulk registration/removal: add many actors, remove
    /// some, verify counts.
    #[test]
    fn test_bulk_register_and_remove() {
        let mgr = ActorManager::new();
        let mut aids = Vec::new();

        for i in 0..20 {
            let h = make_handle(&format!("bulk_{}", i));
            let aid = h.actor_id();
            mgr.register_actor_handle(aid, h);
            aids.push(aid);
        }
        assert_eq!(mgr.num_handles(), 20);

        // Remove every other one.
        for (i, aid) in aids.iter().enumerate() {
            if i % 2 == 0 {
                mgr.remove_actor_handle(aid);
            }
        }
        assert_eq!(mgr.num_handles(), 10);

        // Verify the remaining ones.
        for (i, aid) in aids.iter().enumerate() {
            if i % 2 == 0 {
                assert!(mgr.get_actor_handle(aid).is_none());
            } else {
                assert!(mgr.get_actor_handle(aid).is_some());
            }
        }
    }

    /// Port of empty namespace filter: get_all_named_actors with
    /// None returns all.
    #[test]
    fn test_get_all_named_actors_unfiltered() {
        let mgr = ActorManager::new();
        for i in 0..5 {
            let aid = ActorID::from_random();
            mgr.register_named_actor(format!("ns_{}", i), format!("actor_{}", i), aid);
        }
        let all = mgr.get_all_named_actors(None);
        assert_eq!(all.len(), 5);
    }
}
