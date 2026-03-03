// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Namespace Manager — isolates named actors by namespace.
//!
//! Ray namespaces allow multiple jobs to have actors with the same name
//! without collision. Each job is assigned a namespace, and named actors
//! are scoped to their namespace.
//!
//! Replaces namespace-related logic from `src/ray/gcs/gcs_actor_manager.cc`.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use ray_common::id::{ActorID, JobID};

/// Default namespace for jobs that don't specify one.
pub const DEFAULT_NAMESPACE: &str = "";

/// A named actor entry in the namespace.
#[derive(Debug, Clone)]
pub struct NamedActorEntry {
    pub actor_id: ActorID,
    pub name: String,
    pub namespace: String,
    pub job_id: JobID,
    /// Whether this actor requires a specific ray namespace for access.
    pub require_namespace: bool,
}

/// The namespace manager tracks named actors across namespaces.
pub struct NamespaceManager {
    /// Map from (namespace, actor_name) → actor entry.
    named_actors: RwLock<HashMap<(String, String), NamedActorEntry>>,
    /// Map from actor_id → (namespace, name) for reverse lookup.
    actor_to_name: RwLock<HashMap<ActorID, (String, String)>>,
    /// Map from job_id → namespace.
    job_namespaces: RwLock<HashMap<JobID, String>>,
    /// Set of all namespaces that have been used.
    namespaces: RwLock<HashSet<String>>,
}

impl NamespaceManager {
    pub fn new() -> Self {
        let mut namespaces = HashSet::new();
        namespaces.insert(DEFAULT_NAMESPACE.to_string());
        Self {
            named_actors: RwLock::new(HashMap::new()),
            actor_to_name: RwLock::new(HashMap::new()),
            job_namespaces: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(namespaces),
        }
    }

    /// Register a job's namespace.
    pub fn register_job_namespace(&self, job_id: JobID, namespace: String) {
        self.namespaces.write().insert(namespace.clone());
        self.job_namespaces.write().insert(job_id, namespace);
    }

    /// Get the namespace for a job.
    pub fn get_job_namespace(&self, job_id: &JobID) -> String {
        self.job_namespaces
            .read()
            .get(job_id)
            .cloned()
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string())
    }

    /// Remove a job's namespace mapping.
    pub fn remove_job_namespace(&self, job_id: &JobID) {
        self.job_namespaces.write().remove(job_id);
    }

    /// Register a named actor in its namespace.
    ///
    /// Returns `false` if an actor with the same name already exists in the namespace.
    pub fn register_named_actor(&self, entry: NamedActorEntry) -> bool {
        let key = (entry.namespace.clone(), entry.name.clone());
        let mut actors = self.named_actors.write();

        if actors.contains_key(&key) {
            return false;
        }

        self.actor_to_name
            .write()
            .insert(entry.actor_id, key.clone());
        actors.insert(key, entry);
        true
    }

    /// Look up a named actor by namespace and name.
    pub fn get_named_actor(&self, namespace: &str, name: &str) -> Option<NamedActorEntry> {
        self.named_actors
            .read()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned()
    }

    /// Remove a named actor (on actor death).
    pub fn remove_named_actor(&self, actor_id: &ActorID) -> Option<NamedActorEntry> {
        let key = self.actor_to_name.write().remove(actor_id)?;
        self.named_actors.write().remove(&key)
    }

    /// List all named actors in a namespace.
    pub fn list_named_actors(&self, namespace: &str) -> Vec<NamedActorEntry> {
        self.named_actors
            .read()
            .iter()
            .filter(|((ns, _), _)| ns == namespace)
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    /// List all named actors across all namespaces.
    pub fn list_all_named_actors(&self) -> Vec<NamedActorEntry> {
        self.named_actors.read().values().cloned().collect()
    }

    /// Check if a named actor exists.
    pub fn has_named_actor(&self, namespace: &str, name: &str) -> bool {
        self.named_actors
            .read()
            .contains_key(&(namespace.to_string(), name.to_string()))
    }

    /// Check if a job can access a named actor.
    ///
    /// An actor is accessible if:
    /// 1. The actor doesn't require namespace isolation, OR
    /// 2. The job's namespace matches the actor's namespace.
    pub fn can_access_actor(
        &self,
        job_id: &JobID,
        actor_namespace: &str,
        actor_name: &str,
    ) -> bool {
        let entry = match self.get_named_actor(actor_namespace, actor_name) {
            Some(e) => e,
            None => return false,
        };

        if !entry.require_namespace {
            return true;
        }

        let job_ns = self.get_job_namespace(job_id);
        job_ns == actor_namespace
    }

    /// Number of named actors.
    pub fn num_named_actors(&self) -> usize {
        self.named_actors.read().len()
    }

    /// Get all namespaces.
    pub fn all_namespaces(&self) -> Vec<String> {
        self.namespaces.read().iter().cloned().collect()
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(ns: &str, name: &str, require_ns: bool) -> NamedActorEntry {
        NamedActorEntry {
            actor_id: ActorID::from_random(),
            name: name.to_string(),
            namespace: ns.to_string(),
            job_id: JobID::from_int(1),
            require_namespace: require_ns,
        }
    }

    #[test]
    fn test_register_and_get_named_actor() {
        let mgr = NamespaceManager::new();
        let entry = make_entry("ns1", "my_actor", false);
        let aid = entry.actor_id;

        assert!(mgr.register_named_actor(entry));
        let found = mgr.get_named_actor("ns1", "my_actor").unwrap();
        assert_eq!(found.actor_id, aid);
        assert_eq!(found.name, "my_actor");
    }

    #[test]
    fn test_duplicate_name_rejected() {
        let mgr = NamespaceManager::new();
        assert!(mgr.register_named_actor(make_entry("ns1", "actor_a", false)));
        assert!(!mgr.register_named_actor(make_entry("ns1", "actor_a", false)));
    }

    #[test]
    fn test_same_name_different_namespace() {
        let mgr = NamespaceManager::new();
        assert!(mgr.register_named_actor(make_entry("ns1", "worker", false)));
        assert!(mgr.register_named_actor(make_entry("ns2", "worker", false)));
        assert_eq!(mgr.num_named_actors(), 2);
    }

    #[test]
    fn test_remove_named_actor() {
        let mgr = NamespaceManager::new();
        let entry = make_entry("ns1", "removable", false);
        let aid = entry.actor_id;

        mgr.register_named_actor(entry);
        assert!(mgr.has_named_actor("ns1", "removable"));

        let removed = mgr.remove_named_actor(&aid).unwrap();
        assert_eq!(removed.name, "removable");
        assert!(!mgr.has_named_actor("ns1", "removable"));
    }

    #[test]
    fn test_list_named_actors() {
        let mgr = NamespaceManager::new();
        mgr.register_named_actor(make_entry("ns1", "a", false));
        mgr.register_named_actor(make_entry("ns1", "b", false));
        mgr.register_named_actor(make_entry("ns2", "c", false));

        let ns1_actors = mgr.list_named_actors("ns1");
        assert_eq!(ns1_actors.len(), 2);

        let all = mgr.list_all_named_actors();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_job_namespace() {
        let mgr = NamespaceManager::new();
        let job1 = JobID::from_int(1);
        let job2 = JobID::from_int(2);

        assert_eq!(mgr.get_job_namespace(&job1), DEFAULT_NAMESPACE);

        mgr.register_job_namespace(job1, "prod".to_string());
        mgr.register_job_namespace(job2, "dev".to_string());

        assert_eq!(mgr.get_job_namespace(&job1), "prod");
        assert_eq!(mgr.get_job_namespace(&job2), "dev");

        mgr.remove_job_namespace(&job1);
        assert_eq!(mgr.get_job_namespace(&job1), DEFAULT_NAMESPACE);
    }

    #[test]
    fn test_access_control_no_restriction() {
        let mgr = NamespaceManager::new();
        let job = JobID::from_int(1);
        mgr.register_job_namespace(job, "ns_other".to_string());

        // Actor without namespace requirement → accessible from any namespace.
        mgr.register_named_actor(make_entry("ns1", "open_actor", false));
        assert!(mgr.can_access_actor(&job, "ns1", "open_actor"));
    }

    #[test]
    fn test_access_control_with_restriction() {
        let mgr = NamespaceManager::new();
        let job_same = JobID::from_int(1);
        let job_diff = JobID::from_int(2);

        mgr.register_job_namespace(job_same, "ns1".to_string());
        mgr.register_job_namespace(job_diff, "ns2".to_string());

        // Actor requires namespace → only accessible from same namespace.
        mgr.register_named_actor(make_entry("ns1", "restricted", true));

        assert!(mgr.can_access_actor(&job_same, "ns1", "restricted"));
        assert!(!mgr.can_access_actor(&job_diff, "ns1", "restricted"));
    }

    #[test]
    fn test_access_nonexistent_actor() {
        let mgr = NamespaceManager::new();
        let job = JobID::from_int(1);
        assert!(!mgr.can_access_actor(&job, "ns1", "nonexistent"));
    }

    #[test]
    fn test_all_namespaces() {
        let mgr = NamespaceManager::new();
        let nss = mgr.all_namespaces();
        assert!(nss.contains(&DEFAULT_NAMESPACE.to_string()));

        mgr.register_job_namespace(JobID::from_int(1), "prod".to_string());
        let nss = mgr.all_namespaces();
        assert!(nss.contains(&"prod".to_string()));
    }
}
