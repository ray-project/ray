// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object lineage tracking and reconstruction.
//!
//! Tracks which task produced which object, enabling re-execution of
//! the producing task when an object is lost. This is the core mechanism
//! for fault tolerance in Ray's object store.
//!
//! Replaces lineage-related logic in `src/ray/core_worker/reference_counter.cc`
//! and `src/ray/core_worker/core_worker.cc`.

use std::collections::{HashMap, HashSet, VecDeque};

use parking_lot::Mutex;
use ray_common::id::{ObjectID, TaskID};

/// A lineage entry tracking which task produced an object.
#[derive(Debug, Clone)]
pub struct LineageEntry {
    /// The task that produced this object.
    pub task_id: TaskID,
    /// The task specification (serialized protobuf) for re-execution.
    pub task_spec: Vec<u8>,
    /// Objects that this task depends on (its inputs).
    pub dependencies: Vec<ObjectID>,
    /// Objects that this task produced (its outputs).
    pub output_objects: Vec<ObjectID>,
    /// Number of times this task has been re-executed for recovery.
    pub reconstruction_count: u32,
    /// Maximum number of times to attempt reconstruction.
    pub max_reconstructions: u32,
}

/// Lineage eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Never evict lineage (keep forever).
    Never,
    /// Evict lineage for objects that have been consumed by all downstream tasks.
    OnConsume,
    /// Evict lineage when the lineage cache exceeds a size limit.
    SizeLimit,
}

/// Configuration for the lineage reconstructor.
#[derive(Debug, Clone)]
pub struct LineageConfig {
    /// Maximum number of lineage entries to keep.
    pub max_lineage_entries: usize,
    /// Eviction policy.
    pub eviction_policy: EvictionPolicy,
    /// Maximum reconstruction attempts per object.
    pub max_reconstructions: u32,
}

impl Default for LineageConfig {
    fn default() -> Self {
        Self {
            max_lineage_entries: 100_000,
            eviction_policy: EvictionPolicy::SizeLimit,
            max_reconstructions: 5,
        }
    }
}

/// The lineage reconstructor tracks task lineage and manages object reconstruction.
pub struct LineageReconstructor {
    config: LineageConfig,
    /// Map from task_id → lineage entry.
    task_lineage: Mutex<HashMap<TaskID, LineageEntry>>,
    /// Map from object_id → task_id that produced it.
    object_to_task: Mutex<HashMap<ObjectID, TaskID>>,
    /// Objects currently being reconstructed.
    reconstructing: Mutex<HashSet<ObjectID>>,
}

impl LineageReconstructor {
    pub fn new(config: LineageConfig) -> Self {
        Self {
            config,
            task_lineage: Mutex::new(HashMap::new()),
            object_to_task: Mutex::new(HashMap::new()),
            reconstructing: Mutex::new(HashSet::new()),
        }
    }

    /// Record the lineage for a task and its output objects.
    pub fn record_lineage(
        &self,
        task_id: TaskID,
        task_spec: Vec<u8>,
        dependencies: Vec<ObjectID>,
        output_objects: Vec<ObjectID>,
    ) {
        let entry = LineageEntry {
            task_id,
            task_spec,
            dependencies,
            output_objects: output_objects.clone(),
            reconstruction_count: 0,
            max_reconstructions: self.config.max_reconstructions,
        };

        // Map each output object to this task.
        {
            let mut obj_map = self.object_to_task.lock();
            for oid in &output_objects {
                obj_map.insert(*oid, task_id);
            }
        }

        self.task_lineage.lock().insert(task_id, entry);
        self.maybe_evict();
    }

    /// Check if an object can be reconstructed from lineage.
    pub fn can_reconstruct(&self, object_id: &ObjectID) -> bool {
        let obj_map = self.object_to_task.lock();
        if let Some(task_id) = obj_map.get(object_id) {
            let lineage = self.task_lineage.lock();
            if let Some(entry) = lineage.get(task_id) {
                return entry.reconstruction_count < entry.max_reconstructions;
            }
        }
        false
    }

    /// Get the task spec for reconstructing an object.
    ///
    /// Returns `(task_spec, dependencies)` if lineage exists and reconstruction
    /// is possible, `None` otherwise.
    pub fn get_reconstruction_info(
        &self,
        object_id: &ObjectID,
    ) -> Option<(Vec<u8>, Vec<ObjectID>)> {
        let task_id = self.object_to_task.lock().get(object_id).copied()?;
        let lineage = self.task_lineage.lock();
        let entry = lineage.get(&task_id)?;

        if entry.reconstruction_count >= entry.max_reconstructions {
            return None;
        }

        Some((entry.task_spec.clone(), entry.dependencies.clone()))
    }

    /// Mark an object as being reconstructed.
    pub fn start_reconstruction(&self, object_id: &ObjectID) -> bool {
        let task_id = match self.object_to_task.lock().get(object_id).copied() {
            Some(tid) => tid,
            None => return false,
        };

        let mut lineage = self.task_lineage.lock();
        if let Some(entry) = lineage.get_mut(&task_id) {
            if entry.reconstruction_count >= entry.max_reconstructions {
                return false;
            }
            entry.reconstruction_count += 1;
            self.reconstructing.lock().insert(*object_id);
            true
        } else {
            false
        }
    }

    /// Mark reconstruction as complete for an object.
    pub fn finish_reconstruction(&self, object_id: &ObjectID) {
        self.reconstructing.lock().remove(object_id);
    }

    /// Mark reconstruction as failed for an object.
    pub fn fail_reconstruction(&self, object_id: &ObjectID) {
        self.reconstructing.lock().remove(object_id);
    }

    /// Check if an object is currently being reconstructed.
    pub fn is_reconstructing(&self, object_id: &ObjectID) -> bool {
        self.reconstructing.lock().contains(object_id)
    }

    /// Remove lineage for a task.
    pub fn remove_lineage(&self, task_id: &TaskID) {
        if let Some(entry) = self.task_lineage.lock().remove(task_id) {
            let mut obj_map = self.object_to_task.lock();
            for oid in &entry.output_objects {
                obj_map.remove(oid);
            }
        }
    }

    /// Get the full lineage chain for an object (BFS from the object's
    /// producing task backward through dependencies).
    pub fn get_lineage_chain(&self, object_id: &ObjectID) -> Vec<TaskID> {
        let obj_map = self.object_to_task.lock();
        let lineage = self.task_lineage.lock();

        let mut chain = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        if let Some(&task_id) = obj_map.get(object_id) {
            queue.push_back(task_id);
        }

        while let Some(task_id) = queue.pop_front() {
            if !visited.insert(task_id) {
                continue;
            }
            chain.push(task_id);

            if let Some(entry) = lineage.get(&task_id) {
                for dep in &entry.dependencies {
                    if let Some(&dep_task) = obj_map.get(dep) {
                        queue.push_back(dep_task);
                    }
                }
            }
        }

        chain
    }

    /// Number of lineage entries.
    pub fn num_entries(&self) -> usize {
        self.task_lineage.lock().len()
    }

    /// Number of objects with lineage.
    pub fn num_objects_with_lineage(&self) -> usize {
        self.object_to_task.lock().len()
    }

    /// Number of objects currently being reconstructed.
    pub fn num_reconstructing(&self) -> usize {
        self.reconstructing.lock().len()
    }

    /// Evict old lineage entries if over the limit.
    fn maybe_evict(&self) {
        if self.config.eviction_policy != EvictionPolicy::SizeLimit {
            return;
        }

        let mut lineage = self.task_lineage.lock();
        if lineage.len() <= self.config.max_lineage_entries {
            return;
        }

        // Simple eviction: remove entries for tasks with no dependencies
        // (leaf tasks) first. This is a simplification — a full implementation
        // would track age and use LRU.
        let to_remove = lineage.len() - self.config.max_lineage_entries;
        let mut obj_map = self.object_to_task.lock();
        let reconstructing = self.reconstructing.lock();

        let removable: Vec<TaskID> = lineage
            .iter()
            .filter(|(_, entry)| {
                // Don't evict entries for objects being reconstructed.
                !entry
                    .output_objects
                    .iter()
                    .any(|oid| reconstructing.contains(oid))
            })
            .take(to_remove)
            .map(|(tid, _)| *tid)
            .collect();

        for tid in removable {
            if let Some(entry) = lineage.remove(&tid) {
                for oid in &entry.output_objects {
                    obj_map.remove(oid);
                }
            }
        }
    }
}

impl Default for LineageReconstructor {
    fn default() -> Self {
        Self::new(LineageConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_oid() -> ObjectID {
        ObjectID::from_random()
    }

    fn random_tid() -> TaskID {
        TaskID::from_random()
    }

    #[test]
    fn test_record_and_query_lineage() {
        let lr = LineageReconstructor::default();
        let task_id = random_tid();
        let oid = random_oid();

        lr.record_lineage(task_id, b"spec".to_vec(), vec![], vec![oid]);

        assert!(lr.can_reconstruct(&oid));
        assert_eq!(lr.num_entries(), 1);
        assert_eq!(lr.num_objects_with_lineage(), 1);
    }

    #[test]
    fn test_get_reconstruction_info() {
        let lr = LineageReconstructor::default();
        let task_id = random_tid();
        let oid = random_oid();
        let dep = random_oid();

        lr.record_lineage(task_id, b"my_spec".to_vec(), vec![dep], vec![oid]);

        let (spec, deps) = lr.get_reconstruction_info(&oid).unwrap();
        assert_eq!(spec, b"my_spec");
        assert_eq!(deps, vec![dep]);
    }

    #[test]
    fn test_reconstruction_limit() {
        let config = LineageConfig {
            max_reconstructions: 2,
            ..Default::default()
        };
        let lr = LineageReconstructor::new(config);
        let task_id = random_tid();
        let oid = random_oid();

        lr.record_lineage(task_id, b"spec".to_vec(), vec![], vec![oid]);

        assert!(lr.start_reconstruction(&oid));
        lr.finish_reconstruction(&oid);

        assert!(lr.start_reconstruction(&oid));
        lr.finish_reconstruction(&oid);

        // Third attempt should fail (max=2).
        assert!(!lr.start_reconstruction(&oid));
        assert!(!lr.can_reconstruct(&oid));
    }

    #[test]
    fn test_reconstruction_tracking() {
        let lr = LineageReconstructor::default();
        let task_id = random_tid();
        let oid = random_oid();

        lr.record_lineage(task_id, b"spec".to_vec(), vec![], vec![oid]);

        assert!(!lr.is_reconstructing(&oid));
        lr.start_reconstruction(&oid);
        assert!(lr.is_reconstructing(&oid));
        assert_eq!(lr.num_reconstructing(), 1);

        lr.finish_reconstruction(&oid);
        assert!(!lr.is_reconstructing(&oid));
    }

    #[test]
    fn test_remove_lineage() {
        let lr = LineageReconstructor::default();
        let task_id = random_tid();
        let oid = random_oid();

        lr.record_lineage(task_id, b"spec".to_vec(), vec![], vec![oid]);
        assert!(lr.can_reconstruct(&oid));

        lr.remove_lineage(&task_id);
        assert!(!lr.can_reconstruct(&oid));
        assert_eq!(lr.num_entries(), 0);
    }

    #[test]
    fn test_lineage_chain() {
        let lr = LineageReconstructor::default();

        // Task A produces obj_a.
        let task_a = random_tid();
        let obj_a = random_oid();
        lr.record_lineage(task_a, b"spec_a".to_vec(), vec![], vec![obj_a]);

        // Task B depends on obj_a, produces obj_b.
        let task_b = random_tid();
        let obj_b = random_oid();
        lr.record_lineage(task_b, b"spec_b".to_vec(), vec![obj_a], vec![obj_b]);

        let chain = lr.get_lineage_chain(&obj_b);
        assert_eq!(chain.len(), 2);
        assert!(chain.contains(&task_a));
        assert!(chain.contains(&task_b));
    }

    #[test]
    fn test_no_lineage_returns_none() {
        let lr = LineageReconstructor::default();
        let oid = random_oid();
        assert!(!lr.can_reconstruct(&oid));
        assert!(lr.get_reconstruction_info(&oid).is_none());
    }

    #[test]
    fn test_eviction() {
        let config = LineageConfig {
            max_lineage_entries: 3,
            eviction_policy: EvictionPolicy::SizeLimit,
            max_reconstructions: 5,
        };
        let lr = LineageReconstructor::new(config);

        // Add 5 entries (over the limit of 3).
        for _ in 0..5 {
            lr.record_lineage(random_tid(), b"spec".to_vec(), vec![], vec![random_oid()]);
        }

        // Should have evicted down to 3.
        assert!(lr.num_entries() <= 3);
    }

    #[test]
    fn test_multiple_outputs() {
        let lr = LineageReconstructor::default();
        let task_id = random_tid();
        let oid1 = random_oid();
        let oid2 = random_oid();

        lr.record_lineage(task_id, b"spec".to_vec(), vec![], vec![oid1, oid2]);

        assert!(lr.can_reconstruct(&oid1));
        assert!(lr.can_reconstruct(&oid2));
        assert_eq!(lr.num_objects_with_lineage(), 2);
    }
}
