// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Local resource manager — tracks this node's resources at instance granularity.
//!
//! Replaces `src/ray/raylet/scheduling/local_resource_manager.h/cc`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;
use ray_common::scheduling::{FixedPoint, ResourceSet};

use crate::scheduling_resources::{NodeResourceInstances, TaskResourceInstances};

/// The local resource manager tracks this node's resources at instance granularity.
pub struct LocalResourceManager {
    /// The local node ID (hex string).
    local_node_id: String,
    /// Instance-level resource state.
    local_resources: RwLock<NodeResourceInstances>,
    /// Monotonic version counter (incremented on any state change).
    version: AtomicI64,
    /// Whether this node is draining.
    is_draining: RwLock<bool>,
    /// Draining deadline timestamp (ms).
    draining_deadline_ms: RwLock<u64>,
}

impl LocalResourceManager {
    pub fn new(
        local_node_id: String,
        total_resources: ResourceSet,
        labels: HashMap<String, String>,
    ) -> Self {
        Self {
            local_node_id,
            local_resources: RwLock::new(NodeResourceInstances::new(total_resources, labels)),
            version: AtomicI64::new(0),
            is_draining: RwLock::new(false),
            draining_deadline_ms: RwLock::new(0),
        }
    }

    /// Allocate resources for a task on this node.
    /// Returns the per-instance allocation, or None if insufficient resources.
    pub fn allocate_local_task_resources(
        &self,
        request: &ResourceSet,
    ) -> Option<TaskResourceInstances> {
        let mut resources = self.local_resources.write();
        let result = resources.available.try_allocate(request);
        if result.is_some() {
            self.bump_version();
        }
        result
    }

    /// Release resources from a completed task.
    pub fn release_worker_resources(&self, allocation: &TaskResourceInstances) {
        let mut resources = self.local_resources.write();
        resources.available.free(allocation);
        self.bump_version();
    }

    /// Get the current available resources as a flat ResourceSet.
    pub fn get_local_available_resources(&self) -> ResourceSet {
        self.local_resources.read().available.to_resource_set()
    }

    /// Get the total resources as a flat ResourceSet.
    pub fn get_local_total_resources(&self) -> ResourceSet {
        self.local_resources.read().total.to_resource_set()
    }

    /// Get node labels.
    pub fn get_labels(&self) -> HashMap<String, String> {
        self.local_resources.read().labels.clone()
    }

    /// Check if the local node can ever satisfy the request.
    pub fn is_local_node_feasible(&self, request: &ResourceSet) -> bool {
        let resources = self.local_resources.read();
        resources.total.to_resource_set().is_superset_of(request)
    }

    /// Check if the local node currently has enough resources.
    pub fn is_local_node_available(&self, request: &ResourceSet) -> bool {
        let resources = self.local_resources.read();
        resources
            .available
            .to_resource_set()
            .is_superset_of(request)
    }

    /// Set the local node as draining.
    pub fn set_local_node_draining(&self, deadline_ms: u64) {
        *self.is_draining.write() = true;
        *self.draining_deadline_ms.write() = deadline_ms;
        self.bump_version();
    }

    /// Check if the local node is draining.
    pub fn is_local_node_draining(&self) -> bool {
        *self.is_draining.read()
    }

    /// Get the draining deadline.
    pub fn draining_deadline_ms(&self) -> u64 {
        *self.draining_deadline_ms.read()
    }

    /// Add a new local resource or increase an existing one.
    pub fn add_local_resource_instances(&self, resource: String, instances: Vec<FixedPoint>) {
        let mut resources = self.local_resources.write();
        let total_instances = resources.total.get_instances(&resource).map(|s| s.to_vec());
        let avail_instances = resources
            .available
            .get_instances(&resource)
            .map(|s| s.to_vec());

        let mut new_total = total_instances.unwrap_or_default();
        let mut new_avail = avail_instances.unwrap_or_default();

        // Extend slots if needed
        while new_total.len() < instances.len() {
            new_total.push(FixedPoint::ZERO);
            new_avail.push(FixedPoint::ZERO);
        }

        for (i, amount) in instances.iter().enumerate() {
            new_total[i] += *amount;
            new_avail[i] += *amount;
        }

        resources.total.set_instances(resource.clone(), new_total);
        resources.available.set_instances(resource, new_avail);
        self.bump_version();
    }

    /// Delete a local resource.
    pub fn delete_local_resource(&self, resource: &str) {
        let mut resources = self.local_resources.write();
        resources.total.set_instances(resource.to_string(), vec![]);
        resources
            .available
            .set_instances(resource.to_string(), vec![]);
        self.bump_version();
    }

    /// Get the current version counter.
    pub fn version(&self) -> i64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Get the local node ID.
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Check if the local node is idle (no resources in use).
    pub fn is_local_node_idle(&self) -> bool {
        let resources = self.local_resources.read();
        let total = resources.total.to_resource_set();
        let avail = resources.available.to_resource_set();
        for (name, total_amount) in total.iter() {
            let avail_amount = avail.get(name);
            if avail_amount < total_amount {
                return false;
            }
        }
        true
    }

    fn bump_version(&self) {
        self.version.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_resources() -> ResourceSet {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        rs.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        rs.set("memory".to_string(), FixedPoint::from_f64(8192.0));
        rs
    }

    #[test]
    fn test_allocate_and_release() {
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), HashMap::new());

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        request.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let alloc = mgr.allocate_local_task_resources(&request).unwrap();
        let avail = mgr.get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(2.0));
        assert_eq!(avail.get("GPU"), FixedPoint::from_f64(1.0));

        mgr.release_worker_resources(&alloc);
        let avail = mgr.get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(avail.get("GPU"), FixedPoint::from_f64(2.0));
    }

    #[test]
    fn test_allocate_insufficient() {
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), HashMap::new());

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(5.0));

        assert!(mgr.allocate_local_task_resources(&request).is_none());
    }

    #[test]
    fn test_feasibility_check() {
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), HashMap::new());

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        assert!(mgr.is_local_node_feasible(&request));

        request.set("CPU".to_string(), FixedPoint::from_f64(5.0));
        assert!(!mgr.is_local_node_feasible(&request));
    }

    #[test]
    fn test_draining() {
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), HashMap::new());
        assert!(!mgr.is_local_node_draining());

        mgr.set_local_node_draining(1000);
        assert!(mgr.is_local_node_draining());
        assert_eq!(mgr.draining_deadline_ms(), 1000);
    }

    #[test]
    fn test_idle_check() {
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), HashMap::new());
        assert!(mgr.is_local_node_idle());

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let alloc = mgr.allocate_local_task_resources(&request).unwrap();
        assert!(!mgr.is_local_node_idle());

        mgr.release_worker_resources(&alloc);
        assert!(mgr.is_local_node_idle());
    }

    #[test]
    fn test_version_increments() {
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), HashMap::new());
        let v0 = mgr.version();

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let alloc = mgr.allocate_local_task_resources(&request).unwrap();
        assert!(mgr.version() > v0);

        let v1 = mgr.version();
        mgr.release_worker_resources(&alloc);
        assert!(mgr.version() > v1);
    }
}
