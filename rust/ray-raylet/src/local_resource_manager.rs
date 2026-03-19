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

    /// Resize a local resource to a new total capacity.
    ///
    /// Adjusts total and available for the named resource. If the new total is
    /// less than the current total, available is clamped to non-negative.
    /// Returns true if the resource existed and was resized, false if the
    /// resource does not exist on this node.
    pub fn resize_local_resource(&self, resource: &str, new_total: FixedPoint) -> bool {
        let new_total = if new_total < FixedPoint::ZERO {
            FixedPoint::ZERO
        } else {
            new_total
        };

        let mut resources = self.local_resources.write();
        let old_total_instances = match resources.total.get_instances(resource) {
            Some(s) if !s.is_empty() => s.to_vec(),
            _ => return false,
        };

        let old_total: FixedPoint = old_total_instances.iter().copied().fold(FixedPoint::ZERO, |a, b| a + b);
        let old_avail_instances = resources
            .available
            .get_instances(resource)
            .map(|s| s.to_vec())
            .unwrap_or_default();
        let old_avail: FixedPoint = old_avail_instances.iter().copied().fold(FixedPoint::ZERO, |a, b| a + b);

        // Compute new available: available + (new_total - old_total), clamped to [0, new_total]
        let delta = new_total - old_total;
        let mut new_avail = old_avail + delta;
        if new_avail < FixedPoint::ZERO {
            new_avail = FixedPoint::ZERO;
        }
        if new_avail > new_total {
            new_avail = new_total;
        }

        resources.total.set_instances(resource.to_string(), vec![new_total]);
        resources.available.set_instances(resource.to_string(), vec![new_avail]);
        self.bump_version();
        true
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

    // ─── Ported from C++ local_resource_manager_test.cc ──────────────

    /// Port of NodeDrainingTest: verify draining state after setting.
    #[test]
    fn test_node_draining_with_resources() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // Allocate some resources (node is not idle)
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let _alloc = mgr.allocate_local_task_resources(&req).unwrap();

        mgr.set_local_node_draining(i64::MAX as u64);
        assert!(mgr.is_local_node_draining());
        assert!(!mgr.is_local_node_idle());
    }

    /// Port of ObjectStoreMemoryDrainingTest: draining with object store memory.
    #[test]
    fn test_object_store_memory_draining() {
        let mut total = ResourceSet::new();
        total.set(
            "object_store_memory".to_string(),
            FixedPoint::from_f64(100.0),
        );
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // Initially idle
        assert!(mgr.is_local_node_idle());

        // Allocate some object store memory
        let mut req = ResourceSet::new();
        req.set(
            "object_store_memory".to_string(),
            FixedPoint::from_f64(50.0),
        );
        let alloc = mgr.allocate_local_task_resources(&req).unwrap();
        assert!(!mgr.is_local_node_idle());

        mgr.set_local_node_draining(i64::MAX as u64);
        assert!(mgr.is_local_node_draining());

        // Release resources — node becomes idle
        mgr.release_worker_resources(&alloc);
        assert!(mgr.is_local_node_idle());
    }

    /// Port of IdleResourceTimeTest: verify idle state tracks allocations.
    #[test]
    fn test_idle_resource_time() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        total.set("CUSTOM".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // Initially idle
        assert!(mgr.is_local_node_idle());

        // Allocate CPU + CUSTOM → not idle
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("CUSTOM".to_string(), FixedPoint::from_f64(1.0));
        let _alloc1 = mgr.allocate_local_task_resources(&req).unwrap();
        assert!(!mgr.is_local_node_idle());

        // Release only CPU — still not idle (CUSTOM still allocated)
        let mut cpu_only = crate::scheduling_resources::TaskResourceInstances::new();
        cpu_only
            .resources
            .insert("CPU".to_string(), vec![FixedPoint::from_f64(1.0)]);
        mgr.release_worker_resources(&cpu_only);
        assert!(!mgr.is_local_node_idle());

        // Release CUSTOM — now idle
        let mut custom_only = crate::scheduling_resources::TaskResourceInstances::new();
        custom_only
            .resources
            .insert("CUSTOM".to_string(), vec![FixedPoint::from_f64(1.0)]);
        mgr.release_worker_resources(&custom_only);
        assert!(mgr.is_local_node_idle());

        // Allocate again, release all at once
        let alloc2 = mgr.allocate_local_task_resources(&req).unwrap();
        assert!(!mgr.is_local_node_idle());
        mgr.release_worker_resources(&alloc2);
        assert!(mgr.is_local_node_idle());
    }

    /// Port of CreateSyncMessageNegativeResourceAvailability: verify that
    /// available resources never go below zero in the reported view.
    #[test]
    fn test_available_never_negative() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // Allocate all resources
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let _alloc = mgr.allocate_local_task_resources(&req).unwrap();

        // Available should be exactly 0, not negative
        let avail = mgr.get_local_available_resources();
        assert!(avail.get("CPU") >= FixedPoint::ZERO);
    }

    /// Test add_local_resource_instances and delete_local_resource.
    #[test]
    fn test_add_and_delete_local_resources() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // Add GPU instances
        mgr.add_local_resource_instances(
            "GPU".to_string(),
            vec![FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0)],
        );

        let total = mgr.get_local_total_resources();
        assert_eq!(total.get("GPU"), FixedPoint::from_f64(2.0));
        let avail = mgr.get_local_available_resources();
        assert_eq!(avail.get("GPU"), FixedPoint::from_f64(2.0));

        // Delete GPU
        mgr.delete_local_resource("GPU");
        let total = mgr.get_local_total_resources();
        assert_eq!(total.get("GPU"), FixedPoint::ZERO);
    }

    /// Test labels.
    #[test]
    fn test_labels() {
        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "us-east".to_string());
        labels.insert("tier".to_string(), "prod".to_string());
        let mgr = LocalResourceManager::new("node1".to_string(), make_resources(), labels);

        let retrieved = mgr.get_labels();
        assert_eq!(retrieved.get("zone").unwrap(), "us-east");
        assert_eq!(retrieved.get("tier").unwrap(), "prod");
    }

    /// Test local_node_id accessor.
    #[test]
    fn test_local_node_id() {
        let mgr =
            LocalResourceManager::new("test-node".to_string(), make_resources(), HashMap::new());
        assert_eq!(mgr.local_node_id(), "test-node");
    }

    /// Test multiple allocations exhaust resources.
    #[test]
    fn test_multiple_allocations_exhaust() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let alloc1 = mgr.allocate_local_task_resources(&req).unwrap();
        let alloc2 = mgr.allocate_local_task_resources(&req).unwrap();

        // Should be fully exhausted now
        assert!(!mgr.is_local_node_available(&req));
        assert!(mgr.allocate_local_task_resources(&req).is_none());

        // Release one
        mgr.release_worker_resources(&alloc1);
        assert!(mgr.is_local_node_available(&req));

        mgr.release_worker_resources(&alloc2);
        assert!(mgr.is_local_node_idle());
    }

    // ─── Additional ports from C++ local_resource_manager_test.cc ─────

    /// Port of BasicGetResourceUsageMapTest: resource usage reporting.
    /// Verifies that available and total resources track correctly.
    #[test]
    fn test_basic_resource_usage_reporting() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        total.set("CUSTOM".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // No allocation: available == total
        let avail = mgr.get_local_available_resources();
        let total = mgr.get_local_total_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(8.0));
        assert_eq!(total.get("CPU"), FixedPoint::from_f64(8.0));
        assert_eq!(avail.get("GPU"), FixedPoint::from_f64(2.0));
        assert_eq!(avail.get("CUSTOM"), FixedPoint::from_f64(4.0));

        // After allocation
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(0.5));
        req.set("CUSTOM".to_string(), FixedPoint::from_f64(2.0));
        let alloc = mgr.allocate_local_task_resources(&req).unwrap();

        let avail = mgr.get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(7.0));
        assert_eq!(avail.get("GPU"), FixedPoint::from_f64(1.5));
        assert_eq!(avail.get("CUSTOM"), FixedPoint::from_f64(2.0));

        // Total unchanged
        let total = mgr.get_local_total_resources();
        assert_eq!(total.get("CPU"), FixedPoint::from_f64(8.0));

        mgr.release_worker_resources(&alloc);
    }

    /// Port of PopulateResourceViewSyncMessage: version increments on
    /// every resource change.
    #[test]
    fn test_version_on_draining() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        let v0 = mgr.version();
        mgr.set_local_node_draining(1000);
        let v1 = mgr.version();
        assert!(v1 > v0, "version should increment on draining");
    }

    /// Port of MaybeMarkFootprintAsBusyPreservesIdleTime: verifying
    /// idle state after allocate + release cycle.
    #[test]
    fn test_idle_preserved_after_release() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        assert!(mgr.is_local_node_idle());

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let alloc = mgr.allocate_local_task_resources(&req).unwrap();
        assert!(!mgr.is_local_node_idle());

        mgr.release_worker_resources(&alloc);
        assert!(mgr.is_local_node_idle());
    }

    /// Port: feasibility does not change with availability.
    #[test]
    fn test_feasibility_independent_of_availability() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        // Feasible before and after allocation
        assert!(mgr.is_local_node_feasible(&req));
        assert!(mgr.is_local_node_available(&req));

        let alloc = mgr.allocate_local_task_resources(&req).unwrap();
        assert!(mgr.is_local_node_feasible(&req)); // still feasible
        assert!(!mgr.is_local_node_available(&req)); // but not available

        mgr.release_worker_resources(&alloc);
        assert!(mgr.is_local_node_available(&req));
    }

    /// Port: version increments on add/delete resource operations.
    #[test]
    fn test_version_on_add_delete_resource() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        let v0 = mgr.version();
        mgr.add_local_resource_instances("GPU".to_string(), vec![FixedPoint::from_f64(1.0)]);
        let v1 = mgr.version();
        assert!(v1 > v0);

        mgr.delete_local_resource("GPU");
        let v2 = mgr.version();
        assert!(v2 > v1);
    }

    /// Port: resource operations on non-existent resource types.
    #[test]
    fn test_allocate_nonexistent_resource_fails() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        // Request a resource type that doesn't exist
        let mut req = ResourceSet::new();
        req.set("TPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(mgr.allocate_local_task_resources(&req).is_none());
    }

    /// Port: draining deadline persistence.
    #[test]
    fn test_draining_deadline() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mgr = LocalResourceManager::new("node1".to_string(), total, HashMap::new());

        assert_eq!(mgr.draining_deadline_ms(), 0);
        assert!(!mgr.is_local_node_draining());

        mgr.set_local_node_draining(99999);
        assert!(mgr.is_local_node_draining());
        assert_eq!(mgr.draining_deadline_ms(), 99999);
    }
}
