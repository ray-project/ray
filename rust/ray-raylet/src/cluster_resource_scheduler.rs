// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Cluster resource scheduler — the main scheduling facade.
//!
//! Replaces `src/ray/raylet/scheduling/cluster_resource_scheduler.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use ray_common::scheduling::ResourceSet;

use crate::cluster_resource_manager::ClusterResourceManager;
use crate::local_resource_manager::LocalResourceManager;
use crate::scheduling_policy::{
    BundleSchedulingResult, CompositeBundleSchedulingPolicy, CompositeSchedulingPolicy,
    SchedulingPolicy,
};
use crate::scheduling_resources::{
    LabelSelector, NodeResources, SchedulingOptions, TaskResourceInstances,
};

/// The main scheduler facade combining local resource management,
/// cluster resource view, and scheduling policies.
pub struct ClusterResourceScheduler {
    local_node_id: String,
    local_resource_manager: Arc<LocalResourceManager>,
    cluster_resource_manager: Arc<ClusterResourceManager>,
    scheduling_policy: CompositeSchedulingPolicy,
    bundle_scheduling_policy: CompositeBundleSchedulingPolicy,
}

impl ClusterResourceScheduler {
    pub fn new(
        local_node_id: String,
        local_resource_manager: Arc<LocalResourceManager>,
        cluster_resource_manager: Arc<ClusterResourceManager>,
    ) -> Self {
        Self {
            local_node_id,
            local_resource_manager,
            cluster_resource_manager,
            scheduling_policy: CompositeSchedulingPolicy::new(),
            bundle_scheduling_policy: CompositeBundleSchedulingPolicy,
        }
    }

    /// Get the best node for a single task.
    pub fn get_best_schedulable_node(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
    ) -> Option<String> {
        let mut view = self.cluster_resource_manager.get_resource_view();

        // Include local node in the view
        let local_resources = NodeResources {
            total: self.local_resource_manager.get_local_total_resources(),
            available: self.local_resource_manager.get_local_available_resources(),
            load: ResourceSet::new(),
            labels: self.local_resource_manager.get_labels(),
            is_draining: self.local_resource_manager.is_local_node_draining(),
            draining_deadline_ms: self.local_resource_manager.draining_deadline_ms(),
        };
        view.insert(self.local_node_id.clone(), local_resources);

        self.scheduling_policy
            .schedule(request, options, &view, &self.local_node_id)
    }

    /// Schedule a set of bundles for a placement group.
    pub fn schedule_bundles(
        &self,
        requests: &[&ResourceSet],
        options: &SchedulingOptions,
    ) -> BundleSchedulingResult {
        let mut view = self.cluster_resource_manager.get_resource_view();

        let local_resources = NodeResources {
            total: self.local_resource_manager.get_local_total_resources(),
            available: self.local_resource_manager.get_local_available_resources(),
            load: ResourceSet::new(),
            labels: self.local_resource_manager.get_labels(),
            is_draining: self.local_resource_manager.is_local_node_draining(),
            draining_deadline_ms: self.local_resource_manager.draining_deadline_ms(),
        };
        view.insert(self.local_node_id.clone(), local_resources);

        self.bundle_scheduling_policy
            .schedule(requests, options, &view)
    }

    /// Allocate resources on a remote node (update local view).
    pub fn allocate_remote_task_resources(&self, node_id: &str, request: &ResourceSet) -> bool {
        self.cluster_resource_manager
            .subtract_node_available_resources(node_id, request)
    }

    /// Allocate resources on the local node.
    pub fn allocate_local_task_resources(
        &self,
        request: &ResourceSet,
    ) -> Option<TaskResourceInstances> {
        self.local_resource_manager
            .allocate_local_task_resources(request)
    }

    /// Release local task resources.
    pub fn release_worker_resources(&self, allocation: &TaskResourceInstances) {
        self.local_resource_manager
            .release_worker_resources(allocation);
    }

    /// Check if a node can schedule the request.
    pub fn is_schedulable_on_node(
        &self,
        node_id: &str,
        request: &ResourceSet,
        selector: &LabelSelector,
    ) -> bool {
        if node_id == self.local_node_id {
            return self.local_resource_manager.is_local_node_available(request);
        }
        self.cluster_resource_manager
            .has_available_resources(node_id, request, selector)
    }

    /// Check if a request is feasible on any node.
    pub fn is_feasible(&self, request: &ResourceSet) -> bool {
        if self.local_resource_manager.is_local_node_feasible(request) {
            return true;
        }
        for node_id in self.cluster_resource_manager.get_all_node_ids() {
            let empty_selector = LabelSelector::new();
            if self.cluster_resource_manager.has_feasible_resources(
                &node_id,
                request,
                &empty_selector,
            ) {
                return true;
            }
        }
        false
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn local_resource_manager(&self) -> &Arc<LocalResourceManager> {
        &self.local_resource_manager
    }

    pub fn cluster_resource_manager(&self) -> &Arc<ClusterResourceManager> {
        &self.cluster_resource_manager
    }

    /// Get a combined resource view including the local node.
    pub fn get_full_resource_view(&self) -> HashMap<String, NodeResources> {
        let mut view = self.cluster_resource_manager.get_resource_view();
        let local_resources = NodeResources {
            total: self.local_resource_manager.get_local_total_resources(),
            available: self.local_resource_manager.get_local_available_resources(),
            load: ResourceSet::new(),
            labels: self.local_resource_manager.get_labels(),
            is_draining: self.local_resource_manager.is_local_node_draining(),
            draining_deadline_ms: self.local_resource_manager.draining_deadline_ms(),
        };
        view.insert(self.local_node_id.clone(), local_resources);
        view
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::scheduling::FixedPoint;

    fn make_scheduler() -> ClusterResourceScheduler {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(2.0));

        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        // Add a remote node
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr)
    }

    #[test]
    fn test_schedule_on_local() {
        let scheduler = make_scheduler();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let node = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert!(node.is_some());
    }

    #[test]
    fn test_schedule_spillback_to_remote() {
        let scheduler = make_scheduler();

        // Request more CPUs than local has (4), but remote has 8
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(6.0));

        let node = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert_eq!(node, Some("remote".to_string()));
    }

    #[test]
    fn test_allocate_and_release_local() {
        let scheduler = make_scheduler();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let alloc = scheduler.allocate_local_task_resources(&req).unwrap();
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(2.0));

        scheduler.release_worker_resources(&alloc);
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(4.0));
    }

    #[test]
    fn test_is_feasible() {
        let scheduler = make_scheduler();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(6.0));
        assert!(scheduler.is_feasible(&req)); // remote has 8

        req.set("CPU".to_string(), FixedPoint::from_f64(100.0));
        assert!(!scheduler.is_feasible(&req)); // nobody has 100
    }

    #[test]
    fn test_bundle_scheduling() {
        let scheduler = make_scheduler();

        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let mut r2 = ResourceSet::new();
        r2.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let requests: Vec<&ResourceSet> = vec![&r1, &r2];
        let opts = SchedulingOptions {
            scheduling_type: crate::scheduling_resources::SchedulingType::BundleSpread,
            ..SchedulingOptions::default()
        };

        let result = scheduler.schedule_bundles(&requests, &opts);
        match result {
            BundleSchedulingResult::Success(assignments) => {
                assert_eq!(assignments.len(), 2);
            }
            _ => panic!("expected success"),
        }
    }
}
