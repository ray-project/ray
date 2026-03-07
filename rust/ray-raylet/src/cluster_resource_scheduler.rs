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
        // Hybrid scheduling picks among tied-score nodes nondeterministically.
        // Verify a valid schedulable node is returned.
        let valid_nodes = ["local", "remote"];
        assert!(
            node.as_ref()
                .map_or(false, |n| valid_nodes.contains(&n.as_str())),
            "scheduler should return a valid node, got {:?}",
            node
        );

        // With preferred_node_id and sufficient top-k, it should prefer local
        // (top-k must include the preferred node for the preference to kick in)
        let opts = SchedulingOptions {
            preferred_node_id: Some("local".to_string()),
            schedule_top_k_absolute: 10,
            ..SchedulingOptions::hybrid()
        };
        let node = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(
            node,
            Some("local".to_string()),
            "scheduler should prefer local node when preferred_node_id is set with sufficient top-k"
        );
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
                // BundleSpread should place bundles on different nodes
                assert_ne!(
                    assignments[0], assignments[1],
                    "BundleSpread should assign to different nodes, got both on {:?}",
                    assignments[0]
                );
            }
            _ => panic!("expected success"),
        }
    }

    /// Test scheduling with no available nodes: when no node has the required
    /// resources (request exceeds every node's capacity), get_best_schedulable_node
    /// should return None.
    #[test]
    fn test_scheduling_no_available_nodes() {
        // Create a scheduler with very limited resources on both nodes
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        // Request more CPUs than any node has
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(10.0));

        let node = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert!(
            node.is_none(),
            "Should return None when no node can satisfy the request, got {:?}",
            node
        );

        // Also test is_schedulable_on_node for each node
        let empty_selector = LabelSelector::new();
        assert!(!scheduler.is_schedulable_on_node("local", &req, &empty_selector));
        assert!(!scheduler.is_schedulable_on_node("remote", &req, &empty_selector));
    }

    // ─── Ported from C++ cluster_resource_scheduler_test.cc ─────────

    /// Port of SchedulingFixedPointTest: verify FixedPoint arithmetic.
    #[test]
    fn test_scheduling_fixed_point() {
        let fp1 = FixedPoint::from_f64(1.0);
        let fp2 = FixedPoint::from_f64(2.0);

        assert!(fp1 < fp2);
        assert!(fp2 > fp1);
        assert!(fp1 != fp2);
        assert!(fp1 == FixedPoint::from_f64(1.0));
        assert!((fp1 + fp2) == FixedPoint::from_f64(3.0));
        assert!((fp2 - fp1) == FixedPoint::from_f64(1.0));
        assert_eq!(fp1.to_f64(), 1.0);
    }

    /// Port of SchedulingIdTest: verify node IDs in the scheduler.
    #[test]
    fn test_scheduling_id() {
        let scheduler = make_scheduler();
        assert_eq!(scheduler.local_node_id(), "local");
        let node_ids = scheduler.cluster_resource_manager().get_all_node_ids();
        assert!(node_ids.contains(&"remote".to_string()));
    }

    /// Port of NodeAffinitySchedulingStrategyTest via the scheduler facade.
    #[test]
    fn test_node_affinity_scheduling_strategy() {
        let scheduler = make_scheduler();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard affinity to local
        let opts = SchedulingOptions::node_affinity("local".to_string(), false, false, false);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(result, Some("local".to_string()));

        // Hard affinity to remote
        let opts = SchedulingOptions::node_affinity("remote".to_string(), false, false, false);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(result, Some("remote".to_string()));

        // Hard affinity to nonexistent → None
        let opts = SchedulingOptions::node_affinity("nonexistent".to_string(), false, false, false);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert!(result.is_none());

        // Soft affinity to nonexistent → fallback
        let opts = SchedulingOptions::node_affinity("nonexistent".to_string(), true, true, false);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert!(result.is_some());
    }

    /// Port of SpreadSchedulingStrategyTest via the scheduler facade.
    #[test]
    fn test_spread_scheduling_strategy() {
        let scheduler = make_scheduler();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..20 {
            if let Some(id) =
                scheduler.get_best_schedulable_node(&req, &SchedulingOptions::spread())
            {
                seen.insert(id);
            }
        }
        assert!(seen.len() >= 2, "spread should visit both nodes");
    }

    /// Port of dynamic resource test: add resources then verify scheduling.
    #[test]
    fn test_dynamic_resources() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler =
            ClusterResourceScheduler::new("local".to_string(), local_mgr.clone(), cluster_mgr);

        // GPU request should be infeasible initially
        let mut gpu_req = ResourceSet::new();
        gpu_req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(!scheduler.is_feasible(&gpu_req));

        // Add GPU resources
        local_mgr.add_local_resource_instances("GPU".to_string(), vec![FixedPoint::from_f64(1.0)]);

        // Now GPU request should be feasible
        assert!(scheduler.is_feasible(&gpu_req));
    }

    /// Port of resource allocation test: allocate and release on local and remote.
    #[test]
    fn test_allocate_remote_task_resources() {
        let scheduler = make_scheduler();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        // Allocate remote resources
        assert!(scheduler.allocate_remote_task_resources("remote", &req));

        // Remote should have less available now
        let view = scheduler.get_full_resource_view();
        assert_eq!(
            view["remote"].available.get("CPU"),
            FixedPoint::from_f64(6.0)
        );

        // Allocate more than available should fail
        let mut big_req = ResourceSet::new();
        big_req.set("CPU".to_string(), FixedPoint::from_f64(100.0));
        assert!(!scheduler.allocate_remote_task_resources("remote", &big_req));
    }

    /// Port of label selector tests via the scheduler.
    #[test]
    fn test_label_selector_scheduling() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "us-east".to_string());

        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            labels,
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        // Remote node with different label
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        let mut remote_nr = NodeResources::new(remote_total);
        remote_nr
            .labels
            .insert("zone".to_string(), "us-west".to_string());
        cluster_mgr.add_or_update_node("remote".to_string(), remote_nr);

        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Label selector for us-east → only local matches
        let selector = crate::scheduling_resources::LabelSelector {
            constraints: vec![crate::scheduling_resources::LabelConstraint {
                key: "zone".to_string(),
                operator: crate::scheduling_resources::LabelOperator::In,
                values: vec!["us-east".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label(selector);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(result, Some("local".to_string()));
    }

    /// Port of fallback strategy test: when hybrid can't find a node,
    /// try a different strategy.
    #[test]
    fn test_fallback_strategy() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(0.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // No node has CPU → should return None
        let result = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert!(result.is_none());
    }

    /// Port of GCS-level scheduling tests: multi-node cluster.
    #[test]
    fn test_multi_node_scheduling() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        // Add multiple remote nodes
        for i in 0..5 {
            let mut rt = ResourceSet::new();
            rt.set("CPU".to_string(), FixedPoint::from_f64(8.0));
            cluster_mgr.add_or_update_node(format!("remote-{}", i), NodeResources::new(rt));
        }

        let scheduler =
            ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr.clone());

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Should be able to schedule
        let result = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert!(result.is_some());

        // Remove all remote nodes
        for i in 0..5 {
            cluster_mgr.remove_node(&format!("remote-{}", i));
        }

        // 6 CPU request should fail (local only has 4)
        let mut big_req = ResourceSet::new();
        big_req.set("CPU".to_string(), FixedPoint::from_f64(6.0));
        let result = scheduler.get_best_schedulable_node(&big_req, &SchedulingOptions::hybrid());
        assert!(result.is_none());
    }

    /// Port of full resource view test.
    #[test]
    fn test_get_full_resource_view() {
        let scheduler = make_scheduler();
        let view = scheduler.get_full_resource_view();
        assert_eq!(view.len(), 2); // local + remote
        assert!(view.contains_key("local"));
        assert!(view.contains_key("remote"));
        assert_eq!(view["local"].total.get("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(view["remote"].total.get("CPU"), FixedPoint::from_f64(8.0));
    }

    /// Test spillback behavior: when the local node is exhausted but a remote
    /// node has capacity, the scheduler should spill to the remote node.
    /// Also verifies that avoid_local_node forces spillback.
    #[test]
    fn test_spillback_exhausted_local() {
        let scheduler = make_scheduler();

        // Exhaust local CPUs (local has 4 total)
        let mut exhaust = ResourceSet::new();
        exhaust.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let _alloc = scheduler.allocate_local_task_resources(&exhaust).unwrap();

        // Now request 1 CPU — local is exhausted, should go to remote
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let node = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert_eq!(
            node,
            Some("remote".to_string()),
            "Should spillback to remote when local is exhausted"
        );

        // Also test explicit avoid_local_node option (even if local had capacity)
        let opts = SchedulingOptions {
            avoid_local_node: true,
            ..SchedulingOptions::hybrid()
        };
        // Release local resources so local would normally be schedulable
        scheduler.release_worker_resources(&_alloc);
        let node = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(
            node,
            Some("remote".to_string()),
            "Should prefer remote when avoid_local_node is set"
        );
    }

    // ─── Additional ports from C++ cluster_resource_scheduler_test.cc ──

    /// Port of SchedulingInitClusterTest: init multiple nodes, verify count.
    #[test]
    fn test_scheduling_init_cluster() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        for i in 0..10 {
            let mut rt = ResourceSet::new();
            rt.set("CPU".to_string(), FixedPoint::from_f64((i + 1) as f64));
            cluster_mgr.add_or_update_node(format!("n{}", i), NodeResources::new(rt));
        }
        let scheduler =
            ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr.clone());
        // 10 remote + local via get_full_resource_view
        let view = scheduler.get_full_resource_view();
        assert_eq!(view.len(), 11);
        assert_eq!(cluster_mgr.num_nodes(), 10);
    }

    /// Port of SchedulingDeleteClusterNodeTest: remove a node, verify count.
    #[test]
    fn test_scheduling_delete_cluster_node() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let _local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        for i in 0..4 {
            let mut rt = ResourceSet::new();
            rt.set("CPU".to_string(), FixedPoint::from_f64(2.0));
            cluster_mgr.add_or_update_node(format!("n{}", i), NodeResources::new(rt));
        }
        assert_eq!(cluster_mgr.num_nodes(), 4);

        cluster_mgr.remove_node("n2");
        assert_eq!(cluster_mgr.num_nodes(), 3);
    }

    /// Port of SchedulingModifyClusterNodeTest: update a node's resources.
    #[test]
    fn test_scheduling_modify_cluster_node() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let _local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        for i in 0..4 {
            let mut rt = ResourceSet::new();
            rt.set("CPU".to_string(), FixedPoint::from_f64(2.0));
            cluster_mgr.add_or_update_node(format!("n{}", i), NodeResources::new(rt));
        }
        assert_eq!(cluster_mgr.num_nodes(), 4);

        // Update n2 with more resources
        let mut new_total = ResourceSet::new();
        new_total.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        cluster_mgr.add_or_update_node("n2".to_string(), NodeResources::new(new_total));
        assert_eq!(cluster_mgr.num_nodes(), 4); // same count, updated node

        let nr = cluster_mgr.get_node_resources("n2").unwrap();
        assert_eq!(nr.total.get("CPU"), FixedPoint::from_f64(20.0));
    }

    /// Port of SchedulingResourceRequestTest: hard constraint violations.
    #[test]
    fn test_scheduling_resource_request_violations() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(5.0));
        total.set("memory".to_string(), FixedPoint::from_f64(5.0));
        total.set("custom1".to_string(), FixedPoint::from_f64(10.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        // Remote node with different resources
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        remote_total.set("memory".to_string(), FixedPoint::from_f64(2.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(3.0));
        remote_total.set("custom1".to_string(), FixedPoint::from_f64(5.0));
        remote_total.set("custom2".to_string(), FixedPoint::from_f64(5.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);
        let opts = SchedulingOptions::hybrid();

        // CPU=11 exceeds everyone
        let mut req_infeasible = ResourceSet::new();
        req_infeasible.set("CPU".to_string(), FixedPoint::from_f64(11.0));
        assert!(scheduler
            .get_best_schedulable_node(&req_infeasible, &opts)
            .is_none());

        // CPU=5 feasible on both
        let mut req_ok = ResourceSet::new();
        req_ok.set("CPU".to_string(), FixedPoint::from_f64(5.0));
        assert!(scheduler
            .get_best_schedulable_node(&req_ok, &opts)
            .is_some());

        // custom100 missing resource → infeasible
        let mut req_missing = ResourceSet::new();
        req_missing.set("CPU".to_string(), FixedPoint::from_f64(5.0));
        req_missing.set("memory".to_string(), FixedPoint::from_f64(2.0));
        req_missing.set("custom100".to_string(), FixedPoint::from_f64(5.0));
        assert!(scheduler
            .get_best_schedulable_node(&req_missing, &opts)
            .is_none());
    }

    /// Port of SchedulingUpdateAvailableResourcesTest: allocate local, verify view.
    #[test]
    fn test_scheduling_update_available_resources() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        total.set("memory".to_string(), FixedPoint::from_f64(5.0));
        total.set("custom1".to_string(), FixedPoint::from_f64(5.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(7.0));
        req.set("memory".to_string(), FixedPoint::from_f64(5.0));
        req.set("custom1".to_string(), FixedPoint::from_f64(3.0));

        let alloc = scheduler.allocate_local_task_resources(&req).unwrap();
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(3.0));
        assert_eq!(avail.get("memory"), FixedPoint::from_f64(0.0));
        assert_eq!(avail.get("custom1"), FixedPoint::from_f64(2.0));

        scheduler.release_worker_resources(&alloc);
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("CPU"), FixedPoint::from_f64(10.0));
    }

    /// Port of TaskResourceInstancesAllocationFailureTest: no leak when
    /// allocation fails due to missing resource.
    #[test]
    fn test_task_resource_instances_allocation_failure() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        total.set("custom1".to_string(), FixedPoint::from_f64(4.0));
        total.set("custom3".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        // Request custom5 which doesn't exist → should fail
        let mut req = ResourceSet::new();
        req.set("custom1".to_string(), FixedPoint::from_f64(3.0));
        req.set("custom3".to_string(), FixedPoint::from_f64(3.0));
        req.set("custom5".to_string(), FixedPoint::from_f64(4.0));

        assert!(scheduler.allocate_local_task_resources(&req).is_none());

        // Verify no resources leaked
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("custom1"), FixedPoint::from_f64(4.0));
        assert_eq!(avail.get("custom3"), FixedPoint::from_f64(4.0));
    }

    /// Port of SchedulingUpdateTotalResourcesTest: add resource instances.
    #[test]
    fn test_scheduling_update_total_resources() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        total.set("custom1".to_string(), FixedPoint::from_f64(1.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler =
            ClusterResourceScheduler::new("local".to_string(), local_mgr.clone(), cluster_mgr);

        // Add more CPU instances: [0, 1, 1]
        local_mgr.add_local_resource_instances(
            "CPU".to_string(),
            vec![
                FixedPoint::ZERO,
                FixedPoint::from_f64(1.0),
                FixedPoint::from_f64(1.0),
            ],
        );
        local_mgr.add_local_resource_instances(
            "custom1".to_string(),
            vec![
                FixedPoint::ZERO,
                FixedPoint::from_f64(1.0),
                FixedPoint::from_f64(1.0),
            ],
        );

        let total = scheduler
            .local_resource_manager()
            .get_local_total_resources();
        assert_eq!(total.get("CPU"), FixedPoint::from_f64(3.0));
        assert_eq!(total.get("custom1"), FixedPoint::from_f64(3.0));
    }

    /// Port of DynamicResourceTest: add and delete resources.
    #[test]
    fn test_dynamic_resource_add_delete() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler =
            ClusterResourceScheduler::new("local".to_string(), local_mgr.clone(), cluster_mgr);

        // No custom resource initially
        let mut custom_req = ResourceSet::new();
        custom_req.set("custom1".to_string(), FixedPoint::from_f64(1.0));
        assert!(!scheduler.is_feasible(&custom_req));

        // Add custom1
        local_mgr
            .add_local_resource_instances("custom1".to_string(), vec![FixedPoint::from_f64(5.0)]);
        assert!(scheduler.is_feasible(&custom_req));

        // Delete custom1
        local_mgr.delete_local_resource("custom1");
        assert!(!scheduler.is_feasible(&custom_req));
    }

    /// Port of AvailableResourceEmptyTest: scheduling with zero resources.
    #[test]
    fn test_available_resource_empty() {
        let total = ResourceSet::new(); // zero resources
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let result = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert!(result.is_none());
    }

    /// Port of TestForceSpillback: avoid_local_node picks feasible remote.
    #[test]
    fn test_force_spillback() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(10.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            avoid_local_node: true,
            ..SchedulingOptions::hybrid()
        };
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(result, Some("remote".to_string()));
    }

    /// Port of TestAlwaysSpillInfeasibleTask: infeasible on local,
    /// feasible on remote, should spill to remote.
    #[test]
    fn test_always_spill_infeasible_task() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        // No GPU locally
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let result = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert_eq!(result, Some("remote".to_string()));
    }

    /// Port of CustomResourceInstanceTest: scheduling with custom resources.
    #[test]
    fn test_custom_resource_instance() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        total.set("custom1".to_string(), FixedPoint::from_f64(8.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("custom1".to_string(), FixedPoint::from_f64(3.0));

        let alloc = scheduler.allocate_local_task_resources(&req).unwrap();
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("custom1"), FixedPoint::from_f64(5.0));

        scheduler.release_worker_resources(&alloc);
        let avail = scheduler
            .local_resource_manager()
            .get_local_available_resources();
        assert_eq!(avail.get("custom1"), FixedPoint::from_f64(8.0));
    }

    /// Port of DirtyLocalViewTest: local allocation updates the scheduling
    /// view, so a second request sees reduced availability.
    #[test]
    fn test_dirty_local_view() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        // Allocate 3 CPUs
        let mut req1 = ResourceSet::new();
        req1.set("CPU".to_string(), FixedPoint::from_f64(3.0));
        let _alloc = scheduler.allocate_local_task_resources(&req1).unwrap();

        // Now request 2 CPUs — only 1 remaining, should fail
        let mut req2 = ResourceSet::new();
        req2.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        assert!(scheduler.allocate_local_task_resources(&req2).is_none());
    }

    /// Port of AffinityWithBundleScheduleTest: node affinity scheduling
    /// to a specific remote node.
    #[test]
    fn test_affinity_with_bundle_schedule() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler = ClusterResourceScheduler::new("local".to_string(), local_mgr, cluster_mgr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard affinity to remote
        let opts = SchedulingOptions::node_affinity("remote".to_string(), false, false, false);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(result, Some("remote".to_string()));

        // Hard affinity to local
        let opts = SchedulingOptions::node_affinity("local".to_string(), false, false, false);
        let result = scheduler.get_best_schedulable_node(&req, &opts);
        assert_eq!(result, Some("local".to_string()));
    }

    /// Port of draining local node test: draining node should not be scheduled.
    #[test]
    fn test_draining_local_node_not_scheduled() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler =
            ClusterResourceScheduler::new("local".to_string(), local_mgr.clone(), cluster_mgr);

        // Drain the local node
        local_mgr.set_local_node_draining(i64::MAX as u64);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hybrid scheduling should skip draining local and go to remote
        let result = scheduler.get_best_schedulable_node(&req, &SchedulingOptions::hybrid());
        assert_eq!(result, Some("remote".to_string()));
    }
}
