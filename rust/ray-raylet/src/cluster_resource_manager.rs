// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Cluster resource manager — maintains the cluster-wide resource view.
//!
//! Replaces `src/ray/raylet/scheduling/cluster_resource_manager.h/cc`.

use std::collections::HashMap;

use parking_lot::RwLock;
use ray_common::scheduling::ResourceSet;

use crate::scheduling_resources::{LabelSelector, NodeResources};

/// A node in the cluster resource view.
#[derive(Debug, Clone)]
pub struct Node {
    pub resources: NodeResources,
    pub last_update_time: u64,
}

/// The cluster resource manager maintains the resource view of all nodes.
pub struct ClusterResourceManager {
    /// All nodes in the cluster.
    nodes: RwLock<HashMap<String, Node>>,
}

impl ClusterResourceManager {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
        }
    }

    /// Add or update a node's resources.
    pub fn add_or_update_node(&self, node_id: String, resources: NodeResources) {
        let node = Node {
            resources,
            last_update_time: ray_util::time::current_time_ms(),
        };
        self.nodes.write().insert(node_id, node);
    }

    /// Remove a node (on failure or deregistration).
    pub fn remove_node(&self, node_id: &str) -> bool {
        self.nodes.write().remove(node_id).is_some()
    }

    /// Update a node's available resources (from syncer message).
    pub fn update_node_available(&self, node_id: &str, available: ResourceSet) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.resources.available = available;
            node.last_update_time = ray_util::time::current_time_ms();
        }
    }

    /// Subtract from a node's available resources (when scheduling a spillback).
    pub fn subtract_node_available_resources(&self, node_id: &str, request: &ResourceSet) -> bool {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            if node.resources.available.is_superset_of(request) {
                node.resources.available.subtract(request);
                return true;
            }
        }
        false
    }

    /// Add to a node's available resources (when restoring after dry-run).
    pub fn add_node_available_resources(&self, node_id: &str, resources: &ResourceSet) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.resources.available.add(resources);
        }
    }

    /// Check if a node has available resources for the request.
    pub fn has_available_resources(
        &self,
        node_id: &str,
        request: &ResourceSet,
        selector: &LabelSelector,
    ) -> bool {
        let nodes = self.nodes.read();
        if let Some(node) = nodes.get(node_id) {
            node.resources.is_available(request) && node.resources.has_required_labels(selector)
        } else {
            false
        }
    }

    /// Check if a node could ever satisfy the request (total >= request).
    pub fn has_feasible_resources(
        &self,
        node_id: &str,
        request: &ResourceSet,
        selector: &LabelSelector,
    ) -> bool {
        let nodes = self.nodes.read();
        if let Some(node) = nodes.get(node_id) {
            node.resources.is_feasible(request) && node.resources.has_required_labels(selector)
        } else {
            false
        }
    }

    /// Get node resources (cloned).
    pub fn get_node_resources(&self, node_id: &str) -> Option<NodeResources> {
        self.nodes.read().get(node_id).map(|n| n.resources.clone())
    }

    /// Get all node IDs.
    pub fn get_all_node_ids(&self) -> Vec<String> {
        self.nodes.read().keys().cloned().collect()
    }

    /// Get a snapshot of all nodes.
    pub fn get_resource_view(&self) -> HashMap<String, NodeResources> {
        self.nodes
            .read()
            .iter()
            .map(|(id, node)| (id.clone(), node.resources.clone()))
            .collect()
    }

    /// Number of nodes.
    pub fn num_nodes(&self) -> usize {
        self.nodes.read().len()
    }

    /// Set a node as draining.
    pub fn set_node_draining(&self, node_id: &str, deadline_ms: u64) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.resources.is_draining = true;
            node.resources.draining_deadline_ms = deadline_ms;
        }
    }

    /// Check if a node is alive (present in the view).
    pub fn is_node_alive(&self, node_id: &str) -> bool {
        self.nodes.read().contains_key(node_id)
    }

    /// Check if a node is draining.
    pub fn is_node_draining(&self, node_id: &str) -> bool {
        self.nodes
            .read()
            .get(node_id)
            .is_some_and(|n| n.resources.is_draining)
    }
}

impl Default for ClusterResourceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::scheduling::FixedPoint;

    fn make_node(cpus: f64, gpus: f64) -> NodeResources {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(cpus));
        if gpus > 0.0 {
            total.set("GPU".to_string(), FixedPoint::from_f64(gpus));
        }
        NodeResources::new(total)
    }

    #[test]
    fn test_add_and_remove_node() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n1".to_string(), make_node(4.0, 2.0));
        assert_eq!(mgr.num_nodes(), 1);
        assert!(mgr.is_node_alive("n1"));

        mgr.remove_node("n1");
        assert_eq!(mgr.num_nodes(), 0);
        assert!(!mgr.is_node_alive("n1"));
    }

    #[test]
    fn test_has_available_resources() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n1".to_string(), make_node(4.0, 2.0));

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let empty_selector = LabelSelector::new();

        assert!(mgr.has_available_resources("n1", &req, &empty_selector));

        req.set("CPU".to_string(), FixedPoint::from_f64(5.0));
        assert!(!mgr.has_available_resources("n1", &req, &empty_selector));
    }

    #[test]
    fn test_subtract_and_add_resources() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n1".to_string(), make_node(4.0, 0.0));

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        assert!(mgr.subtract_node_available_resources("n1", &req));

        let resources = mgr.get_node_resources("n1").unwrap();
        assert_eq!(resources.available.get("CPU"), FixedPoint::from_f64(2.0));

        mgr.add_node_available_resources("n1", &req);
        let resources = mgr.get_node_resources("n1").unwrap();
        assert_eq!(resources.available.get("CPU"), FixedPoint::from_f64(4.0));
    }

    #[test]
    fn test_draining() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n1".to_string(), make_node(4.0, 0.0));
        assert!(!mgr.is_node_draining("n1"));

        mgr.set_node_draining("n1", 5000);
        assert!(mgr.is_node_draining("n1"));
    }

    #[test]
    fn test_get_resource_view() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n1".to_string(), make_node(4.0, 2.0));
        mgr.add_or_update_node("n2".to_string(), make_node(8.0, 0.0));

        let view = mgr.get_resource_view();
        assert_eq!(view.len(), 2);
        assert!(view.contains_key("n1"));
        assert!(view.contains_key("n2"));
    }

    // ─── Ported from C++ cluster_resource_manager_test.cc ──────────

    /// Port of UpdateNode: update a node's resources and verify view reflects changes.
    #[test]
    fn test_update_node() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(1.0, 0.0));

        // Update with new resources
        let mut new_total = ResourceSet::new();
        new_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        let mut new_avail = new_total.clone();
        new_avail.set("CPU".to_string(), FixedPoint::from_f64(5.0));
        let mut updated = NodeResources::new(new_total);
        updated.available = new_avail;
        updated.labels.insert("zone".to_string(), "us-east-1a".to_string());
        updated.is_draining = true;
        updated.draining_deadline_ms = 123456;

        mgr.add_or_update_node("n0".to_string(), updated);

        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.total.get("CPU"), FixedPoint::from_f64(10.0));
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(5.0));
        assert_eq!(nr.labels.get("zone").unwrap(), "us-east-1a");
        assert!(nr.is_draining);
        assert_eq!(nr.draining_deadline_ms, 123456);
    }

    /// Port of DebugStringTest: verify node count in the resource view.
    #[test]
    fn test_debug_string() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(1.0, 0.0));
        mgr.add_or_update_node("n1".to_string(), make_node(2.0, 0.0));
        mgr.add_or_update_node("n2".to_string(), make_node(3.0, 0.0));

        assert_eq!(mgr.num_nodes(), 3);
        let view = mgr.get_resource_view();
        assert_eq!(view.len(), 3);
    }

    /// Port of HasFeasibleResourcesTest: verify feasibility checks.
    #[test]
    fn test_has_feasible_resources() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(1.0, 0.0));

        let empty_selector = LabelSelector::new();

        // Non-existent node
        assert!(!mgr.has_feasible_resources("n3", &ResourceSet::new(), &empty_selector));

        // GPU request on CPU-only node
        let mut gpu_req = ResourceSet::new();
        gpu_req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(!mgr.has_feasible_resources("n0", &gpu_req, &empty_selector));

        // CPU request on CPU node
        let mut cpu_req = ResourceSet::new();
        cpu_req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(mgr.has_feasible_resources("n0", &cpu_req, &empty_selector));

        // After subtracting all available, still feasible
        mgr.subtract_node_available_resources("n0", &cpu_req);
        assert!(mgr.has_feasible_resources("n0", &cpu_req, &empty_selector));
    }

    /// Port of HasAvailableResourcesTest.
    #[test]
    fn test_has_available_resources_extended() {
        let mgr = ClusterResourceManager::new();
        let empty_selector = LabelSelector::new();

        // n0: CPU=1
        mgr.add_or_update_node("n0".to_string(), make_node(1.0, 0.0));

        // n1: CUSTOM=1 (use GPU field for this test)
        let mut n1_total = ResourceSet::new();
        n1_total.set("CUSTOM".to_string(), FixedPoint::from_f64(1.0));
        mgr.add_or_update_node("n1".to_string(), NodeResources::new(n1_total));

        // Non-existent node
        assert!(!mgr.has_available_resources("n3", &ResourceSet::new(), &empty_selector));

        // n0 has CPU
        let mut cpu_req = ResourceSet::new();
        cpu_req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(mgr.has_available_resources("n0", &cpu_req, &empty_selector));

        // n0 does not have CUSTOM
        let mut custom_req = ResourceSet::new();
        custom_req.set("CUSTOM".to_string(), FixedPoint::from_f64(1.0));
        assert!(!mgr.has_available_resources("n0", &custom_req, &empty_selector));

        // n1 has CUSTOM
        assert!(mgr.has_available_resources("n1", &custom_req, &empty_selector));
    }

    /// Port of SubtractAndAddNodeAvailableResources: verify clamping behavior.
    #[test]
    fn test_subtract_and_add_resources_extended() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(1.0, 0.0));

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // First subtract succeeds
        assert!(mgr.subtract_node_available_resources("n0", &req));
        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(0.0));

        // Second subtract fails (not enough resources)
        assert!(!mgr.subtract_node_available_resources("n0", &req));
        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(0.0));

        // Add resources back
        mgr.add_node_available_resources("n0", &req);
        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(1.0));
    }

    /// Test update_node_available: update a node's available resources directly.
    #[test]
    fn test_update_node_available() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(4.0, 0.0));

        let mut new_avail = ResourceSet::new();
        new_avail.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        mgr.update_node_available("n0", new_avail);

        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(2.0));
        assert_eq!(nr.total.get("CPU"), FixedPoint::from_f64(4.0));
    }

    /// Test is_node_alive returns false for removed nodes.
    #[test]
    fn test_is_node_alive_after_removal() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(1.0, 0.0));
        assert!(mgr.is_node_alive("n0"));

        mgr.remove_node("n0");
        assert!(!mgr.is_node_alive("n0"));
        assert!(!mgr.is_node_alive("nonexistent"));
    }

    /// Test label-based feasibility.
    #[test]
    fn test_label_based_feasibility() {
        let mgr = ClusterResourceManager::new();

        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mut nr = NodeResources::new(total);
        nr.labels.insert("zone".to_string(), "us-east".to_string());
        mgr.add_or_update_node("n0".to_string(), nr);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Matching label
        let selector = LabelSelector {
            constraints: vec![crate::scheduling_resources::LabelConstraint {
                key: "zone".to_string(),
                operator: crate::scheduling_resources::LabelOperator::In,
                values: vec!["us-east".to_string()],
            }],
        };
        assert!(mgr.has_feasible_resources("n0", &req, &selector));

        // Non-matching label
        let selector2 = LabelSelector {
            constraints: vec![crate::scheduling_resources::LabelConstraint {
                key: "zone".to_string(),
                operator: crate::scheduling_resources::LabelOperator::In,
                values: vec!["eu-west".to_string()],
            }],
        };
        assert!(!mgr.has_feasible_resources("n0", &req, &selector2));
    }

    // ─── Additional ports from C++ cluster_resource_manager_test.cc ───

    /// Port of SubtractAndAddNodeAvailableResources with custom resources.
    #[test]
    fn test_subtract_add_custom_resources() {
        let mgr = ClusterResourceManager::new();
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        total.set("custom1".to_string(), FixedPoint::from_f64(8.0));
        mgr.add_or_update_node("n0".to_string(), NodeResources::new(total));

        let mut req = ResourceSet::new();
        req.set("custom1".to_string(), FixedPoint::from_f64(3.0));

        assert!(mgr.subtract_node_available_resources("n0", &req));
        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("custom1"), FixedPoint::from_f64(5.0));

        // Subtract from non-existent node
        assert!(!mgr.subtract_node_available_resources("nonexistent", &req));

        // Add back
        mgr.add_node_available_resources("n0", &req);
        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("custom1"), FixedPoint::from_f64(8.0));
    }

    /// Port of UpdateNodeNormalTaskResources: update_node_available with
    /// a new available snapshot.
    #[test]
    fn test_update_node_normal_task_resources() {
        let mgr = ClusterResourceManager::new();
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(4.0));
        mgr.add_or_update_node("n0".to_string(), NodeResources::new(total));

        // Update available to reflect some usage
        let mut new_avail = ResourceSet::new();
        new_avail.set("CPU".to_string(), FixedPoint::from_f64(6.0));
        new_avail.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        mgr.update_node_available("n0", new_avail);

        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(6.0));
        assert_eq!(nr.available.get("GPU"), FixedPoint::from_f64(2.0));
        assert_eq!(nr.total.get("CPU"), FixedPoint::from_f64(10.0));
        assert_eq!(nr.total.get("GPU"), FixedPoint::from_f64(4.0));
    }

    /// Port of multi-node add/remove/update sequence.
    #[test]
    fn test_multi_node_lifecycle() {
        let mgr = ClusterResourceManager::new();

        // Add 5 nodes
        for i in 0..5 {
            mgr.add_or_update_node(format!("n{}", i), make_node((i + 1) as f64, 0.0));
        }
        assert_eq!(mgr.num_nodes(), 5);

        // Remove 2
        mgr.remove_node("n1");
        mgr.remove_node("n3");
        assert_eq!(mgr.num_nodes(), 3);

        // Verify correct nodes remain
        assert!(mgr.is_node_alive("n0"));
        assert!(!mgr.is_node_alive("n1"));
        assert!(mgr.is_node_alive("n2"));
        assert!(!mgr.is_node_alive("n3"));
        assert!(mgr.is_node_alive("n4"));

        // Update n0 to have GPUs
        let mut updated = make_node(10.0, 4.0);
        updated.labels.insert("type".to_string(), "gpu".to_string());
        mgr.add_or_update_node("n0".to_string(), updated);

        let nr = mgr.get_node_resources("n0").unwrap();
        assert_eq!(nr.total.get("GPU"), FixedPoint::from_f64(4.0));
        assert_eq!(nr.labels.get("type").unwrap(), "gpu");
    }

    /// Port: get_node_resources for non-existent node returns None.
    #[test]
    fn test_get_node_resources_nonexistent() {
        let mgr = ClusterResourceManager::new();
        assert!(mgr.get_node_resources("nonexistent").is_none());
    }

    /// Port: draining state persists through updates.
    #[test]
    fn test_draining_persists() {
        let mgr = ClusterResourceManager::new();
        mgr.add_or_update_node("n0".to_string(), make_node(4.0, 0.0));
        mgr.set_node_draining("n0", 12345);
        assert!(mgr.is_node_draining("n0"));

        // Draining on non-existent node doesn't crash
        assert!(!mgr.is_node_draining("nonexistent"));
        mgr.set_node_draining("nonexistent", 999);
        assert!(!mgr.is_node_draining("nonexistent"));
    }

    /// Port: remove returns false for non-existent node.
    #[test]
    fn test_remove_nonexistent() {
        let mgr = ClusterResourceManager::new();
        assert!(!mgr.remove_node("nonexistent"));
    }
}
