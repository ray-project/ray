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
}
