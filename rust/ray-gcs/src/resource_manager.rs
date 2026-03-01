// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Resource Manager — tracks per-node resource availability.
//!
//! Replaces `src/ray/gcs/gcs_resource_manager.h/cc`.

use std::collections::HashMap;

use parking_lot::RwLock;
use ray_common::id::NodeID;

/// Per-node resource usage data.
#[derive(Debug, Clone, Default)]
pub struct NodeResourceUsage {
    /// Total resources on the node.
    pub total_resources: HashMap<String, f64>,
    /// Available (unused) resources.
    pub available_resources: HashMap<String, f64>,
    /// Resource load (demand).
    pub resource_load: HashMap<String, f64>,
}

/// The GCS resource manager tracks resources across all nodes.
pub struct GcsResourceManager {
    /// Per-node resource usage.
    node_resources: RwLock<HashMap<NodeID, NodeResourceUsage>>,
    /// Draining nodes: node_id → (is_draining, deadline_ms).
    draining_nodes: RwLock<HashMap<NodeID, (bool, i64)>>,
    /// Number of alive nodes.
    num_alive_nodes: std::sync::atomic::AtomicUsize,
}

impl GcsResourceManager {
    pub fn new() -> Self {
        Self {
            node_resources: RwLock::new(HashMap::new()),
            draining_nodes: RwLock::new(HashMap::new()),
            num_alive_nodes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Handle node registration.
    pub fn on_node_add(&self, node_id: &NodeID) {
        self.node_resources.write().entry(*node_id).or_default();
        self.num_alive_nodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Handle node death.
    pub fn on_node_dead(&self, node_id: &NodeID) {
        self.node_resources.write().remove(node_id);
        self.draining_nodes.write().remove(node_id);
        self.num_alive_nodes
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update resource usage for a node.
    pub fn update_resource_usage(&self, node_id: &NodeID, usage: NodeResourceUsage) {
        self.node_resources.write().insert(*node_id, usage);
    }

    /// Set node draining state.
    pub fn set_node_draining(&self, node_id: &NodeID, is_draining: bool, deadline_ms: i64) {
        if is_draining {
            self.draining_nodes
                .write()
                .insert(*node_id, (true, deadline_ms));
        } else {
            self.draining_nodes.write().remove(node_id);
        }
    }

    /// Handle GetAllAvailableResources RPC.
    pub fn handle_get_all_available_resources(&self) -> Vec<(NodeID, HashMap<String, f64>)> {
        let resources = self.node_resources.read();
        resources
            .iter()
            .map(|(nid, usage)| (*nid, usage.available_resources.clone()))
            .collect()
    }

    /// Handle GetAllTotalResources RPC.
    pub fn handle_get_all_total_resources(&self) -> Vec<(NodeID, HashMap<String, f64>)> {
        let resources = self.node_resources.read();
        resources
            .iter()
            .map(|(nid, usage)| (*nid, usage.total_resources.clone()))
            .collect()
    }

    /// Handle GetDrainingNodes RPC.
    pub fn handle_get_draining_nodes(&self) -> Vec<NodeID> {
        self.draining_nodes.read().keys().copied().collect()
    }

    /// Get all resource usage (for autoscaler).
    pub fn get_all_resource_usage(&self) -> HashMap<NodeID, NodeResourceUsage> {
        self.node_resources.read().clone()
    }

    pub fn num_alive_nodes(&self) -> usize {
        self.num_alive_nodes
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for GcsResourceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(v: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = v;
        NodeID::from_binary(&data)
    }

    #[test]
    fn test_resource_tracking() {
        let mgr = GcsResourceManager::new();
        let nid = node_id(1);

        mgr.on_node_add(&nid);
        assert_eq!(mgr.num_alive_nodes(), 1);

        let usage = NodeResourceUsage {
            total_resources: HashMap::from([("CPU".into(), 8.0)]),
            available_resources: HashMap::from([("CPU".into(), 4.0)]),
            ..Default::default()
        };
        mgr.update_resource_usage(&nid, usage);

        let available = mgr.handle_get_all_available_resources();
        assert_eq!(available.len(), 1);
        assert_eq!(available[0].1.get("CPU"), Some(&4.0));

        mgr.on_node_dead(&nid);
        assert_eq!(mgr.num_alive_nodes(), 0);
    }

    #[test]
    fn test_draining_nodes() {
        let mgr = GcsResourceManager::new();
        let nid = node_id(1);

        mgr.on_node_add(&nid);
        mgr.set_node_draining(&nid, true, 5000);
        assert_eq!(mgr.handle_get_draining_nodes().len(), 1);

        mgr.set_node_draining(&nid, false, 0);
        assert_eq!(mgr.handle_get_draining_nodes().len(), 0);
    }

    #[test]
    fn test_multiple_nodes() {
        let mgr = GcsResourceManager::new();
        let n1 = node_id(1);
        let n2 = node_id(2);
        let n3 = node_id(3);

        mgr.on_node_add(&n1);
        mgr.on_node_add(&n2);
        mgr.on_node_add(&n3);
        assert_eq!(mgr.num_alive_nodes(), 3);

        mgr.on_node_dead(&n2);
        assert_eq!(mgr.num_alive_nodes(), 2);
        assert_eq!(mgr.handle_get_all_available_resources().len(), 2);
    }

    #[test]
    fn test_node_dead_clears_draining() {
        let mgr = GcsResourceManager::new();
        let nid = node_id(1);

        mgr.on_node_add(&nid);
        mgr.set_node_draining(&nid, true, 5000);
        assert_eq!(mgr.handle_get_draining_nodes().len(), 1);

        mgr.on_node_dead(&nid);
        assert_eq!(mgr.handle_get_draining_nodes().len(), 0);
        assert_eq!(mgr.num_alive_nodes(), 0);
    }

    #[test]
    fn test_get_all_total_resources() {
        let mgr = GcsResourceManager::new();
        let nid = node_id(1);
        mgr.on_node_add(&nid);

        let usage = NodeResourceUsage {
            total_resources: HashMap::from([("CPU".into(), 16.0), ("GPU".into(), 4.0)]),
            available_resources: HashMap::from([("CPU".into(), 8.0), ("GPU".into(), 2.0)]),
            ..Default::default()
        };
        mgr.update_resource_usage(&nid, usage);

        let total = mgr.handle_get_all_total_resources();
        assert_eq!(total.len(), 1);
        assert_eq!(total[0].1.get("CPU"), Some(&16.0));
        assert_eq!(total[0].1.get("GPU"), Some(&4.0));
    }

    #[test]
    fn test_get_all_resource_usage() {
        let mgr = GcsResourceManager::new();
        let nid = node_id(1);
        mgr.on_node_add(&nid);

        let usage = NodeResourceUsage {
            total_resources: HashMap::from([("CPU".into(), 4.0)]),
            resource_load: HashMap::from([("CPU".into(), 2.0)]),
            ..Default::default()
        };
        mgr.update_resource_usage(&nid, usage);

        let all = mgr.get_all_resource_usage();
        assert_eq!(all.len(), 1);
        let node_usage = all.get(&nid).unwrap();
        assert_eq!(node_usage.resource_load.get("CPU"), Some(&2.0));
    }

    #[test]
    fn test_update_resource_overwrites() {
        let mgr = GcsResourceManager::new();
        let nid = node_id(1);
        mgr.on_node_add(&nid);

        let usage1 = NodeResourceUsage {
            available_resources: HashMap::from([("CPU".into(), 8.0)]),
            ..Default::default()
        };
        mgr.update_resource_usage(&nid, usage1);

        let usage2 = NodeResourceUsage {
            available_resources: HashMap::from([("CPU".into(), 2.0)]),
            ..Default::default()
        };
        mgr.update_resource_usage(&nid, usage2);

        let available = mgr.handle_get_all_available_resources();
        assert_eq!(available[0].1.get("CPU"), Some(&2.0));
    }
}
