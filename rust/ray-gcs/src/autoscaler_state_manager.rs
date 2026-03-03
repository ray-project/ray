// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Autoscaler State Manager — provides cluster state to the autoscaler.
//!
//! Replaces `src/ray/gcs/gcs_autoscaler_state_manager.h/cc`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::node_manager::GcsNodeManager;
use crate::resource_manager::GcsResourceManager;

/// Per-node resource demand reported by raylets.
#[derive(Debug, Clone, Default)]
pub struct NodeResourceDemand {
    /// Resource demands from pending tasks on this node.
    pub resource_demands: Vec<ResourceDemandEntry>,
    /// Total backlog size.
    pub backlog_size: u64,
}

/// A single resource demand entry (a resource shape and count).
#[derive(Debug, Clone)]
pub struct ResourceDemandEntry {
    /// Resource shape: resource name → amount.
    pub shape: HashMap<String, f64>,
    /// Number of pending tasks/actors with this shape.
    pub count: u64,
    /// Number of tasks that are infeasible on any existing node.
    pub infeasible_count: u64,
}

/// Drain status for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStatus {
    /// Node is running normally.
    Running,
    /// Node is draining — no new tasks, existing tasks continue.
    Draining,
    /// Node is fully drained and can be terminated.
    Drained,
}

/// The GCS autoscaler state manager provides cluster resource state
/// to the autoscaler for scaling decisions.
pub struct GcsAutoscalerStateManager {
    /// Session name for this Ray cluster.
    session_name: String,
    /// Monotonically increasing version for cluster state.
    cluster_resource_state_version: AtomicI64,
    /// Last version seen by autoscaler.
    last_seen_autoscaler_state_version: AtomicI64,
    /// Cached autoscaler state.
    autoscaling_state: RwLock<Option<Vec<u8>>>,
    /// Cluster resource constraints from request_resources() API.
    cluster_resource_constraint: RwLock<Option<Vec<u8>>>,
    /// Per-node resource demands reported by raylets.
    node_resource_demands: RwLock<HashMap<Vec<u8>, NodeResourceDemand>>,
    /// Node drain status.
    drain_status: RwLock<HashMap<Vec<u8>, DrainStatus>>,
    /// Idle duration threshold in seconds — nodes idle longer than this
    /// are reported to the autoscaler for potential scale-down.
    #[allow(dead_code)]
    idle_threshold_secs: u64,
    /// References to other managers.
    node_manager: Arc<GcsNodeManager>,
    resource_manager: Arc<GcsResourceManager>,
}

impl GcsAutoscalerStateManager {
    pub fn new(
        session_name: String,
        node_manager: Arc<GcsNodeManager>,
        resource_manager: Arc<GcsResourceManager>,
    ) -> Self {
        Self {
            session_name,
            cluster_resource_state_version: AtomicI64::new(0),
            last_seen_autoscaler_state_version: AtomicI64::new(0),
            autoscaling_state: RwLock::new(None),
            cluster_resource_constraint: RwLock::new(None),
            node_resource_demands: RwLock::new(HashMap::new()),
            drain_status: RwLock::new(HashMap::new()),
            idle_threshold_secs: 60,
            node_manager,
            resource_manager,
        }
    }

    /// Handle GetClusterResourceState RPC.
    pub fn handle_get_cluster_resource_state(
        &self,
    ) -> (i64, Vec<ray_proto::ray::rpc::autoscaler::NodeState>) {
        let version = self.increment_version();

        let alive_nodes = self.node_manager.get_all_alive_nodes();
        let resource_usage = self.resource_manager.get_all_resource_usage();

        let node_states: Vec<ray_proto::ray::rpc::autoscaler::NodeState> = alive_nodes
            .iter()
            .map(|(node_id, node_info)| {
                let resources = resource_usage.get(node_id);
                let mut total_resources = std::collections::HashMap::new();
                let mut available_resources = std::collections::HashMap::new();

                if let Some(usage) = resources {
                    total_resources = usage.total_resources.clone();
                    available_resources = usage.available_resources.clone();
                }

                ray_proto::ray::rpc::autoscaler::NodeState {
                    node_id: node_info.node_id.clone(),
                    instance_id: node_info.instance_id.clone(),
                    ray_node_type_name: node_info.node_type_name.clone(),
                    node_ip_address: node_info.node_manager_address.clone(),
                    total_resources,
                    available_resources,
                    status: 0, // RUNNING
                    ..Default::default()
                }
            })
            .collect();

        (version, node_states)
    }

    /// Handle ReportAutoscalingState RPC.
    pub fn handle_report_autoscaling_state(&self, state: Vec<u8>, autoscaler_state_version: i64) {
        *self.autoscaling_state.write() = Some(state);
        self.last_seen_autoscaler_state_version
            .store(autoscaler_state_version, Ordering::SeqCst);
    }

    /// Handle RequestClusterResourceConstraint RPC.
    pub fn handle_request_cluster_resource_constraint(&self, constraint: Vec<u8>) {
        *self.cluster_resource_constraint.write() = Some(constraint);
    }

    /// Update resource demands from a raylet node.
    pub fn update_resource_demands(
        &self,
        node_id: Vec<u8>,
        demands: NodeResourceDemand,
    ) {
        self.node_resource_demands.write().insert(node_id, demands);
        // Bump version since cluster state changed.
        self.cluster_resource_state_version
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Remove resource demands for a node (on node death).
    pub fn remove_node_demands(&self, node_id: &[u8]) {
        self.node_resource_demands.write().remove(node_id);
    }

    /// Get aggregated resource demands across all nodes.
    pub fn get_aggregated_demands(&self) -> Vec<ResourceDemandEntry> {
        let demands = self.node_resource_demands.read();
        let mut aggregated: HashMap<String, ResourceDemandEntry> = HashMap::new();

        for node_demand in demands.values() {
            for entry in &node_demand.resource_demands {
                let key = format!("{:?}", entry.shape);
                let agg = aggregated.entry(key).or_insert_with(|| ResourceDemandEntry {
                    shape: entry.shape.clone(),
                    count: 0,
                    infeasible_count: 0,
                });
                agg.count += entry.count;
                agg.infeasible_count += entry.infeasible_count;
            }
        }

        aggregated.into_values().collect()
    }

    /// Get total backlog size across all nodes.
    pub fn get_total_backlog_size(&self) -> u64 {
        self.node_resource_demands
            .read()
            .values()
            .map(|d| d.backlog_size)
            .sum()
    }

    /// Start draining a node. Returns true if the node was found.
    pub fn drain_node(&self, node_id: &[u8]) -> bool {
        let nid = ray_common::id::NodeID::from_binary(node_id);
        if !self.node_manager.get_all_alive_nodes().contains_key(&nid) {
            return false;
        }
        self.drain_status
            .write()
            .insert(node_id.to_vec(), DrainStatus::Draining);
        true
    }

    /// Mark a node as fully drained.
    pub fn mark_node_drained(&self, node_id: &[u8]) {
        self.drain_status
            .write()
            .insert(node_id.to_vec(), DrainStatus::Drained);
    }

    /// Get the drain status of a node.
    pub fn get_drain_status(&self, node_id: &[u8]) -> DrainStatus {
        self.drain_status
            .read()
            .get(node_id)
            .copied()
            .unwrap_or(DrainStatus::Running)
    }

    /// Remove drain status for a node (on node death).
    pub fn remove_drain_status(&self, node_id: &[u8]) {
        self.drain_status.write().remove(node_id);
    }

    /// Get the list of nodes that are idle — where available == total.
    pub fn get_idle_node_ids(&self) -> Vec<Vec<u8>> {
        let alive_nodes = self.node_manager.get_all_alive_nodes();
        let resource_usage = self.resource_manager.get_all_resource_usage();

        alive_nodes
            .keys()
            .filter(|node_id| {
                if let Some(usage) = resource_usage.get(node_id) {
                    // A node is idle if available == total for all resources.
                    usage.total_resources == usage.available_resources
                } else {
                    // No usage data = assume idle.
                    true
                }
            })
            .map(|node_id| node_id.binary())
            .collect()
    }

    /// Get nodes that are being drained.
    pub fn get_draining_nodes(&self) -> Vec<Vec<u8>> {
        self.drain_status
            .read()
            .iter()
            .filter(|(_, status)| **status == DrainStatus::Draining)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Set the idle threshold in seconds.
    pub fn set_idle_threshold_secs(&self, secs: u64) {
        // This would need interior mutability to store; for now just log.
        let _ = secs;
        // In a full impl, this would update self.idle_threshold_secs
    }

    /// Get the last autoscaler state version.
    pub fn last_seen_autoscaler_version(&self) -> i64 {
        self.last_seen_autoscaler_state_version.load(Ordering::SeqCst)
    }

    /// Get the current cluster resource constraint.
    pub fn get_cluster_resource_constraint(&self) -> Option<Vec<u8>> {
        self.cluster_resource_constraint.read().clone()
    }

    pub fn session_name(&self) -> &str {
        &self.session_name
    }

    fn increment_version(&self) -> i64 {
        self.cluster_resource_state_version
            .fetch_add(1, Ordering::SeqCst)
            + 1
    }

    pub fn current_version(&self) -> i64 {
        self.cluster_resource_state_version.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;
    use crate::table_storage::GcsTableStorage;

    fn make_manager() -> GcsAutoscalerStateManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let resource_mgr = Arc::new(GcsResourceManager::new());
        GcsAutoscalerStateManager::new("test-session".to_string(), node_mgr, resource_mgr)
    }

    #[test]
    fn test_version_increment() {
        let mgr = make_manager();

        assert_eq!(mgr.current_version(), 0);
        let (v1, _) = mgr.handle_get_cluster_resource_state();
        assert_eq!(v1, 1);
        let (v2, _) = mgr.handle_get_cluster_resource_state();
        assert_eq!(v2, 2);
    }

    #[test]
    fn test_report_autoscaling_state() {
        let mgr = make_manager();
        mgr.handle_report_autoscaling_state(b"state_data".to_vec(), 5);
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);
    }

    #[test]
    fn test_resource_constraint() {
        let mgr = make_manager();
        assert!(mgr.get_cluster_resource_constraint().is_none());
        mgr.handle_request_cluster_resource_constraint(b"constraint".to_vec());
        assert_eq!(
            mgr.get_cluster_resource_constraint(),
            Some(b"constraint".to_vec())
        );
    }

    #[test]
    fn test_update_resource_demands() {
        let mgr = make_manager();

        let demand = NodeResourceDemand {
            resource_demands: vec![ResourceDemandEntry {
                shape: HashMap::from([("CPU".to_string(), 4.0)]),
                count: 10,
                infeasible_count: 2,
            }],
            backlog_size: 15,
        };

        let v_before = mgr.current_version();
        mgr.update_resource_demands(vec![1u8; 28], demand);
        // Version should have bumped.
        assert!(mgr.current_version() > v_before);

        assert_eq!(mgr.get_total_backlog_size(), 15);
    }

    #[test]
    fn test_aggregated_demands() {
        let mgr = make_manager();

        // Two nodes with same demand shape.
        let shape = HashMap::from([("CPU".to_string(), 2.0)]);
        mgr.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![ResourceDemandEntry {
                    shape: shape.clone(),
                    count: 5,
                    infeasible_count: 0,
                }],
                backlog_size: 5,
            },
        );
        mgr.update_resource_demands(
            vec![2u8; 28],
            NodeResourceDemand {
                resource_demands: vec![ResourceDemandEntry {
                    shape: shape.clone(),
                    count: 3,
                    infeasible_count: 1,
                }],
                backlog_size: 3,
            },
        );

        let aggregated = mgr.get_aggregated_demands();
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].count, 8);
        assert_eq!(aggregated[0].infeasible_count, 1);
        assert_eq!(mgr.get_total_backlog_size(), 8);
    }

    #[test]
    fn test_remove_node_demands() {
        let mgr = make_manager();
        mgr.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 10,
            },
        );
        assert_eq!(mgr.get_total_backlog_size(), 10);
        mgr.remove_node_demands(&[1u8; 28]);
        assert_eq!(mgr.get_total_backlog_size(), 0);
    }

    #[test]
    fn test_drain_status() {
        let mgr = make_manager();
        let node_id = vec![1u8; 28];

        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);

        // drain_node returns false for non-existent node.
        assert!(!mgr.drain_node(&node_id));

        // Manually set drain status.
        mgr.drain_status
            .write()
            .insert(node_id.clone(), DrainStatus::Draining);
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Draining);

        mgr.mark_node_drained(&node_id);
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Drained);

        let draining = mgr.get_draining_nodes();
        assert!(draining.is_empty()); // Drained, not draining.

        mgr.remove_drain_status(&node_id);
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);
    }

    #[test]
    fn test_get_idle_nodes() {
        let mgr = make_manager();
        // No nodes registered → no idle nodes.
        assert!(mgr.get_idle_node_ids().is_empty());
    }
}
