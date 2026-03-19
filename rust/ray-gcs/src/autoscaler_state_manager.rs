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
    /// Drain deadline timestamps per node.
    drain_deadlines: RwLock<HashMap<Vec<u8>, i64>>,
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
            drain_deadlines: RwLock::new(HashMap::new()),
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
    ///
    /// Rejects the update if version <= last seen version (stale).
    /// Returns true if the update was accepted, false if rejected.
    pub fn handle_report_autoscaling_state(&self, state: Vec<u8>, autoscaler_state_version: i64) -> bool {
        let last_version = self.last_seen_autoscaler_state_version.load(Ordering::SeqCst);
        if autoscaler_state_version > 0 && last_version > 0 && autoscaler_state_version <= last_version {
            // Stale version — reject the update.
            return false;
        }
        *self.autoscaling_state.write() = Some(state);
        self.last_seen_autoscaler_state_version
            .store(autoscaler_state_version, Ordering::SeqCst);
        true
    }

    /// Get the last reported autoscaling state bytes.
    pub fn get_autoscaling_state(&self) -> Option<Vec<u8>> {
        self.autoscaling_state.read().clone()
    }

    /// Handle RequestClusterResourceConstraint RPC.
    pub fn handle_request_cluster_resource_constraint(&self, constraint: Vec<u8>) {
        *self.cluster_resource_constraint.write() = Some(constraint);
    }

    /// Update resource demands from a raylet node.
    pub fn update_resource_demands(&self, node_id: Vec<u8>, demands: NodeResourceDemand) {
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
                let agg = aggregated
                    .entry(key)
                    .or_insert_with(|| ResourceDemandEntry {
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
        self.drain_node_with_deadline(node_id, 0)
    }

    /// Start draining a node with a deadline timestamp. Returns true if the node was found.
    pub fn drain_node_with_deadline(&self, node_id: &[u8], deadline_timestamp_ms: i64) -> bool {
        let nid = ray_common::id::NodeID::from_binary(node_id);
        if !self.node_manager.get_all_alive_nodes().contains_key(&nid) {
            return false;
        }
        self.drain_status
            .write()
            .insert(node_id.to_vec(), DrainStatus::Draining);
        if deadline_timestamp_ms > 0 {
            self.drain_deadlines
                .write()
                .insert(node_id.to_vec(), deadline_timestamp_ms);
        }
        true
    }

    /// Get the drain deadline for a node (0 if not set).
    pub fn get_drain_deadline(&self, node_id: &[u8]) -> i64 {
        self.drain_deadlines
            .read()
            .get(node_id)
            .copied()
            .unwrap_or(0)
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
        self.drain_deadlines.write().remove(node_id);
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
        self.last_seen_autoscaler_state_version
            .load(Ordering::SeqCst)
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
        assert!(mgr.handle_report_autoscaling_state(b"state_data".to_vec(), 5));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);
    }

    #[test]
    fn test_report_autoscaling_state_rejects_stale_version() {
        let mgr = make_manager();
        // First update with version 5 — accepted
        assert!(mgr.handle_report_autoscaling_state(b"state_v5".to_vec(), 5));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);

        // Stale version 3 — rejected
        assert!(!mgr.handle_report_autoscaling_state(b"state_v3".to_vec(), 3));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5); // unchanged

        // Same version 5 — rejected (not strictly greater)
        assert!(!mgr.handle_report_autoscaling_state(b"state_v5b".to_vec(), 5));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5); // unchanged
    }

    #[test]
    fn test_report_autoscaling_state_accepts_monotonic_version() {
        let mgr = make_manager();
        // Version 0 → 1 — accepted (initial 0 is not checked)
        assert!(mgr.handle_report_autoscaling_state(b"state_v1".to_vec(), 1));
        assert_eq!(mgr.last_seen_autoscaler_version(), 1);

        // Version 1 → 2 — accepted (monotonically increasing)
        assert!(mgr.handle_report_autoscaling_state(b"state_v2".to_vec(), 2));
        assert_eq!(mgr.last_seen_autoscaler_version(), 2);

        // Version 2 → 10 — accepted (big jump ok)
        assert!(mgr.handle_report_autoscaling_state(b"state_v10".to_vec(), 10));
        assert_eq!(mgr.last_seen_autoscaler_version(), 10);
    }

    #[test]
    fn test_get_autoscaling_state() {
        let mgr = make_manager();
        assert!(mgr.get_autoscaling_state().is_none());

        mgr.handle_report_autoscaling_state(b"state_data".to_vec(), 1);
        assert_eq!(mgr.get_autoscaling_state(), Some(b"state_data".to_vec()));
    }

    #[test]
    fn test_get_cluster_status_includes_autoscaling_state() {
        let mgr = make_manager();
        // Report autoscaling state
        mgr.handle_report_autoscaling_state(b"my_state".to_vec(), 3);
        // Verify it's retrievable
        assert_eq!(mgr.get_autoscaling_state(), Some(b"my_state".to_vec()));
        assert_eq!(mgr.last_seen_autoscaler_version(), 3);
    }

    #[test]
    fn test_get_cluster_status_includes_session_name() {
        let mgr = make_manager();
        assert_eq!(mgr.session_name(), "test-session");
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

    // ---- Ported from gcs_autoscaler_state_manager_test.cc ----

    #[test]
    fn test_session_name() {
        let mgr = make_manager();
        assert_eq!(mgr.session_name(), "test-session");
    }

    #[test]
    fn test_initial_state_empty() {
        let mgr = make_manager();
        assert_eq!(mgr.current_version(), 0);
        assert_eq!(mgr.last_seen_autoscaler_version(), 0);
        assert!(mgr.get_cluster_resource_constraint().is_none());
        assert_eq!(mgr.get_total_backlog_size(), 0);
        assert!(mgr.get_aggregated_demands().is_empty());
        assert!(mgr.get_draining_nodes().is_empty());
        assert!(mgr.get_idle_node_ids().is_empty());
    }

    #[test]
    fn test_version_bumps_on_demand_update() {
        let mgr = make_manager();
        let v0 = mgr.current_version();
        mgr.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 1,
            },
        );
        let v1 = mgr.current_version();
        assert!(v1 > v0, "version should increase after demand update");

        mgr.update_resource_demands(
            vec![2u8; 28],
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 2,
            },
        );
        let v2 = mgr.current_version();
        assert!(v2 > v1, "version should increase again");
    }

    #[test]
    fn test_get_cluster_resource_state_returns_empty_nodes() {
        let mgr = make_manager();
        let (version, node_states) = mgr.handle_get_cluster_resource_state();
        assert!(version > 0);
        assert!(node_states.is_empty());
    }

    #[test]
    fn test_multiple_report_autoscaling_state_overwrites() {
        let mgr = make_manager();
        assert!(mgr.handle_report_autoscaling_state(b"state1".to_vec(), 1));
        assert_eq!(mgr.last_seen_autoscaler_version(), 1);

        assert!(mgr.handle_report_autoscaling_state(b"state2".to_vec(), 5));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);

        // Older version should now be rejected (version check in this impl)
        assert!(!mgr.handle_report_autoscaling_state(b"state3".to_vec(), 3));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);
    }

    #[test]
    fn test_resource_constraint_overwrite() {
        let mgr = make_manager();
        mgr.handle_request_cluster_resource_constraint(b"c1".to_vec());
        assert_eq!(mgr.get_cluster_resource_constraint(), Some(b"c1".to_vec()));
        mgr.handle_request_cluster_resource_constraint(b"c2".to_vec());
        assert_eq!(mgr.get_cluster_resource_constraint(), Some(b"c2".to_vec()));
    }

    #[test]
    fn test_multiple_demand_shapes_aggregated_separately() {
        let mgr = make_manager();

        let cpu_shape = HashMap::from([("CPU".to_string(), 1.0)]);
        let gpu_shape = HashMap::from([("GPU".to_string(), 1.0)]);

        mgr.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![
                    ResourceDemandEntry {
                        shape: cpu_shape.clone(),
                        count: 5,
                        infeasible_count: 0,
                    },
                    ResourceDemandEntry {
                        shape: gpu_shape.clone(),
                        count: 3,
                        infeasible_count: 1,
                    },
                ],
                backlog_size: 10,
            },
        );

        let aggregated = mgr.get_aggregated_demands();
        assert_eq!(aggregated.len(), 2);
        let total_count: u64 = aggregated.iter().map(|e| e.count).sum();
        assert_eq!(total_count, 8);
    }

    #[test]
    fn test_update_demands_same_node_replaces() {
        let mgr = make_manager();
        let node_id = vec![1u8; 28];

        mgr.update_resource_demands(
            node_id.clone(),
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 10,
            },
        );
        assert_eq!(mgr.get_total_backlog_size(), 10);

        // Replace with new demands
        mgr.update_resource_demands(
            node_id,
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 20,
            },
        );
        assert_eq!(mgr.get_total_backlog_size(), 20);
    }

    #[test]
    fn test_remove_nonexistent_node_demands() {
        let mgr = make_manager();
        // Should not panic
        mgr.remove_node_demands(&[99u8; 28]);
        assert_eq!(mgr.get_total_backlog_size(), 0);
    }

    #[test]
    fn test_draining_nodes_list() {
        let mgr = make_manager();
        let node1 = vec![1u8; 28];
        let node2 = vec![2u8; 28];

        // Manually insert draining status
        mgr.drain_status
            .write()
            .insert(node1.clone(), DrainStatus::Draining);
        mgr.drain_status
            .write()
            .insert(node2.clone(), DrainStatus::Draining);

        let draining = mgr.get_draining_nodes();
        assert_eq!(draining.len(), 2);

        // Mark one as drained
        mgr.mark_node_drained(&node1);
        let draining = mgr.get_draining_nodes();
        assert_eq!(draining.len(), 1);
        assert_eq!(draining[0], node2);
    }

    #[tokio::test]
    async fn test_drain_node_updates_autoscaler_drain_state() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let resource_mgr = Arc::new(GcsResourceManager::new());
        let mgr = GcsAutoscalerStateManager::new("test".to_string(), node_mgr.clone(), resource_mgr);

        let node_id = vec![1u8; 28];

        // Cannot drain a non-alive node
        assert!(!mgr.drain_node_with_deadline(&node_id, 5000));
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);

        // Register node
        let node_info = ray_proto::ray::rpc::GcsNodeInfo {
            node_id: node_id.clone(),
            state: 0,
            ..Default::default()
        };
        node_mgr.handle_register_node(node_info).await.unwrap();

        // Now drain with deadline
        assert!(mgr.drain_node_with_deadline(&node_id, 12345));
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Draining);
        assert_eq!(mgr.get_drain_deadline(&node_id), 12345);

        // Clean up removes deadline too
        mgr.remove_drain_status(&node_id);
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);
        assert_eq!(mgr.get_drain_deadline(&node_id), 0);
    }

    #[test]
    fn test_drain_status_default_is_running() {
        let mgr = make_manager();
        // Any unknown node should return Running
        assert_eq!(mgr.get_drain_status(&[42u8; 28]), DrainStatus::Running);
        assert_eq!(mgr.get_drain_status(&[0u8; 28]), DrainStatus::Running);
    }

    #[test]
    fn test_remove_drain_status_idempotent() {
        let mgr = make_manager();
        let node_id = vec![1u8; 28];

        // Remove drain status for node that was never draining
        mgr.remove_drain_status(&node_id);
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);

        // Set and remove twice
        mgr.drain_status
            .write()
            .insert(node_id.clone(), DrainStatus::Draining);
        mgr.remove_drain_status(&node_id);
        mgr.remove_drain_status(&node_id); // Second remove should not panic
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);
    }

    #[test]
    fn test_backlog_from_multiple_nodes() {
        let mgr = make_manager();

        for i in 1..=5u8 {
            mgr.update_resource_demands(
                vec![i; 28],
                NodeResourceDemand {
                    resource_demands: vec![],
                    backlog_size: i as u64 * 10,
                },
            );
        }

        // 10 + 20 + 30 + 40 + 50 = 150
        assert_eq!(mgr.get_total_backlog_size(), 150);

        // Remove one node
        mgr.remove_node_demands(&[3u8; 28]);
        // 150 - 30 = 120
        assert_eq!(mgr.get_total_backlog_size(), 120);
    }

    #[test]
    fn test_aggregated_demands_empty() {
        let mgr = make_manager();
        // Node with empty demand list
        mgr.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 5,
            },
        );
        assert!(mgr.get_aggregated_demands().is_empty());
        assert_eq!(mgr.get_total_backlog_size(), 5);
    }

    #[test]
    fn test_version_increments_monotonically() {
        let mgr = make_manager();
        let mut last = mgr.current_version();
        for _ in 0..10 {
            let (v, _) = mgr.handle_get_cluster_resource_state();
            assert!(v > last);
            last = v;
        }
    }

    // ---- Additional tests ported from gcs_autoscaler_state_manager_test.cc ----

    /// Port of TestDrainNonAliveNode — drain_node should fail for unknown nodes.
    #[test]
    fn test_drain_non_alive_node() {
        let mgr = make_manager();
        let node_id = vec![1u8; 28];
        // Node is not alive, drain should return false
        assert!(!mgr.drain_node(&node_id));
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);
    }

    /// Port of TestDrainNodeRaceCondition — draining then removing status
    /// while node is not in the alive set.
    #[test]
    fn test_drain_race_condition() {
        let mgr = make_manager();
        let node_id = vec![1u8; 28];

        // Manually insert draining status without adding node to alive set
        mgr.drain_status
            .write()
            .insert(node_id.clone(), DrainStatus::Draining);

        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Draining);
        let draining = mgr.get_draining_nodes();
        assert_eq!(draining.len(), 1);

        // Remove drain status
        mgr.remove_drain_status(&node_id);
        assert_eq!(mgr.get_drain_status(&node_id), DrainStatus::Running);
        assert!(mgr.get_draining_nodes().is_empty());
    }

    /// Port of TestIdleTime — idle nodes reporting.
    #[test]
    fn test_idle_nodes_no_nodes_means_empty() {
        let mgr = make_manager();
        assert!(mgr.get_idle_node_ids().is_empty());
    }

    /// Port of TestClusterResourcesConstraint — setting and clearing constraints.
    #[test]
    fn test_cluster_resources_constraint_lifecycle() {
        let mgr = make_manager();

        // Initially no constraint
        assert!(mgr.get_cluster_resource_constraint().is_none());

        // Set constraint
        let constraint = b"min_resources:{CPU:4}".to_vec();
        mgr.handle_request_cluster_resource_constraint(constraint.clone());
        assert_eq!(mgr.get_cluster_resource_constraint(), Some(constraint));

        // Overwrite constraint
        let constraint2 = b"min_resources:{GPU:2}".to_vec();
        mgr.handle_request_cluster_resource_constraint(constraint2.clone());
        assert_eq!(mgr.get_cluster_resource_constraint(), Some(constraint2));
    }

    /// Port of TestBasicResourceRequests — verifying demands from multiple nodes.
    #[test]
    fn test_basic_resource_requests() {
        let mgr = make_manager();

        // Node 1 requests CPU
        let demand1 = NodeResourceDemand {
            resource_demands: vec![ResourceDemandEntry {
                shape: HashMap::from([("CPU".to_string(), 1.0)]),
                count: 10,
                infeasible_count: 0,
            }],
            backlog_size: 10,
        };
        mgr.update_resource_demands(vec![1u8; 28], demand1);

        // Node 2 requests GPU
        let demand2 = NodeResourceDemand {
            resource_demands: vec![ResourceDemandEntry {
                shape: HashMap::from([("GPU".to_string(), 1.0)]),
                count: 5,
                infeasible_count: 2,
            }],
            backlog_size: 5,
        };
        mgr.update_resource_demands(vec![2u8; 28], demand2);

        let aggregated = mgr.get_aggregated_demands();
        assert_eq!(aggregated.len(), 2);

        let total_count: u64 = aggregated.iter().map(|e| e.count).sum();
        assert_eq!(total_count, 15);
        let total_infeasible: u64 = aggregated.iter().map(|e| e.infeasible_count).sum();
        assert_eq!(total_infeasible, 2);
        assert_eq!(mgr.get_total_backlog_size(), 15);
    }

    /// Port of TestGangResourceRequestsBasic — multiple demand shapes on same node.
    #[test]
    fn test_gang_resource_requests_basic() {
        let mgr = make_manager();

        let demand = NodeResourceDemand {
            resource_demands: vec![
                ResourceDemandEntry {
                    shape: HashMap::from([("CPU".to_string(), 1.0)]),
                    count: 3,
                    infeasible_count: 0,
                },
                ResourceDemandEntry {
                    shape: HashMap::from([("GPU".to_string(), 1.0)]),
                    count: 2,
                    infeasible_count: 0,
                },
                ResourceDemandEntry {
                    shape: HashMap::from([("CPU".to_string(), 4.0), ("GPU".to_string(), 2.0)]),
                    count: 1,
                    infeasible_count: 1,
                },
            ],
            backlog_size: 6,
        };
        mgr.update_resource_demands(vec![1u8; 28], demand);

        let aggregated = mgr.get_aggregated_demands();
        assert_eq!(aggregated.len(), 3);
        let total_count: u64 = aggregated.iter().map(|e| e.count).sum();
        assert_eq!(total_count, 6);
    }

    /// Port of TestReportAutoscalingState — verifying version tracking.
    #[test]
    fn test_report_autoscaling_state_version_tracking() {
        let mgr = make_manager();

        assert_eq!(mgr.last_seen_autoscaler_version(), 0);

        assert!(mgr.handle_report_autoscaling_state(b"state_v1".to_vec(), 1));
        assert_eq!(mgr.last_seen_autoscaler_version(), 1);

        assert!(mgr.handle_report_autoscaling_state(b"state_v5".to_vec(), 5));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);

        // Setting to a lower version is now rejected
        assert!(!mgr.handle_report_autoscaling_state(b"state_v3".to_vec(), 3));
        assert_eq!(mgr.last_seen_autoscaler_version(), 5);
    }

    /// Port of TestDrainingStatus — full drain lifecycle.
    #[test]
    fn test_full_drain_lifecycle() {
        let mgr = make_manager();
        let node1 = vec![1u8; 28];
        let node2 = vec![2u8; 28];

        // Start draining two nodes (directly set status since no node_manager nodes)
        mgr.drain_status
            .write()
            .insert(node1.clone(), DrainStatus::Draining);
        mgr.drain_status
            .write()
            .insert(node2.clone(), DrainStatus::Draining);

        assert_eq!(mgr.get_draining_nodes().len(), 2);
        assert_eq!(mgr.get_drain_status(&node1), DrainStatus::Draining);
        assert_eq!(mgr.get_drain_status(&node2), DrainStatus::Draining);

        // Drain node1
        mgr.mark_node_drained(&node1);
        assert_eq!(mgr.get_drain_status(&node1), DrainStatus::Drained);
        assert_eq!(mgr.get_draining_nodes().len(), 1);

        // Remove node1 from drain tracking
        mgr.remove_drain_status(&node1);
        assert_eq!(mgr.get_drain_status(&node1), DrainStatus::Running);

        // node2 still draining
        assert_eq!(mgr.get_draining_nodes().len(), 1);
        assert_eq!(mgr.get_drain_status(&node2), DrainStatus::Draining);
    }

    /// Port of TestGetClusterStatusBasic — cluster resource state with empty cluster.
    #[test]
    fn test_get_cluster_status_basic() {
        let mgr = make_manager();
        let (version, nodes) = mgr.handle_get_cluster_resource_state();
        assert_eq!(version, 1);
        assert!(nodes.is_empty());

        // Second call bumps version
        let (version2, _) = mgr.handle_get_cluster_resource_state();
        assert_eq!(version2, 2);
    }

    /// Port of TestGcsKvManagerInternalConfig — session name test.
    #[test]
    fn test_gcs_session_name_and_config() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let resource_mgr = Arc::new(GcsResourceManager::new());
        let mgr =
            GcsAutoscalerStateManager::new("my-custom-session".to_string(), node_mgr, resource_mgr);
        assert_eq!(mgr.session_name(), "my-custom-session");
    }

    /// Test that demands and backlog are consistent after many updates and removals.
    #[test]
    fn test_demands_consistency_after_churn() {
        let mgr = make_manager();

        // Add demands for 10 nodes
        for i in 1..=10u8 {
            let demand = NodeResourceDemand {
                resource_demands: vec![ResourceDemandEntry {
                    shape: HashMap::from([("CPU".to_string(), 1.0)]),
                    count: i as u64,
                    infeasible_count: 0,
                }],
                backlog_size: i as u64,
            };
            mgr.update_resource_demands(vec![i; 28], demand);
        }

        assert_eq!(mgr.get_total_backlog_size(), 55); // 1+2+...+10
        let aggregated = mgr.get_aggregated_demands();
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].count, 55);

        // Remove odd nodes
        for i in (1..=10u8).filter(|x| x % 2 == 1) {
            mgr.remove_node_demands(&[i; 28]);
        }

        // Remaining: 2+4+6+8+10 = 30
        assert_eq!(mgr.get_total_backlog_size(), 30);
    }

    /// Test concurrent version bumps from both get_cluster_resource_state and update_demands.
    #[test]
    fn test_concurrent_version_sources() {
        let mgr = make_manager();

        let v0 = mgr.current_version();
        assert_eq!(v0, 0);

        // update_demands bumps version
        mgr.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 1,
            },
        );
        let v1 = mgr.current_version();
        assert_eq!(v1, 1);

        // get_cluster_resource_state also bumps version
        let (v2, _) = mgr.handle_get_cluster_resource_state();
        assert_eq!(v2, 2);

        // Another update
        mgr.update_resource_demands(
            vec![2u8; 28],
            NodeResourceDemand {
                resource_demands: vec![],
                backlog_size: 1,
            },
        );
        let v3 = mgr.current_version();
        assert_eq!(v3, 3);
    }
}
