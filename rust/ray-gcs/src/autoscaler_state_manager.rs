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

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::node_manager::GcsNodeManager;
use crate::resource_manager::GcsResourceManager;

/// The GCS autoscaler state manager provides cluster resource state
/// to the autoscaler for scaling decisions.
pub struct GcsAutoscalerStateManager {
    /// Session name for this Ray cluster.
    session_name: String,
    /// Monotonically increasing version for cluster state.
    cluster_resource_state_version: AtomicI64,
    /// Last version seen by autoscaler.
    #[allow(dead_code)]
    last_seen_autoscaler_state_version: AtomicI64,
    /// Cached autoscaler state.
    autoscaling_state: RwLock<Option<Vec<u8>>>,
    /// Cluster resource constraints from request_resources() API.
    cluster_resource_constraint: RwLock<Option<Vec<u8>>>,
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

    #[test]
    fn test_version_increment() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let node_mgr = Arc::new(GcsNodeManager::new(storage));
        let resource_mgr = Arc::new(GcsResourceManager::new());

        let mgr =
            GcsAutoscalerStateManager::new("test-session".to_string(), node_mgr, resource_mgr);

        assert_eq!(mgr.current_version(), 0);
        let (v1, _) = mgr.handle_get_cluster_resource_state();
        assert_eq!(v1, 1);
        let (v2, _) = mgr.handle_get_cluster_resource_state();
        assert_eq!(v2, 2);
    }
}
