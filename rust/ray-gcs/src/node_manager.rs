// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Node Manager — tracks cluster node lifecycle.
//!
//! Replaces `src/ray/gcs/gcs_node_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use ray_common::id::NodeID;

use crate::table_storage::GcsTableStorage;

/// Callback invoked when a node is added to the cluster.
pub type NodeAddedCallback = Box<dyn Fn(&ray_proto::ray::rpc::GcsNodeInfo) + Send + Sync>;
/// Callback invoked when a node is removed from the cluster.
pub type NodeRemovedCallback = Box<dyn Fn(&ray_proto::ray::rpc::GcsNodeInfo) + Send + Sync>;

/// The GCS node manager tracks all nodes in the cluster.
pub struct GcsNodeManager {
    /// Currently alive nodes.
    alive_nodes: RwLock<HashMap<NodeID, Arc<ray_proto::ray::rpc::GcsNodeInfo>>>,
    /// Dead nodes (cached for queries).
    dead_nodes: RwLock<HashMap<NodeID, Arc<ray_proto::ray::rpc::GcsNodeInfo>>>,
    /// Nodes being drained.
    draining_nodes: RwLock<HashMap<NodeID, i64>>, // node_id → deadline_ms
    /// Cluster ID (raw 28-byte binary, matching C++ ClusterID::Binary()).
    cluster_id: RwLock<Vec<u8>>,
    /// Listeners.
    node_added_listeners: RwLock<Vec<NodeAddedCallback>>,
    node_removed_listeners: RwLock<Vec<NodeRemovedCallback>>,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
}

impl GcsNodeManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            alive_nodes: RwLock::new(HashMap::new()),
            dead_nodes: RwLock::new(HashMap::new()),
            draining_nodes: RwLock::new(HashMap::new()),
            cluster_id: RwLock::new(Vec::new()),
            node_added_listeners: RwLock::new(Vec::new()),
            node_removed_listeners: RwLock::new(Vec::new()),
            table_storage,
        }
    }

    /// Initialize from persisted data.
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let all_nodes = self
            .table_storage
            .node_table()
            .get_all()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let mut alive = self.alive_nodes.write();
        let mut dead = self.dead_nodes.write();

        for (key, node) in all_nodes {
            let node_id = NodeID::from_hex(&key);
            let node = Arc::new(node);
            // GcsNodeInfo.state: 0 = ALIVE, 1 = DEAD
            if node.state == 1 {
                dead.insert(node_id, node);
            } else {
                alive.insert(node_id, node);
            }
        }
        Ok(())
    }

    /// Set the cluster ID (raw 28-byte binary).
    pub fn set_cluster_id(&self, cluster_id: Vec<u8>) {
        *self.cluster_id.write() = cluster_id;
    }

    /// Get the cluster ID (raw 28-byte binary).
    pub fn cluster_id(&self) -> Vec<u8> {
        self.cluster_id.read().clone()
    }

    /// Handle RegisterNode RPC.
    pub async fn handle_register_node(
        &self,
        node_info: ray_proto::ray::rpc::GcsNodeInfo,
    ) -> Result<(), tonic::Status> {
        let node_id = NodeID::from_binary(
            node_info
                .node_id
                .as_slice()
                .try_into()
                .unwrap_or(&[0u8; 28]),
        );
        let key = hex::encode(&node_info.node_id);

        let node = Arc::new(node_info.clone());

        // Persist
        self.table_storage
            .node_table()
            .put(&key, &node_info)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Add to alive nodes
        self.alive_nodes.write().insert(node_id, node.clone());

        // Notify listeners
        let listeners = self.node_added_listeners.read();
        for listener in listeners.iter() {
            listener(&node);
        }

        tracing::info!(?node_id, "Node registered");
        Ok(())
    }

    /// Handle UnregisterNode RPC (graceful shutdown).
    pub async fn handle_unregister_node(&self, node_id_bytes: &[u8]) -> Result<(), tonic::Status> {
        let node_id = NodeID::from_binary(node_id_bytes.try_into().unwrap_or(&[0u8; 28]));
        self.remove_node(&node_id).await
    }

    /// Handle node failure (from health check manager).
    pub async fn on_node_failure(&self, node_id: &NodeID) -> Result<(), tonic::Status> {
        self.remove_node(node_id).await
    }

    /// Remove a node from the alive set.
    async fn remove_node(&self, node_id: &NodeID) -> Result<(), tonic::Status> {
        let node = {
            let mut alive = self.alive_nodes.write();
            alive.remove(node_id)
        };

        if let Some(node) = node {
            // Update state to DEAD
            let mut dead_node = (*node).clone();
            dead_node.state = 1; // DEAD
            dead_node.end_time_ms = ray_util::time::current_time_ms();

            let key = hex::encode(&dead_node.node_id);
            let _ = self.table_storage.node_table().put(&key, &dead_node).await;

            let dead_node = Arc::new(dead_node);
            self.dead_nodes.write().insert(*node_id, dead_node.clone());
            self.draining_nodes.write().remove(node_id);

            // Notify listeners
            let listeners = self.node_removed_listeners.read();
            for listener in listeners.iter() {
                listener(&dead_node);
            }

            tracing::info!(?node_id, "Node removed");
        }
        Ok(())
    }

    /// Handle DrainNode RPC.
    pub fn handle_drain_node(&self, node_id: &NodeID, deadline_ms: i64) {
        if self.alive_nodes.read().contains_key(node_id) {
            self.draining_nodes.write().insert(*node_id, deadline_ms);
            tracing::info!(?node_id, deadline_ms, "Node draining");
        }
    }

    /// Handle GetAllNodeInfo RPC.
    pub fn handle_get_all_node_info(&self) -> Vec<ray_proto::ray::rpc::GcsNodeInfo> {
        let alive = self.alive_nodes.read();
        let dead = self.dead_nodes.read();
        alive
            .values()
            .chain(dead.values())
            .map(|n| (**n).clone())
            .collect()
    }

    /// Handle GetClusterId RPC — returns raw 28-byte cluster ID.
    pub fn handle_get_cluster_id(&self) -> Vec<u8> {
        self.cluster_id.read().clone()
    }

    /// Check if a node is alive.
    pub fn is_node_alive(&self, node_id: &NodeID) -> bool {
        self.alive_nodes.read().contains_key(node_id)
    }

    /// Check if a node is dead.
    pub fn is_node_dead(&self, node_id: &NodeID) -> bool {
        self.dead_nodes.read().contains_key(node_id)
    }

    /// Get an alive node.
    pub fn get_alive_node(
        &self,
        node_id: &NodeID,
    ) -> Option<Arc<ray_proto::ray::rpc::GcsNodeInfo>> {
        self.alive_nodes.read().get(node_id).cloned()
    }

    /// Get all alive nodes.
    pub fn get_all_alive_nodes(&self) -> HashMap<NodeID, Arc<ray_proto::ray::rpc::GcsNodeInfo>> {
        self.alive_nodes.read().clone()
    }

    /// Get all draining node IDs with their deadlines.
    pub fn get_draining_nodes(&self) -> HashMap<NodeID, i64> {
        self.draining_nodes.read().clone()
    }

    /// Number of alive nodes.
    pub fn num_alive_nodes(&self) -> usize {
        self.alive_nodes.read().len()
    }

    /// Register a node-added listener.
    pub fn add_node_added_listener(&self, callback: NodeAddedCallback) {
        self.node_added_listeners.write().push(callback);
    }

    /// Register a node-removed listener.
    pub fn add_node_removed_listener(&self, callback: NodeRemovedCallback) {
        self.node_removed_listeners.write().push(callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;

    fn make_node_info(id: u8) -> ray_proto::ray::rpc::GcsNodeInfo {
        let mut node_id = vec![0u8; 28];
        node_id[0] = id;
        ray_proto::ray::rpc::GcsNodeInfo {
            node_id,
            node_name: format!("node-{}", id),
            state: 0, // ALIVE
            ..Default::default()
        }
    }

    fn node_id(id: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = id;
        NodeID::from_binary(&data)
    }

    #[tokio::test]
    async fn test_register_and_unregister_node() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsNodeManager::new(storage);

        mgr.handle_register_node(make_node_info(1)).await.unwrap();
        assert_eq!(mgr.num_alive_nodes(), 1);
        assert!(mgr.is_node_alive(&node_id(1)));

        let nid = node_id(1);
        let nid_bytes = nid.binary();
        mgr.handle_unregister_node(&nid_bytes).await.unwrap();
        assert_eq!(mgr.num_alive_nodes(), 0);
        assert!(mgr.is_node_dead(&node_id(1)));
    }

    #[tokio::test]
    async fn test_get_all_node_info() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsNodeManager::new(storage);

        mgr.handle_register_node(make_node_info(1)).await.unwrap();
        mgr.handle_register_node(make_node_info(2)).await.unwrap();

        let all = mgr.handle_get_all_node_info();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_drain_node() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsNodeManager::new(storage);

        mgr.handle_register_node(make_node_info(1)).await.unwrap();
        mgr.handle_drain_node(&node_id(1), 1000);
        assert_eq!(mgr.get_draining_nodes().len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_id() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsNodeManager::new(storage);

        let id = vec![1u8; 28];
        mgr.set_cluster_id(id.clone());
        assert_eq!(mgr.cluster_id(), id);
    }
}
