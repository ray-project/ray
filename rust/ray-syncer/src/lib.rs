// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! State synchronization for Ray.
//!
//! Replaces `src/ray/ray_syncer/` — bidirectional streaming gRPC service
//! for syncing state between GCS and raylets.
//!
//! The syncer broadcasts two message types:
//! - `ResourceView` — node resource availability (from raylets to GCS)
//! - `Commands` — global commands like GC (from GCS to raylets)
//!
//! Messages are versioned with monotonic sequence numbers for reliable
//! delivery, and each node maintains a local snapshot of all peers' state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use ray_common::id::NodeID;
use ray_proto::ray::rpc::syncer::{
    CommandsSyncMessage, MessageType, RaySyncMessage, RaySyncMessageBatch,
    ResourceViewSyncMessage,
};

/// Callback invoked when a resource view update is received from a peer.
pub type ResourceViewCallback =
    Box<dyn Fn(&NodeID, &ResourceViewSyncMessage) + Send + Sync>;

/// Callback invoked when a command is received from a peer.
pub type CommandsCallback =
    Box<dyn Fn(&NodeID, &CommandsSyncMessage) + Send + Sync>;

/// Snapshot of a peer node's state as known by this syncer.
#[derive(Debug, Clone, Default)]
pub struct NodeSnapshot {
    /// Last known resource view from this node.
    pub resource_view: Option<ResourceViewSyncMessage>,
    /// Resource view version.
    pub resource_view_version: i64,
    /// Last known commands from this node.
    pub commands: Option<CommandsSyncMessage>,
    /// Commands version.
    pub commands_version: i64,
}

/// The local syncer component that manages state for this node.
///
/// Each node runs a `NodeSyncState` that:
/// 1. Publishes its own resource view and commands to peers
/// 2. Receives and caches peer snapshots
/// 3. Provides the latest view of all nodes in the cluster
pub struct NodeSyncState {
    /// This node's ID.
    node_id: NodeID,
    /// Current resource view version (monotonically increasing).
    resource_view_version: AtomicI64,
    /// Current commands version.
    commands_version: AtomicI64,
    /// Our latest resource view.
    local_resource_view: Mutex<Option<ResourceViewSyncMessage>>,
    /// Our latest commands.
    local_commands: Mutex<Option<CommandsSyncMessage>>,
    /// Snapshots of all known peer nodes.
    peer_snapshots: DashMap<NodeID, NodeSnapshot>,
    /// Callbacks for resource view updates.
    resource_view_callbacks: Mutex<Vec<ResourceViewCallback>>,
    /// Callbacks for commands updates.
    commands_callbacks: Mutex<Vec<CommandsCallback>>,
}

impl NodeSyncState {
    pub fn new(node_id: NodeID) -> Self {
        Self {
            node_id,
            resource_view_version: AtomicI64::new(0),
            commands_version: AtomicI64::new(0),
            local_resource_view: Mutex::new(None),
            local_commands: Mutex::new(None),
            peer_snapshots: DashMap::new(),
            resource_view_callbacks: Mutex::new(Vec::new()),
            commands_callbacks: Mutex::new(Vec::new()),
        }
    }

    /// Register a callback for resource view updates from peers.
    pub fn on_resource_view_update(&self, callback: ResourceViewCallback) {
        self.resource_view_callbacks.lock().push(callback);
    }

    /// Register a callback for commands updates from peers.
    pub fn on_commands_update(&self, callback: CommandsCallback) {
        self.commands_callbacks.lock().push(callback);
    }

    /// Publish a new resource view for this node.
    pub fn publish_resource_view(&self, view: ResourceViewSyncMessage) {
        self.resource_view_version.fetch_add(1, Ordering::Relaxed);
        *self.local_resource_view.lock() = Some(view);
    }

    /// Publish a new command for this node.
    pub fn publish_commands(&self, cmd: CommandsSyncMessage) {
        self.commands_version.fetch_add(1, Ordering::Relaxed);
        *self.local_commands.lock() = Some(cmd);
    }

    /// Build a sync message batch containing this node's latest state.
    ///
    /// Only includes messages that have versions newer than `since_resource_version`
    /// and `since_commands_version`.
    pub fn build_outgoing_batch(
        &self,
        since_resource_version: i64,
        since_commands_version: i64,
    ) -> RaySyncMessageBatch {
        let mut messages = Vec::new();

        let rv = self.resource_view_version.load(Ordering::Relaxed);
        if rv > since_resource_version {
            if let Some(ref view) = *self.local_resource_view.lock() {
                let encoded = prost::Message::encode_to_vec(view);
                messages.push(RaySyncMessage {
                    version: rv,
                    message_type: MessageType::ResourceView as i32,
                    sync_message: encoded,
                    node_id: self.node_id.binary(),
                });
            }
        }

        let cv = self.commands_version.load(Ordering::Relaxed);
        if cv > since_commands_version {
            if let Some(ref cmd) = *self.local_commands.lock() {
                let encoded = prost::Message::encode_to_vec(cmd);
                messages.push(RaySyncMessage {
                    version: cv,
                    message_type: MessageType::Commands as i32,
                    sync_message: encoded,
                    node_id: self.node_id.binary(),
                });
            }
        }

        RaySyncMessageBatch { messages }
    }

    /// Apply an incoming batch of sync messages from a peer.
    ///
    /// Returns the number of messages that were actually applied (had newer versions).
    pub fn apply_incoming_batch(&self, batch: &RaySyncMessageBatch) -> usize {
        let mut applied = 0;

        for msg in &batch.messages {
            let sender_node = NodeID::from_binary(&msg.node_id);
            let msg_type = MessageType::try_from(msg.message_type).ok();

            match msg_type {
                Some(MessageType::ResourceView) => {
                    if self.apply_resource_view(&sender_node, msg) {
                        applied += 1;
                    }
                }
                Some(MessageType::Commands) => {
                    if self.apply_commands(&sender_node, msg) {
                        applied += 1;
                    }
                }
                None => {
                    tracing::warn!(
                        message_type = msg.message_type,
                        "Unknown sync message type"
                    );
                }
            }
        }

        applied
    }

    fn apply_resource_view(&self, sender: &NodeID, msg: &RaySyncMessage) -> bool {
        let mut snapshot = self
            .peer_snapshots
            .entry(*sender)
            .or_default();

        // Only apply if version is newer.
        if msg.version <= snapshot.resource_view_version {
            return false;
        }

        let view: ResourceViewSyncMessage =
            match prost::Message::decode(msg.sync_message.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to decode ResourceViewSyncMessage");
                    return false;
                }
            };

        snapshot.resource_view_version = msg.version;
        snapshot.resource_view = Some(view.clone());
        drop(snapshot);

        // Notify callbacks.
        for cb in self.resource_view_callbacks.lock().iter() {
            cb(sender, &view);
        }

        true
    }

    fn apply_commands(&self, sender: &NodeID, msg: &RaySyncMessage) -> bool {
        let mut snapshot = self
            .peer_snapshots
            .entry(*sender)
            .or_default();

        if msg.version <= snapshot.commands_version {
            return false;
        }

        let cmd: CommandsSyncMessage =
            match prost::Message::decode(msg.sync_message.as_slice()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to decode CommandsSyncMessage");
                    return false;
                }
            };

        snapshot.commands_version = msg.version;
        snapshot.commands = Some(cmd);
        drop(snapshot);

        for cb in self.commands_callbacks.lock().iter() {
            cb(sender, &cmd);
        }

        true
    }

    /// Get a snapshot of a specific peer node's state.
    pub fn get_peer_snapshot(&self, node_id: &NodeID) -> Option<NodeSnapshot> {
        self.peer_snapshots.get(node_id).map(|s| s.clone())
    }

    /// Get the resource view for a specific peer.
    pub fn get_peer_resource_view(
        &self,
        node_id: &NodeID,
    ) -> Option<ResourceViewSyncMessage> {
        self.peer_snapshots
            .get(node_id)
            .and_then(|s| s.resource_view.clone())
    }

    /// Get all known peer node IDs.
    pub fn get_all_peer_ids(&self) -> Vec<NodeID> {
        self.peer_snapshots
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }

    /// Get cluster-wide resource totals.
    pub fn get_cluster_resources(&self) -> HashMap<String, f64> {
        let mut totals: HashMap<String, f64> = HashMap::new();
        for entry in self.peer_snapshots.iter() {
            if let Some(ref view) = entry.resource_view {
                for (resource, &amount) in &view.resources_total {
                    *totals.entry(resource.clone()).or_default() += amount;
                }
            }
        }
        totals
    }

    /// Get cluster-wide available resources.
    pub fn get_cluster_available_resources(&self) -> HashMap<String, f64> {
        let mut available: HashMap<String, f64> = HashMap::new();
        for entry in self.peer_snapshots.iter() {
            if let Some(ref view) = entry.resource_view {
                for (resource, &amount) in &view.resources_available {
                    *available.entry(resource.clone()).or_default() += amount;
                }
            }
        }
        available
    }

    /// Remove a peer node (e.g., when node dies).
    pub fn remove_peer(&self, node_id: &NodeID) {
        self.peer_snapshots.remove(node_id);
    }

    /// Number of known peers.
    pub fn num_peers(&self) -> usize {
        self.peer_snapshots.len()
    }

    /// This node's ID.
    pub fn node_id(&self) -> &NodeID {
        &self.node_id
    }

    /// Current resource view version.
    pub fn resource_view_version(&self) -> i64 {
        self.resource_view_version.load(Ordering::Relaxed)
    }

    /// Current commands version.
    pub fn commands_version(&self) -> i64 {
        self.commands_version.load(Ordering::Relaxed)
    }
}

/// The RaySyncer manages bidirectional state synchronization with peers.
///
/// In a full implementation, this wraps the gRPC bidirectional streaming
/// service. For now, it provides the local state management and message
/// building/applying logic.
pub struct RaySyncer {
    /// Local sync state.
    state: Arc<NodeSyncState>,
}

impl RaySyncer {
    pub fn new(node_id: NodeID) -> Self {
        Self {
            state: Arc::new(NodeSyncState::new(node_id)),
        }
    }

    /// Access the underlying sync state.
    pub fn state(&self) -> &Arc<NodeSyncState> {
        &self.state
    }

    /// Publish a resource view update.
    pub fn publish_resource_view(&self, view: ResourceViewSyncMessage) {
        self.state.publish_resource_view(view);
    }

    /// Publish a commands update.
    pub fn publish_commands(&self, cmd: CommandsSyncMessage) {
        self.state.publish_commands(cmd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    fn make_resource_view(cpu: f64, mem: f64) -> ResourceViewSyncMessage {
        let mut available = HashMap::new();
        let mut total = HashMap::new();
        available.insert("CPU".to_string(), cpu);
        available.insert("memory".to_string(), mem);
        total.insert("CPU".to_string(), cpu);
        total.insert("memory".to_string(), mem);
        ResourceViewSyncMessage {
            resources_available: available,
            resources_total: total,
            ..Default::default()
        }
    }

    #[test]
    fn test_publish_and_build_batch() {
        let nid = make_nid(1);
        let state = NodeSyncState::new(nid);

        let view = make_resource_view(4.0, 8192.0);
        state.publish_resource_view(view);

        assert_eq!(state.resource_view_version(), 1);

        let batch = state.build_outgoing_batch(0, 0);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(
            batch.messages[0].message_type,
            MessageType::ResourceView as i32
        );
        assert_eq!(batch.messages[0].version, 1);
    }

    #[test]
    fn test_build_batch_skips_old_versions() {
        let nid = make_nid(1);
        let state = NodeSyncState::new(nid);

        state.publish_resource_view(make_resource_view(4.0, 8192.0));

        // Request from version 1 — nothing new
        let batch = state.build_outgoing_batch(1, 0);
        assert!(batch.messages.is_empty());

        // Request from version 0 — includes the update
        let batch = state.build_outgoing_batch(0, 0);
        assert_eq!(batch.messages.len(), 1);
    }

    #[test]
    fn test_apply_incoming_batch() {
        let local = NodeSyncState::new(make_nid(1));
        let remote_nid = make_nid(2);

        // Build a batch as if from remote node
        let remote = NodeSyncState::new(remote_nid);
        remote.publish_resource_view(make_resource_view(8.0, 16384.0));
        let batch = remote.build_outgoing_batch(0, 0);

        // Apply to local
        let applied = local.apply_incoming_batch(&batch);
        assert_eq!(applied, 1);

        // Verify peer snapshot
        let snapshot = local.get_peer_snapshot(&remote_nid).unwrap();
        assert_eq!(snapshot.resource_view_version, 1);
        let view = snapshot.resource_view.unwrap();
        assert_eq!(view.resources_available.get("CPU"), Some(&8.0));
    }

    #[test]
    fn test_apply_duplicate_is_rejected() {
        let local = NodeSyncState::new(make_nid(1));
        let remote = NodeSyncState::new(make_nid(2));

        remote.publish_resource_view(make_resource_view(4.0, 8192.0));
        let batch = remote.build_outgoing_batch(0, 0);

        assert_eq!(local.apply_incoming_batch(&batch), 1);
        // Same batch again — version not newer
        assert_eq!(local.apply_incoming_batch(&batch), 0);
    }

    #[test]
    fn test_commands_sync() {
        let local = NodeSyncState::new(make_nid(1));
        let remote = NodeSyncState::new(make_nid(2));

        remote.publish_commands(CommandsSyncMessage {
            should_global_gc: true,
        });
        let batch = remote.build_outgoing_batch(0, 0);

        let applied = local.apply_incoming_batch(&batch);
        assert_eq!(applied, 1);

        let snapshot = local.get_peer_snapshot(&make_nid(2)).unwrap();
        assert!(snapshot.commands.unwrap().should_global_gc);
    }

    #[test]
    fn test_resource_view_callback() {
        let local = NodeSyncState::new(make_nid(1));

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&call_count);
        local.on_resource_view_update(Box::new(move |_nid, _view| {
            count_clone.fetch_add(1, AtomicOrdering::Relaxed);
        }));

        let remote = NodeSyncState::new(make_nid(2));
        remote.publish_resource_view(make_resource_view(4.0, 8192.0));
        let batch = remote.build_outgoing_batch(0, 0);

        local.apply_incoming_batch(&batch);
        assert_eq!(call_count.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn test_commands_callback() {
        let local = NodeSyncState::new(make_nid(1));

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&call_count);
        local.on_commands_update(Box::new(move |_nid, _cmd| {
            count_clone.fetch_add(1, AtomicOrdering::Relaxed);
        }));

        let remote = NodeSyncState::new(make_nid(2));
        remote.publish_commands(CommandsSyncMessage {
            should_global_gc: false,
        });
        let batch = remote.build_outgoing_batch(0, 0);

        local.apply_incoming_batch(&batch);
        assert_eq!(call_count.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn test_multiple_peers() {
        let local = NodeSyncState::new(make_nid(1));

        let node2 = NodeSyncState::new(make_nid(2));
        let node3 = NodeSyncState::new(make_nid(3));

        node2.publish_resource_view(make_resource_view(4.0, 8192.0));
        node3.publish_resource_view(make_resource_view(8.0, 16384.0));

        local.apply_incoming_batch(&node2.build_outgoing_batch(0, 0));
        local.apply_incoming_batch(&node3.build_outgoing_batch(0, 0));

        assert_eq!(local.num_peers(), 2);

        let peers = local.get_all_peer_ids();
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn test_get_cluster_resources() {
        let local = NodeSyncState::new(make_nid(1));

        let node2 = NodeSyncState::new(make_nid(2));
        let node3 = NodeSyncState::new(make_nid(3));

        node2.publish_resource_view(make_resource_view(4.0, 8192.0));
        node3.publish_resource_view(make_resource_view(8.0, 16384.0));

        local.apply_incoming_batch(&node2.build_outgoing_batch(0, 0));
        local.apply_incoming_batch(&node3.build_outgoing_batch(0, 0));

        let totals = local.get_cluster_resources();
        assert_eq!(totals.get("CPU"), Some(&12.0));
        assert_eq!(totals.get("memory"), Some(&24576.0));
    }

    #[test]
    fn test_get_cluster_available_resources() {
        let local = NodeSyncState::new(make_nid(1));

        let node2 = NodeSyncState::new(make_nid(2));
        node2.publish_resource_view(make_resource_view(2.0, 4096.0));
        local.apply_incoming_batch(&node2.build_outgoing_batch(0, 0));

        let available = local.get_cluster_available_resources();
        assert_eq!(available.get("CPU"), Some(&2.0));
    }

    #[test]
    fn test_remove_peer() {
        let local = NodeSyncState::new(make_nid(1));
        let remote = NodeSyncState::new(make_nid(2));
        remote.publish_resource_view(make_resource_view(4.0, 8192.0));
        local.apply_incoming_batch(&remote.build_outgoing_batch(0, 0));

        assert_eq!(local.num_peers(), 1);
        local.remove_peer(&make_nid(2));
        assert_eq!(local.num_peers(), 0);
        assert!(local.get_peer_snapshot(&make_nid(2)).is_none());
    }

    #[test]
    fn test_ray_syncer_wrapper() {
        let syncer = RaySyncer::new(make_nid(1));

        syncer.publish_resource_view(make_resource_view(4.0, 8192.0));
        syncer.publish_commands(CommandsSyncMessage {
            should_global_gc: true,
        });

        assert_eq!(syncer.state().resource_view_version(), 1);
        assert_eq!(syncer.state().commands_version(), 1);
    }

    #[test]
    fn test_build_batch_with_both_types() {
        let state = NodeSyncState::new(make_nid(1));
        state.publish_resource_view(make_resource_view(4.0, 8192.0));
        state.publish_commands(CommandsSyncMessage {
            should_global_gc: true,
        });

        let batch = state.build_outgoing_batch(0, 0);
        assert_eq!(batch.messages.len(), 2);

        // One resource view, one commands
        let types: Vec<i32> = batch.messages.iter().map(|m| m.message_type).collect();
        assert!(types.contains(&(MessageType::ResourceView as i32)));
        assert!(types.contains(&(MessageType::Commands as i32)));
    }

    #[test]
    fn test_version_monotonically_increases() {
        let state = NodeSyncState::new(make_nid(1));

        state.publish_resource_view(make_resource_view(1.0, 1.0));
        assert_eq!(state.resource_view_version(), 1);

        state.publish_resource_view(make_resource_view(2.0, 2.0));
        assert_eq!(state.resource_view_version(), 2);

        state.publish_resource_view(make_resource_view(3.0, 3.0));
        assert_eq!(state.resource_view_version(), 3);
    }

    #[test]
    fn test_get_peer_resource_view() {
        let local = NodeSyncState::new(make_nid(1));
        let remote = NodeSyncState::new(make_nid(2));

        assert!(local.get_peer_resource_view(&make_nid(2)).is_none());

        remote.publish_resource_view(make_resource_view(4.0, 8192.0));
        local.apply_incoming_batch(&remote.build_outgoing_batch(0, 0));

        let view = local.get_peer_resource_view(&make_nid(2)).unwrap();
        assert_eq!(view.resources_available.get("CPU"), Some(&4.0));
    }
}
