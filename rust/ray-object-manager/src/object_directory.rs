// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object directory — maps object IDs to node locations.
//!
//! Replaces `src/ray/object_manager/ownership_based_object_directory.h/cc`.
//!
//! Tracks which nodes hold copies of each object. Supports caching of
//! location lookups and subscription to location changes.

use std::collections::{HashMap, HashSet};

use ray_common::id::{NodeID, ObjectID};

/// Callback for object location changes.
pub type LocationCallback = Box<dyn Fn(&ObjectID, &[NodeID]) + Send + Sync>;

/// Information about an object's locations in the cluster.
#[derive(Debug, Clone, Default)]
pub struct ObjectLocationInfo {
    /// Nodes that have a copy of this object.
    pub node_ids: HashSet<NodeID>,
    /// Object size in bytes (0 if unknown).
    pub object_size: u64,
    /// Whether the object was spilled to external storage.
    pub spilled: bool,
    /// External storage URL (empty if not spilled).
    pub spilled_url: String,
    /// Node that spilled the object.
    pub spilled_node_id: NodeID,
}

/// The object directory maintains a mapping from object IDs to their
/// locations across the cluster.
///
/// Used by the pull manager to determine where to fetch objects from,
/// and by the object manager to respond to location queries.
pub struct ObjectDirectory {
    /// Cached object locations.
    locations: HashMap<ObjectID, ObjectLocationInfo>,
    /// Subscriptions: object_id → list of callbacks.
    subscriptions: HashMap<ObjectID, Vec<LocationCallback>>,
}

impl ObjectDirectory {
    pub fn new() -> Self {
        Self {
            locations: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    /// Report that an object is located on a specific node.
    ///
    /// If the location is new, triggers any registered callbacks.
    pub fn report_object_added(&mut self, object_id: ObjectID, node_id: NodeID) {
        let info = self.locations.entry(object_id).or_default();
        let is_new = info.node_ids.insert(node_id);

        if is_new {
            self.notify_subscribers(&object_id);
        }
    }

    /// Report that an object was removed from a node.
    pub fn report_object_removed(&mut self, object_id: &ObjectID, node_id: &NodeID) {
        if let Some(info) = self.locations.get_mut(object_id) {
            let was_present = info.node_ids.remove(node_id);
            if was_present {
                self.notify_subscribers(object_id);
            }
        }
    }

    /// Report that an object was spilled to external storage.
    pub fn report_object_spilled(
        &mut self,
        object_id: ObjectID,
        url: String,
        spill_node_id: NodeID,
    ) {
        let info = self.locations.entry(object_id).or_default();
        info.spilled = true;
        info.spilled_url = url;
        info.spilled_node_id = spill_node_id;
        self.notify_subscribers(&object_id);
    }

    /// Set the size of an object.
    pub fn set_object_size(&mut self, object_id: &ObjectID, size: u64) {
        if let Some(info) = self.locations.get_mut(object_id) {
            info.object_size = size;
        }
    }

    /// Look up the current locations of an object.
    pub fn get_locations(&self, object_id: &ObjectID) -> Option<&ObjectLocationInfo> {
        self.locations.get(object_id)
    }

    /// Get the nodes that hold a copy of an object.
    pub fn get_node_ids(&self, object_id: &ObjectID) -> Vec<NodeID> {
        self.locations
            .get(object_id)
            .map(|info| info.node_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Subscribe to location changes for an object.
    pub fn subscribe(&mut self, object_id: ObjectID, callback: LocationCallback) {
        self.subscriptions
            .entry(object_id)
            .or_default()
            .push(callback);
    }

    /// Unsubscribe from all location changes for an object.
    pub fn unsubscribe(&mut self, object_id: &ObjectID) {
        self.subscriptions.remove(object_id);
    }

    /// Remove all location info for a node (e.g., when node dies).
    pub fn handle_node_removed(&mut self, node_id: &NodeID) {
        let mut affected = Vec::new();
        for (oid, info) in &mut self.locations {
            if info.node_ids.remove(node_id) {
                affected.push(*oid);
            }
        }
        for oid in &affected {
            self.notify_subscribers(oid);
        }
    }

    /// Number of objects with known locations.
    pub fn num_objects_tracked(&self) -> usize {
        self.locations.len()
    }

    /// Number of active subscriptions.
    pub fn num_subscriptions(&self) -> usize {
        self.subscriptions.len()
    }

    /// Remove location info for an object.
    pub fn remove_object(&mut self, object_id: &ObjectID) {
        self.locations.remove(object_id);
        self.subscriptions.remove(object_id);
    }

    fn notify_subscribers(&self, object_id: &ObjectID) {
        if let Some(callbacks) = self.subscriptions.get(object_id) {
            let nodes: Vec<NodeID> = self
                .locations
                .get(object_id)
                .map(|info| info.node_ids.iter().copied().collect())
                .unwrap_or_default();
            for cb in callbacks {
                cb(object_id, &nodes);
            }
        }
    }
}

impl Default for ObjectDirectory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    #[test]
    fn test_report_object_added() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        dir.report_object_added(oid, node);

        let locs = dir.get_locations(&oid).unwrap();
        assert!(locs.node_ids.contains(&node));
        assert_eq!(dir.num_objects_tracked(), 1);
    }

    #[test]
    fn test_report_object_removed() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        dir.report_object_added(oid, node);
        dir.report_object_removed(&oid, &node);

        let locs = dir.get_locations(&oid).unwrap();
        assert!(locs.node_ids.is_empty());
    }

    #[test]
    fn test_multiple_locations() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node1 = make_nid(1);
        let node2 = make_nid(2);

        dir.report_object_added(oid, node1);
        dir.report_object_added(oid, node2);

        let nodes = dir.get_node_ids(&oid);
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&node1));
        assert!(nodes.contains(&node2));
    }

    #[test]
    fn test_duplicate_add_is_idempotent() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        dir.report_object_added(oid, node);
        dir.report_object_added(oid, node);

        let nodes = dir.get_node_ids(&oid);
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_report_object_spilled() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let spill_node = make_nid(5);

        dir.report_object_spilled(oid, "s3://bucket/key".into(), spill_node);

        let locs = dir.get_locations(&oid).unwrap();
        assert!(locs.spilled);
        assert_eq!(locs.spilled_url, "s3://bucket/key");
        assert_eq!(locs.spilled_node_id, spill_node);
    }

    #[test]
    fn test_get_locations_unknown_object() {
        let dir = ObjectDirectory::new();
        let oid = make_oid(99);
        assert!(dir.get_locations(&oid).is_none());
        assert!(dir.get_node_ids(&oid).is_empty());
    }

    #[test]
    fn test_subscription_callback() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&call_count);
        dir.subscribe(
            oid,
            Box::new(move |_oid, _nodes| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        // Adding triggers callback
        dir.report_object_added(oid, node);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // Removing triggers callback
        dir.report_object_removed(&oid, &node);
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&call_count);
        dir.subscribe(
            oid,
            Box::new(move |_oid, _nodes| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );
        assert_eq!(dir.num_subscriptions(), 1);

        dir.unsubscribe(&oid);
        assert_eq!(dir.num_subscriptions(), 0);

        // No callback after unsubscribe
        dir.report_object_added(oid, make_nid(1));
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_handle_node_removed() {
        let mut dir = ObjectDirectory::new();
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        let node1 = make_nid(1);
        let node2 = make_nid(2);

        dir.report_object_added(oid1, node1);
        dir.report_object_added(oid1, node2);
        dir.report_object_added(oid2, node1);

        dir.handle_node_removed(&node1);

        // oid1 should only have node2
        let nodes1 = dir.get_node_ids(&oid1);
        assert_eq!(nodes1.len(), 1);
        assert!(nodes1.contains(&node2));

        // oid2 should have no nodes
        let nodes2 = dir.get_node_ids(&oid2);
        assert!(nodes2.is_empty());
    }

    #[test]
    fn test_remove_object() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        dir.report_object_added(oid, node);
        dir.subscribe(oid, Box::new(|_, _| {}));

        dir.remove_object(&oid);
        assert!(dir.get_locations(&oid).is_none());
        assert_eq!(dir.num_subscriptions(), 0);
    }

    #[test]
    fn test_set_object_size() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        dir.report_object_added(oid, node);
        dir.set_object_size(&oid, 4096);

        let locs = dir.get_locations(&oid).unwrap();
        assert_eq!(locs.object_size, 4096);
    }

    #[test]
    fn test_node_removed_triggers_callbacks() {
        let mut dir = ObjectDirectory::new();
        let oid = make_oid(1);
        let node = make_nid(1);

        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&call_count);
        dir.subscribe(
            oid,
            Box::new(move |_oid, _nodes| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        dir.report_object_added(oid, node);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        dir.handle_node_removed(&node);
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }
}
