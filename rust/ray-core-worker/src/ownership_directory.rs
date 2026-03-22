// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Ownership directory for tracking object owners.
//!
//! Ports ownership tracking from C++ `reference_counter.cc`.
//!
//! The OwnershipDirectory maps ObjectIDs to their owner's address. This is
//! used to:
//! 1. Look up who owns an object (to contact them for status queries)
//! 2. Track borrowers of objects we own (so we can notify them on deletion)
//! 3. Track what objects we're borrowing from others

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

use ray_common::id::ObjectID;
use ray_proto::ray::rpc::Address;

/// Information about an object we own.
#[derive(Debug, Clone)]
struct OwnedObjectInfo {
    /// Our address as the owner.
    owner_address: Address,
    /// Processes that have borrowed this object from us.
    borrowers: HashSet<String>,
    /// Whether the object is still being created.
    is_pending_creation: bool,
    /// Whether the object has been spilled to external storage.
    is_spilled: bool,
    /// Spill URL if the object has been spilled.
    spill_url: Option<String>,
    /// Node where the object was spilled from (preserved across pin/unpin).
    /// C++ stores this separately from pinned_at_node_id.
    spilled_node_id: Option<Vec<u8>>,
    /// Node where the object is pinned (in plasma).
    pinned_at_node_id: Option<Vec<u8>>,
}

/// Information about an object we're borrowing.
#[derive(Debug, Clone)]
struct BorrowedObjectInfo {
    /// The owner's address.
    owner_address: Address,
    /// Whether we've already started monitoring this object's owner.
    is_monitoring_owner: bool,
    /// Whether the owner has been detected as dead.
    is_owner_dead: bool,
}

/// Tracks object ownership across the cluster.
///
/// Each worker maintains an OwnershipDirectory that knows:
/// - Which objects it owns (and who has borrowed them)
/// - Which objects it has borrowed (and who the owner is)
///
/// This enables distributed reference counting and failure detection.
pub struct OwnershipDirectory {
    inner: Mutex<OwnershipDirectoryInner>,
}

struct OwnershipDirectoryInner {
    /// Objects owned by this worker.
    owned_objects: HashMap<ObjectID, OwnedObjectInfo>,
    /// Objects borrowed from other workers.
    borrowed_objects: HashMap<ObjectID, BorrowedObjectInfo>,
    /// Set of node IDs that have been marked dead.
    /// C++ uses this to filter object location additions from dead nodes.
    dead_nodes: HashSet<Vec<u8>>,
}

impl OwnershipDirectory {
    /// Create a new empty ownership directory.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(OwnershipDirectoryInner {
                owned_objects: HashMap::new(),
                borrowed_objects: HashMap::new(),
                dead_nodes: HashSet::new(),
            }),
        }
    }

    // ─── Owned Objects ────────────────────────────────────────────────

    /// Register an object that we own.
    pub fn add_owned_object(&self, object_id: ObjectID, owner_address: Address) {
        let mut inner = self.inner.lock();
        inner.owned_objects.insert(
            object_id,
            OwnedObjectInfo {
                owner_address,
                borrowers: HashSet::new(),
                is_pending_creation: true,
                is_spilled: false,
                spill_url: None,
                spilled_node_id: None,
                pinned_at_node_id: None,
            },
        );
    }

    /// Remove an owned object (when we delete it or it goes out of scope).
    /// Returns the set of borrowers that need to be notified.
    pub fn remove_owned_object(&self, object_id: &ObjectID) -> Vec<String> {
        let mut inner = self.inner.lock();
        inner
            .owned_objects
            .remove(object_id)
            .map(|info| info.borrowers.into_iter().collect())
            .unwrap_or_default()
    }

    /// Check if we own this object.
    pub fn is_owned(&self, object_id: &ObjectID) -> bool {
        self.inner.lock().owned_objects.contains_key(object_id)
    }

    /// Mark an owned object as fully created (no longer pending).
    pub fn mark_object_created(&self, object_id: &ObjectID) {
        if let Some(entry) = self.inner.lock().owned_objects.get_mut(object_id) {
            entry.is_pending_creation = false;
        }
    }

    /// Check if an owned object is still being created.
    pub fn is_pending_creation(&self, object_id: &ObjectID) -> bool {
        self.inner
            .lock()
            .owned_objects
            .get(object_id)
            .is_some_and(|e| e.is_pending_creation)
    }

    /// Add a borrower to an owned object.
    pub fn add_borrower(&self, object_id: &ObjectID, borrower_id: String) {
        if let Some(entry) = self.inner.lock().owned_objects.get_mut(object_id) {
            entry.borrowers.insert(borrower_id);
        }
    }

    /// Remove a borrower from an owned object.
    pub fn remove_borrower(&self, object_id: &ObjectID, borrower_id: &str) {
        if let Some(entry) = self.inner.lock().owned_objects.get_mut(object_id) {
            entry.borrowers.remove(borrower_id);
        }
    }

    /// Get all borrowers of an owned object.
    pub fn get_borrowers(&self, object_id: &ObjectID) -> Vec<String> {
        self.inner
            .lock()
            .owned_objects
            .get(object_id)
            .map(|e| e.borrowers.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Record that an owned object has been pinned at a specific node.
    pub fn set_pinned_at_node(&self, object_id: &ObjectID, node_id: Vec<u8>) {
        if let Some(entry) = self.inner.lock().owned_objects.get_mut(object_id) {
            entry.pinned_at_node_id = Some(node_id);
            entry.is_spilled = false;
            entry.spill_url = None;
        }
    }

    /// Record that an owned object has been spilled to external storage.
    /// `spilled_node_id` is the node that performed the spill (preserved separately
    /// from pinned_at_node_id, which gets cleared on spill). This matches C++ behavior
    /// where spilled_node_id is stored independently.
    pub fn set_spilled(&self, object_id: &ObjectID, spill_url: String, spilled_node_id: Vec<u8>) {
        if let Some(entry) = self.inner.lock().owned_objects.get_mut(object_id) {
            entry.is_spilled = true;
            entry.spill_url = Some(spill_url);
            entry.spilled_node_id = if spilled_node_id.iter().all(|&b| b == 0) {
                None
            } else {
                Some(spilled_node_id)
            };
            entry.pinned_at_node_id = None;
        }
    }

    /// Get the spill URL for an owned object.
    pub fn get_spill_url(&self, object_id: &ObjectID) -> Option<String> {
        self.inner
            .lock()
            .owned_objects
            .get(object_id)
            .and_then(|e| e.spill_url.clone())
    }

    /// Get the node that spilled the object (stored separately from pinned node).
    pub fn get_spilled_node_id(&self, object_id: &ObjectID) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .owned_objects
            .get(object_id)
            .and_then(|e| e.spilled_node_id.clone())
    }

    /// Get the node where an owned object is pinned.
    pub fn get_pinned_node(&self, object_id: &ObjectID) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .owned_objects
            .get(object_id)
            .and_then(|e| e.pinned_at_node_id.clone())
    }

    // ─── Borrowed Objects ─────────────────────────────────────────────

    /// Register an object that we're borrowing from another worker.
    pub fn add_borrowed_object(&self, object_id: ObjectID, owner_address: Address) {
        let mut inner = self.inner.lock();
        inner.borrowed_objects.insert(
            object_id,
            BorrowedObjectInfo {
                owner_address,
                is_monitoring_owner: false,
                is_owner_dead: false,
            },
        );
    }

    /// Remove a borrowed object (when we're done with it).
    pub fn remove_borrowed_object(&self, object_id: &ObjectID) {
        self.inner.lock().borrowed_objects.remove(object_id);
    }

    /// Check if we're borrowing this object.
    pub fn is_borrowed(&self, object_id: &ObjectID) -> bool {
        self.inner.lock().borrowed_objects.contains_key(object_id)
    }

    /// Get the owner address for any tracked object (owned or borrowed).
    pub fn get_owner(&self, object_id: &ObjectID) -> Option<Address> {
        let inner = self.inner.lock();
        if let Some(owned) = inner.owned_objects.get(object_id) {
            return Some(owned.owner_address.clone());
        }
        if let Some(borrowed) = inner.borrowed_objects.get(object_id) {
            return Some(borrowed.owner_address.clone());
        }
        None
    }

    /// Mark that we're monitoring the owner of a borrowed object for failure.
    pub fn set_monitoring_owner(&self, object_id: &ObjectID) {
        if let Some(entry) = self.inner.lock().borrowed_objects.get_mut(object_id) {
            entry.is_monitoring_owner = true;
        }
    }

    /// Check if we're monitoring the owner of a borrowed object.
    pub fn is_monitoring_owner(&self, object_id: &ObjectID) -> bool {
        self.inner
            .lock()
            .borrowed_objects
            .get(object_id)
            .is_some_and(|e| e.is_monitoring_owner)
    }

    // ─── Borrower & Owner Death Handling ────────────────────────────

    /// Remove a dead borrower from all owned objects' borrower sets.
    ///
    /// When a borrower (identified by worker_id string) dies, it should be
    /// removed from every owned object's borrower set so that we no longer
    /// attempt to notify it on deletion.
    ///
    /// Returns the list of object IDs from which the borrower was removed.
    pub fn handle_borrower_died(&self, worker_id: &str) -> Vec<ObjectID> {
        let mut inner = self.inner.lock();
        let mut affected = Vec::new();
        for (oid, info) in inner.owned_objects.iter_mut() {
            if info.borrowers.remove(worker_id) {
                affected.push(*oid);
            }
        }
        affected
    }

    /// Propagate owner death: find all borrowed objects owned by the given
    /// worker and mark them as having a dead owner.
    ///
    /// Returns the list of affected (borrowed) object IDs whose owner died.
    pub fn propagate_owner_death(&self, owner_worker_id: &[u8]) -> Vec<ObjectID> {
        let mut inner = self.inner.lock();
        let mut affected = Vec::new();
        for (oid, info) in inner.borrowed_objects.iter_mut() {
            if info.owner_address.worker_id == owner_worker_id && !info.is_owner_dead {
                info.is_owner_dead = true;
                affected.push(*oid);
            }
        }
        affected
    }

    /// Check if the owner of a borrowed object has been marked dead.
    pub fn is_owner_dead(&self, object_id: &ObjectID) -> bool {
        self.inner
            .lock()
            .borrowed_objects
            .get(object_id)
            .is_some_and(|e| e.is_owner_dead)
    }

    // ─── Dead Node Tracking ───────────────────────────────────────────

    /// Mark a node as dead. Subsequent calls to `is_node_dead` will return true.
    /// C++ filters object location additions from dead nodes.
    pub fn mark_node_dead(&self, node_id: &[u8]) {
        self.inner.lock().dead_nodes.insert(node_id.to_vec());
    }

    /// Check if a node has been marked dead.
    pub fn is_node_dead(&self, node_id: &[u8]) -> bool {
        self.inner.lock().dead_nodes.contains(node_id)
    }

    // ─── Bulk Operations ──────────────────────────────────────────────

    /// Number of objects we own.
    pub fn num_owned(&self) -> usize {
        self.inner.lock().owned_objects.len()
    }

    /// Number of objects we're borrowing.
    pub fn num_borrowed(&self) -> usize {
        self.inner.lock().borrowed_objects.len()
    }

    /// Get all owned object IDs.
    pub fn all_owned_ids(&self) -> Vec<ObjectID> {
        self.inner.lock().owned_objects.keys().copied().collect()
    }

    /// Get all borrowed object IDs.
    pub fn all_borrowed_ids(&self) -> Vec<ObjectID> {
        self.inner.lock().borrowed_objects.keys().copied().collect()
    }

    /// Get all objects affected by a specific node dying.
    /// Returns owned objects pinned at that node, and borrowed objects
    /// whose owner was on that node.
    pub fn objects_affected_by_node_death(
        &self,
        dead_node_id: &[u8],
    ) -> (Vec<ObjectID>, Vec<ObjectID>) {
        let inner = self.inner.lock();

        let lost_owned: Vec<ObjectID> = inner
            .owned_objects
            .iter()
            .filter(|(_, info)| {
                info.pinned_at_node_id
                    .as_ref()
                    .is_some_and(|nid| nid == dead_node_id)
            })
            .map(|(id, _)| *id)
            .collect();

        let lost_borrowed: Vec<ObjectID> = inner
            .borrowed_objects
            .iter()
            .filter(|(_, info)| info.owner_address.node_id == dead_node_id)
            .map(|(id, _)| *id)
            .collect();

        (lost_owned, lost_borrowed)
    }

    /// Handle the death of a node. Processes side-effects:
    /// - For owned objects pinned at the dead node: clears their pinned location
    /// - For borrowed objects whose owner was on the dead node: marks owner as dead
    ///   (removes them from the borrowed table, as the owner can no longer be contacted)
    ///
    /// Returns `(owned_objects_affected, borrowed_objects_with_dead_owner)`.
    pub fn handle_node_death(
        &self,
        dead_node_id: &[u8],
    ) -> (Vec<ObjectID>, Vec<ObjectID>) {
        let mut inner = self.inner.lock();

        // Track dead node for filtering future location additions.
        inner.dead_nodes.insert(dead_node_id.to_vec());

        // Clear pinned location for owned objects pinned at the dead node.
        let mut affected_owned = Vec::new();
        for (oid, info) in inner.owned_objects.iter_mut() {
            if info
                .pinned_at_node_id
                .as_ref()
                .is_some_and(|nid| nid == dead_node_id)
            {
                info.pinned_at_node_id = None;
                affected_owned.push(*oid);
            }
        }

        // Collect borrowed objects whose owner was on the dead node.
        let mut dead_owner_borrowed = Vec::new();
        let mut to_remove = Vec::new();
        for (oid, info) in inner.borrowed_objects.iter() {
            if info.owner_address.node_id == dead_node_id {
                dead_owner_borrowed.push(*oid);
                to_remove.push(*oid);
            }
        }

        // Remove borrowed objects with dead owners (owner can no longer be contacted).
        for oid in &to_remove {
            inner.borrowed_objects.remove(oid);
        }

        (affected_owned, dead_owner_borrowed)
    }

    /// Get all owned objects with their pinned/spill state.
    /// Used for re-registration after GCS restart.
    ///
    /// Returns a list of `(object_id, pinned_at_node_id, spill_url)`.
    pub fn get_all_owned_with_locations(
        &self,
    ) -> Vec<(ObjectID, Option<Vec<u8>>, Option<String>)> {
        self.inner
            .lock()
            .owned_objects
            .iter()
            .map(|(oid, info)| {
                (
                    *oid,
                    info.pinned_at_node_id.clone(),
                    info.spill_url.clone(),
                )
            })
            .collect()
    }
}

impl Default for OwnershipDirectory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_address(ip: &str, port: i32) -> Address {
        Address {
            node_id: vec![0u8; 28],
            ip_address: ip.to_string(),
            port,
            worker_id: vec![0u8; 28],
        }
    }

    fn make_address_with_node(ip: &str, node_val: u8) -> Address {
        let mut node_id = vec![0u8; 28];
        node_id[0] = node_val;
        Address {
            node_id,
            ip_address: ip.to_string(),
            port: 1234,
            worker_id: vec![0u8; 28],
        }
    }

    #[test]
    fn test_add_and_check_owned() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        let addr = make_address("10.0.0.1", 5000);

        dir.add_owned_object(oid, addr);
        assert!(dir.is_owned(&oid));
        assert!(!dir.is_borrowed(&oid));
        assert_eq!(dir.num_owned(), 1);
    }

    #[test]
    fn test_add_and_check_borrowed() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        let addr = make_address("10.0.0.2", 6000);

        dir.add_borrowed_object(oid, addr);
        assert!(!dir.is_owned(&oid));
        assert!(dir.is_borrowed(&oid));
        assert_eq!(dir.num_borrowed(), 1);
    }

    #[test]
    fn test_get_owner_for_owned_and_borrowed() {
        let dir = OwnershipDirectory::new();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();

        dir.add_owned_object(oid1, make_address("10.0.0.1", 1000));
        dir.add_borrowed_object(oid2, make_address("10.0.0.2", 2000));

        assert_eq!(dir.get_owner(&oid1).unwrap().ip_address, "10.0.0.1");
        assert_eq!(dir.get_owner(&oid2).unwrap().ip_address, "10.0.0.2");
        assert!(dir.get_owner(&ObjectID::from_random()).is_none());
    }

    #[test]
    fn test_remove_owned_returns_borrowers() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));

        dir.add_borrower(&oid, "worker-1".to_string());
        dir.add_borrower(&oid, "worker-2".to_string());

        let borrowers = dir.remove_owned_object(&oid);
        assert_eq!(borrowers.len(), 2);
        assert!(borrowers.contains(&"worker-1".to_string()));
        assert!(borrowers.contains(&"worker-2".to_string()));
        assert!(!dir.is_owned(&oid));
    }

    #[test]
    fn test_borrower_management() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));

        dir.add_borrower(&oid, "w1".to_string());
        dir.add_borrower(&oid, "w2".to_string());
        assert_eq!(dir.get_borrowers(&oid).len(), 2);

        dir.remove_borrower(&oid, "w1");
        let borrowers = dir.get_borrowers(&oid);
        assert_eq!(borrowers.len(), 1);
        assert_eq!(borrowers[0], "w2");
    }

    #[test]
    fn test_pending_creation() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));

        assert!(dir.is_pending_creation(&oid));
        dir.mark_object_created(&oid);
        assert!(!dir.is_pending_creation(&oid));
    }

    #[test]
    fn test_pinned_and_spilled() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));

        // Pin at a node.
        let node_id = vec![1u8; 28];
        dir.set_pinned_at_node(&oid, node_id.clone());
        assert_eq!(dir.get_pinned_node(&oid), Some(node_id));
        assert!(dir.get_spill_url(&oid).is_none());

        // Spill clears the pinned location.
        dir.set_spilled(&oid, "file:///spill/obj123".to_string(), vec![1u8; 28]);
        assert!(dir.get_pinned_node(&oid).is_none());
        assert_eq!(
            dir.get_spill_url(&oid),
            Some("file:///spill/obj123".to_string())
        );

        // Re-pin clears the spill info.
        let node_id2 = vec![2u8; 28];
        dir.set_pinned_at_node(&oid, node_id2.clone());
        assert_eq!(dir.get_pinned_node(&oid), Some(node_id2));
        assert!(dir.get_spill_url(&oid).is_none());
    }

    #[test]
    fn test_monitoring_owner() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_borrowed_object(oid, make_address("10.0.0.2", 2000));

        assert!(!dir.is_monitoring_owner(&oid));
        dir.set_monitoring_owner(&oid);
        assert!(dir.is_monitoring_owner(&oid));
    }

    #[test]
    fn test_objects_affected_by_node_death() {
        let dir = OwnershipDirectory::new();

        // Owned object pinned at node 1.
        let oid1 = ObjectID::from_random();
        dir.add_owned_object(oid1, make_address("10.0.0.1", 1000));
        let mut dead_node = vec![0u8; 28];
        dead_node[0] = 1;
        dir.set_pinned_at_node(&oid1, dead_node.clone());

        // Owned object pinned at node 2 (different node).
        let oid2 = ObjectID::from_random();
        dir.add_owned_object(oid2, make_address("10.0.0.1", 1000));
        let mut other_node = vec![0u8; 28];
        other_node[0] = 2;
        dir.set_pinned_at_node(&oid2, other_node);

        // Borrowed object whose owner is on node 1.
        let oid3 = ObjectID::from_random();
        dir.add_borrowed_object(oid3, make_address_with_node("10.0.0.3", 1));

        // Borrowed object whose owner is on node 2 (different node).
        let oid4 = ObjectID::from_random();
        dir.add_borrowed_object(oid4, make_address_with_node("10.0.0.4", 2));

        let (lost_owned, lost_borrowed) = dir.objects_affected_by_node_death(&dead_node);

        assert_eq!(lost_owned.len(), 1);
        assert_eq!(lost_owned[0], oid1);

        assert_eq!(lost_borrowed.len(), 1);
        assert_eq!(lost_borrowed[0], oid3);
    }

    #[test]
    fn test_bulk_operations() {
        let dir = OwnershipDirectory::new();

        for _ in 0..5 {
            dir.add_owned_object(ObjectID::from_random(), make_address("10.0.0.1", 1000));
        }
        for _ in 0..3 {
            dir.add_borrowed_object(ObjectID::from_random(), make_address("10.0.0.2", 2000));
        }

        assert_eq!(dir.num_owned(), 5);
        assert_eq!(dir.num_borrowed(), 3);
        assert_eq!(dir.all_owned_ids().len(), 5);
        assert_eq!(dir.all_borrowed_ids().len(), 3);
    }

    // ── Tests for handle_node_death ─────────────────────────────────

    #[test]
    fn test_handle_node_death_clears_pinned_location() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));

        let mut dead_node = vec![0u8; 28];
        dead_node[0] = 1;
        dir.set_pinned_at_node(&oid, dead_node.clone());
        assert!(dir.get_pinned_node(&oid).is_some());

        let (affected_owned, _) = dir.handle_node_death(&dead_node);
        assert_eq!(affected_owned.len(), 1);
        assert_eq!(affected_owned[0], oid);
        // Pinned location should be cleared.
        assert!(dir.get_pinned_node(&oid).is_none());
        // Object should still be owned.
        assert!(dir.is_owned(&oid));
    }

    #[test]
    fn test_handle_node_death_removes_dead_owner_borrowed() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        let mut dead_node = vec![0u8; 28];
        dead_node[0] = 5;
        dir.add_borrowed_object(oid, make_address_with_node("10.0.0.5", 5));

        assert!(dir.is_borrowed(&oid));

        let (_, dead_owner) = dir.handle_node_death(&dead_node);
        assert_eq!(dead_owner.len(), 1);
        assert_eq!(dead_owner[0], oid);
        // Borrowed object should be removed.
        assert!(!dir.is_borrowed(&oid));
    }

    #[test]
    fn test_handle_node_death_does_not_affect_other_nodes() {
        let dir = OwnershipDirectory::new();

        // Owned pinned at node 1.
        let oid1 = ObjectID::from_random();
        dir.add_owned_object(oid1, make_address("10.0.0.1", 1000));
        let mut node1 = vec![0u8; 28];
        node1[0] = 1;
        dir.set_pinned_at_node(&oid1, node1);

        // Owned pinned at node 2.
        let oid2 = ObjectID::from_random();
        dir.add_owned_object(oid2, make_address("10.0.0.1", 1000));
        let mut node2 = vec![0u8; 28];
        node2[0] = 2;
        dir.set_pinned_at_node(&oid2, node2.clone());

        // Borrowed from owner on node 2.
        let oid3 = ObjectID::from_random();
        dir.add_borrowed_object(oid3, make_address_with_node("10.0.0.2", 2));

        // Kill node 99 — should affect nothing.
        let mut dead = vec![0u8; 28];
        dead[0] = 99;
        let (aff_owned, aff_borrowed) = dir.handle_node_death(&dead);
        assert!(aff_owned.is_empty());
        assert!(aff_borrowed.is_empty());

        // All objects should remain.
        assert!(dir.get_pinned_node(&oid1).is_some());
        assert!(dir.get_pinned_node(&oid2).is_some());
        assert!(dir.is_borrowed(&oid3));
    }

    #[test]
    fn test_handle_node_death_mixed() {
        let dir = OwnershipDirectory::new();
        let mut dead_node = vec![0u8; 28];
        dead_node[0] = 3;

        // Owned object pinned at dead node.
        let oid_owned = ObjectID::from_random();
        dir.add_owned_object(oid_owned, make_address("10.0.0.1", 1000));
        dir.set_pinned_at_node(&oid_owned, dead_node.clone());

        // Borrowed object with owner on dead node.
        let oid_borrowed = ObjectID::from_random();
        dir.add_borrowed_object(oid_borrowed, make_address_with_node("10.0.0.3", 3));

        // Owned object NOT at dead node.
        let oid_safe = ObjectID::from_random();
        dir.add_owned_object(oid_safe, make_address("10.0.0.1", 1000));
        let mut safe_node = vec![0u8; 28];
        safe_node[0] = 7;
        dir.set_pinned_at_node(&oid_safe, safe_node.clone());

        let (aff_owned, aff_borrowed) = dir.handle_node_death(&dead_node);
        assert_eq!(aff_owned, vec![oid_owned]);
        assert_eq!(aff_borrowed, vec![oid_borrowed]);

        // Verify state.
        assert!(dir.get_pinned_node(&oid_owned).is_none());
        assert!(!dir.is_borrowed(&oid_borrowed));
        assert_eq!(dir.get_pinned_node(&oid_safe), Some(safe_node));
    }

    // ── Tests for handle_borrower_died ─────────────────────────────

    #[test]
    fn test_borrower_cleanup_on_death() {
        let dir = OwnershipDirectory::new();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        let oid3 = ObjectID::from_random();

        dir.add_owned_object(oid1, make_address("10.0.0.1", 1000));
        dir.add_owned_object(oid2, make_address("10.0.0.1", 1000));
        dir.add_owned_object(oid3, make_address("10.0.0.1", 1000));

        // Add borrower "dead-worker" to oid1 and oid2 but not oid3.
        dir.add_borrower(&oid1, "dead-worker".to_string());
        dir.add_borrower(&oid1, "alive-worker".to_string());
        dir.add_borrower(&oid2, "dead-worker".to_string());
        dir.add_borrower(&oid3, "alive-worker".to_string());

        // Kill "dead-worker".
        let affected = dir.handle_borrower_died("dead-worker");
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&oid1));
        assert!(affected.contains(&oid2));

        // "dead-worker" should be gone from oid1 and oid2.
        let b1 = dir.get_borrowers(&oid1);
        assert_eq!(b1.len(), 1);
        assert_eq!(b1[0], "alive-worker");

        let b2 = dir.get_borrowers(&oid2);
        assert!(b2.is_empty());

        // oid3 should be unaffected.
        let b3 = dir.get_borrowers(&oid3);
        assert_eq!(b3.len(), 1);
        assert_eq!(b3[0], "alive-worker");
    }

    #[test]
    fn test_borrower_died_not_a_borrower() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));
        dir.add_borrower(&oid, "worker-1".to_string());

        // Killing a non-existent borrower should be a no-op.
        let affected = dir.handle_borrower_died("nonexistent");
        assert!(affected.is_empty());
        assert_eq!(dir.get_borrowers(&oid).len(), 1);
    }

    // ── Tests for propagate_owner_death ──────────────────────────────

    #[test]
    fn test_owner_death_propagates_to_borrowed_objects() {
        let dir = OwnershipDirectory::new();

        // Create borrowed objects with different owners (distinguished by worker_id).
        let oid1 = ObjectID::from_random();
        let mut addr1 = make_address_with_node("10.0.0.1", 1);
        addr1.worker_id = vec![42u8; 28]; // dead owner
        dir.add_borrowed_object(oid1, addr1);

        let oid2 = ObjectID::from_random();
        let mut addr2 = make_address_with_node("10.0.0.1", 1);
        addr2.worker_id = vec![42u8; 28]; // same dead owner
        dir.add_borrowed_object(oid2, addr2);

        let oid3 = ObjectID::from_random();
        let mut addr3 = make_address_with_node("10.0.0.2", 2);
        addr3.worker_id = vec![99u8; 28]; // alive owner
        dir.add_borrowed_object(oid3, addr3);

        // Propagate death of owner with worker_id [42; 28].
        let dead_worker_id = vec![42u8; 28];
        let affected = dir.propagate_owner_death(&dead_worker_id);

        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&oid1));
        assert!(affected.contains(&oid2));

        // oid1 and oid2 should be marked as having dead owner.
        assert!(dir.is_owner_dead(&oid1));
        assert!(dir.is_owner_dead(&oid2));
        // oid3 should be unaffected.
        assert!(!dir.is_owner_dead(&oid3));
    }

    #[test]
    fn test_propagate_owner_death_idempotent() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        let mut addr = make_address("10.0.0.1", 1000);
        addr.worker_id = vec![7u8; 28];
        dir.add_borrowed_object(oid, addr);

        let dead_wid = vec![7u8; 28];
        let affected1 = dir.propagate_owner_death(&dead_wid);
        assert_eq!(affected1.len(), 1);

        // Calling again should return empty (already marked).
        let affected2 = dir.propagate_owner_death(&dead_wid);
        assert!(affected2.is_empty());
    }

    // ── Tests for get_all_owned_with_locations ──────────────────────

    #[test]
    fn test_get_all_owned_with_locations_empty() {
        let dir = OwnershipDirectory::new();
        assert!(dir.get_all_owned_with_locations().is_empty());
    }

    #[test]
    fn test_get_all_owned_with_locations_pinned() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));
        let node_id = vec![42u8; 28];
        dir.set_pinned_at_node(&oid, node_id.clone());

        let locs = dir.get_all_owned_with_locations();
        assert_eq!(locs.len(), 1);
        let (id, pinned, spill) = &locs[0];
        assert_eq!(*id, oid);
        assert_eq!(pinned.as_ref().unwrap(), &node_id);
        assert!(spill.is_none());
    }

    #[test]
    fn test_get_all_owned_with_locations_spilled() {
        let dir = OwnershipDirectory::new();
        let oid = ObjectID::from_random();
        dir.add_owned_object(oid, make_address("10.0.0.1", 1000));
        dir.set_spilled(&oid, "s3://bucket/obj".to_string(), vec![5u8; 28]);

        let locs = dir.get_all_owned_with_locations();
        assert_eq!(locs.len(), 1);
        let (id, pinned, spill) = &locs[0];
        assert_eq!(*id, oid);
        assert!(pinned.is_none());
        assert_eq!(spill.as_ref().unwrap(), "s3://bucket/obj");
    }

    #[test]
    fn test_get_all_owned_with_locations_mixed() {
        let dir = OwnershipDirectory::new();

        let oid1 = ObjectID::from_random();
        dir.add_owned_object(oid1, make_address("10.0.0.1", 1000));
        dir.set_pinned_at_node(&oid1, vec![1u8; 28]);

        let oid2 = ObjectID::from_random();
        dir.add_owned_object(oid2, make_address("10.0.0.1", 1000));
        dir.set_spilled(&oid2, "file:///spill/obj2".to_string(), vec![2u8; 28]);

        let oid3 = ObjectID::from_random();
        dir.add_owned_object(oid3, make_address("10.0.0.1", 1000));
        // oid3 has no location info.

        let locs = dir.get_all_owned_with_locations();
        assert_eq!(locs.len(), 3);

        // Borrowed objects should NOT appear.
        let borrowed_oid = ObjectID::from_random();
        dir.add_borrowed_object(borrowed_oid, make_address("10.0.0.2", 2000));
        let locs = dir.get_all_owned_with_locations();
        assert_eq!(locs.len(), 3); // Still 3, borrowed not included.
    }
}
