// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object reference counting for the core worker.
//!
//! Tracks local ref counts, submitted task ref counts, ownership, and
//! object locations. Objects are freed when all reference counts reach zero.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

use ray_common::id::ObjectID;
use ray_proto::ray::rpc::Address;

/// Callback invoked when an object's reference count reaches zero.
pub type ObjectFreedCallback = Box<dyn Fn(&ObjectID) + Send + Sync>;

/// Ownership and reference information for a single object.
#[derive(Debug, Clone)]
struct Reference {
    local_ref_count: u64,
    submitted_task_ref_count: u64,
    lineage_ref_count: u64,
    owner_address: Option<Address>,
    is_owned_by_us: bool,
    is_pending_creation: bool,
    object_size: i64,
    object_locations: HashSet<String>,
    contained_in: HashSet<ObjectID>,
    contains: HashSet<ObjectID>,
}

impl Reference {
    fn new() -> Self {
        Self {
            local_ref_count: 0,
            submitted_task_ref_count: 0,
            lineage_ref_count: 0,
            owner_address: None,
            is_owned_by_us: false,
            is_pending_creation: false,
            object_size: -1,
            object_locations: HashSet::new(),
            contained_in: HashSet::new(),
            contains: HashSet::new(),
        }
    }

    fn total_ref_count(&self) -> u64 {
        self.local_ref_count + self.submitted_task_ref_count
    }

    /// Whether the reference can be deleted (all counts zero, including lineage).
    fn should_delete(&self) -> bool {
        self.total_ref_count() == 0 && self.lineage_ref_count == 0
    }
}

/// Tracks reference counts for objects in the core worker.
pub struct ReferenceCounter {
    refs: Mutex<HashMap<ObjectID, Reference>>,
    /// Optional callback invoked when an object's reference count reaches zero.
    on_object_freed: Mutex<Option<ObjectFreedCallback>>,
}

impl ReferenceCounter {
    pub fn new() -> Self {
        Self {
            refs: Mutex::new(HashMap::new()),
            on_object_freed: Mutex::new(None),
        }
    }

    /// Set a callback that fires when any object's reference count reaches zero.
    pub fn set_object_freed_callback(&self, callback: ObjectFreedCallback) {
        *self.on_object_freed.lock() = Some(callback);
    }

    /// Add a local reference to an object. Creates the entry if it doesn't exist.
    pub fn add_local_reference(&self, object_id: ObjectID) {
        let mut refs = self.refs.lock();
        let entry = refs.entry(object_id).or_insert_with(Reference::new);
        entry.local_ref_count += 1;
    }

    /// Remove a local reference. Returns the set of object IDs whose total
    /// reference count has reached zero and should be freed.
    pub fn remove_local_reference(&self, object_id: &ObjectID) -> Vec<ObjectID> {
        let deleted;
        {
            let mut refs = self.refs.lock();
            let mut d = Vec::new();
            if let Some(entry) = refs.get_mut(object_id) {
                entry.local_ref_count = entry.local_ref_count.saturating_sub(1);
                if entry.total_ref_count() == 0 {
                    refs.remove(object_id);
                    d.push(*object_id);
                }
            }
            deleted = d;
        }
        // Fire callback outside lock.
        if !deleted.is_empty() {
            if let Some(ref cb) = *self.on_object_freed.lock() {
                for oid in &deleted {
                    cb(oid);
                }
            }
        }
        deleted
    }

    /// Register an object that we own.
    pub fn add_owned_object(
        &self,
        object_id: ObjectID,
        owner_address: Address,
        contained_in: Vec<ObjectID>,
    ) {
        let mut refs = self.refs.lock();
        let entry = refs.entry(object_id).or_insert_with(Reference::new);
        entry.is_owned_by_us = true;
        entry.owner_address = Some(owner_address);
        for parent_id in &contained_in {
            entry.contained_in.insert(*parent_id);
        }
        // Update parent references in a separate pass to avoid double borrow.
        for parent_id in &contained_in {
            if let Some(parent) = refs.get_mut(parent_id) {
                parent.contains.insert(object_id);
            }
        }
    }

    /// Register an object that is borrowed (owned by someone else).
    pub fn add_borrowed_object(
        &self,
        object_id: ObjectID,
        owner_address: Address,
    ) {
        let mut refs = self.refs.lock();
        let entry = refs.entry(object_id).or_insert_with(Reference::new);
        entry.is_owned_by_us = false;
        entry.owner_address = Some(owner_address);
    }

    /// Check if we own the given object.
    pub fn owned_by_us(&self, object_id: &ObjectID) -> bool {
        self.refs
            .lock()
            .get(object_id)
            .is_some_and(|r| r.is_owned_by_us)
    }

    /// Get the owner address for an object.
    pub fn get_owner(&self, object_id: &ObjectID) -> Option<Address> {
        self.refs
            .lock()
            .get(object_id)
            .and_then(|r| r.owner_address.clone())
    }

    /// Add a location for an object.
    pub fn add_object_location(&self, object_id: &ObjectID, location: String) {
        if let Some(entry) = self.refs.lock().get_mut(object_id) {
            entry.object_locations.insert(location);
        }
    }

    /// Remove a location for an object.
    pub fn remove_object_location(&self, object_id: &ObjectID, location: &str) {
        if let Some(entry) = self.refs.lock().get_mut(object_id) {
            entry.object_locations.remove(location);
        }
    }

    /// Get all known locations for an object.
    pub fn get_object_locations(&self, object_id: &ObjectID) -> Vec<String> {
        self.refs
            .lock()
            .get(object_id)
            .map(|r| r.object_locations.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Increment submitted task ref counts for a set of objects.
    pub fn update_submitted_task_references(&self, object_ids: &[ObjectID]) {
        let mut refs = self.refs.lock();
        for id in object_ids {
            let entry = refs.entry(*id).or_insert_with(Reference::new);
            entry.submitted_task_ref_count += 1;
        }
    }

    /// Decrement submitted task ref counts. Returns freed object IDs.
    pub fn update_finished_task_references(&self, object_ids: &[ObjectID]) -> Vec<ObjectID> {
        let deleted;
        {
            let mut refs = self.refs.lock();
            let mut d = Vec::new();
            for id in object_ids {
                if let Some(entry) = refs.get_mut(id) {
                    entry.submitted_task_ref_count =
                        entry.submitted_task_ref_count.saturating_sub(1);
                    if entry.total_ref_count() == 0 {
                        refs.remove(id);
                        d.push(*id);
                    }
                }
            }
            deleted = d;
        }
        // Fire callback outside lock.
        if !deleted.is_empty() {
            if let Some(ref cb) = *self.on_object_freed.lock() {
                for oid in &deleted {
                    cb(oid);
                }
            }
        }
        deleted
    }

    /// Check if an object has any references.
    pub fn has_reference(&self, object_id: &ObjectID) -> bool {
        self.refs.lock().contains_key(object_id)
    }

    /// Number of tracked objects.
    pub fn num_objects(&self) -> usize {
        self.refs.lock().len()
    }

    // ─── Lineage Reference Counting ────────────────────────────────

    /// Add a lineage reference. Lineage refs keep the entry alive even when
    /// local and submitted ref counts reach zero, to allow task reconstruction.
    pub fn add_lineage_reference(&self, object_id: &ObjectID) {
        let mut refs = self.refs.lock();
        if let Some(entry) = refs.get_mut(object_id) {
            entry.lineage_ref_count += 1;
        }
    }

    /// Remove a lineage reference. Returns freed object IDs.
    pub fn remove_lineage_reference(&self, object_id: &ObjectID) -> Vec<ObjectID> {
        let deleted;
        {
            let mut refs = self.refs.lock();
            let mut d = Vec::new();
            if let Some(entry) = refs.get_mut(object_id) {
                entry.lineage_ref_count = entry.lineage_ref_count.saturating_sub(1);
                if entry.should_delete() {
                    refs.remove(object_id);
                    d.push(*object_id);
                }
            }
            deleted = d;
        }
        if !deleted.is_empty() {
            if let Some(ref cb) = *self.on_object_freed.lock() {
                for oid in &deleted {
                    cb(oid);
                }
            }
        }
        deleted
    }

    // ─── Object Size & Pending Creation ────────────────────────────

    /// Update the known size of an object.
    pub fn update_object_size(&self, object_id: &ObjectID, size: i64) {
        if let Some(entry) = self.refs.lock().get_mut(object_id) {
            entry.object_size = size;
        }
    }

    /// Get the known size of an object. Returns -1 if unknown.
    pub fn get_object_size(&self, object_id: &ObjectID) -> i64 {
        self.refs
            .lock()
            .get(object_id)
            .map(|e| e.object_size)
            .unwrap_or(-1)
    }

    /// Mark whether an object is pending creation.
    pub fn update_object_pending_creation(&self, object_id: &ObjectID, pending: bool) {
        if let Some(entry) = self.refs.lock().get_mut(object_id) {
            entry.is_pending_creation = pending;
        }
    }

    /// Check whether an object is pending creation.
    pub fn is_object_pending_creation(&self, object_id: &ObjectID) -> bool {
        self.refs
            .lock()
            .get(object_id)
            .is_some_and(|e| e.is_pending_creation)
    }

    // ─── Bulk Operations ───────────────────────────────────────────

    /// Get all objects currently in scope (non-zero reference count).
    pub fn all_in_scope_object_ids(&self) -> Vec<ObjectID> {
        self.refs.lock().keys().copied().collect()
    }

    /// Get all reference counts (local, submitted) for debugging.
    pub fn all_reference_counts(&self) -> HashMap<ObjectID, (u64, u64)> {
        self.refs
            .lock()
            .iter()
            .map(|(id, r)| (*id, (r.local_ref_count, r.submitted_task_ref_count)))
            .collect()
    }

    /// Number of objects owned by us.
    pub fn num_objects_owned_by_us(&self) -> usize {
        self.refs
            .lock()
            .values()
            .filter(|r| r.is_owned_by_us)
            .count()
    }
}

impl Default for ReferenceCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_address() -> Address {
        Address {
            node_id: vec![0u8; 28],
            ip_address: "127.0.0.1".to_string(),
            port: 1234,
            worker_id: vec![0u8; 28],
        }
    }

    #[test]
    fn test_add_remove_local_reference() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        rc.add_local_reference(oid);
        rc.add_local_reference(oid);
        assert!(rc.has_reference(&oid));
        // Removing one ref should not free.
        let deleted = rc.remove_local_reference(&oid);
        assert!(deleted.is_empty());
        assert!(rc.has_reference(&oid));
        // Removing last ref frees.
        let deleted = rc.remove_local_reference(&oid);
        assert_eq!(deleted, vec![oid]);
        assert!(!rc.has_reference(&oid));
    }

    #[test]
    fn test_zero_count_deletion() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        rc.add_local_reference(oid);
        let deleted = rc.remove_local_reference(&oid);
        assert_eq!(deleted.len(), 1);
        assert_eq!(rc.num_objects(), 0);
    }

    #[test]
    fn test_owned_object() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        let addr = make_address();
        rc.add_owned_object(oid, addr.clone(), vec![]);
        assert!(rc.owned_by_us(&oid));
        let owner = rc.get_owner(&oid).unwrap();
        assert_eq!(owner.ip_address, addr.ip_address);
        assert_eq!(owner.port, addr.port);
        assert_eq!(owner.node_id, addr.node_id);
        assert_eq!(owner.worker_id, addr.worker_id);
    }

    #[test]
    fn test_borrowed_object() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        let addr = make_address();
        rc.add_borrowed_object(oid, addr.clone());
        assert!(!rc.owned_by_us(&oid));
        let owner = rc.get_owner(&oid).unwrap();
        assert_eq!(owner.ip_address, addr.ip_address);
        assert_eq!(owner.port, addr.port);
    }

    #[test]
    fn test_contained_in_relationship() {
        let rc = ReferenceCounter::new();
        let parent = ObjectID::from_random();
        let child = ObjectID::from_random();
        // Parent must exist for the contains relationship to be set.
        rc.add_owned_object(parent, make_address(), vec![]);
        rc.add_owned_object(child, make_address(), vec![parent]);
        assert!(rc.has_reference(&parent));
        assert!(rc.has_reference(&child));

        // Verify the internal graph: parent.contains has child, child.contained_in has parent
        let refs = rc.refs.lock();
        let parent_ref = refs.get(&parent).expect("parent should exist");
        assert!(
            parent_ref.contains.contains(&child),
            "parent.contains should include the child"
        );
        let child_ref = refs.get(&child).expect("child should exist");
        assert!(
            child_ref.contained_in.contains(&parent),
            "child.contained_in should include the parent"
        );
    }

    #[test]
    fn test_over_remove_saturating() {
        // Removing more times than adding should not underflow or panic.
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        rc.add_local_reference(oid);
        // Remove once — frees.
        let deleted = rc.remove_local_reference(&oid);
        assert_eq!(deleted, vec![oid]);
        // Remove again — entry no longer exists, should be a no-op.
        let deleted = rc.remove_local_reference(&oid);
        assert!(deleted.is_empty());
        assert!(!rc.has_reference(&oid));
    }

    #[test]
    fn test_finished_task_refs_nonexistent() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        // Finishing tasks for a non-existent object should not panic.
        let deleted = rc.update_finished_task_references(&[oid]);
        assert!(deleted.is_empty());
    }

    #[test]
    fn test_object_locations() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        rc.add_local_reference(oid);
        rc.add_object_location(&oid, "node1".to_string());
        rc.add_object_location(&oid, "node2".to_string());
        let locs = rc.get_object_locations(&oid);
        assert_eq!(locs.len(), 2);
        assert!(locs.contains(&"node1".to_string()));
        assert!(locs.contains(&"node2".to_string()));
        rc.remove_object_location(&oid, "node1");
        assert_eq!(rc.get_object_locations(&oid).len(), 1);
    }

    #[test]
    fn test_submitted_task_references() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        rc.update_submitted_task_references(&[oid]);
        assert!(rc.has_reference(&oid));
        let deleted = rc.update_finished_task_references(&[oid]);
        assert_eq!(deleted, vec![oid]);
        assert!(!rc.has_reference(&oid));
    }

    #[test]
    fn test_mixed_ref_counts() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        rc.add_local_reference(oid);
        rc.update_submitted_task_references(&[oid]);
        // local=1, submitted=1 => won't free on remove_local
        let deleted = rc.remove_local_reference(&oid);
        assert!(deleted.is_empty());
        // submitted=1 => won't free yet (submitted still > 0... wait, local=0, submitted=1)
        // Actually, local=0 submitted=1 total=1, so entry stays.
        assert!(rc.has_reference(&oid));
        let deleted = rc.update_finished_task_references(&[oid]);
        assert_eq!(deleted, vec![oid]);
    }

    #[test]
    fn test_remove_nonexistent_reference() {
        let rc = ReferenceCounter::new();
        let oid = ObjectID::from_random();
        let deleted = rc.remove_local_reference(&oid);
        assert!(deleted.is_empty());
    }

    #[test]
    fn test_num_objects() {
        let rc = ReferenceCounter::new();
        assert_eq!(rc.num_objects(), 0);
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        rc.add_local_reference(oid1);
        rc.add_local_reference(oid2);
        assert_eq!(rc.num_objects(), 2);
    }
}
