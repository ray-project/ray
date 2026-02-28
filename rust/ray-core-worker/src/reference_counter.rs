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

/// Ownership and reference information for a single object.
#[derive(Debug, Clone)]
struct Reference {
    local_ref_count: u64,
    submitted_task_ref_count: u64,
    owner_address: Option<Address>,
    is_owned_by_us: bool,
    object_locations: HashSet<String>,
    contained_in: HashSet<ObjectID>,
    contains: HashSet<ObjectID>,
}

impl Reference {
    fn new() -> Self {
        Self {
            local_ref_count: 0,
            submitted_task_ref_count: 0,
            owner_address: None,
            is_owned_by_us: false,
            object_locations: HashSet::new(),
            contained_in: HashSet::new(),
            contains: HashSet::new(),
        }
    }

    fn total_ref_count(&self) -> u64 {
        self.local_ref_count + self.submitted_task_ref_count
    }
}

/// Tracks reference counts for objects in the core worker.
pub struct ReferenceCounter {
    refs: Mutex<HashMap<ObjectID, Reference>>,
}

impl ReferenceCounter {
    pub fn new() -> Self {
        Self {
            refs: Mutex::new(HashMap::new()),
        }
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
        let mut refs = self.refs.lock();
        let mut deleted = Vec::new();
        if let Some(entry) = refs.get_mut(object_id) {
            entry.local_ref_count = entry.local_ref_count.saturating_sub(1);
            if entry.total_ref_count() == 0 {
                refs.remove(object_id);
                deleted.push(*object_id);
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
        let mut refs = self.refs.lock();
        let mut deleted = Vec::new();
        for id in object_ids {
            if let Some(entry) = refs.get_mut(id) {
                entry.submitted_task_ref_count =
                    entry.submitted_task_ref_count.saturating_sub(1);
                if entry.total_ref_count() == 0 {
                    refs.remove(id);
                    deleted.push(*id);
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
