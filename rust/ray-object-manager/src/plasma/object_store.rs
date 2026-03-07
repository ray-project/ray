// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Plasma object store — manages the mapping from ObjectID to LocalObject.
//!
//! Replaces `src/ray/object_manager/plasma/object_store.h/cc`.

use std::collections::HashMap;

use ray_common::id::ObjectID;

use crate::common::{ObjectInfo, ObjectSource};
use crate::plasma::allocator::Allocation;

/// State of a plasma object in the local store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    /// Object is being created (not yet sealed).
    Created = 1,
    /// Object is sealed (immutable, readable).
    Sealed = 2,
}

/// A local object in the plasma store.
///
/// Matches C++ `LocalObject`.
pub struct LocalObject {
    /// Memory allocation backing this object.
    allocation: Allocation,
    /// Object metadata.
    object_info: ObjectInfo,
    /// Reference count (number of clients using this object).
    ref_count: i32,
    /// Unix epoch time when the object was created (ms).
    create_time: i64,
    /// Time to construct (seal_time - create_time) in ms.
    construct_duration: i64,
    /// Current state.
    state: ObjectState,
    /// Source of this object.
    source: ObjectSource,
}

impl LocalObject {
    pub fn new(allocation: Allocation, object_info: ObjectInfo, source: ObjectSource) -> Self {
        Self {
            allocation,
            object_info,
            ref_count: 0,
            create_time: ray_util::time::current_time_ms() as i64,
            construct_duration: -1,
            state: ObjectState::Created,
            source,
        }
    }

    pub fn object_info(&self) -> &ObjectInfo {
        &self.object_info
    }

    pub fn object_id(&self) -> &ObjectID {
        &self.object_info.object_id
    }

    pub fn allocation(&self) -> &Allocation {
        &self.allocation
    }

    pub fn state(&self) -> ObjectState {
        self.state
    }

    pub fn is_sealed(&self) -> bool {
        self.state == ObjectState::Sealed
    }

    pub fn ref_count(&self) -> i32 {
        self.ref_count
    }

    pub fn source(&self) -> ObjectSource {
        self.source
    }

    pub fn data_size(&self) -> i64 {
        self.object_info.data_size
    }

    pub fn metadata_size(&self) -> i64 {
        self.object_info.metadata_size
    }

    /// Total object size: data + metadata.
    pub fn object_size(&self) -> i64 {
        self.object_info.get_object_size()
    }

    /// Seal the object, making it immutable.
    pub fn seal(&mut self) {
        assert_eq!(self.state, ObjectState::Created, "Object already sealed");
        self.state = ObjectState::Sealed;
        self.construct_duration = ray_util::time::current_time_ms() as i64 - self.create_time;
    }

    /// Increment the reference count.
    pub fn incr_ref(&mut self) {
        self.ref_count += 1;
    }

    /// Decrement the reference count.
    pub fn decr_ref(&mut self) {
        self.ref_count -= 1;
        assert!(self.ref_count >= 0, "Negative reference count");
    }
}

/// The plasma object store — maps ObjectIDs to LocalObjects.
///
/// Matches C++ `ObjectStore`.
pub struct ObjectStore {
    /// Map from ObjectID to the local object.
    object_table: HashMap<ObjectID, LocalObject>,
    /// Cumulative bytes created (for metrics).
    cumulative_created_bytes: i64,
    /// Bytes currently in sealed objects.
    num_bytes_sealed: i64,
    /// Bytes currently in unsealed objects.
    num_bytes_unsealed: i64,
}

impl ObjectStore {
    pub fn new() -> Self {
        Self {
            object_table: HashMap::new(),
            cumulative_created_bytes: 0,
            num_bytes_sealed: 0,
            num_bytes_unsealed: 0,
        }
    }

    /// Create a new object in the store.
    pub fn create_object(
        &mut self,
        allocation: Allocation,
        object_info: ObjectInfo,
        source: ObjectSource,
    ) -> Result<&LocalObject, crate::common::PlasmaError> {
        let object_id = object_info.object_id;
        if self.object_table.contains_key(&object_id) {
            return Err(crate::common::PlasmaError::ObjectExists);
        }

        let size = object_info.get_object_size();
        let local_object = LocalObject::new(allocation, object_info, source);
        self.object_table.insert(object_id, local_object);

        self.num_bytes_unsealed += size;
        self.cumulative_created_bytes += size;

        Ok(self.object_table.get(&object_id).unwrap())
    }

    /// Get an object by ID.
    pub fn get_object(&self, object_id: &ObjectID) -> Option<&LocalObject> {
        self.object_table.get(object_id)
    }

    /// Get a mutable reference to an object by ID.
    pub fn get_object_mut(&mut self, object_id: &ObjectID) -> Option<&mut LocalObject> {
        self.object_table.get_mut(object_id)
    }

    /// Seal an object, making it immutable.
    pub fn seal_object(
        &mut self,
        object_id: &ObjectID,
    ) -> Result<&LocalObject, crate::common::PlasmaError> {
        let obj = self
            .object_table
            .get_mut(object_id)
            .ok_or(crate::common::PlasmaError::ObjectNonexistent)?;

        if obj.is_sealed() {
            return Err(crate::common::PlasmaError::ObjectSealed);
        }

        let size = obj.object_size();
        obj.seal();
        self.num_bytes_unsealed -= size;
        self.num_bytes_sealed += size;

        Ok(self.object_table.get(object_id).unwrap())
    }

    /// Delete an object from the store. Returns the allocation for freeing.
    pub fn delete_object(
        &mut self,
        object_id: &ObjectID,
    ) -> Result<Allocation, crate::common::PlasmaError> {
        // Check ref_count before removing to avoid leaking the allocation
        if let Some(obj) = self.object_table.get(object_id) {
            if obj.ref_count() > 0 {
                return Err(crate::common::PlasmaError::ObjectInUse);
            }
        } else {
            return Err(crate::common::PlasmaError::ObjectNonexistent);
        }

        let obj = self.object_table.remove(object_id).unwrap();
        let size = obj.object_size();
        if obj.is_sealed() {
            self.num_bytes_sealed -= size;
        } else {
            self.num_bytes_unsealed -= size;
        }

        Ok(obj.allocation)
    }

    /// Check if an object exists in the store.
    pub fn contains(&self, object_id: &ObjectID) -> bool {
        self.object_table.contains_key(object_id)
    }

    pub fn num_objects(&self) -> usize {
        self.object_table.len()
    }

    pub fn num_bytes_sealed(&self) -> i64 {
        self.num_bytes_sealed
    }

    pub fn num_bytes_unsealed(&self) -> i64 {
        self.num_bytes_unsealed
    }

    pub fn num_bytes_in_use(&self) -> i64 {
        self.num_bytes_sealed + self.num_bytes_unsealed
    }

    pub fn cumulative_created_bytes(&self) -> i64 {
        self.cumulative_created_bytes
    }
}

impl Default for ObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    fn dummy_allocation() -> Allocation {
        Allocation {
            address: std::ptr::null_mut(),
            size: 0,
            fd: -1,
            offset: 0,
            device_num: 0,
            mmap_size: 0,
            fallback_allocated: false,
        }
    }

    #[test]
    fn test_create_and_seal() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);

        let info = ObjectInfo {
            object_id: oid,
            data_size: 1024,
            metadata_size: 64,
            ..Default::default()
        };

        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();
        assert!(store.contains(&oid));
        assert_eq!(store.num_bytes_unsealed(), 1088);

        store.seal_object(&oid).unwrap();
        let obj = store.get_object(&oid).unwrap();
        assert!(obj.is_sealed());
        assert_eq!(store.num_bytes_sealed(), 1088);
        assert_eq!(store.num_bytes_unsealed(), 0);
    }

    #[test]
    fn test_duplicate_create() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 100,
            ..Default::default()
        };

        store
            .create_object(
                dummy_allocation(),
                info.clone(),
                ObjectSource::CreatedByWorker,
            )
            .unwrap();
        let result = store.create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker);
        assert!(matches!(
            result,
            Err(crate::common::PlasmaError::ObjectExists)
        ));
    }

    #[test]
    fn test_delete() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 256,
            ..Default::default()
        };

        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();
        store.seal_object(&oid).unwrap();
        store.delete_object(&oid).unwrap();
        assert!(!store.contains(&oid));
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut store = ObjectStore::new();
        let oid = make_oid(99);
        let result = store.delete_object(&oid);
        assert!(matches!(
            result,
            Err(crate::common::PlasmaError::ObjectNonexistent)
        ));
    }

    #[test]
    fn test_delete_while_in_use() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 256,
            ..Default::default()
        };

        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();
        store.seal_object(&oid).unwrap();

        // Increment ref count
        store.get_object_mut(&oid).unwrap().incr_ref();

        // Should fail with ObjectInUse
        let result = store.delete_object(&oid);
        assert!(matches!(
            result,
            Err(crate::common::PlasmaError::ObjectInUse)
        ));
        // Object should still be in the store with correct state
        assert!(store.contains(&oid));
        let obj = store.get_object(&oid).unwrap();
        assert_eq!(obj.ref_count(), 1);
        assert!(obj.is_sealed());
    }

    #[test]
    fn test_seal_nonexistent() {
        let mut store = ObjectStore::new();
        let oid = make_oid(99);
        let result = store.seal_object(&oid);
        assert!(matches!(
            result,
            Err(crate::common::PlasmaError::ObjectNonexistent)
        ));
    }

    #[test]
    fn test_seal_already_sealed() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 100,
            ..Default::default()
        };
        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();
        store.seal_object(&oid).unwrap();
        let result = store.seal_object(&oid);
        assert!(matches!(
            result,
            Err(crate::common::PlasmaError::ObjectSealed)
        ));
    }

    #[test]
    fn test_ref_count_increment_decrement() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 100,
            ..Default::default()
        };
        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();
        store.seal_object(&oid).unwrap();

        let obj = store.get_object_mut(&oid).unwrap();
        assert_eq!(obj.ref_count(), 0);
        obj.incr_ref();
        obj.incr_ref();
        assert_eq!(obj.ref_count(), 2);
        obj.decr_ref();
        assert_eq!(obj.ref_count(), 1);
        obj.decr_ref();
        assert_eq!(obj.ref_count(), 0);
    }

    #[test]
    fn test_byte_tracking_across_operations() {
        let mut store = ObjectStore::new();
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        let info1 = ObjectInfo {
            object_id: oid1,
            data_size: 100,
            ..Default::default()
        };
        let info2 = ObjectInfo {
            object_id: oid2,
            data_size: 200,
            ..Default::default()
        };

        store
            .create_object(dummy_allocation(), info1, ObjectSource::CreatedByWorker)
            .unwrap();
        store
            .create_object(dummy_allocation(), info2, ObjectSource::CreatedByWorker)
            .unwrap();
        assert_eq!(store.num_bytes_unsealed(), 300);
        assert_eq!(store.num_bytes_sealed(), 0);
        assert_eq!(store.cumulative_created_bytes(), 300);

        store.seal_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_unsealed(), 200);
        assert_eq!(store.num_bytes_sealed(), 100);

        store.delete_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_sealed(), 0);
        assert_eq!(store.num_bytes_unsealed(), 200);
        assert_eq!(store.num_bytes_in_use(), 200);
        assert_eq!(store.num_objects(), 1);
    }

    #[test]
    fn test_delete_unsealed_object() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 100,
            ..Default::default()
        };
        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();
        // Delete without sealing
        store.delete_object(&oid).unwrap();
        assert_eq!(store.num_bytes_unsealed(), 0);
        assert_eq!(store.num_objects(), 0);
    }

    #[test]
    fn test_object_source_preserved() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 100,
            ..Default::default()
        };
        store
            .create_object(
                dummy_allocation(),
                info,
                ObjectSource::ReceivedFromRemoteRaylet,
            )
            .unwrap();
        let obj = store.get_object(&oid).unwrap();
        assert_eq!(obj.source(), ObjectSource::ReceivedFromRemoteRaylet);
        assert_eq!(obj.state(), ObjectState::Created);
    }

    #[test]
    fn test_local_object_field_accessors() {
        let mut store = ObjectStore::new();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 512,
            metadata_size: 64,
            ..Default::default()
        };
        store
            .create_object(dummy_allocation(), info, ObjectSource::CreatedByWorker)
            .unwrap();

        let obj = store.get_object(&oid).unwrap();
        assert_eq!(obj.data_size(), 512);
        assert_eq!(obj.metadata_size(), 64);
        assert_eq!(obj.object_size(), 576);
        assert_eq!(*obj.object_id(), oid);
        assert_eq!(obj.object_info().data_size, 512);
        assert!(obj.allocation().address.is_null()); // dummy allocation
    }

    #[test]
    fn test_num_bytes_sealed() {
        let mut store = ObjectStore::new();
        assert_eq!(store.num_bytes_sealed(), 0);

        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid1,
                    data_size: 100,
                    ..Default::default()
                },
                ObjectSource::CreatedByWorker,
            )
            .unwrap();
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid2,
                    data_size: 200,
                    ..Default::default()
                },
                ObjectSource::CreatedByWorker,
            )
            .unwrap();

        store.seal_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_sealed(), 100);

        store.seal_object(&oid2).unwrap();
        assert_eq!(store.num_bytes_sealed(), 300);
    }

    // ─── Tests ported from stats_collector_test.cc ────────────────────────

    #[test]
    fn test_stats_create_and_abort() {
        // Port of ObjectStatsCollectorTest::CreateAndAbort
        // Create objects with all source types, then delete (abort) them.
        let mut store = ObjectStore::new();
        let sources = [
            ObjectSource::CreatedByWorker,
            ObjectSource::RestoredFromStorage,
            ObjectSource::ReceivedFromRemoteRaylet,
            ObjectSource::ErrorStoredByRaylet,
        ];

        let mut oids = Vec::new();
        let mut total_created = 0i64;

        for (i, &source) in sources.iter().enumerate() {
            let size = (i as i64 + 1) * 50;
            let oid = make_oid(100 + i as u8);
            let info = ObjectInfo {
                object_id: oid,
                data_size: size,
                ..Default::default()
            };
            total_created += info.get_object_size();
            store
                .create_object(dummy_allocation(), info, source)
                .unwrap();
            oids.push(oid);
        }

        assert_eq!(store.cumulative_created_bytes(), total_created);
        assert_eq!(store.num_objects(), 4);

        // "Abort" = delete unsealed objects
        for oid in &oids {
            store.delete_object(oid).unwrap();
        }

        assert_eq!(store.num_objects(), 0);
        assert_eq!(store.num_bytes_unsealed(), 0);
        assert_eq!(store.num_bytes_sealed(), 0);
        // Cumulative created should remain
        assert_eq!(store.cumulative_created_bytes(), total_created);
    }

    #[test]
    fn test_stats_create_and_delete() {
        // Port of ObjectStatsCollectorTest::CreateAndDelete
        // Create objects, optionally seal some, add refs, then delete.
        let mut store = ObjectStore::new();
        let sources = [
            ObjectSource::CreatedByWorker,
            ObjectSource::RestoredFromStorage,
            ObjectSource::ReceivedFromRemoteRaylet,
            ObjectSource::ErrorStoredByRaylet,
        ];

        let mut oids = Vec::new();
        let mut total_created = 0i64;

        for (i, &source) in sources.iter().enumerate() {
            let size = (i as i64 + 1) * 100;
            let oid = make_oid(110 + i as u8);
            let info = ObjectInfo {
                object_id: oid,
                data_size: size,
                ..Default::default()
            };
            total_created += info.get_object_size();
            store
                .create_object(dummy_allocation(), info, source)
                .unwrap();
            oids.push(oid);
        }

        // Seal some objects (first two)
        store.seal_object(&oids[0]).unwrap();
        store.seal_object(&oids[1]).unwrap();

        let sealed_bytes = 100 + 200; // first two objects
        let unsealed_bytes = 300 + 400; // last two objects
        assert_eq!(store.num_bytes_sealed(), sealed_bytes);
        assert_eq!(store.num_bytes_unsealed(), unsealed_bytes);

        // Add reference counts and verify
        store.get_object_mut(&oids[0]).unwrap().incr_ref();
        assert_eq!(store.get_object(&oids[0]).unwrap().ref_count(), 1);

        // Delete all objects
        // First, drop refs
        store.get_object_mut(&oids[0]).unwrap().decr_ref();

        for oid in &oids {
            store.delete_object(oid).unwrap();
        }

        assert_eq!(store.num_objects(), 0);
        assert_eq!(store.num_bytes_in_use(), 0);
        assert_eq!(store.cumulative_created_bytes(), total_created);
    }

    #[test]
    fn test_stats_eviction() {
        // Port of ObjectStatsCollectorTest::Eviction
        // Create objects, seal them, then evict (delete) them.
        let mut store = ObjectStore::new();
        let sources = [
            ObjectSource::CreatedByWorker,
            ObjectSource::RestoredFromStorage,
            ObjectSource::ReceivedFromRemoteRaylet,
            ObjectSource::ErrorStoredByRaylet,
        ];

        let mut oids = Vec::new();
        let mut total_created = 0i64;

        for (i, &source) in sources.iter().enumerate() {
            let size = (100 + i) as i64;
            let oid = make_oid(120 + i as u8);
            let info = ObjectInfo {
                object_id: oid,
                data_size: size,
                ..Default::default()
            };
            total_created += info.get_object_size();
            store
                .create_object(dummy_allocation(), info, source)
                .unwrap();
            oids.push(oid);
        }

        // Seal all
        for oid in &oids {
            store.seal_object(oid).unwrap();
        }

        assert_eq!(store.num_bytes_unsealed(), 0);
        let sealed = store.num_bytes_sealed();
        assert_eq!(sealed, total_created);

        // Evict (delete) all
        for oid in &oids {
            store.delete_object(oid).unwrap();
        }

        assert_eq!(store.num_objects(), 0);
        assert_eq!(store.num_bytes_sealed(), 0);
        assert_eq!(store.num_bytes_in_use(), 0);
    }

    #[test]
    fn test_stats_ref_count_pass_through() {
        // Port of ObjectStatsCollectorTest::RefCountPassThrough
        let mut store = ObjectStore::new();

        let oid1 = make_oid(130);
        let info1 = ObjectInfo {
            object_id: oid1,
            data_size: 100,
            ..Default::default()
        };
        store
            .create_object(dummy_allocation(), info1, ObjectSource::CreatedByWorker)
            .unwrap();

        let oid2 = make_oid(131);
        let info2 = ObjectInfo {
            object_id: oid2,
            data_size: 200,
            ..Default::default()
        };
        store
            .create_object(dummy_allocation(), info2, ObjectSource::RestoredFromStorage)
            .unwrap();

        assert_eq!(store.num_bytes_unsealed(), 300);
        assert_eq!(store.num_bytes_sealed(), 0);

        // Add ref to oid1
        store.get_object_mut(&oid1).unwrap().incr_ref();
        assert_eq!(store.get_object(&oid1).unwrap().ref_count(), 1);

        // Seal oid1
        store.seal_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_sealed(), 100);
        assert_eq!(store.num_bytes_unsealed(), 200);

        // Add another ref to oid1
        store.get_object_mut(&oid1).unwrap().incr_ref();
        assert_eq!(store.get_object(&oid1).unwrap().ref_count(), 2);

        // Add ref to oid2
        store.get_object_mut(&oid2).unwrap().incr_ref();

        // Seal oid2
        store.seal_object(&oid2).unwrap();
        assert_eq!(store.num_bytes_sealed(), 300);
        assert_eq!(store.num_bytes_unsealed(), 0);

        // Add another ref to oid2
        store.get_object_mut(&oid2).unwrap().incr_ref();

        // Remove refs from oid2
        store.get_object_mut(&oid2).unwrap().decr_ref();
        assert_eq!(store.get_object(&oid2).unwrap().ref_count(), 1);

        store.get_object_mut(&oid2).unwrap().decr_ref();
        assert_eq!(store.get_object(&oid2).unwrap().ref_count(), 0);

        // Remove refs from oid1
        store.get_object_mut(&oid1).unwrap().decr_ref();
        assert_eq!(store.get_object(&oid1).unwrap().ref_count(), 1);

        store.get_object_mut(&oid1).unwrap().decr_ref();
        assert_eq!(store.get_object(&oid1).unwrap().ref_count(), 0);

        // Delete both
        store.delete_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_sealed(), 200);

        store.delete_object(&oid2).unwrap();
        assert_eq!(store.num_bytes_sealed(), 0);
        assert_eq!(store.num_objects(), 0);
        assert_eq!(store.cumulative_created_bytes(), 300);
    }

    #[test]
    fn test_stats_source_tracking() {
        // Port of stats_collector_test source tracking: verify source is preserved
        let mut store = ObjectStore::new();

        let oid1 = make_oid(140);
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid1,
                    data_size: 100,
                    ..Default::default()
                },
                ObjectSource::CreatedByWorker,
            )
            .unwrap();

        let oid2 = make_oid(141);
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid2,
                    data_size: 200,
                    ..Default::default()
                },
                ObjectSource::RestoredFromStorage,
            )
            .unwrap();

        let oid3 = make_oid(142);
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid3,
                    data_size: 300,
                    ..Default::default()
                },
                ObjectSource::ReceivedFromRemoteRaylet,
            )
            .unwrap();

        let oid4 = make_oid(143);
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid4,
                    data_size: 400,
                    ..Default::default()
                },
                ObjectSource::ErrorStoredByRaylet,
            )
            .unwrap();

        assert_eq!(
            store.get_object(&oid1).unwrap().source(),
            ObjectSource::CreatedByWorker
        );
        assert_eq!(
            store.get_object(&oid2).unwrap().source(),
            ObjectSource::RestoredFromStorage
        );
        assert_eq!(
            store.get_object(&oid3).unwrap().source(),
            ObjectSource::ReceivedFromRemoteRaylet
        );
        assert_eq!(
            store.get_object(&oid4).unwrap().source(),
            ObjectSource::ErrorStoredByRaylet
        );
    }

    #[test]
    fn test_stats_sealed_vs_unsealed_bytes() {
        // Port of stats tracking: verify sealed/unsealed byte counts through lifecycle
        let mut store = ObjectStore::new();

        let oid1 = make_oid(150);
        let oid2 = make_oid(151);
        let oid3 = make_oid(152);

        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid1,
                    data_size: 100,
                    ..Default::default()
                },
                ObjectSource::CreatedByWorker,
            )
            .unwrap();
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid2,
                    data_size: 200,
                    ..Default::default()
                },
                ObjectSource::CreatedByWorker,
            )
            .unwrap();
        store
            .create_object(
                dummy_allocation(),
                ObjectInfo {
                    object_id: oid3,
                    data_size: 300,
                    ..Default::default()
                },
                ObjectSource::CreatedByWorker,
            )
            .unwrap();

        assert_eq!(store.num_bytes_unsealed(), 600);
        assert_eq!(store.num_bytes_sealed(), 0);

        store.seal_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_unsealed(), 500);
        assert_eq!(store.num_bytes_sealed(), 100);

        store.seal_object(&oid2).unwrap();
        assert_eq!(store.num_bytes_unsealed(), 300);
        assert_eq!(store.num_bytes_sealed(), 300);

        // Delete unsealed object
        store.delete_object(&oid3).unwrap();
        assert_eq!(store.num_bytes_unsealed(), 0);
        assert_eq!(store.num_bytes_sealed(), 300);

        // Delete sealed object
        store.delete_object(&oid1).unwrap();
        assert_eq!(store.num_bytes_sealed(), 200);

        store.delete_object(&oid2).unwrap();
        assert_eq!(store.num_bytes_sealed(), 0);
        assert_eq!(store.num_bytes_in_use(), 0);
        assert_eq!(store.cumulative_created_bytes(), 600);
    }

    #[test]
    fn test_object_store_default() {
        let store = ObjectStore::default();
        assert_eq!(store.num_objects(), 0);
        assert_eq!(store.cumulative_created_bytes(), 0);
    }
}
