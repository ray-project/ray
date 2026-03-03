// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Plasma store server — the local shared-memory object store.
//!
//! Replaces `src/ray/object_manager/plasma/store.h/cc`.
//!
//! The PlasmaStore manages the lifecycle of objects in shared memory:
//! CREATE → SEAL → GET/RELEASE → DELETE

use parking_lot::Mutex;
use std::sync::Arc;

use ray_common::id::ObjectID;

use crate::common::{
    AddObjectCallback, DeleteObjectCallback, ObjectInfo, ObjectSource, PlasmaError,
};
use crate::plasma::allocator::IAllocator;
use crate::plasma::eviction::EvictionPolicy;
use crate::plasma::object_store::ObjectStore;

/// Configuration for the plasma store.
#[derive(Debug, Clone)]
pub struct PlasmaStoreConfig {
    /// Maximum memory for the object store.
    pub object_store_memory: i64,
    /// Directory for primary mmap (e.g., /dev/shm).
    pub plasma_directory: String,
    /// Directory for fallback mmap (disk overflow).
    pub fallback_directory: String,
    /// Whether to use huge pages.
    pub huge_pages: bool,
}

/// The plasma store manages the lifecycle of all objects in shared memory.
///
/// Thread safety: The store is protected by a mutex. In C++ it runs in a
/// dedicated single-threaded context; here we use a Mutex for safety.
pub struct PlasmaStore {
    inner: Mutex<PlasmaStoreInner>,
}

struct PlasmaStoreInner {
    /// The object store tracking all local objects.
    object_store: ObjectStore,
    /// Eviction policy for memory management.
    eviction_policy: EvictionPolicy,
    /// Callback when an object is added (sealed).
    add_object_callback: Option<AddObjectCallback>,
    /// Callback when an object is deleted.
    delete_object_callback: Option<DeleteObjectCallback>,
}

impl PlasmaStore {
    /// Create a new plasma store.
    pub fn new(allocator: Arc<dyn IAllocator>, config: &PlasmaStoreConfig) -> Self {
        let _ = allocator; // Will be used in create_object
        Self {
            inner: Mutex::new(PlasmaStoreInner {
                object_store: ObjectStore::new(),
                eviction_policy: EvictionPolicy::new(config.object_store_memory),
                add_object_callback: None,
                delete_object_callback: None,
            }),
        }
    }

    /// Set the callback invoked when a new object is sealed.
    pub fn set_add_object_callback(&self, callback: AddObjectCallback) {
        self.inner.lock().add_object_callback = Some(callback);
    }

    /// Set the callback invoked when an object is deleted.
    pub fn set_delete_object_callback(&self, callback: DeleteObjectCallback) {
        self.inner.lock().delete_object_callback = Some(callback);
    }

    /// Create a new object in the store.
    ///
    /// The object starts in the CREATED state. The caller must seal it
    /// after writing data.
    pub fn create_object(
        &self,
        object_info: ObjectInfo,
        source: ObjectSource,
        allocator: &dyn IAllocator,
    ) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        let size = object_info.get_object_size() as usize;

        // Try primary allocation
        let allocation = allocator
            .allocate(size)
            .or_else(|| {
                // Try eviction to free space
                let mut objects_to_evict = Vec::new();
                inner
                    .eviction_policy
                    .require_space(size as i64, &mut objects_to_evict);

                // Evict objects
                for oid in &objects_to_evict {
                    if let Ok(alloc) = inner.object_store.delete_object(oid) {
                        allocator.free(alloc);
                        inner.eviction_policy.remove_object(oid);
                    }
                }

                allocator.allocate(size)
            })
            .or_else(|| allocator.fallback_allocate(size))
            .ok_or(PlasmaError::OutOfMemory)?;

        let object_id = object_info.object_id;
        inner
            .object_store
            .create_object(allocation, object_info, source)?;

        inner.eviction_policy.object_created(object_id, size as i64);

        Ok(())
    }

    /// Seal an object, making it immutable and available for reading.
    pub fn seal_object(&self, object_id: &ObjectID) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        inner.object_store.seal_object(object_id)?;

        // Notify via callback
        if let Some(ref callback) = inner.add_object_callback {
            if let Some(obj) = inner.object_store.get_object(object_id) {
                callback(obj.object_info());
            }
        }

        Ok(())
    }

    /// Get an object's info (for sealed objects).
    pub fn get_object_info(&self, object_id: &ObjectID) -> Option<ObjectInfo> {
        let inner = self.inner.lock();
        inner
            .object_store
            .get_object(object_id)
            .filter(|obj| obj.is_sealed())
            .map(|obj| obj.object_info().clone())
    }

    /// Get the raw data and metadata bytes for a sealed object.
    ///
    /// Returns `Some((data, metadata))` if the object exists and is sealed,
    /// `None` otherwise. For null-pointer allocations (test stubs), returns
    /// zero-filled buffers of the correct size.
    pub fn get_object_data(&self, object_id: &ObjectID) -> Option<(Vec<u8>, Vec<u8>)> {
        let inner = self.inner.lock();
        let obj = inner.object_store.get_object(object_id)?;
        if !obj.is_sealed() {
            return None;
        }

        let alloc = obj.allocation();
        let data_size = obj.data_size() as usize;
        let metadata_size = obj.metadata_size() as usize;

        if alloc.address.is_null() {
            // Null address (test allocator) — return zeroed buffers.
            return Some((vec![0u8; data_size], vec![0u8; metadata_size]));
        }

        // Safety: The allocation address points to a valid mmap'd region of at
        // least data_size + metadata_size bytes. The object is sealed (immutable),
        // so there are no concurrent writes. The Allocation's lifetime is managed
        // by the ObjectStore, which we hold under lock.
        unsafe {
            let data = std::slice::from_raw_parts(alloc.address, data_size).to_vec();
            let metadata = if metadata_size > 0 {
                std::slice::from_raw_parts(alloc.address.add(data_size), metadata_size).to_vec()
            } else {
                Vec::new()
            };
            Some((data, metadata))
        }
    }

    /// Begin access to an object (increments ref count, pins it).
    pub fn begin_object_access(&self, object_id: &ObjectID) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        let obj = inner
            .object_store
            .get_object_mut(object_id)
            .ok_or(PlasmaError::ObjectNonexistent)?;
        obj.incr_ref();
        inner.eviction_policy.begin_object_access(object_id);
        Ok(())
    }

    /// End access to an object (decrements ref count, unpins it).
    pub fn end_object_access(&self, object_id: &ObjectID) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        let (size, obj_id) = {
            let obj = inner
                .object_store
                .get_object_mut(object_id)
                .ok_or(PlasmaError::ObjectNonexistent)?;
            obj.decr_ref();
            (obj.object_size(), *obj.object_id())
        };
        inner.eviction_policy.end_object_access(obj_id, size);
        Ok(())
    }

    /// Delete an object from the store.
    pub fn delete_object(
        &self,
        object_id: &ObjectID,
        allocator: &dyn IAllocator,
    ) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();

        let allocation = inner.object_store.delete_object(object_id)?;
        inner.eviction_policy.remove_object(object_id);
        allocator.free(allocation);

        // Notify via callback
        if let Some(ref callback) = inner.delete_object_callback {
            callback(object_id);
        }

        Ok(())
    }

    /// Check if an object exists.
    pub fn contains(&self, object_id: &ObjectID) -> bool {
        self.inner.lock().object_store.contains(object_id)
    }

    /// Number of objects in the store.
    pub fn num_objects(&self) -> usize {
        self.inner.lock().object_store.num_objects()
    }

    /// Total bytes currently in use (sealed + unsealed).
    pub fn num_bytes_in_use(&self) -> i64 {
        self.inner.lock().object_store.num_bytes_in_use()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plasma::allocator::Allocation;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    /// A simple test allocator that doesn't actually mmap.
    struct TestAllocator;

    impl IAllocator for TestAllocator {
        fn allocate(&self, bytes: usize) -> Option<Allocation> {
            Some(Allocation {
                address: std::ptr::null_mut(),
                size: bytes as i64,
                fd: -1,
                offset: 0,
                device_num: 0,
                mmap_size: bytes as i64,
                fallback_allocated: false,
            })
        }
        fn fallback_allocate(&self, _bytes: usize) -> Option<Allocation> {
            None
        }
        fn free(&self, _allocation: Allocation) {}
        fn footprint_limit(&self) -> i64 {
            i64::MAX
        }
        fn allocated(&self) -> i64 {
            0
        }
        fn fallback_allocated(&self) -> i64 {
            0
        }
    }

    #[test]
    fn test_store_lifecycle() {
        let allocator = Arc::new(TestAllocator);
        let config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let store = PlasmaStore::new(allocator.clone(), &config);

        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 1024,
            metadata_size: 0,
            ..Default::default()
        };

        // Create
        store
            .create_object(info, ObjectSource::CreatedByWorker, allocator.as_ref())
            .unwrap();
        assert!(store.contains(&oid));

        // Seal
        store.seal_object(&oid).unwrap();
        let info = store.get_object_info(&oid).unwrap();
        assert_eq!(info.data_size, 1024);

        // Access
        store.begin_object_access(&oid).unwrap();
        store.end_object_access(&oid).unwrap();

        // Delete
        store.delete_object(&oid, allocator.as_ref()).unwrap();
        assert!(!store.contains(&oid));
    }

    #[test]
    fn test_get_object_data_sealed() {
        let allocator = Arc::new(TestAllocator);
        let config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let store = PlasmaStore::new(allocator.clone(), &config);

        let oid = make_oid(10);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 256,
            metadata_size: 32,
            ..Default::default()
        };

        store
            .create_object(info, ObjectSource::CreatedByWorker, allocator.as_ref())
            .unwrap();

        // Not sealed yet — should return None.
        assert!(store.get_object_data(&oid).is_none());

        store.seal_object(&oid).unwrap();

        // Sealed — should return Some with correct sizes (zeroed for TestAllocator).
        let (data, metadata) = store.get_object_data(&oid).unwrap();
        assert_eq!(data.len(), 256);
        assert_eq!(metadata.len(), 32);
    }

    #[test]
    fn test_get_object_data_nonexistent() {
        let allocator = Arc::new(TestAllocator);
        let config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let store = PlasmaStore::new(allocator, &config);

        assert!(store.get_object_data(&make_oid(99)).is_none());
    }

    #[test]
    fn test_get_object_data_with_real_allocator() {
        let dir = tempfile::tempdir().unwrap();
        let allocator = Arc::new(crate::plasma::allocator::PlasmaAllocator::new(
            1024 * 1024,
            dir.path().to_str().unwrap(),
            "",
            false,
        ));
        let config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: dir.path().to_str().unwrap().to_string(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let store = PlasmaStore::new(allocator.clone(), &config);

        let oid = make_oid(20);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 64,
            metadata_size: 8,
            ..Default::default()
        };

        store
            .create_object(info, ObjectSource::CreatedByWorker, allocator.as_ref())
            .unwrap();

        // Write some data into the allocation via get_object_info + pointer.
        // The real allocator gives us a valid pointer.
        {
            let inner = store.inner.lock();
            let obj = inner.object_store.get_object(&oid).unwrap();
            let alloc = obj.allocation();
            assert!(!alloc.address.is_null());
            unsafe {
                // Write data region: 64 bytes of 0xAA
                std::ptr::write_bytes(alloc.address, 0xAA, 64);
                // Write metadata region: 8 bytes of 0xBB
                std::ptr::write_bytes(alloc.address.add(64), 0xBB, 8);
            }
        }

        store.seal_object(&oid).unwrap();

        let (data, metadata) = store.get_object_data(&oid).unwrap();
        assert_eq!(data.len(), 64);
        assert_eq!(metadata.len(), 8);
        assert!(data.iter().all(|&b| b == 0xAA));
        assert!(metadata.iter().all(|&b| b == 0xBB));
    }
}
