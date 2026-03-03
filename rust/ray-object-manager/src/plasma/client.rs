// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Plasma client — provides shared-memory object access to workers.
//!
//! Ports C++ `object_store/plasma/client.cc`.
//!
//! The PlasmaClient connects to a PlasmaStore (via in-process reference or
//! potentially via Unix socket) and provides the create/seal/get/release/delete
//! lifecycle for shared-memory objects.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

use ray_common::id::ObjectID;

use crate::common::{ObjectInfo, ObjectSource, PlasmaError, PlasmaObject};
use crate::plasma::allocator::IAllocator;
use crate::plasma::store::PlasmaStore;

/// An object buffer returned from `PlasmaClient::get()`.
///
/// Contains the object's data, metadata, and layout information.
#[derive(Debug, Clone)]
pub struct ObjectBuffer {
    /// The object ID.
    pub object_id: ObjectID,
    /// The data payload.
    pub data: Vec<u8>,
    /// The metadata payload.
    pub metadata: Vec<u8>,
    /// The plasma object layout (fd, offsets, sizes).
    pub plasma_object: PlasmaObject,
}

/// Tracks per-object state on the client side.
#[derive(Debug)]
struct ObjectInUseEntry {
    /// Client-side reference count. Incremented by create/get, decremented by release.
    ref_count: i32,
    /// Whether this object was created (not yet sealed) by this client.
    is_create: bool,
    /// Object info (data_size, metadata_size, etc).
    object_info: ObjectInfo,
}

/// Configuration for the plasma client.
#[derive(Debug, Clone)]
pub struct PlasmaClientConfig {
    /// Whether to exit on connection failure.
    pub exit_on_connection_failure: bool,
}

impl Default for PlasmaClientConfig {
    fn default() -> Self {
        Self {
            exit_on_connection_failure: true,
        }
    }
}

/// The Plasma client provides shared-memory object access.
///
/// In the C++ version, the client communicates with the store over a Unix
/// domain socket using Flatbuffer messages. In this Rust port, we use a
/// direct reference to the PlasmaStore for in-process communication,
/// which is simpler and avoids serialization overhead. The Unix socket
/// IPC path can be added later for multi-process deployments.
pub struct PlasmaClient {
    inner: Mutex<PlasmaClientInner>,
}

struct PlasmaClientInner {
    /// Direct reference to the plasma store.
    store: Arc<PlasmaStore>,
    /// Memory allocator for creating objects.
    allocator: Arc<dyn IAllocator>,
    /// Client-side tracking of objects in use.
    objects_in_use: HashMap<ObjectID, ObjectInUseEntry>,
    /// Objects waiting to be deleted (deferred because ref_count > 0).
    deletion_cache: HashSet<ObjectID>,
    /// Whether the client is connected.
    connected: bool,
    /// Configuration.
    _config: PlasmaClientConfig,
}

impl PlasmaClient {
    /// Create a new PlasmaClient connected to the given store.
    pub fn new(
        store: Arc<PlasmaStore>,
        allocator: Arc<dyn IAllocator>,
        config: PlasmaClientConfig,
    ) -> Self {
        Self {
            inner: Mutex::new(PlasmaClientInner {
                store,
                allocator,
                objects_in_use: HashMap::new(),
                deletion_cache: HashSet::new(),
                connected: true,
                _config: config,
            }),
        }
    }

    /// Check if the client is connected to a store.
    pub fn is_connected(&self) -> bool {
        self.inner.lock().connected
    }

    /// Disconnect the client from the store.
    pub fn disconnect(&self) {
        let mut inner = self.inner.lock();
        // Collect state needed for cleanup before modifying.
        let entries: Vec<(ObjectID, bool, i32)> = inner
            .objects_in_use
            .iter()
            .map(|(oid, e)| (*oid, e.is_create, e.ref_count))
            .collect();

        for (oid, is_create, ref_count) in entries {
            if is_create {
                // Abort uncommitted creates.
                let _ = inner.store.delete_object(&oid, inner.allocator.as_ref());
            } else {
                // Release reads.
                for _ in 0..ref_count {
                    let _ = inner.store.end_object_access(&oid);
                }
            }
        }
        inner.objects_in_use.clear();
        inner.deletion_cache.clear();
        inner.connected = false;
    }

    /// Create a new object in the plasma store.
    ///
    /// The object starts in the CREATED state. The caller should write data
    /// and then call `seal()` to make it available.
    ///
    /// Returns the PlasmaObject layout describing the shared memory region.
    pub fn create(
        &self,
        object_id: ObjectID,
        data_size: i64,
        metadata_size: i64,
        object_info: ObjectInfo,
    ) -> Result<PlasmaObject, PlasmaError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }

        // Check if already in use.
        if inner.objects_in_use.contains_key(&object_id) {
            return Err(PlasmaError::ObjectExists);
        }

        let info = ObjectInfo {
            object_id,
            data_size,
            metadata_size,
            ..object_info
        };

        // Create in the store.
        inner
            .store
            .create_object(info.clone(), ObjectSource::CreatedByWorker, inner.allocator.as_ref())?;

        // Track in client.
        inner.objects_in_use.insert(
            object_id,
            ObjectInUseEntry {
                ref_count: 1,
                is_create: true,
                object_info: info,
            },
        );

        // Return the plasma object layout.
        Ok(PlasmaObject {
            store_fd: -1,
            header_offset: 0,
            data_offset: 0,
            metadata_offset: data_size as isize,
            data_size,
            metadata_size,
            allocated_size: data_size + metadata_size,
            device_num: 0,
            mmap_size: data_size + metadata_size,
            fallback_allocated: false,
            is_experimental_mutable_object: false,
        })
    }

    /// Try to create an object, returning immediately if no space.
    ///
    /// Unlike `create()`, does not attempt eviction or fallback allocation.
    pub fn try_create_immediately(
        &self,
        object_id: ObjectID,
        data_size: i64,
        metadata_size: i64,
        object_info: ObjectInfo,
    ) -> Result<PlasmaObject, PlasmaError> {
        // For now, delegates to create(). The store handles eviction internally.
        self.create(object_id, data_size, metadata_size, object_info)
    }

    /// Seal an object, making it immutable and available for reading.
    ///
    /// After sealing, the create reference is converted to a read reference
    /// and then released.
    pub fn seal(&self, object_id: &ObjectID) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }

        // Seal in the store.
        inner.store.seal_object(object_id)?;

        // Update client state: no longer a create, release the create ref.
        if let Some(entry) = inner.objects_in_use.get_mut(object_id) {
            entry.is_create = false;
            entry.ref_count -= 1;
            if entry.ref_count <= 0 {
                inner.objects_in_use.remove(object_id);
                // Process deferred deletions.
                if inner.deletion_cache.remove(object_id) {
                    let _ = inner.store.delete_object(object_id, inner.allocator.as_ref());
                }
            }
        }

        Ok(())
    }

    /// Abort an object creation. Removes the object from the store.
    pub fn abort(&self, object_id: &ObjectID) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }

        // Remove from store.
        inner
            .store
            .delete_object(object_id, inner.allocator.as_ref())?;

        // Remove from client tracking.
        inner.objects_in_use.remove(object_id);
        Ok(())
    }

    /// Get one or more objects from the store.
    ///
    /// For objects that exist and are sealed, returns `ObjectBuffer` with data.
    /// For objects that don't exist, returns None in that position.
    /// If `timeout_ms < 0`, blocks until all objects are available.
    /// If `timeout_ms == 0`, returns immediately with whatever is available.
    /// If `timeout_ms > 0`, waits up to that many milliseconds.
    pub fn get(
        &self,
        object_ids: &[ObjectID],
        timeout_ms: i64,
    ) -> Result<Vec<Option<ObjectBuffer>>, PlasmaError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }

        let mut results = Vec::with_capacity(object_ids.len());

        for oid in object_ids {
            // Check if already in use by this client.
            if let Some(entry) = inner.objects_in_use.get_mut(oid) {
                entry.ref_count += 1;
                results.push(Some(ObjectBuffer {
                    object_id: *oid,
                    data: Vec::new(), // Data is in shared memory
                    metadata: Vec::new(),
                    plasma_object: PlasmaObject {
                        store_fd: -1,
                        header_offset: 0,
                        data_offset: 0,
                        metadata_offset: entry.object_info.data_size as isize,
                        data_size: entry.object_info.data_size,
                        metadata_size: entry.object_info.metadata_size,
                        allocated_size: entry.object_info.data_size
                            + entry.object_info.metadata_size,
                        device_num: 0,
                        mmap_size: entry.object_info.data_size
                            + entry.object_info.metadata_size,
                        fallback_allocated: false,
                        is_experimental_mutable_object: false,
                    },
                }));
                continue;
            }

            // Look up in the store.
            if let Some(info) = inner.store.get_object_info(oid) {
                // Pin the object.
                let _ = inner.store.begin_object_access(oid);

                // Track in client.
                inner.objects_in_use.insert(
                    *oid,
                    ObjectInUseEntry {
                        ref_count: 1,
                        is_create: false,
                        object_info: info.clone(),
                    },
                );

                results.push(Some(ObjectBuffer {
                    object_id: *oid,
                    data: Vec::new(),
                    metadata: Vec::new(),
                    plasma_object: PlasmaObject {
                        store_fd: -1,
                        header_offset: 0,
                        data_offset: 0,
                        metadata_offset: info.data_size as isize,
                        data_size: info.data_size,
                        metadata_size: info.metadata_size,
                        allocated_size: info.data_size + info.metadata_size,
                        device_num: 0,
                        mmap_size: info.data_size + info.metadata_size,
                        fallback_allocated: false,
                        is_experimental_mutable_object: false,
                    },
                }));
            } else if timeout_ms == 0 {
                results.push(None);
            } else {
                // Object not found and timeout > 0. For now, return None.
                // TODO: implement blocking wait with store notification.
                results.push(None);
            }
        }

        Ok(results)
    }

    /// Release an object. Decrements the client-side reference count.
    ///
    /// When the count reaches zero, the object is unpinned in the store
    /// (making it eligible for eviction), and any deferred deletions
    /// are processed.
    pub fn release(&self, object_id: &ObjectID) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }

        let should_remove = if let Some(entry) = inner.objects_in_use.get_mut(object_id) {
            entry.ref_count -= 1;
            entry.ref_count <= 0
        } else {
            return Ok(());
        };

        if should_remove {
            inner.objects_in_use.remove(object_id);
            // Unpin in store.
            let _ = inner.store.end_object_access(object_id);

            // Process deferred deletion.
            if inner.deletion_cache.remove(object_id) {
                let _ = inner
                    .store
                    .delete_object(object_id, inner.allocator.as_ref());
            }
        }

        Ok(())
    }

    /// Check if an object exists in the store.
    pub fn contains(&self, object_id: &ObjectID) -> Result<bool, PlasmaError> {
        let inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }
        Ok(inner.store.contains(object_id))
    }

    /// Delete objects from the store.
    ///
    /// If an object is currently in use (ref_count > 0), the deletion is
    /// deferred to the `deletion_cache` and processed when the last
    /// reference is released.
    pub fn delete(&self, object_ids: &[ObjectID]) -> Result<(), PlasmaError> {
        let mut inner = self.inner.lock();
        if !inner.connected {
            return Err(PlasmaError::UnexpectedError);
        }

        for oid in object_ids {
            if inner.objects_in_use.contains_key(oid) {
                // Defer deletion until last release.
                inner.deletion_cache.insert(*oid);
            } else {
                // Delete immediately.
                let _ = inner.store.delete_object(oid, inner.allocator.as_ref());
            }
        }

        Ok(())
    }

    /// Number of objects currently in use by this client.
    pub fn num_objects_in_use(&self) -> usize {
        self.inner.lock().objects_in_use.len()
    }

    /// Number of objects in the deferred deletion cache.
    pub fn num_pending_deletions(&self) -> usize {
        self.inner.lock().deletion_cache.len()
    }

    /// Get the reference count for a specific object.
    pub fn get_ref_count(&self, object_id: &ObjectID) -> i32 {
        self.inner
            .lock()
            .objects_in_use
            .get(object_id)
            .map(|e| e.ref_count)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plasma::allocator::Allocation;
    use crate::plasma::store::PlasmaStoreConfig;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

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
        fn fallback_allocate(&self, _: usize) -> Option<Allocation> {
            None
        }
        fn free(&self, _: Allocation) {}
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

    fn make_store() -> (Arc<PlasmaStore>, Arc<dyn IAllocator>) {
        let allocator: Arc<dyn IAllocator> = Arc::new(TestAllocator);
        let config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let store = Arc::new(PlasmaStore::new(allocator.clone(), &config));
        (store, allocator)
    }

    fn make_client() -> PlasmaClient {
        let (store, allocator) = make_store();
        PlasmaClient::new(store, allocator, PlasmaClientConfig::default())
    }

    fn make_info(oid: ObjectID) -> ObjectInfo {
        ObjectInfo {
            object_id: oid,
            ..Default::default()
        }
    }

    #[test]
    fn test_create_and_seal() {
        let client = make_client();
        let oid = make_oid(1);

        let plasma_obj = client
            .create(oid, 1024, 0, make_info(oid))
            .unwrap();
        assert_eq!(plasma_obj.data_size, 1024);
        assert_eq!(client.num_objects_in_use(), 1);

        client.seal(&oid).unwrap();
        // After seal, the create ref is released.
        assert_eq!(client.num_objects_in_use(), 0);
    }

    #[test]
    fn test_create_duplicate_errors() {
        let client = make_client();
        let oid = make_oid(2);

        client.create(oid, 100, 0, make_info(oid)).unwrap();
        let err = client.create(oid, 100, 0, make_info(oid)).unwrap_err();
        assert_eq!(err, PlasmaError::ObjectExists);
    }

    #[test]
    fn test_create_seal_get_release() {
        let client = make_client();
        let oid = make_oid(3);

        client.create(oid, 512, 16, make_info(oid)).unwrap();
        client.seal(&oid).unwrap();

        // Get the object.
        let buffers = client.get(&[oid], 0).unwrap();
        assert!(buffers[0].is_some());
        assert_eq!(buffers[0].as_ref().unwrap().plasma_object.data_size, 512);
        assert_eq!(client.num_objects_in_use(), 1);
        assert_eq!(client.get_ref_count(&oid), 1);

        // Release.
        client.release(&oid).unwrap();
        assert_eq!(client.num_objects_in_use(), 0);
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let client = make_client();
        let oid = make_oid(4);
        let buffers = client.get(&[oid], 0).unwrap();
        assert!(buffers[0].is_none());
    }

    #[test]
    fn test_get_multiple_increments_refcount() {
        let client = make_client();
        let oid = make_oid(5);

        client.create(oid, 100, 0, make_info(oid)).unwrap();
        client.seal(&oid).unwrap();

        // Get twice.
        client.get(&[oid], 0).unwrap();
        client.get(&[oid], 0).unwrap();
        assert_eq!(client.get_ref_count(&oid), 2);

        // Release twice.
        client.release(&oid).unwrap();
        assert_eq!(client.get_ref_count(&oid), 1);
        client.release(&oid).unwrap();
        assert_eq!(client.num_objects_in_use(), 0);
    }

    #[test]
    fn test_contains() {
        let client = make_client();
        let oid = make_oid(6);

        assert!(!client.contains(&oid).unwrap());

        client.create(oid, 64, 0, make_info(oid)).unwrap();
        assert!(client.contains(&oid).unwrap());

        client.seal(&oid).unwrap();
        assert!(client.contains(&oid).unwrap());
    }

    #[test]
    fn test_delete_immediate() {
        let client = make_client();
        let oid = make_oid(7);

        client.create(oid, 100, 0, make_info(oid)).unwrap();
        client.seal(&oid).unwrap();
        assert!(client.contains(&oid).unwrap());

        client.delete(&[oid]).unwrap();
        assert!(!client.contains(&oid).unwrap());
    }

    #[test]
    fn test_delete_deferred_while_in_use() {
        let client = make_client();
        let oid = make_oid(8);

        client.create(oid, 100, 0, make_info(oid)).unwrap();
        client.seal(&oid).unwrap();

        // Get the object (pins it).
        client.get(&[oid], 0).unwrap();
        assert_eq!(client.num_objects_in_use(), 1);

        // Delete while in use -> deferred.
        client.delete(&[oid]).unwrap();
        assert_eq!(client.num_pending_deletions(), 1);
        assert!(client.contains(&oid).unwrap()); // Still there.

        // Release -> processes deferred deletion.
        client.release(&oid).unwrap();
        assert_eq!(client.num_pending_deletions(), 0);
        assert!(!client.contains(&oid).unwrap());
    }

    #[test]
    fn test_abort() {
        let client = make_client();
        let oid = make_oid(9);

        client.create(oid, 200, 0, make_info(oid)).unwrap();
        assert!(client.contains(&oid).unwrap());

        client.abort(&oid).unwrap();
        assert!(!client.contains(&oid).unwrap());
        assert_eq!(client.num_objects_in_use(), 0);
    }

    #[test]
    fn test_disconnect() {
        let client = make_client();
        let oid = make_oid(10);

        client.create(oid, 100, 0, make_info(oid)).unwrap();
        client.seal(&oid).unwrap();
        client.get(&[oid], 0).unwrap();

        assert!(client.is_connected());
        client.disconnect();
        assert!(!client.is_connected());
        assert_eq!(client.num_objects_in_use(), 0);
    }

    #[test]
    fn test_operations_after_disconnect_fail() {
        let client = make_client();
        client.disconnect();

        let oid = make_oid(11);
        assert!(client.create(oid, 100, 0, make_info(oid)).is_err());
        assert!(client.get(&[oid], 0).is_err());
        assert!(client.contains(&oid).is_err());
    }

    #[test]
    fn test_multiple_objects() {
        let client = make_client();
        let oid1 = make_oid(12);
        let oid2 = make_oid(13);

        client.create(oid1, 100, 0, make_info(oid1)).unwrap();
        client.seal(&oid1).unwrap();

        client.create(oid2, 200, 0, make_info(oid2)).unwrap();
        client.seal(&oid2).unwrap();

        let buffers = client.get(&[oid1, oid2], 0).unwrap();
        assert!(buffers[0].is_some());
        assert!(buffers[1].is_some());
        assert_eq!(buffers[0].as_ref().unwrap().plasma_object.data_size, 100);
        assert_eq!(buffers[1].as_ref().unwrap().plasma_object.data_size, 200);
    }
}
