// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object manager — coordinates distributed object transfers.
//!
//! Replaces `src/ray/object_manager/object_manager.h/cc`.
//!
//! Orchestrates inter-node object transfers using the PullManager
//! and PushManager, with local object access via the ObjectBufferPool.

use std::collections::HashMap;
use std::sync::Arc;

use ray_common::id::{NodeID, ObjectID};

use crate::common::{ObjectInfo, ObjectManagerConfig};
use crate::memory_monitor::{MemoryMonitor, MemoryMonitorConfig};
use crate::object_buffer_pool::ObjectBufferPool;
use crate::plasma::store::PlasmaStore;
use crate::pull_manager::{BundlePriority, PullManager};
use crate::push_manager::PushManager;
use crate::spill_manager::SpillManager;

/// Information about a locally available object.
#[derive(Debug, Clone)]
pub struct LocalObjectInfo {
    pub object_info: ObjectInfo,
    /// Whether this object is being pushed to any remote node.
    pub is_being_pushed: bool,
}

/// A push request that was deferred because the object was not yet local.
#[derive(Debug, Clone)]
struct DeferredPush {
    object_id: ObjectID,
    node_id: NodeID,
}

/// The object manager coordinates inter-node object transfers.
///
/// It wraps a local PlasmaStore and manages pull/push operations
/// to move objects between nodes in the cluster.
#[allow(dead_code)]
pub struct ObjectManager {
    config: ObjectManagerConfig,
    /// Self node ID.
    self_node_id: NodeID,
    /// The pull manager for fetching remote objects.
    pull_manager: PullManager,
    /// The push manager for sending objects to remote nodes.
    push_manager: PushManager,
    /// Buffer pool for chunked object transfers.
    buffer_pool: ObjectBufferPool,
    /// Map of locally available objects.
    local_objects: HashMap<ObjectID, LocalObjectInfo>,
    /// Reference to the plasma store.
    plasma_store: Arc<PlasmaStore>,
    /// Memory usage monitor.
    memory_monitor: Arc<MemoryMonitor>,
    /// Running sum of used memory (data + metadata).
    used_memory: i64,
    /// Deferred pushes: push requests for objects not yet local.
    deferred_pushes: Vec<DeferredPush>,
    /// Optional spill manager for restoring spilled objects.
    spill_manager: Option<Arc<SpillManager>>,
}

impl ObjectManager {
    pub fn new(
        config: ObjectManagerConfig,
        self_node_id: NodeID,
        plasma_store: Arc<PlasmaStore>,
    ) -> Self {
        let chunk_size = config.object_chunk_size;
        let max_bytes = config.max_bytes_in_flight;

        let memory_monitor = Arc::new(MemoryMonitor::new(MemoryMonitorConfig {
            object_store_capacity: config.object_store_memory,
            ..Default::default()
        }));

        Self {
            config,
            self_node_id,
            pull_manager: PullManager::new(max_bytes as i64),
            push_manager: PushManager::new(max_bytes, chunk_size),
            buffer_pool: ObjectBufferPool::new(chunk_size),
            local_objects: HashMap::new(),
            plasma_store,
            memory_monitor,
            used_memory: 0,
            deferred_pushes: Vec::new(),
            spill_manager: None,
        }
    }

    /// Pull an object from a remote node.
    ///
    /// Returns a bundle request ID that can be used to cancel the pull.
    pub fn pull(&mut self, object_ids: Vec<ObjectID>, priority: BundlePriority) -> u64 {
        self.pull_manager.pull(object_ids, priority)
    }

    /// Cancel a pull request.
    pub fn cancel_pull(&mut self, request_id: u64) {
        self.pull_manager.cancel_pull(request_id);
    }

    /// Push an object to a remote node.
    ///
    /// If the object is local, the push starts immediately.
    /// If not local, the push is deferred and will be initiated when the object
    /// becomes available via `object_added()`.
    pub fn push(&mut self, object_id: ObjectID, node_id: NodeID) -> bool {
        if let Some(local_obj) = self.local_objects.get(&object_id) {
            let size =
                (local_obj.object_info.data_size + local_obj.object_info.metadata_size) as u64;
            self.push_manager.start_push(node_id, object_id, size)
        } else {
            // Defer the push until the object arrives.
            self.deferred_pushes.push(DeferredPush {
                object_id,
                node_id,
            });
            false
        }
    }

    /// Handle notification that an object is now available locally.
    pub fn object_added(&mut self, object_info: ObjectInfo) {
        let object_id = object_info.object_id;
        let size = object_info.data_size + object_info.metadata_size;
        self.used_memory += size;
        self.memory_monitor
            .object_added(object_info.data_size, object_info.metadata_size, false);
        let obj_size = (object_info.data_size + object_info.metadata_size) as u64;
        self.local_objects.insert(
            object_id,
            LocalObjectInfo {
                object_info,
                is_being_pushed: false,
            },
        );
        self.pull_manager.object_available_locally(&object_id);

        // Drain deferred pushes for this object.
        let deferred: Vec<DeferredPush> = self
            .deferred_pushes
            .drain(..)
            .collect();
        for dp in deferred {
            if dp.object_id == object_id {
                self.push_manager
                    .start_push(dp.node_id, dp.object_id, obj_size);
            } else {
                // Put back deferred pushes for other objects.
                self.deferred_pushes.push(dp);
            }
        }
    }

    /// Handle notification that an object was deleted locally.
    pub fn object_deleted(&mut self, object_id: &ObjectID) {
        if let Some(info) = self.local_objects.remove(object_id) {
            let size = info.object_info.data_size + info.object_info.metadata_size;
            self.used_memory -= size;
            self.memory_monitor.object_deleted(
                info.object_info.data_size,
                info.object_info.metadata_size,
                false,
            );
        }
    }

    /// Handle an incoming push request (object chunk from remote node).
    pub fn handle_push(
        &mut self,
        object_id: ObjectID,
        chunk_index: u64,
        data: &[u8],
        object_info: ObjectInfo,
    ) -> bool {
        // Start creating the object if not already
        if !self.buffer_pool.is_creating(&object_id) {
            self.buffer_pool.create_object(object_info.clone());
        }

        // Write the chunk
        let complete = self.buffer_pool.write_chunk(&object_id, chunk_index, data);

        if complete {
            // Object fully received — add to local objects
            self.object_added(object_info);
        }

        complete
    }

    /// Update the known location of an object.
    pub fn update_object_location(&mut self, object_id: &ObjectID, node_id: NodeID) {
        self.pull_manager.update_object_location(object_id, node_id);
    }

    /// Check if an object is available locally.
    pub fn is_object_local(&self, object_id: &ObjectID) -> bool {
        self.local_objects.contains_key(object_id)
    }

    /// Get info about a local object.
    pub fn get_local_object(&self, object_id: &ObjectID) -> Option<&LocalObjectInfo> {
        self.local_objects.get(object_id)
    }

    /// Get the raw data and metadata bytes for a sealed object from the plasma store.
    pub fn get_object_data(&self, object_id: &ObjectID) -> Option<(Vec<u8>, Vec<u8>)> {
        self.plasma_store.get_object_data(object_id)
    }

    pub fn self_node_id(&self) -> &NodeID {
        &self.self_node_id
    }

    pub fn config(&self) -> &ObjectManagerConfig {
        &self.config
    }

    pub fn num_local_objects(&self) -> usize {
        self.local_objects.len()
    }

    pub fn num_active_pulls(&self) -> usize {
        self.pull_manager.num_active_pulls()
    }

    pub fn num_active_pushes(&self) -> usize {
        self.push_manager.num_active_pushes()
    }

    /// Access the pull manager (immutable).
    pub fn pull_manager(&self) -> &PullManager {
        &self.pull_manager
    }

    /// Access the pull manager (mutable).
    pub fn pull_manager_mut(&mut self) -> &mut PullManager {
        &mut self.pull_manager
    }

    /// Access the push manager (immutable).
    pub fn push_manager(&self) -> &PushManager {
        &self.push_manager
    }

    /// Access the push manager (mutable).
    pub fn push_manager_mut(&mut self) -> &mut PushManager {
        &mut self.push_manager
    }

    /// Access the memory monitor.
    pub fn memory_monitor(&self) -> &Arc<MemoryMonitor> {
        &self.memory_monitor
    }

    /// Running sum of used memory (data + metadata).
    pub fn used_memory(&self) -> i64 {
        self.used_memory
    }

    /// Number of deferred (pending) push requests.
    pub fn num_deferred_pushes(&self) -> usize {
        self.deferred_pushes.len()
    }

    /// Attach a spill manager to enable spill-assisted pulls.
    pub fn set_spill_manager(&mut self, mgr: Arc<SpillManager>) {
        self.spill_manager = Some(mgr);
    }

    /// Try to restore an object from spill storage and make it local.
    ///
    /// Returns `true` if the object was successfully restored and is now local.
    pub fn try_restore_from_spill(&mut self, object_id: &ObjectID) -> bool {
        let spill_mgr = match &self.spill_manager {
            Some(m) => m.clone(),
            None => return false,
        };

        let url = match spill_mgr.get_spill_url(object_id) {
            Some(u) => u,
            None => return false,
        };

        match spill_mgr.restore_object(&url) {
            Ok((data, metadata)) => {
                let info = ObjectInfo {
                    object_id: *object_id,
                    data_size: data.len() as i64,
                    metadata_size: metadata.len() as i64,
                    ..Default::default()
                };
                self.object_added(info);
                true
            }
            Err(_) => false,
        }
    }

    /// Handle a pull request from a remote node.
    ///
    /// If the object is local, initiates a push immediately.
    /// If not local but spilled, restores from spill then pushes.
    /// Otherwise defers the push.
    pub fn handle_pull_request(
        &mut self,
        object_id: ObjectID,
        requester_node_id: NodeID,
    ) -> bool {
        if self.is_object_local(&object_id) {
            return self.push(object_id, requester_node_id);
        }

        // Try restoring from spill.
        if self.try_restore_from_spill(&object_id) {
            return self.push(object_id, requester_node_id);
        }

        // Defer the push -- it will fire when the object arrives.
        self.push(object_id, requester_node_id);
        false
    }

    /// Used memory as a fraction of capacity.
    pub fn used_memory_fraction(&self) -> f64 {
        self.memory_monitor.usage_fraction()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plasma::allocator::IAllocator;
    use crate::plasma::store::PlasmaStoreConfig;

    struct DummyAllocator;
    impl IAllocator for DummyAllocator {
        fn allocate(&self, bytes: usize) -> Option<crate::plasma::allocator::Allocation> {
            Some(crate::plasma::allocator::Allocation {
                address: std::ptr::null_mut(),
                size: bytes as i64,
                fd: -1,
                offset: 0,
                device_num: 0,
                mmap_size: bytes as i64,
                fallback_allocated: false,
            })
        }
        fn fallback_allocate(&self, _: usize) -> Option<crate::plasma::allocator::Allocation> {
            None
        }
        fn free(&self, _: crate::plasma::allocator::Allocation) {}
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
    fn test_object_manager_pull_push() {
        let config = ObjectManagerConfig::default();
        let store_config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let allocator = Arc::new(DummyAllocator);
        let store = Arc::new(PlasmaStore::new(allocator, &store_config));
        let self_id = make_nid(0);

        let mut om = ObjectManager::new(config, self_id, store);

        // Add a local object
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 1024,
            ..Default::default()
        };
        om.object_added(info);
        assert!(om.is_object_local(&oid));

        // Push to remote
        let remote_node = make_nid(1);
        assert!(om.push(oid, remote_node));

        // Pull request
        let o2 = make_oid(2);
        let req_id = om.pull(vec![o2], BundlePriority::GetRequest);
        assert_eq!(om.num_active_pulls(), 1);

        om.cancel_pull(req_id);
        assert_eq!(om.num_active_pulls(), 0);
    }

    fn make_test_om() -> (ObjectManager, Arc<PlasmaStore>) {
        let config = ObjectManagerConfig::default();
        let store_config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let allocator = Arc::new(DummyAllocator);
        let store = Arc::new(PlasmaStore::new(allocator, &store_config));
        let om = ObjectManager::new(config, make_nid(0), store.clone());
        (om, store)
    }

    #[test]
    fn test_object_added_and_deleted() {
        let (mut om, _) = make_test_om();
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 512,
            metadata_size: 64,
            ..Default::default()
        };

        assert_eq!(om.num_local_objects(), 0);
        om.object_added(info);
        assert_eq!(om.num_local_objects(), 1);
        assert!(om.is_object_local(&oid));
        assert_eq!(om.used_memory(), 576);

        om.object_deleted(&oid);
        assert!(!om.is_object_local(&oid));
        assert_eq!(om.num_local_objects(), 0);
        assert_eq!(om.used_memory(), 0);
    }

    #[test]
    fn test_object_deleted_nonexistent() {
        let (mut om, _) = make_test_om();
        // Should not panic for unknown object
        om.object_deleted(&make_oid(99));
        assert_eq!(om.num_local_objects(), 0);
    }

    #[test]
    fn test_push_nonexistent_returns_false() {
        let (mut om, _) = make_test_om();
        assert!(!om.push(make_oid(1), make_nid(1)));
    }

    #[test]
    fn test_get_local_object() {
        let (mut om, _) = make_test_om();
        let oid = make_oid(1);
        assert!(om.get_local_object(&oid).is_none());

        let info = ObjectInfo {
            object_id: oid,
            data_size: 256,
            ..Default::default()
        };
        om.object_added(info);
        let local = om.get_local_object(&oid).unwrap();
        assert_eq!(local.object_info.data_size, 256);
        assert!(!local.is_being_pushed);
    }

    #[test]
    fn test_accessors() {
        let (om, _) = make_test_om();
        assert_eq!(*om.self_node_id(), make_nid(0));
        assert!(om.config().object_chunk_size > 0);
        assert_eq!(om.num_active_pushes(), 0);
        let _ = om.pull_manager();
        let _ = om.push_manager();
        let _ = om.memory_monitor();
    }

    #[test]
    fn test_handle_push_single_chunk() {
        let (mut om, _) = make_test_om();
        let oid = make_oid(5);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 100,
            metadata_size: 0,
            ..Default::default()
        };
        let data = vec![0xABu8; 100];

        let complete = om.handle_push(oid, 0, &data, info);
        assert!(complete);
        assert!(om.is_object_local(&oid));
        assert_eq!(om.num_local_objects(), 1);
    }

    #[test]
    fn test_update_object_location() {
        let (mut om, _) = make_test_om();
        let oid = make_oid(1);
        // Should not panic
        om.update_object_location(&oid, make_nid(2));
    }

    #[test]
    fn test_multiple_objects() {
        let (mut om, _) = make_test_om();
        for i in 0..10u8 {
            let info = ObjectInfo {
                object_id: make_oid(i),
                data_size: 100,
                ..Default::default()
            };
            om.object_added(info);
        }
        assert_eq!(om.num_local_objects(), 10);
        assert_eq!(om.used_memory(), 1000);

        for i in 0..5u8 {
            om.object_deleted(&make_oid(i));
        }
        assert_eq!(om.num_local_objects(), 5);
        assert_eq!(om.used_memory(), 500);
    }

    #[test]
    fn test_deferred_push_served_after_object_arrives() {
        let (mut om, _) = make_test_om();
        let oid = make_oid(50);
        let remote = make_nid(5);

        // Push for a non-local object is deferred.
        assert!(!om.push(oid, remote));
        assert_eq!(om.num_active_pushes(), 0);
        assert_eq!(om.num_deferred_pushes(), 1);

        // Object arrives -- deferred push should fire.
        om.object_added(ObjectInfo {
            object_id: oid,
            data_size: 256,
            metadata_size: 0,
            ..Default::default()
        });
        assert_eq!(om.num_active_pushes(), 1);
        assert_eq!(om.num_deferred_pushes(), 0);
    }

    #[test]
    fn test_deferred_push_only_matching_object_served() {
        let (mut om, _) = make_test_om();
        let oid1 = make_oid(60);
        let oid2 = make_oid(61);
        let remote = make_nid(5);

        om.push(oid1, remote);
        om.push(oid2, remote);
        assert_eq!(om.num_deferred_pushes(), 2);

        // Only oid1 arrives.
        om.object_added(ObjectInfo {
            object_id: oid1,
            data_size: 100,
            ..Default::default()
        });
        assert_eq!(om.num_active_pushes(), 1);
        assert_eq!(om.num_deferred_pushes(), 1); // oid2 still deferred
    }

    #[test]
    fn test_spill_assisted_pull_targets() {
        let (mut om, _) = make_test_om();
        let oid = make_oid(70);
        let spill_node = make_nid(9);

        om.pull(vec![oid], BundlePriority::GetRequest);
        om.pull_manager_mut()
            .set_spilled_url(&oid, "s3://bucket/obj70".into(), spill_node);

        let spilled = om.pull_manager().get_spilled_pull_targets();
        assert_eq!(spilled.len(), 1);
        assert_eq!(spilled[0].0, oid);
        assert_eq!(spilled[0].1, "s3://bucket/obj70");
        assert_eq!(spilled[0].2, spill_node);
    }
}
