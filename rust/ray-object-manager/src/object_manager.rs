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
use crate::object_buffer_pool::ObjectBufferPool;
use crate::plasma::store::PlasmaStore;
use crate::pull_manager::{BundlePriority, PullManager};
use crate::push_manager::PushManager;

/// Information about a locally available object.
#[derive(Debug, Clone)]
pub struct LocalObjectInfo {
    pub object_info: ObjectInfo,
    /// Whether this object is being pushed to any remote node.
    pub is_being_pushed: bool,
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
}

impl ObjectManager {
    pub fn new(
        config: ObjectManagerConfig,
        self_node_id: NodeID,
        plasma_store: Arc<PlasmaStore>,
    ) -> Self {
        let chunk_size = config.object_chunk_size;
        let max_bytes = config.max_bytes_in_flight;

        Self {
            config,
            self_node_id,
            pull_manager: PullManager::new(max_bytes as i64),
            push_manager: PushManager::new(max_bytes, chunk_size),
            buffer_pool: ObjectBufferPool::new(chunk_size),
            local_objects: HashMap::new(),
            plasma_store,
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
    pub fn push(&mut self, object_id: ObjectID, node_id: NodeID) -> bool {
        if let Some(local_obj) = self.local_objects.get(&object_id) {
            let size =
                (local_obj.object_info.data_size + local_obj.object_info.metadata_size) as u64;
            self.push_manager.start_push(node_id, object_id, size)
        } else {
            false
        }
    }

    /// Handle notification that an object is now available locally.
    pub fn object_added(&mut self, object_info: ObjectInfo) {
        let object_id = object_info.object_id;
        self.local_objects.insert(
            object_id,
            LocalObjectInfo {
                object_info,
                is_being_pushed: false,
            },
        );
        self.pull_manager.object_available_locally(&object_id);
    }

    /// Handle notification that an object was deleted locally.
    pub fn object_deleted(&mut self, object_id: &ObjectID) {
        self.local_objects.remove(object_id);
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
}
