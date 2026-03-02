// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC service implementation for inter-node object transfers.
//!
//! Implements the `ObjectManagerService` tonic trait with Push, Pull, and
//! FreeObjects handlers, delegating to the `ObjectManager`.

use std::sync::Arc;

use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use ray_common::id::{NodeID, ObjectID, WorkerID};
use ray_proto::ray::rpc;

use crate::common::ObjectInfo;
use crate::object_manager::ObjectManager;

/// gRPC service implementation for the ObjectManagerService.
pub struct ObjectManagerServiceImpl {
    pub object_manager: Arc<Mutex<ObjectManager>>,
}

#[tonic::async_trait]
impl rpc::object_manager_service_server::ObjectManagerService for ObjectManagerServiceImpl {
    /// Handle an incoming push — receive a chunk of an object from a remote node.
    async fn push(
        &self,
        request: Request<rpc::PushRequest>,
    ) -> Result<Response<rpc::PushReply>, Status> {
        let req = request.into_inner();

        let object_id = ObjectID::from_binary(&req.object_id);
        let chunk_index = req.chunk_index as u64;

        // Build ObjectInfo from the push request.
        let owner_address = req.owner_address.as_ref();
        let object_info = ObjectInfo {
            object_id,
            is_mutable: false,
            data_size: req.data_size as i64,
            metadata_size: req.metadata_size as i64,
            owner_node_id: owner_address
                .map(|a| NodeID::from_binary(&a.node_id))
                .unwrap_or_else(NodeID::nil),
            owner_ip_address: owner_address
                .map(|a| a.ip_address.clone())
                .unwrap_or_default(),
            owner_port: owner_address.map(|a| a.port).unwrap_or(0),
            owner_worker_id: owner_address
                .map(|a| WorkerID::from_binary(&a.worker_id))
                .unwrap_or_else(WorkerID::nil),
        };

        let mut om = self.object_manager.lock();
        om.handle_push(object_id, chunk_index, &req.data, object_info);

        Ok(Response::new(rpc::PushReply {}))
    }

    /// Handle a pull request — push the requested object back to the requester.
    async fn pull(
        &self,
        request: Request<rpc::PullRequest>,
    ) -> Result<Response<rpc::PullReply>, Status> {
        let req = request.into_inner();

        let object_id = ObjectID::from_binary(&req.object_id);
        let requester_node_id = NodeID::from_binary(&req.node_id);

        let mut om = self.object_manager.lock();
        om.push(object_id, requester_node_id);

        Ok(Response::new(rpc::PullReply {}))
    }

    /// Handle a free-objects request — delete the specified objects.
    async fn free_objects(
        &self,
        request: Request<rpc::FreeObjectsRequest>,
    ) -> Result<Response<rpc::FreeObjectsReply>, Status> {
        let req = request.into_inner();

        let mut om = self.object_manager.lock();
        for oid_bytes in &req.object_ids {
            let object_id = ObjectID::from_binary(oid_bytes);
            om.object_deleted(&object_id);
        }

        Ok(Response::new(rpc::FreeObjectsReply {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ObjectManagerConfig;
    use crate::plasma::allocator::IAllocator;
    use rpc::object_manager_service_server::ObjectManagerService;
    use crate::plasma::store::{PlasmaStore, PlasmaStoreConfig};

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

    fn make_service() -> ObjectManagerServiceImpl {
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
        let om = ObjectManager::new(config, self_id, store);
        ObjectManagerServiceImpl {
            object_manager: Arc::new(Mutex::new(om)),
        }
    }

    #[tokio::test]
    async fn test_push_single_chunk_object() {
        let svc = make_service();
        let oid = make_oid(1);

        // Push a single-chunk object (data_size fits in one chunk).
        let req = Request::new(rpc::PushRequest {
            push_id: vec![1, 2, 3],
            object_id: oid.binary(),
            node_id: make_nid(1).binary(),
            owner_address: Some(rpc::Address {
                node_id: make_nid(10).binary(),
                ip_address: "10.0.0.1".to_string(),
                port: 5000,
                worker_id: vec![0; 28],
                ..Default::default()
            }),
            chunk_index: 0,
            data_size: 100,
            metadata_size: 0,
            data: vec![42u8; 100],
        });

        let reply = svc.push(req).await.unwrap();
        assert_eq!(reply.into_inner(), rpc::PushReply {});

        // Object should now be local.
        let om = svc.object_manager.lock();
        assert!(om.is_object_local(&oid));
    }

    #[tokio::test]
    async fn test_push_multi_chunk_object() {
        let svc = make_service();
        let oid = make_oid(2);
        let chunk_size = svc.object_manager.lock().config().object_chunk_size;

        // Object with 2 chunks.
        let data_size = chunk_size + 100;

        // First chunk — object not complete yet.
        let req1 = Request::new(rpc::PushRequest {
            object_id: oid.binary(),
            chunk_index: 0,
            data_size: data_size,
            data: vec![0u8; chunk_size as usize],
            ..Default::default()
        });
        svc.push(req1).await.unwrap();
        assert!(!svc.object_manager.lock().is_object_local(&oid));

        // Second chunk — object complete.
        let req2 = Request::new(rpc::PushRequest {
            object_id: oid.binary(),
            chunk_index: 1,
            data_size: data_size,
            data: vec![0u8; 100],
            ..Default::default()
        });
        svc.push(req2).await.unwrap();
        assert!(svc.object_manager.lock().is_object_local(&oid));
    }

    #[tokio::test]
    async fn test_pull_local_object() {
        let svc = make_service();
        let oid = make_oid(3);

        // Add a local object first.
        {
            let mut om = svc.object_manager.lock();
            om.object_added(ObjectInfo {
                object_id: oid,
                data_size: 512,
                ..Default::default()
            });
        }

        // Pull should initiate a push back to the requester.
        let req = Request::new(rpc::PullRequest {
            node_id: make_nid(5).binary(),
            object_id: oid.binary(),
        });
        let reply = svc.pull(req).await.unwrap();
        assert_eq!(reply.into_inner(), rpc::PullReply {});

        // Verify a push was started to the requester.
        let om = svc.object_manager.lock();
        assert_eq!(om.num_active_pushes(), 1);
    }

    #[tokio::test]
    async fn test_pull_nonexistent_object() {
        let svc = make_service();
        let oid = make_oid(99);

        // Pull for a non-local object — push won't start but no error.
        let req = Request::new(rpc::PullRequest {
            node_id: make_nid(5).binary(),
            object_id: oid.binary(),
        });
        let reply = svc.pull(req).await.unwrap();
        assert_eq!(reply.into_inner(), rpc::PullReply {});

        let om = svc.object_manager.lock();
        assert_eq!(om.num_active_pushes(), 0);
    }

    #[tokio::test]
    async fn test_free_objects() {
        let svc = make_service();
        let oid1 = make_oid(10);
        let oid2 = make_oid(11);

        // Add two local objects.
        {
            let mut om = svc.object_manager.lock();
            om.object_added(ObjectInfo {
                object_id: oid1,
                data_size: 100,
                ..Default::default()
            });
            om.object_added(ObjectInfo {
                object_id: oid2,
                data_size: 200,
                ..Default::default()
            });
            assert_eq!(om.num_local_objects(), 2);
        }

        // Free both.
        let req = Request::new(rpc::FreeObjectsRequest {
            object_ids: vec![oid1.binary(), oid2.binary()],
        });
        let reply = svc.free_objects(req).await.unwrap();
        assert_eq!(reply.into_inner(), rpc::FreeObjectsReply {});

        let om = svc.object_manager.lock();
        assert_eq!(om.num_local_objects(), 0);
        assert!(!om.is_object_local(&oid1));
        assert!(!om.is_object_local(&oid2));
    }

    #[tokio::test]
    async fn test_free_nonexistent_objects() {
        let svc = make_service();

        // Freeing non-existent objects is a no-op, no error.
        let req = Request::new(rpc::FreeObjectsRequest {
            object_ids: vec![make_oid(77).binary()],
        });
        let reply = svc.free_objects(req).await.unwrap();
        assert_eq!(reply.into_inner(), rpc::FreeObjectsReply {});
    }
}
