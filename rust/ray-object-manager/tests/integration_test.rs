// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the ObjectManager gRPC service.

use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;

use ray_common::id::{NodeID, ObjectID};
use ray_object_manager::common::{ObjectInfo, ObjectManagerConfig};
use ray_object_manager::grpc_service::ObjectManagerServiceImpl;
use ray_object_manager::object_manager::ObjectManager;
use ray_object_manager::plasma::allocator::{Allocation, IAllocator};
use ray_object_manager::plasma::store::{PlasmaStore, PlasmaStoreConfig};
use ray_proto::ray::rpc;

struct DummyAllocator;
impl IAllocator for DummyAllocator {
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

fn make_nid(val: u8) -> NodeID {
    let mut data = [0u8; 28];
    data[0] = val;
    NodeID::from_binary(&data)
}

fn make_oid(val: u8) -> ObjectID {
    let mut data = [0u8; 28];
    data[0] = val;
    ObjectID::from_binary(&data)
}

fn make_object_manager(node_val: u8) -> Arc<Mutex<ObjectManager>> {
    let config = ObjectManagerConfig::default();
    let store_config = PlasmaStoreConfig {
        object_store_memory: 1024 * 1024,
        plasma_directory: String::new(),
        fallback_directory: String::new(),
        huge_pages: false,
    };
    let allocator = Arc::new(DummyAllocator);
    let store = Arc::new(PlasmaStore::new(allocator, &store_config));
    Arc::new(Mutex::new(ObjectManager::new(
        config,
        make_nid(node_val),
        store,
    )))
}

/// Start an ObjectManager gRPC server on a random port, return the address.
async fn start_object_manager_server(
    om: Arc<Mutex<ObjectManager>>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let svc = ObjectManagerServiceImpl {
        object_manager: om,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        let server = tonic::transport::Server::builder().add_service(
            rpc::object_manager_service_server::ObjectManagerServiceServer::new(svc),
        );
        server.serve_with_incoming(incoming).await.ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, handle)
}

/// Test pushing a single-chunk object to a remote ObjectManager via gRPC.
#[tokio::test]
async fn test_push_object_via_grpc() {
    let receiver_om = make_object_manager(1);
    let (addr, handle) = start_object_manager_server(Arc::clone(&receiver_om)).await;

    // Connect a client.
    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel);

    let oid = make_oid(42);
    let data = vec![0xABu8; 256];

    // Push the object as a single chunk.
    let reply = client
        .push(rpc::PushRequest {
            push_id: vec![1],
            object_id: oid.binary(),
            node_id: make_nid(99).binary(),
            owner_address: Some(rpc::Address {
                node_id: make_nid(10).binary(),
                ip_address: "10.0.0.1".to_string(),
                port: 5000,
                worker_id: vec![0; 28],
                ..Default::default()
            }),
            chunk_index: 0,
            data_size: 256,
            metadata_size: 0,
            data: data.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(reply, rpc::PushReply {});

    // Verify the object is now local on the receiver.
    let om = receiver_om.lock();
    assert!(om.is_object_local(&oid));
    let local_obj = om.get_local_object(&oid).unwrap();
    assert_eq!(local_obj.object_info.data_size, 256);
    assert_eq!(local_obj.object_info.owner_ip_address, "10.0.0.1");

    handle.abort();
}

/// Test the pull-then-push flow via gRPC.
#[tokio::test]
async fn test_pull_triggers_push_via_grpc() {
    let om = make_object_manager(1);
    let oid = make_oid(7);

    // Add a local object to the manager before starting the server.
    {
        let mut locked = om.lock();
        locked.object_added(ObjectInfo {
            object_id: oid,
            data_size: 512,
            metadata_size: 0,
            ..Default::default()
        });
    }

    let (addr, handle) = start_object_manager_server(Arc::clone(&om)).await;

    // Connect a client and send a Pull request.
    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel);

    let reply = client
        .pull(rpc::PullRequest {
            node_id: make_nid(5).binary(),
            object_id: oid.binary(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(reply, rpc::PullReply {});

    // The pull handler should have started a push to node 5.
    let locked = om.lock();
    assert_eq!(locked.num_active_pushes(), 1);

    handle.abort();
}

/// Test freeing objects via gRPC.
#[tokio::test]
async fn test_free_objects_via_grpc() {
    let om = make_object_manager(1);
    let oid1 = make_oid(20);
    let oid2 = make_oid(21);

    // Add local objects.
    {
        let mut locked = om.lock();
        locked.object_added(ObjectInfo {
            object_id: oid1,
            data_size: 100,
            ..Default::default()
        });
        locked.object_added(ObjectInfo {
            object_id: oid2,
            data_size: 200,
            ..Default::default()
        });
        assert_eq!(locked.num_local_objects(), 2);
    }

    let (addr, handle) = start_object_manager_server(Arc::clone(&om)).await;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel);

    let reply = client
        .free_objects(rpc::FreeObjectsRequest {
            object_ids: vec![oid1.binary(), oid2.binary()],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(reply, rpc::FreeObjectsReply {});

    // Verify objects are gone.
    let locked = om.lock();
    assert_eq!(locked.num_local_objects(), 0);

    handle.abort();
}

/// Test pushing a multi-chunk object via gRPC.
#[tokio::test]
async fn test_push_multi_chunk_object_via_grpc() {
    // Use a small chunk size (1KB) to test multi-chunk without hitting gRPC limits.
    let config = ObjectManagerConfig {
        object_chunk_size: 1024,
        ..ObjectManagerConfig::default()
    };
    let store_config = PlasmaStoreConfig {
        object_store_memory: 1024 * 1024,
        plasma_directory: String::new(),
        fallback_directory: String::new(),
        huge_pages: false,
    };
    let allocator = Arc::new(DummyAllocator);
    let store = Arc::new(PlasmaStore::new(allocator, &store_config));
    let receiver_om = Arc::new(Mutex::new(ObjectManager::new(
        config,
        make_nid(2),
        store,
    )));

    let (addr, handle) = start_object_manager_server(Arc::clone(&receiver_om)).await;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel);

    let oid = make_oid(55);
    // Object that requires exactly 3 chunks at 1024 bytes each.
    let total_size: u64 = 1024 * 2 + 100;

    // Send 3 chunks.
    for i in 0..3u32 {
        client
            .push(rpc::PushRequest {
                push_id: vec![1],
                object_id: oid.binary(),
                node_id: make_nid(99).binary(),
                chunk_index: i,
                data_size: total_size,
                metadata_size: 0,
                data: vec![i as u8; if i < 2 { 1024 } else { 100 }],
                ..Default::default()
            })
            .await
            .unwrap();
    }

    // After all 3 chunks, object should be local.
    let om = receiver_om.lock();
    assert!(om.is_object_local(&oid));

    handle.abort();
}
