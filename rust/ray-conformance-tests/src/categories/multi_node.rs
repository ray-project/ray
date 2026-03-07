#![allow(clippy::needless_update)]
// Protobuf structs use ..Default::default() for forward-compat
// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Multi-node integration tests.
//!
//! Verifies cross-node behavior: GCS registration of multiple nodes,
//! object transfer between ObjectManagers, and coordinated lifecycles.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use ray_common::config::RayConfig;
use ray_common::id::{NodeID, ObjectID, WorkerID};
use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
use ray_object_manager::common::{ObjectInfo, ObjectManagerConfig};
use ray_object_manager::grpc_service::ObjectManagerServiceImpl;
use ray_object_manager::object_manager::ObjectManager;
use ray_object_manager::plasma::allocator::{Allocation, IAllocator};
use ray_object_manager::plasma::store::{PlasmaStore, PlasmaStoreConfig};
use ray_proto::ray::rpc;
use ray_raylet::node_manager::{NodeManager, RayletConfig};
use ray_rpc::client::RetryConfig;

// ─── Helpers ────────────────────────────────────────────────────────────────

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

fn make_node_id(val: u8) -> NodeID {
    let mut data = [0u8; 28];
    data[0] = val;
    NodeID::from_binary(&data)
}

fn make_object_id(val: u8) -> ObjectID {
    let mut data = [0u8; 28];
    data[0] = val;
    ObjectID::from_binary(&data)
}

fn make_object_manager(node_id: NodeID) -> Arc<Mutex<ObjectManager>> {
    let config = ObjectManagerConfig::default();
    let store_config = PlasmaStoreConfig {
        object_store_memory: 1024 * 1024,
        plasma_directory: String::new(),
        fallback_directory: String::new(),
        huge_pages: false,
    };
    let allocator = Arc::new(TestAllocator);
    let store = Arc::new(PlasmaStore::new(allocator, &store_config));
    Arc::new(Mutex::new(ObjectManager::new(config, node_id, store)))
}

async fn start_om_server(
    om: Arc<Mutex<ObjectManager>>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let svc = ObjectManagerServiceImpl { object_manager: om };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(rpc::object_manager_service_server::ObjectManagerServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

fn make_raylet_config(node_id: &NodeID, gcs_address: &str) -> RayletConfig {
    RayletConfig {
        node_ip_address: "127.0.0.1".to_string(),
        port: 0,
        object_store_socket: String::new(),
        gcs_address: gcs_address.to_string(),
        log_dir: None,
        ray_config: RayConfig::default(),
        node_id: node_id.hex(),
        resources: HashMap::from([("CPU".to_string(), 4.0)]),
        labels: HashMap::new(),
        session_name: "multi-node-test".to_string(),
        auth_token: None,
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// Two nodes register with GCS and can see each other.
#[tokio::test]
async fn test_two_nodes_register_with_gcs() {
    // 1. Start GCS.
    let gcs = ray_test_utils::start_test_gcs_server().await;
    let gcs_addr = format!("http://{}", gcs.addr);

    // 2. Start two raylets pointing at GCS.
    let node_a_id = NodeID::from_random();
    let node_b_id = NodeID::from_random();

    let nm_a = Arc::new(NodeManager::new(make_raylet_config(&node_a_id, &gcs_addr)));
    let nm_b = Arc::new(NodeManager::new(make_raylet_config(&node_b_id, &gcs_addr)));

    let nm_a_clone = Arc::clone(&nm_a);
    let nm_b_clone = Arc::clone(&nm_b);
    let handle_a = tokio::spawn(async move { nm_a_clone.run().await });
    let handle_b = tokio::spawn(async move { nm_b_clone.run().await });

    // Wait for registration.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Query GCS for registered nodes.
    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(50),
        ..Default::default()
    };
    let gcs_client = GcsRpcClient::connect(&gcs_addr, retry_config)
        .await
        .unwrap();

    let reply = gcs_client
        .get_all_node_info(rpc::GetAllNodeInfoRequest::default())
        .await
        .unwrap();

    let registered_node_ids: Vec<String> = reply
        .node_info_list
        .iter()
        .filter(|n| n.state == rpc::gcs_node_info::GcsNodeState::Alive as i32)
        .map(|n| hex::encode(&n.node_id))
        .collect();

    assert!(
        registered_node_ids.contains(&node_a_id.hex()),
        "Node A should be registered with GCS"
    );
    assert!(
        registered_node_ids.contains(&node_b_id.hex()),
        "Node B should be registered with GCS"
    );
    assert!(
        registered_node_ids.len() >= 2,
        "At least 2 nodes should be registered"
    );

    handle_a.abort();
    handle_b.abort();
}

/// Object pushed from Node A's ObjectManager arrives at Node B via gRPC.
#[tokio::test]
async fn test_cross_node_object_push_via_grpc() {
    let node_a_id = make_node_id(1);
    let node_b_id = make_node_id(2);

    // Create two ObjectManagers.
    let om_a = make_object_manager(node_a_id);
    let om_b = make_object_manager(node_b_id);

    // Start gRPC servers for both.
    let (addr_a, handle_a) = start_om_server(Arc::clone(&om_a)).await;
    let (addr_b, handle_b) = start_om_server(Arc::clone(&om_b)).await;

    // Node A creates a local object.
    let oid = make_object_id(42);
    let object_data = vec![0xABu8; 512];
    {
        let mut locked = om_a.lock();
        locked.object_added(ObjectInfo {
            object_id: oid,
            is_mutable: false,
            data_size: 512,
            metadata_size: 0,
            owner_ip_address: "10.0.0.1".to_string(),
            owner_port: 5000,
            owner_node_id: node_a_id,
            owner_worker_id: WorkerID::from_binary(&[0; 28]),
        });
    }

    // Node B connects to Node A and sends a Pull request.
    let channel_a = tonic::transport::Endpoint::from_shared(format!("http://{}", addr_a))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_a =
        rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel_a);

    let pull_reply = client_a
        .pull(rpc::PullRequest {
            node_id: node_b_id.binary(),
            object_id: oid.binary(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pull_reply, rpc::PullReply {});

    // Node A now has a pending push to Node B.
    assert_eq!(om_a.lock().num_active_pushes(), 1);

    // Simulate the actual push: Node A reads data and sends Push gRPC to Node B.
    let channel_b = tonic::transport::Endpoint::from_shared(format!("http://{}", addr_b))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_b =
        rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel_b);

    let push_reply = client_b
        .push(rpc::PushRequest {
            push_id: vec![1],
            object_id: oid.binary(),
            node_id: node_a_id.binary(),
            owner_address: Some(rpc::Address {
                node_id: node_a_id.binary(),
                ip_address: "10.0.0.1".to_string(),
                port: 5000,
                worker_id: vec![0; 28],
                ..Default::default()
            }),
            chunk_index: 0,
            data_size: 512,
            metadata_size: 0,
            data: object_data.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(push_reply, rpc::PushReply {});

    // Verify the object arrived on Node B.
    let locked_b = om_b.lock();
    assert!(
        locked_b.is_object_local(&oid),
        "Object should be local on Node B after push"
    );
    let local_obj = locked_b.get_local_object(&oid).unwrap();
    assert_eq!(local_obj.object_info.data_size, 512);
    assert_eq!(local_obj.object_info.owner_ip_address, "10.0.0.1");

    // Also verify Node A still has the object.
    assert!(
        om_a.lock().is_object_local(&oid),
        "Object should still be local on Node A"
    );

    handle_a.abort();
    handle_b.abort();
}

/// FreeObjects on one node doesn't affect the other node.
#[tokio::test]
async fn test_cross_node_free_objects_independent() {
    let node_a_id = make_node_id(10);
    let node_b_id = make_node_id(20);

    let om_a = make_object_manager(node_a_id);
    let om_b = make_object_manager(node_b_id);

    let (addr_a, handle_a) = start_om_server(Arc::clone(&om_a)).await;
    let (addr_b, handle_b) = start_om_server(Arc::clone(&om_b)).await;

    let oid = make_object_id(99);

    // Both nodes have the same object.
    for om in [&om_a, &om_b] {
        let mut locked = om.lock();
        locked.object_added(ObjectInfo {
            object_id: oid,
            data_size: 100,
            ..Default::default()
        });
    }

    // Free the object on Node A via gRPC.
    let channel_a = tonic::transport::Endpoint::from_shared(format!("http://{}", addr_a))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_a =
        rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel_a);

    client_a
        .free_objects(rpc::FreeObjectsRequest {
            object_ids: vec![oid.binary()],
        })
        .await
        .unwrap();

    // Node A should not have the object.
    assert!(
        !om_a.lock().is_object_local(&oid),
        "Object should be freed on Node A"
    );

    // Node B should still have the object.
    assert!(
        om_b.lock().is_object_local(&oid),
        "Object should still exist on Node B"
    );

    // Cleanup — suppress unused variable warnings.
    let _ = addr_b;
    handle_a.abort();
    handle_b.abort();
}

/// Multi-chunk object transfer: large object split across chunks arrives correctly.
#[tokio::test]
async fn test_cross_node_multi_chunk_push() {
    let node_a_id = make_node_id(30);
    let node_b_id = make_node_id(40);

    // Use a small chunk size (256 bytes) for the ObjectManagers.
    let config = ObjectManagerConfig {
        object_chunk_size: 256,
        ..ObjectManagerConfig::default()
    };
    let store_config = PlasmaStoreConfig {
        object_store_memory: 1024 * 1024,
        plasma_directory: String::new(),
        fallback_directory: String::new(),
        huge_pages: false,
    };

    let allocator_b = Arc::new(TestAllocator);
    let store_b = Arc::new(PlasmaStore::new(allocator_b, &store_config));
    let om_b = Arc::new(Mutex::new(ObjectManager::new(config, node_b_id, store_b)));

    let (addr_b, handle_b) = start_om_server(Arc::clone(&om_b)).await;

    let channel_b = tonic::transport::Endpoint::from_shared(format!("http://{}", addr_b))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_b =
        rpc::object_manager_service_client::ObjectManagerServiceClient::new(channel_b);

    let oid = make_object_id(77);
    let total_data_size: u64 = 600; // 3 chunks at 256 bytes: 256 + 256 + 88

    // Send 3 chunks.
    for chunk_idx in 0..3u32 {
        let chunk_size = if chunk_idx < 2 { 256 } else { 88 };
        client_b
            .push(rpc::PushRequest {
                push_id: vec![1],
                object_id: oid.binary(),
                node_id: node_a_id.binary(),
                owner_address: Some(rpc::Address {
                    node_id: node_a_id.binary(),
                    ip_address: "10.0.0.5".to_string(),
                    port: 6000,
                    worker_id: vec![0; 28],
                    ..Default::default()
                }),
                chunk_index: chunk_idx,
                data_size: total_data_size,
                metadata_size: 0,
                data: vec![chunk_idx as u8; chunk_size],
            })
            .await
            .unwrap();
    }

    // Verify the object arrived on Node B with correct size.
    let locked_b = om_b.lock();
    assert!(
        locked_b.is_object_local(&oid),
        "Object should be local after multi-chunk push"
    );
    let local_obj = locked_b.get_local_object(&oid).unwrap();
    assert_eq!(local_obj.object_info.data_size, total_data_size as i64);
    assert_eq!(local_obj.object_info.owner_ip_address, "10.0.0.5");

    handle_b.abort();
}
