// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the Raylet gRPC server.

use std::collections::HashMap;
use std::sync::Arc;

use ray_common::id::NodeID;
use ray_proto::ray::rpc;
use ray_raylet::node_manager::{NodeManager, RayletConfig};
use ray_test_utils::start_test_raylet_server;

#[tokio::test]
async fn test_raylet_binds_and_health_check() {
    let server = start_test_raylet_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);

    let resp = client
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await;
    assert!(resp.is_ok());

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_raylet_get_system_config() {
    let server = start_test_raylet_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_manager_service_client::NodeManagerServiceClient::new(channel);

    let resp = client
        .get_system_config(rpc::GetSystemConfigRequest::default())
        .await
        .unwrap();
    let config = resp.into_inner().system_config;
    assert!(!config.is_empty());
    // Should be valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();
    assert!(parsed.is_object());

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_raylet_drain() {
    let server = start_test_raylet_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_manager_service_client::NodeManagerServiceClient::new(channel);

    let resp = client
        .drain_raylet(rpc::DrainRayletRequest {
            deadline_timestamp_ms: 5000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.into_inner().is_accepted);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

/// Start a GCS server and a raylet, verify the raylet registers with GCS.
#[tokio::test]
async fn test_raylet_registers_with_gcs() {
    use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
    use ray_rpc::client::RetryConfig;

    // 1. Start a test GCS server.
    let gcs_server = ray_test_utils::start_test_gcs_server().await;
    let gcs_addr = format!("http://{}", gcs_server.addr);

    // 2. Configure a raylet that points at this GCS.
    let node_id = NodeID::from_random();
    let config = RayletConfig {
        node_ip_address: "127.0.0.1".to_string(),
        port: 0,
        object_store_socket: "/tmp/test-plasma".to_string(),
        gcs_address: gcs_addr.clone(),
        log_dir: None,
        ray_config: ray_common::config::RayConfig::default(),
        node_id: node_id.hex(),
        resources: HashMap::from([("CPU".to_string(), 4.0)]),
        labels: HashMap::from([("env".to_string(), "test".to_string())]),
        session_name: "integration-test".to_string(),
    };

    let nm = Arc::new(NodeManager::new(config));

    // 3. Run the raylet in a background task.
    let nm_clone = Arc::clone(&nm);
    let raylet_handle = tokio::spawn(async move {
        nm_clone.run().await
    });

    // Give the raylet time to register.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Query GCS for all nodes — our raylet should appear.
    let gcs_client = GcsRpcClient::connect(&gcs_addr, RetryConfig::default())
        .await
        .unwrap();
    let reply = gcs_client
        .get_all_node_info(rpc::GetAllNodeInfoRequest::default())
        .await
        .unwrap();

    assert!(
        !reply.node_info_list.is_empty(),
        "Expected at least one registered node"
    );

    // Find our specific node.
    let our_node = reply
        .node_info_list
        .iter()
        .find(|n| n.node_id == node_id.binary())
        .expect("Our raylet should be in the node list");

    assert_eq!(our_node.node_manager_address, "127.0.0.1");
    assert!(our_node.node_manager_port > 0);
    assert_eq!(*our_node.resources_total.get("CPU").unwrap(), 4.0);
    assert_eq!(our_node.labels.get("env").unwrap(), "test");
    assert_eq!(
        our_node.state,
        rpc::gcs_node_info::GcsNodeState::Alive as i32
    );

    // 5. Clean up.
    raylet_handle.abort();
    gcs_server.shutdown_tx.send(()).ok();
    gcs_server.join_handle.await.ok();
}
