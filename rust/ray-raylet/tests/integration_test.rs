// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the Raylet gRPC server.

use ray_proto::ray::rpc;
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
