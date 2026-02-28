// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the GCS gRPC server.

use ray_proto::ray::rpc;
use ray_test_utils::start_test_gcs_server;

#[tokio::test]
async fn test_gcs_server_binds_and_health_check() {
    let server = start_test_gcs_server().await;
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
async fn test_gcs_job_rpc_roundtrip() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::job_info_gcs_service_client::JobInfoGcsServiceClient::new(channel);

    // AddJob
    let add_resp = client
        .add_job(rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                is_dead: false,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    assert!(add_resp.is_ok());

    // GetAllJobInfo
    let all_resp = client
        .get_all_job_info(rpc::GetAllJobInfoRequest::default())
        .await
        .unwrap();
    assert_eq!(all_resp.into_inner().job_info_list.len(), 1);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_job_finish_roundtrip() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::job_info_gcs_service_client::JobInfoGcsServiceClient::new(channel);

    // AddJob
    client
        .add_job(rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![2, 0, 0, 0],
                is_dead: false,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // MarkJobFinished
    client
        .mark_job_finished(rpc::MarkJobFinishedRequest {
            job_id: vec![2, 0, 0, 0],
            ..Default::default()
        })
        .await
        .unwrap();

    // GetAllJobInfo should still return the job (but marked dead)
    let all_resp = client
        .get_all_job_info(rpc::GetAllJobInfoRequest::default())
        .await
        .unwrap();
    let jobs = all_resp.into_inner().job_info_list;
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].is_dead);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_node_register_roundtrip() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel);

    // RegisterNode
    let node_id = vec![42u8; 28];
    client
        .register_node(rpc::RegisterNodeRequest {
            node_info: Some(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                state: 0, // ALIVE
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 12345,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // GetAllNodeInfo
    let all_resp = client
        .get_all_node_info(rpc::GetAllNodeInfoRequest::default())
        .await
        .unwrap();
    let nodes = all_resp.into_inner().node_info_list;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id, node_id);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_node_unregister() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel);

    let node_id = vec![7u8; 28];
    client
        .register_node(rpc::RegisterNodeRequest {
            node_info: Some(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                state: 0,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // Unregister
    client
        .unregister_node(rpc::UnregisterNodeRequest {
            node_id: node_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Should still have the node in list but marked dead
    let all_resp = client
        .get_all_node_info(rpc::GetAllNodeInfoRequest::default())
        .await
        .unwrap();
    let nodes = all_resp.into_inner().node_info_list;
    assert_eq!(nodes.len(), 1);
    // State 1 = DEAD
    assert_eq!(nodes[0].state, 1);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_kv_roundtrip() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient::new(channel);

    // Put
    let put_resp = client
        .internal_kv_put(rpc::InternalKvPutRequest {
            namespace: b"test-ns".to_vec(),
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            overwrite: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(put_resp.into_inner().added);

    // Get
    let get_resp = client
        .internal_kv_get(rpc::InternalKvGetRequest {
            namespace: b"test-ns".to_vec(),
            key: b"hello".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(get_resp.into_inner().value, b"world");

    // Exists
    let exists_resp = client
        .internal_kv_exists(rpc::InternalKvExistsRequest {
            namespace: b"test-ns".to_vec(),
            key: b"hello".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(exists_resp.into_inner().exists);

    // Keys
    let keys_resp = client
        .internal_kv_keys(rpc::InternalKvKeysRequest {
            namespace: b"test-ns".to_vec(),
            prefix: b"hel".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(keys_resp.into_inner().results.len(), 1);

    // Del
    let del_resp = client
        .internal_kv_del(rpc::InternalKvDelRequest {
            namespace: b"test-ns".to_vec(),
            key: b"hello".to_vec(),
            del_by_prefix: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(del_resp.into_inner().deleted_num, 1);

    // Verify deleted
    let exists_resp = client
        .internal_kv_exists(rpc::InternalKvExistsRequest {
            namespace: b"test-ns".to_vec(),
            key: b"hello".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!exists_resp.into_inner().exists);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_cluster_id() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel);

    let resp = client
        .get_cluster_id(rpc::GetClusterIdRequest::default())
        .await
        .unwrap();
    let cluster_id = resp.into_inner().cluster_id;
    assert!(!cluster_id.is_empty());

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_actor_register_and_get() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient::new(channel);

    let actor_id = vec![99u8; 16];
    let job_id = vec![1, 0, 0, 0];

    // RegisterActor
    client
        .register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.clone(),
                    ..Default::default()
                }),
                job_id: job_id.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // GetActorInfo
    let resp = client
        .get_actor_info(rpc::GetActorInfoRequest {
            actor_id: actor_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let info = resp.into_inner();
    assert!(info.actor_table_data.is_some());

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_get_next_job_id_monotonic() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::job_info_gcs_service_client::JobInfoGcsServiceClient::new(channel);

    let resp1 = client
        .get_next_job_id(rpc::GetNextJobIdRequest::default())
        .await
        .unwrap();
    let id1 = resp1.into_inner().job_id;

    let resp2 = client
        .get_next_job_id(rpc::GetNextJobIdRequest::default())
        .await
        .unwrap();
    let id2 = resp2.into_inner().job_id;

    assert!(id2 > id1, "Job IDs should be monotonically increasing");

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}
