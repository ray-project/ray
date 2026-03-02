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
    assert_eq!(
        cluster_id.len(),
        28,
        "cluster ID should be exactly 28 bytes, got {}",
        cluster_id.len()
    );

    // Verify consistency: a second call returns the same cluster ID
    let resp2 = client
        .get_cluster_id(rpc::GetClusterIdRequest::default())
        .await
        .unwrap();
    assert_eq!(
        cluster_id,
        resp2.into_inner().cluster_id,
        "cluster ID should be consistent across calls"
    );

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
    let actor_data = info
        .actor_table_data
        .expect("actor_table_data should be present");
    assert_eq!(
        actor_data.actor_id, actor_id,
        "returned actor_id should match what was registered"
    );

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_list_named_actors() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient::new(channel);

    // Register a named actor
    let actor_id = vec![88u8; 16];
    client
        .register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.clone(),
                    name: "my_named_actor".to_string(),
                    ray_namespace: "test_ns".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // ListNamedActors should return it
    let resp = client
        .list_named_actors(rpc::ListNamedActorsRequest {
            ray_namespace: "test_ns".to_string(),
            all_namespaces: false,
            ..Default::default()
        })
        .await
        .unwrap();
    let named = resp.into_inner().named_actors_list;
    assert_eq!(named.len(), 1);
    assert_eq!(named[0].name, "my_named_actor");
    assert_eq!(named[0].ray_namespace, "test_ns");

    // ListNamedActors with different namespace should return empty
    let resp2 = client
        .list_named_actors(rpc::ListNamedActorsRequest {
            ray_namespace: "other_ns".to_string(),
            all_namespaces: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp2.into_inner().named_actors_list.len(), 0);

    // ListNamedActors with all_namespaces should return it
    let resp3 = client
        .list_named_actors(rpc::ListNamedActorsRequest {
            all_namespaces: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp3.into_inner().named_actors_list.len(), 1);

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

#[tokio::test]
async fn test_gcs_worker_info_roundtrip() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::worker_info_gcs_service_client::WorkerInfoGcsServiceClient::new(channel);

    // AddWorkerInfo
    client
        .add_worker_info(rpc::AddWorkerInfoRequest {
            worker_data: Some(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    ip_address: "10.0.0.1".to_string(),
                    port: 5000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // GetAllWorkerInfo
    let resp = client
        .get_all_worker_info(rpc::GetAllWorkerInfoRequest::default())
        .await
        .unwrap();
    let workers = resp.into_inner().worker_table_data;
    assert_eq!(workers.len(), 1);
    let addr = workers[0].worker_address.as_ref().unwrap();
    assert_eq!(addr.ip_address, "10.0.0.1");

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_placement_group_roundtrip() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::placement_group_info_gcs_service_client::PlacementGroupInfoGcsServiceClient::new(
            channel,
        );

    let pg_id = vec![10u8; 18]; // PlacementGroupID is 18 bytes

    // CreatePlacementGroup
    client
        .create_placement_group(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "test-pg".to_string(),
                strategy: 0,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // GetPlacementGroup
    let resp = client
        .get_placement_group(rpc::GetPlacementGroupRequest {
            placement_group_id: pg_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let pg = resp.into_inner().placement_group_table_data.unwrap();
    assert_eq!(pg.placement_group_id, pg_id);
    assert_eq!(pg.name, "test-pg");

    // GetAllPlacementGroup
    let resp = client
        .get_all_placement_group(rpc::GetAllPlacementGroupRequest::default())
        .await
        .unwrap();
    assert_eq!(resp.into_inner().placement_group_table_data.len(), 1);

    // RemovePlacementGroup
    client
        .remove_placement_group(rpc::RemovePlacementGroupRequest {
            placement_group_id: pg_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Verify removed
    let resp = client
        .get_placement_group(rpc::GetPlacementGroupRequest {
            placement_group_id: pg_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.into_inner().placement_group_table_data.is_none());

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_node_address_and_liveness() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel);

    // Register a node
    let node_id = vec![50u8; 28];
    client
        .register_node(rpc::RegisterNodeRequest {
            node_info: Some(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                state: 0, // ALIVE
                node_manager_address: "10.0.0.2".to_string(),
                node_manager_port: 6789,
                object_manager_port: 7890,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // GetAllNodeAddressAndLiveness
    let resp = client
        .get_all_node_address_and_liveness(
            rpc::GetAllNodeAddressAndLivenessRequest::default(),
        )
        .await
        .unwrap();
    let nodes = resp.into_inner().node_info_list;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id, node_id);
    assert_eq!(nodes[0].node_manager_address, "10.0.0.2");
    assert_eq!(nodes[0].node_manager_port, 6789);
    assert_eq!(nodes[0].object_manager_port, 7890);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_check_alive() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel);

    let node_id = vec![60u8; 28];
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

    // CheckAlive — registered node
    let resp = client
        .check_alive(rpc::CheckAliveRequest {
            node_ids: vec![node_id.clone()],
            ..Default::default()
        })
        .await
        .unwrap();
    let alive = resp.into_inner().raylet_alive;
    assert_eq!(alive, vec![true]);

    // CheckAlive — unknown node
    let resp = client
        .check_alive(rpc::CheckAliveRequest {
            node_ids: vec![vec![0u8; 28]],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().raylet_alive, vec![false]);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_kv_multi_get() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient::new(channel);

    // Put two keys
    for (k, v) in &[(b"k1".as_slice(), b"v1".as_slice()), (b"k2", b"v2")] {
        client
            .internal_kv_put(rpc::InternalKvPutRequest {
                namespace: b"ns".to_vec(),
                key: k.to_vec(),
                value: v.to_vec(),
                overwrite: true,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    // MultiGet
    let resp = client
        .internal_kv_multi_get(rpc::InternalKvMultiGetRequest {
            namespace: b"ns".to_vec(),
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k_missing".to_vec()],
            ..Default::default()
        })
        .await
        .unwrap();
    let results = resp.into_inner().results;
    assert_eq!(results.len(), 2); // k_missing should not be returned
    let keys: Vec<_> = results.iter().map(|e| e.key.clone()).collect();
    assert!(keys.contains(&b"k1".to_vec()));
    assert!(keys.contains(&b"k2".to_vec()));

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}

#[tokio::test]
async fn test_gcs_drain_node() {
    let server = start_test_gcs_server().await;
    let endpoint = format!("http://{}", server.addr);

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_lazy();
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel);

    // Register a node
    let node_id = vec![70u8; 28];
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

    // DrainNode (batched)
    let resp = client
        .drain_node(rpc::DrainNodeRequest {
            drain_node_data: vec![rpc::DrainNodeData {
                node_id: node_id.clone(),
            }],
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.drain_node_status.len(), 1);
    assert_eq!(inner.drain_node_status[0].node_id, node_id);

    server.shutdown_tx.send(()).unwrap();
    server.join_handle.await.unwrap();
}
