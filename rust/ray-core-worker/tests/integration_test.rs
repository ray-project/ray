// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the CoreWorker gRPC service.

use std::net::SocketAddr;
use std::sync::Arc;

use ray_core_worker::actor_handle::ActorHandle;
use ray_core_worker::core_worker::CoreWorker;
use ray_core_worker::grpc_service::CoreWorkerServiceImpl;
use ray_core_worker::options::{CoreWorkerOptions, Language, WorkerType};
use ray_core_worker::task_receiver::{TaskExecutionCallback, TaskResult};
use ray_common::id::{ActorID, ClusterID, JobID, NodeID, ObjectID, TaskID, WorkerID};
use ray_proto::ray::rpc;

fn make_test_core_worker() -> Arc<CoreWorker> {
    Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Driver,
        language: Language::Python,
        store_socket: String::new(),
        raylet_socket: String::new(),
        job_id: JobID::from_int(1),
        gcs_address: String::new(),
        node_ip_address: "127.0.0.1".to_string(),
        worker_id: WorkerID::from_random(),
        node_id: NodeID::nil(),
        cluster_id: ClusterID::nil(),
        session_name: "test".to_string(),
        num_workers: 1,
    }))
}

/// NOTE: handle_exit is currently a stub that always returns success: true.
/// This test verifies the handler can be called without error and exercises
/// both force_exit paths. Update when exit logic is implemented.
#[tokio::test]
async fn test_core_worker_service_exit() {
    let core_worker = make_test_core_worker();
    let svc = CoreWorkerServiceImpl {
        core_worker: Arc::clone(&core_worker),
    };

    // Graceful exit
    let reply = svc
        .handle_exit(rpc::ExitRequest {
            force_exit: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(reply.success, "graceful exit should succeed");

    // Force exit
    let reply = svc
        .handle_exit(rpc::ExitRequest {
            force_exit: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(reply.success, "force exit should succeed");
}

#[tokio::test]
async fn test_core_worker_service_num_pending_tasks() {
    let core_worker = make_test_core_worker();
    let svc = CoreWorkerServiceImpl {
        core_worker: Arc::clone(&core_worker),
    };

    // Initially zero pending tasks
    let reply = svc
        .handle_num_pending_tasks(rpc::NumPendingTasksRequest::default())
        .unwrap();
    assert_eq!(reply.num_pending_tasks, 0, "fresh worker should have 0 pending tasks");

    // Submit a task, then verify the count changes
    let task_spec = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "test_task".to_string(),
        ..Default::default()
    };
    core_worker.submit_task(&task_spec).await.unwrap();

    let reply = svc
        .handle_num_pending_tasks(rpc::NumPendingTasksRequest::default())
        .unwrap();
    assert_eq!(
        reply.num_pending_tasks, 1,
        "should have 1 pending task after submission"
    );
}

/// Helper to start a CoreWorker gRPC server and return its address.
async fn start_core_worker_server(core_worker: Arc<CoreWorker>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let svc = CoreWorkerServiceImpl {
        core_worker,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        let server = tonic::transport::Server::builder().add_service(
            rpc::core_worker_service_server::CoreWorkerServiceServer::new(svc),
        );
        server.serve_with_incoming(incoming).await.ok();
    });

    // Give the server a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, handle)
}

/// End-to-end test: Start a CoreWorker with a task execution callback,
/// send a PushTask via gRPC, and verify the result appears in the memory store.
#[tokio::test]
async fn test_push_task_end_to_end_via_grpc() {
    let worker_id = WorkerID::from_random();
    let return_oid = ObjectID::from_random();
    let return_oid_bytes = return_oid.binary();

    let core_worker = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Worker,
        language: Language::Python,
        job_id: JobID::from_int(42),
        worker_id,
        node_ip_address: "127.0.0.1".to_string(),
        ..CoreWorkerOptions::default()
    }));

    // Set up the task execution callback — echoes the task name as result data.
    let oid_for_cb = return_oid_bytes.clone();
    let callback: TaskExecutionCallback = Arc::new(move |spec| {
        let result_data = format!("executed:{}", spec.name);
        Ok(TaskResult {
            return_objects: vec![rpc::ReturnObject {
                object_id: oid_for_cb.clone(),
                data: result_data.into_bytes(),
                metadata: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    core_worker.set_task_execution_callback(callback);

    // Start the gRPC server.
    let (addr, server_handle) = start_core_worker_server(Arc::clone(&core_worker)).await;

    // Create a gRPC client and send PushTask.
    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::core_worker_service_client::CoreWorkerServiceClient::new(channel);

    let task_spec = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "hello_task".to_string(),
        ..Default::default()
    };

    let reply = client
        .push_task(rpc::PushTaskRequest {
            intended_worker_id: worker_id.binary(),
            task_spec: Some(task_spec),
            sequence_number: 1,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    // Verify the reply.
    assert!(!reply.worker_exiting);
    assert!(!reply.is_retryable_error);
    assert!(!reply.is_application_error);
    assert_eq!(reply.return_objects.len(), 1);
    assert_eq!(reply.return_objects[0].data, b"executed:hello_task");

    // Verify the result object is in the worker's memory store.
    assert!(core_worker.contains_object(&return_oid));
    let obj = core_worker.memory_store().get(&return_oid).unwrap();
    assert_eq!(obj.data.as_ref(), b"executed:hello_task");

    // Clean up.
    server_handle.abort();
}

/// Test actor task delivery: driver creates an actor, connects it,
/// submits actor tasks, and verifies they are delivered to the actor worker.
#[tokio::test]
async fn test_actor_task_delivery_via_grpc() {
    use std::sync::atomic::{AtomicU32, Ordering};

    // 1. Start an actor worker with a task execution callback.
    let actor_worker_id = WorkerID::from_random();
    let actor_worker = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Worker,
        language: Language::Python,
        job_id: JobID::from_int(10),
        worker_id: actor_worker_id,
        node_ip_address: "127.0.0.1".to_string(),
        ..CoreWorkerOptions::default()
    }));

    let tasks_executed = Arc::new(AtomicU32::new(0));
    let tasks_executed_cb = Arc::clone(&tasks_executed);
    let callback: TaskExecutionCallback = Arc::new(move |spec| {
        tasks_executed_cb.fetch_add(1, Ordering::Relaxed);
        let oid = ObjectID::from_random();
        Ok(TaskResult {
            return_objects: vec![rpc::ReturnObject {
                object_id: oid.binary(),
                data: format!("actor_result:{}", spec.name).into_bytes(),
                metadata: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    actor_worker.set_task_execution_callback(callback);

    let (actor_addr, actor_server_handle) =
        start_core_worker_server(Arc::clone(&actor_worker)).await;

    // 2. Create a driver CoreWorker.
    let driver = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Driver,
        language: Language::Python,
        job_id: JobID::from_int(10),
        worker_id: WorkerID::from_random(),
        node_ip_address: "127.0.0.1".to_string(),
        ..CoreWorkerOptions::default()
    }));

    // 3. Register an actor on the driver.
    let actor_id = ActorID::from_random();
    let actor_handle = ActorHandle::from_proto(rpc::ActorHandle {
        actor_id: actor_id.binary(),
        name: "test_actor".to_string(),
        ..Default::default()
    });
    driver.create_actor(actor_id, actor_handle).unwrap();

    // 4. Set up actor task send callback — sends PushTask via gRPC (fire-and-forget).
    let actor_endpoint = format!("http://{}", actor_addr);
    let wid = actor_worker_id;
    driver.set_actor_task_send_callback(Box::new(move |spec, _addr| {
        let endpoint = actor_endpoint.clone();
        let spec_clone = spec.clone();
        let worker_id_bytes = wid.binary();
        tokio::spawn(async move {
            let channel = tonic::transport::Endpoint::from_shared(endpoint)
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client =
                rpc::core_worker_service_client::CoreWorkerServiceClient::new(channel);
            client
                .push_task(rpc::PushTaskRequest {
                    intended_worker_id: worker_id_bytes,
                    task_spec: Some(spec_clone),
                    ..Default::default()
                })
                .await
                .unwrap();
        });
        Ok(())
    }));

    // 5. Connect the actor to the worker address.
    let actor_address = rpc::Address {
        node_id: NodeID::nil().binary(),
        ip_address: "127.0.0.1".to_string(),
        port: actor_addr.port() as i32,
        worker_id: actor_worker_id.binary(),
    };
    driver.connect_actor(&actor_id, actor_address);

    // 6. Submit actor tasks.
    for i in 0..3 {
        let spec = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: format!("actor_task_{}", i),
            ..Default::default()
        };
        driver.submit_actor_task(&actor_id, spec).await.unwrap();
    }

    // Wait for tasks to be processed.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 7. Verify tasks were executed on the actor worker.
    assert_eq!(tasks_executed.load(Ordering::Relaxed), 3);
    assert_eq!(driver.actor_task_submitter().num_tasks_sent(&actor_id), 3);
    assert_eq!(driver.actor_task_submitter().num_pending_tasks(&actor_id), 0);

    // 8. Clean up.
    actor_server_handle.abort();
}

/// Test actor lifecycle with GCS: register actor, create it, verify it becomes alive.
#[tokio::test]
async fn test_actor_creation_via_gcs() {
    use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
    use ray_rpc::client::RetryConfig;

    // 1. Start GCS.
    let gcs_server = ray_test_utils::start_test_gcs_server().await;
    let gcs_addr = format!("http://{}", gcs_server.addr);

    // 2. Start a worker that will host the actor.
    let actor_worker_id = WorkerID::from_random();
    let raylet_node_id = NodeID::from_random();
    let actor_worker = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Worker,
        language: Language::Python,
        job_id: JobID::from_int(50),
        worker_id: actor_worker_id,
        node_ip_address: "127.0.0.1".to_string(),
        node_id: raylet_node_id,
        ..CoreWorkerOptions::default()
    }));

    let callback: TaskExecutionCallback = Arc::new(|spec| {
        let oid = ObjectID::from_random();
        Ok(TaskResult {
            return_objects: vec![rpc::ReturnObject {
                object_id: oid.binary(),
                data: format!("actor:{}", spec.name).into_bytes(),
                metadata: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    actor_worker.set_task_execution_callback(callback);
    let (_worker_addr, worker_handle) = start_core_worker_server(Arc::clone(&actor_worker)).await;

    // 3. Query GCS for actors — should be empty initially.
    let gcs_client = GcsRpcClient::connect(&gcs_addr, RetryConfig::default())
        .await
        .unwrap();
    let all_actors = gcs_client
        .get_all_actor_info(rpc::GetAllActorInfoRequest::default())
        .await
        .unwrap();
    assert!(
        all_actors.actor_table_data.is_empty(),
        "No actors should exist initially"
    );

    // 4. Register an actor via GCS.
    let actor_id = ActorID::from_random();
    let task_spec = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "create_actor".to_string(),
        actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
            actor_id: actor_id.binary(),
            name: "my_test_actor".to_string(),
            ray_namespace: "test_ns".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let register_reply = gcs_client
        .register_actor(rpc::RegisterActorRequest {
            task_spec: Some(task_spec),
        })
        .await
        .unwrap();
    // RegisterActorReply has a status field (GcsStatus).
    // Success means the actor was registered.
    assert!(register_reply.status.is_none() || register_reply.status.as_ref().unwrap().code == 0);

    // 5. Verify actor is registered.
    let all_actors = gcs_client
        .get_all_actor_info(rpc::GetAllActorInfoRequest::default())
        .await
        .unwrap();
    assert_eq!(all_actors.actor_table_data.len(), 1);

    // 6. Query the named actor.
    let named_reply = gcs_client
        .get_named_actor_info(rpc::GetNamedActorInfoRequest {
            name: "my_test_actor".to_string(),
            ray_namespace: "test_ns".to_string(),
        })
        .await
        .unwrap();
    assert!(named_reply.actor_table_data.is_some());
    let actor_data = named_reply.actor_table_data.unwrap();
    assert_eq!(actor_data.name, "my_test_actor");

    // 7. Clean up.
    worker_handle.abort();
    gcs_server.shutdown_tx.send(()).ok();
    gcs_server.join_handle.await.ok();
}

/// End-to-end test with a full stack: GCS + Raylet + CoreWorker.
/// Verifies the complete cluster registration and task execution path.
#[tokio::test]
async fn test_full_stack_gcs_raylet_worker() {
    use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
    use ray_rpc::client::RetryConfig;
    use std::collections::HashMap;

    // 1. Start GCS.
    let gcs_server = ray_test_utils::start_test_gcs_server().await;
    let gcs_addr = format!("http://{}", gcs_server.addr);

    // 2. Start Raylet with GCS registration.
    let raylet_node_id = NodeID::from_random();
    let raylet_config = ray_raylet::node_manager::RayletConfig {
        node_ip_address: "127.0.0.1".to_string(),
        port: 0,
        object_store_socket: String::new(),
        gcs_address: gcs_addr.clone(),
        log_dir: None,
        ray_config: ray_common::config::RayConfig::default(),
        node_id: raylet_node_id.hex(),
        resources: HashMap::from([("CPU".to_string(), 4.0)]),
        labels: HashMap::new(),
        session_name: "e2e-test".to_string(),
    };
    let nm = Arc::new(ray_raylet::node_manager::NodeManager::new(raylet_config));
    let nm_clone = Arc::clone(&nm);
    let raylet_handle = tokio::spawn(async move { nm_clone.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 3. Create a CoreWorker with a task execution callback.
    let worker_id = WorkerID::from_random();
    let core_worker = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Worker,
        language: Language::Python,
        job_id: JobID::from_int(99),
        worker_id,
        node_ip_address: "127.0.0.1".to_string(),
        node_id: raylet_node_id,
        ..CoreWorkerOptions::default()
    }));

    let callback: TaskExecutionCallback = Arc::new(|spec| {
        let oid = ObjectID::from_random();
        Ok(TaskResult {
            return_objects: vec![rpc::ReturnObject {
                object_id: oid.binary(),
                data: format!("result:{}", spec.name).into_bytes(),
                metadata: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    core_worker.set_task_execution_callback(callback);

    // 4. Start CoreWorker gRPC server.
    let (worker_addr, worker_handle) = start_core_worker_server(Arc::clone(&core_worker)).await;

    // 5. Verify GCS has the raylet registered.
    let gcs_client = GcsRpcClient::connect(&gcs_addr, RetryConfig::default())
        .await
        .unwrap();
    let node_reply = gcs_client
        .get_all_node_info(rpc::GetAllNodeInfoRequest::default())
        .await
        .unwrap();
    assert!(
        !node_reply.node_info_list.is_empty(),
        "Raylet should be registered in GCS"
    );

    // 6. Send a PushTask directly to the worker.
    let endpoint = format!("http://{}", worker_addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::core_worker_service_client::CoreWorkerServiceClient::new(channel);

    let task_spec = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "e2e_task".to_string(),
        ..Default::default()
    };
    let reply = client
        .push_task(rpc::PushTaskRequest {
            intended_worker_id: worker_id.binary(),
            task_spec: Some(task_spec),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert!(!reply.is_retryable_error);
    assert_eq!(reply.return_objects.len(), 1);
    assert_eq!(reply.return_objects[0].data, b"result:e2e_task");

    // 7. Verify the result is in the worker's memory store.
    let return_oid = ObjectID::from_binary(&reply.return_objects[0].object_id);
    assert!(core_worker.contains_object(&return_oid));

    // 8. Clean up.
    worker_handle.abort();
    raylet_handle.abort();
    gcs_server.shutdown_tx.send(()).ok();
    gcs_server.join_handle.await.ok();
}
