// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! A simple Ray program that adds two numbers using the full Rust backend.
//!
//! This demonstrates the complete Ray task execution pipeline:
//!   1. Start an in-process GCS server
//!   2. Start a Raylet (node manager) that registers with GCS
//!   3. Create a CoreWorker with a task execution callback
//!   4. Put two numbers as objects into the object store
//!   5. Submit a remote task (via gRPC PushTask) that reads and adds them
//!   6. Retrieve and print the result

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use ray_common::id::{JobID, NodeID, ObjectID, TaskID, WorkerID};
use ray_core_worker::core_worker::CoreWorker;
use ray_core_worker::grpc_service::CoreWorkerServiceImpl;
use ray_core_worker::options::{CoreWorkerOptions, Language, WorkerType};
use ray_core_worker::task_receiver::{TaskExecutionCallback, TaskResult};
use ray_proto::ray::rpc;

#[tokio::main]
async fn main() {
    // Initialize tracing for log output.
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .compact()
        .init();

    println!("=== Ray Rust Backend: Add Two Numbers ===\n");

    // ── Step 1: Start the GCS server ────────────────────────────────────
    println!("[1/7] Starting GCS server...");
    let gcs_server = ray_test_utils::start_test_gcs_server().await;
    let gcs_addr = format!("http://{}", gcs_server.addr);
    println!("       GCS listening on {}", gcs_server.addr);

    // ── Step 2: Start a Raylet ──────────────────────────────────────────
    println!("[2/7] Starting Raylet (node manager)...");
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
        session_name: "add-two-numbers".to_string(),
        auth_token: None,
    };
    let nm = Arc::new(ray_raylet::node_manager::NodeManager::new(raylet_config));
    let nm_clone = Arc::clone(&nm);
    let _raylet_handle = tokio::spawn(async move { nm_clone.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    println!(
        "       Raylet registered with GCS (node_id={})",
        &raylet_node_id.hex()[..16]
    );

    // ── Step 3: Create the CoreWorker with an "add" task callback ───────
    println!("[3/7] Creating CoreWorker with 'add' task callback...");
    let worker_id = WorkerID::from_random();
    let core_worker = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Worker,
        language: Language::Python,
        job_id: JobID::from_int(1),
        worker_id,
        node_ip_address: "127.0.0.1".to_string(),
        node_id: raylet_node_id,
        ..CoreWorkerOptions::default()
    }));

    // The task execution callback: parses two f64 arguments from the
    // task spec's function descriptor, adds them, and returns the sum.
    let worker_store = Arc::clone(core_worker.memory_store());
    let callback: TaskExecutionCallback = Arc::new(move |spec: &rpc::TaskSpec| {
        // Arguments are passed as ObjectIDs in the task spec's args field.
        // Each arg is an rpc::TaskArg with either inlined data or an object reference.
        let mut values: Vec<f64> = Vec::new();

        for arg in &spec.args {
            if !arg.data.is_empty() {
                // Inlined argument: data is the f64 bytes directly.
                let bytes: [u8; 8] = arg.data[..8].try_into().map_err(|_| {
                    ray_core_worker::error::CoreWorkerError::InvalidArgument(
                        "argument must be 8 bytes (f64)".into(),
                    )
                })?;
                values.push(f64::from_le_bytes(bytes));
            } else if let Some(ref obj_ref) = arg.object_ref {
                // Object reference: look up in the memory store.
                let oid = ObjectID::from_binary(&obj_ref.object_id);
                if let Some(obj) = worker_store.get(&oid) {
                    let bytes: [u8; 8] = obj.data[..8].try_into().map_err(|_| {
                        ray_core_worker::error::CoreWorkerError::InvalidArgument(
                            "object data must be 8 bytes (f64)".into(),
                        )
                    })?;
                    values.push(f64::from_le_bytes(bytes));
                } else {
                    return Err(ray_core_worker::error::CoreWorkerError::InvalidArgument(
                        format!("object {} not found in memory store", oid.hex()),
                    ));
                }
            }
        }

        if values.len() != 2 {
            return Err(ray_core_worker::error::CoreWorkerError::InvalidArgument(
                format!("expected 2 arguments, got {}", values.len()),
            ));
        }

        let sum = values[0] + values[1];
        let return_oid = ObjectID::from_random();

        println!(
            "       [worker] Executing task '{}': {} + {} = {}",
            spec.name, values[0], values[1], sum
        );

        Ok(TaskResult {
            return_objects: vec![rpc::ReturnObject {
                object_id: return_oid.binary(),
                data: sum.to_le_bytes().to_vec(),
                metadata: b"f64".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    core_worker.set_task_execution_callback(callback);

    // ── Step 4: Start CoreWorker gRPC server ────────────────────────────
    println!("[4/7] Starting CoreWorker gRPC server...");
    let svc = CoreWorkerServiceImpl {
        core_worker: Arc::clone(&core_worker),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let _worker_handle = tokio::spawn(async move {
        let server = tonic::transport::Server::builder()
            .add_service(rpc::core_worker_service_server::CoreWorkerServiceServer::new(svc));
        server.serve_with_incoming(incoming).await.ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    println!("       Worker listening on {}", worker_addr);

    // ── Step 5: Put two numbers into the object store ───────────────────
    let a: f64 = 17.0;
    let b: f64 = 25.0;
    let oid_a = ObjectID::from_random();
    let oid_b = ObjectID::from_random();

    println!(
        "[5/7] Putting objects: a={} ({}), b={} ({})",
        a,
        &oid_a.hex()[..12],
        b,
        &oid_b.hex()[..12]
    );

    core_worker
        .put_object(oid_a, Bytes::from(a.to_le_bytes().to_vec()), Bytes::new())
        .unwrap();
    core_worker
        .put_object(oid_b, Bytes::from(b.to_le_bytes().to_vec()), Bytes::new())
        .unwrap();

    // ── Step 6: Submit "add" task via gRPC PushTask ─────────────────────
    println!("[6/7] Submitting remote task: add({}, {})...", a, b);

    let endpoint = format!("http://{}", worker_addr);
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = rpc::core_worker_service_client::CoreWorkerServiceClient::new(channel);

    let task_spec = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "add".to_string(),
        args: vec![
            rpc::TaskArg {
                object_ref: Some(rpc::ObjectReference {
                    object_id: oid_a.binary(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            rpc::TaskArg {
                object_ref: Some(rpc::ObjectReference {
                    object_id: oid_b.binary(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
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

    // ── Step 7: Read the result ─────────────────────────────────────────
    assert_eq!(
        reply.return_objects.len(),
        1,
        "expected exactly one return object"
    );
    let result_data = &reply.return_objects[0].data;
    let result_bytes: [u8; 8] = result_data[..8].try_into().unwrap();
    let result: f64 = f64::from_le_bytes(result_bytes);

    println!("[7/7] Result received!");
    println!();
    println!("  ┌─────────────────────────────────┐");
    println!("  │  {} + {} = {}            │", a, b, result);
    println!("  └─────────────────────────────────┘");
    println!();
    println!("Task executed successfully on the Rust backend.");
    println!("  - GCS server:  {}", gcs_server.addr);
    println!("  - Raylet node: {}", &raylet_node_id.hex()[..16]);
    println!("  - Worker:      {}", worker_addr);

    // Clean up.
    gcs_server.shutdown_tx.send(()).ok();
}
