// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! A Ray actor example running entirely on the Rust backend.
//!
//! Demonstrates the complete actor lifecycle:
//!   1. Start GCS + Raylet (full cluster)
//!   2. Create an actor worker with stateful "Counter" logic
//!   3. Register the actor with GCS
//!   4. Submit actor tasks (increment, get_count) via gRPC
//!   5. Verify results — actor state persists across calls

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use ray_common::id::{ActorID, JobID, NodeID, ObjectID, TaskID, WorkerID};
use ray_core_worker::actor_handle::ActorHandle;
use ray_core_worker::core_worker::CoreWorker;
use ray_core_worker::grpc_service::CoreWorkerServiceImpl;
use ray_core_worker::options::{CoreWorkerOptions, Language, WorkerType};
use ray_core_worker::task_receiver::{TaskExecutionCallback, TaskResult};
use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
use ray_proto::ray::rpc;
use ray_rpc::client::RetryConfig;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .compact()
        .init();

    println!("=== Ray Rust Backend: Actor Counter Example ===\n");

    // ── Step 1: Start cluster (GCS + Raylet) ────────────────────────────
    println!("[1/8] Starting GCS server...");
    let gcs_server = ray_test_utils::start_test_gcs_server().await;
    let gcs_addr = format!("http://{}", gcs_server.addr);
    println!("       GCS listening on {}", gcs_server.addr);

    println!("[2/8] Starting Raylet...");
    let raylet_node_id = NodeID::from_random();
    let nm = Arc::new(ray_raylet::node_manager::NodeManager::new(
        ray_raylet::node_manager::RayletConfig {
            node_ip_address: "127.0.0.1".to_string(),
            port: 0,
            object_store_socket: String::new(),
            gcs_address: gcs_addr.clone(),
            log_dir: None,
            ray_config: ray_common::config::RayConfig::default(),
            node_id: raylet_node_id.hex(),
            resources: HashMap::from([("CPU".to_string(), 4.0)]),
            labels: HashMap::new(),
            session_name: "actor-counter".to_string(),
            auth_token: None,
        },
    ));
    let nm_clone = Arc::clone(&nm);
    let _raylet_handle = tokio::spawn(async move { nm_clone.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    println!("       Raylet registered (node={})", &raylet_node_id.hex()[..16]);

    // ── Step 2: Create the actor worker with stateful Counter callback ──
    println!("[3/8] Creating actor worker with Counter state...");
    let actor_worker_id = WorkerID::from_random();
    let actor_worker = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Worker,
        language: Language::Python,
        job_id: JobID::from_int(1),
        worker_id: actor_worker_id,
        node_ip_address: "127.0.0.1".to_string(),
        node_id: raylet_node_id,
        ..CoreWorkerOptions::default()
    }));

    // Shared actor state: a simple counter.
    let counter = Arc::new(AtomicI64::new(0));
    let counter_cb = Arc::clone(&counter);

    // The task callback dispatches on the task name:
    //   "increment(N)" -> add N to counter, return new value
    //   "get_count"    -> return current counter value
    let callback: TaskExecutionCallback = Arc::new(move |spec: &rpc::TaskSpec| {
        let return_oid = ObjectID::from_random();
        let name = &spec.name;

        let result: i64 = if name.starts_with("increment") {
            // Parse increment amount from first inlined arg.
            let amount = if !spec.args.is_empty() && !spec.args[0].data.is_empty() {
                let bytes: [u8; 8] = spec.args[0].data[..8]
                    .try_into()
                    .unwrap_or([1, 0, 0, 0, 0, 0, 0, 0]);
                i64::from_le_bytes(bytes)
            } else {
                1 // default increment by 1
            };
            let new_val = counter_cb.fetch_add(amount, Ordering::Relaxed) + amount;
            println!("       [actor] increment({}) -> count is now {}", amount, new_val);
            new_val
        } else if name == "get_count" {
            let val = counter_cb.load(Ordering::Relaxed);
            println!("       [actor] get_count() -> {}", val);
            val
        } else {
            return Err(ray_core_worker::error::CoreWorkerError::InvalidArgument(
                format!("unknown method: {}", name),
            ));
        };

        Ok(TaskResult {
            return_objects: vec![rpc::ReturnObject {
                object_id: return_oid.binary(),
                data: result.to_le_bytes().to_vec(),
                metadata: b"i64".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    actor_worker.set_task_execution_callback(callback);

    // ── Step 3: Start actor worker gRPC server ──────────────────────────
    println!("[4/8] Starting actor worker gRPC server...");
    let svc = CoreWorkerServiceImpl {
        core_worker: Arc::clone(&actor_worker),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let actor_addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let _worker_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(rpc::core_worker_service_server::CoreWorkerServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    println!("       Actor worker listening on {}", actor_addr);

    // ── Step 4: Register actor with GCS ─────────────────────────────────
    println!("[5/8] Registering actor 'Counter' with GCS...");
    let actor_id = ActorID::from_random();
    let gcs_client = GcsRpcClient::connect(&gcs_addr, RetryConfig::default())
        .await
        .unwrap();

    let register_reply = gcs_client
        .register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                task_id: TaskID::from_random().binary(),
                name: "Counter.__init__".to_string(),
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.binary(),
                    name: "Counter".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    assert!(
        register_reply.status.is_none() || register_reply.status.as_ref().unwrap().code == 0
    );
    println!(
        "       Actor registered: name=Counter, id={}",
        &actor_id.hex()[..16]
    );

    // ── Step 5: Create a driver CoreWorker and register the actor ───────
    println!("[6/8] Setting up driver with actor handle...");
    let driver = Arc::new(CoreWorker::new(CoreWorkerOptions {
        worker_type: WorkerType::Driver,
        language: Language::Python,
        job_id: JobID::from_int(1),
        worker_id: WorkerID::from_random(),
        node_ip_address: "127.0.0.1".to_string(),
        node_id: raylet_node_id,
        ..CoreWorkerOptions::default()
    }));

    // Register actor handle on driver.
    let handle = ActorHandle::from_proto(rpc::ActorHandle {
        actor_id: actor_id.binary(),
        name: "Counter".to_string(),
        ray_namespace: "default".to_string(),
        ..Default::default()
    });
    driver.create_actor(actor_id, handle).unwrap();

    // Wire the actor task send callback to dispatch via gRPC PushTask.
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

    // Connect the actor to its worker address.
    let actor_address = rpc::Address {
        node_id: raylet_node_id.binary(),
        ip_address: "127.0.0.1".to_string(),
        port: actor_addr.port() as i32,
        worker_id: actor_worker_id.binary(),
    };
    driver.connect_actor(&actor_id, actor_address);

    // ── Step 6: Submit actor tasks ──────────────────────────────────────
    println!("[7/8] Submitting actor tasks...\n");

    // increment(10)
    let spec1 = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "increment".to_string(),
        args: vec![rpc::TaskArg {
            data: 10_i64.to_le_bytes().to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    driver.submit_actor_task(&actor_id, spec1).await.unwrap();

    // increment(7)
    let spec2 = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "increment".to_string(),
        args: vec![rpc::TaskArg {
            data: 7_i64.to_le_bytes().to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    driver.submit_actor_task(&actor_id, spec2).await.unwrap();

    // increment(25)
    let spec3 = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "increment".to_string(),
        args: vec![rpc::TaskArg {
            data: 25_i64.to_le_bytes().to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    driver.submit_actor_task(&actor_id, spec3).await.unwrap();

    // get_count
    let spec4 = rpc::TaskSpec {
        task_id: TaskID::from_random().binary(),
        name: "get_count".to_string(),
        ..Default::default()
    };
    driver.submit_actor_task(&actor_id, spec4).await.unwrap();

    // Wait for all tasks to be processed.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // ── Step 7: Verify results ──────────────────────────────────────────
    let final_count = counter.load(Ordering::Relaxed);
    let tasks_sent = driver.actor_task_submitter().num_tasks_sent(&actor_id);

    println!();
    println!("[8/8] Results:");
    println!();
    println!("  ┌───────────────────────────────────────────┐");
    println!("  │  Counter.increment(10)  ->  count = 10    │");
    println!("  │  Counter.increment(7)   ->  count = 17    │");
    println!("  │  Counter.increment(25)  ->  count = 42    │");
    println!("  │  Counter.get_count()    ->  42            │");
    println!("  │                                           │");
    println!("  │  Final count: {:<28}│", final_count);
    println!("  │  Tasks sent:  {:<28}│", tasks_sent);
    println!("  └───────────────────────────────────────────┘");
    println!();

    assert_eq!(final_count, 42, "Counter should be 42 (10 + 7 + 25)");
    assert_eq!(tasks_sent, 4, "Should have sent 4 actor tasks");

    // Verify actor is registered in GCS.
    let actors = gcs_client
        .get_all_actor_info(rpc::GetAllActorInfoRequest::default())
        .await
        .unwrap();
    assert_eq!(actors.actor_table_data.len(), 1);
    println!("Actor lifecycle verified on the Rust backend.");
    println!("  - GCS server:   {}", gcs_server.addr);
    println!("  - Raylet:       {}", &raylet_node_id.hex()[..16]);
    println!("  - Actor worker: {}", actor_addr);
    println!("  - Actor ID:     {}", &actor_id.hex()[..16]);

    // Clean up.
    gcs_server.shutdown_tx.send(()).ok();
}
