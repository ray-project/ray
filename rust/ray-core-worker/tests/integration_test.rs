// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the CoreWorker gRPC service.

use std::sync::Arc;

use ray_core_worker::core_worker::CoreWorker;
use ray_core_worker::grpc_service::CoreWorkerServiceImpl;
use ray_core_worker::options::{CoreWorkerOptions, Language, WorkerType};
use ray_common::id::{ClusterID, JobID, NodeID, TaskID, WorkerID};
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
