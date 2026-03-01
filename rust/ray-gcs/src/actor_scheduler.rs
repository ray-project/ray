// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Actor Scheduler — orchestrates actor placement on cluster nodes.
//!
//! Replaces `src/ray/gcs/gcs_actor_scheduler.h/cc`.
//!
//! Flow: pick node → RequestWorkerLease → PushTask → report success/failure.

use std::sync::Arc;

use ray_common::id::{LeaseID, NodeID};
use tonic::Status;

use crate::node_manager::GcsNodeManager;

use ray_proto::ray::rpc;

/// Trait for sending RPCs to raylets (mockable for tests).
#[async_trait::async_trait]
pub trait RayletClient: Send + Sync {
    async fn request_worker_lease(
        &self,
        addr: &str,
        request: rpc::RequestWorkerLeaseRequest,
    ) -> Result<rpc::RequestWorkerLeaseReply, Status>;
}

/// Trait for sending RPCs to core workers (mockable for tests).
#[async_trait::async_trait]
pub trait CoreWorkerClient: Send + Sync {
    async fn push_task(
        &self,
        addr: &str,
        request: rpc::PushTaskRequest,
    ) -> Result<rpc::PushTaskReply, Status>;
}

/// Real tonic gRPC implementation of RayletClient.
pub struct GrpcRayletClient;

#[async_trait::async_trait]
impl RayletClient for GrpcRayletClient {
    async fn request_worker_lease(
        &self,
        addr: &str,
        request: rpc::RequestWorkerLeaseRequest,
    ) -> Result<rpc::RequestWorkerLeaseReply, Status> {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| Status::internal(format!("invalid raylet address '{}': {}", addr, e)))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to raylet '{}': {}", addr, e)))?;
        let mut client = rpc::node_manager_service_client::NodeManagerServiceClient::new(channel);
        let resp = client.request_worker_lease(request).await?;
        Ok(resp.into_inner())
    }
}

/// Real tonic gRPC implementation of CoreWorkerClient.
pub struct GrpcCoreWorkerClient;

#[async_trait::async_trait]
impl CoreWorkerClient for GrpcCoreWorkerClient {
    async fn push_task(
        &self,
        addr: &str,
        request: rpc::PushTaskRequest,
    ) -> Result<rpc::PushTaskReply, Status> {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .map_err(|e| Status::internal(format!("invalid worker address '{}': {}", addr, e)))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to worker '{}': {}", addr, e)))?;
        let mut client = rpc::core_worker_service_client::CoreWorkerServiceClient::new(channel);
        let resp = client.push_task(request).await?;
        Ok(resp.into_inner())
    }
}

/// Result of a successful actor scheduling.
#[derive(Debug)]
pub struct ScheduleResult {
    pub worker_address: rpc::Address,
    pub worker_pid: u32,
    pub resource_mapping: Vec<rpc::ResourceMapEntry>,
    /// The node ID where the actor was placed.
    pub node_id: Vec<u8>,
}

/// The GCS actor scheduler. Orchestrates: node selection → lease worker → push task.
pub struct GcsActorScheduler {
    node_manager: Arc<GcsNodeManager>,
    raylet_client: Arc<dyn RayletClient>,
    worker_client: Arc<dyn CoreWorkerClient>,
}

impl GcsActorScheduler {
    pub fn new(
        node_manager: Arc<GcsNodeManager>,
        raylet_client: Arc<dyn RayletClient>,
        worker_client: Arc<dyn CoreWorkerClient>,
    ) -> Self {
        Self {
            node_manager,
            raylet_client,
            worker_client,
        }
    }

    /// Schedule an actor on a cluster node.
    ///
    /// Returns `Ok(ScheduleResult)` on success (worker leased and PushTask accepted),
    /// or `Err(Status)` on failure.
    pub async fn schedule(
        &self,
        task_spec: &rpc::TaskSpec,
    ) -> Result<ScheduleResult, Status> {
        // 1. Select a node
        let (node_id, node_addr) = self.select_node(task_spec)?;

        // 2. Build lease request
        let lease_request = self.build_lease_request(task_spec);

        // 3. Send RequestWorkerLease, handling spillback
        let (lease_reply, final_node_id) = self
            .lease_worker_with_spillback(&node_addr, &node_id, lease_request)
            .await?;

        // 4. Extract worker address from lease reply
        let worker_address = lease_reply.worker_address.ok_or_else(|| {
            Status::internal("raylet granted lease but returned no worker_address")
        })?;
        let worker_pid = lease_reply.worker_pid;
        let resource_mapping = lease_reply.resource_mapping;

        // 5. Build and send PushTask to the worker
        let worker_addr_str = format!("{}:{}", worker_address.ip_address, worker_address.port);
        let push_request = rpc::PushTaskRequest {
            intended_worker_id: worker_address.worker_id.clone(),
            task_spec: Some(task_spec.clone()),
            sequence_number: -1, // disable ordering for actor creation
            resource_mapping: resource_mapping.clone(),
            ..Default::default()
        };

        self.worker_client
            .push_task(&worker_addr_str, push_request)
            .await
            .map_err(|e| Status::internal(format!("PushTask to worker failed: {}", e)))?;

        Ok(ScheduleResult {
            worker_address,
            worker_pid,
            resource_mapping,
            node_id: final_node_id,
        })
    }

    /// Select a node for the actor. Prefers the owner's node if alive, else picks
    /// a random alive node.
    fn select_node(
        &self,
        task_spec: &rpc::TaskSpec,
    ) -> Result<(NodeID, String), Status> {
        let alive_nodes = self.node_manager.get_all_alive_nodes();
        if alive_nodes.is_empty() {
            return Err(Status::unavailable("no alive nodes in the cluster"));
        }

        // Try to schedule on the owner's node
        if let Some(ref caller_address) = task_spec.caller_address {
            if !caller_address.node_id.is_empty() {
                let owner_node_id = NodeID::from_binary(
                    caller_address.node_id.as_slice().try_into().unwrap_or(&[0u8; 28]),
                );
                if let Some(node_info) = alive_nodes.get(&owner_node_id) {
                    let addr = format!(
                        "{}:{}",
                        node_info.node_manager_address, node_info.node_manager_port
                    );
                    return Ok((owner_node_id, addr));
                }
            }
        }

        // Fall back to a random alive node
        let idx = rand::random::<usize>() % alive_nodes.len();
        let (node_id, node_info) = alive_nodes.into_iter().nth(idx).unwrap();
        let addr = format!(
            "{}:{}",
            node_info.node_manager_address, node_info.node_manager_port
        );
        Ok((node_id, addr))
    }

    /// Build a `RequestWorkerLeaseRequest` from a task spec.
    fn build_lease_request(
        &self,
        task_spec: &rpc::TaskSpec,
    ) -> rpc::RequestWorkerLeaseRequest {
        let creation_spec = task_spec.actor_creation_task_spec.as_ref();

        let lease_spec = rpc::LeaseSpec {
            lease_id: LeaseID::from_random().binary().to_vec(),
            job_id: task_spec.job_id.clone(),
            caller_address: task_spec.caller_address.clone(),
            r#type: 1, // ACTOR_CREATION_TASK
            actor_id: creation_spec.map(|s| s.actor_id.clone()).unwrap_or_default(),
            is_detached_actor: creation_spec.map(|s| s.is_detached).unwrap_or(false),
            max_actor_restarts: creation_spec.map(|s| s.max_actor_restarts).unwrap_or(0),
            required_resources: task_spec.required_resources.clone(),
            required_placement_resources: task_spec.required_placement_resources.clone(),
            scheduling_strategy: task_spec.scheduling_strategy.clone(),
            runtime_env_info: task_spec.runtime_env_info.clone(),
            function_descriptor: task_spec.function_descriptor.clone(),
            language: task_spec.language,
            dynamic_worker_options: creation_spec
                .map(|s| s.dynamic_worker_options.clone())
                .unwrap_or_default(),
            ..Default::default()
        };

        rpc::RequestWorkerLeaseRequest {
            lease_spec: Some(lease_spec),
            ..Default::default()
        }
    }

    /// Send RequestWorkerLease, following spillback redirects up to 10 times.
    async fn lease_worker_with_spillback(
        &self,
        initial_addr: &str,
        initial_node_id: &NodeID,
        request: rpc::RequestWorkerLeaseRequest,
    ) -> Result<(rpc::RequestWorkerLeaseReply, Vec<u8>), Status> {
        let mut addr = initial_addr.to_string();
        let mut node_id_bytes = initial_node_id.binary().to_vec();
        let max_retries = 10;

        for attempt in 0..max_retries {
            let reply = self
                .raylet_client
                .request_worker_lease(&addr, request.clone())
                .await?;

            // Check for cancellation / failure
            if reply.canceled {
                return Err(Status::cancelled("worker lease request was canceled"));
            }
            if reply.rejected {
                return Err(Status::resource_exhausted(
                    "worker lease rejected: insufficient resources",
                ));
            }
            // Check failure_type enum (non-zero means failure)
            if reply.failure_type != 0 {
                return Err(Status::internal(format!(
                    "scheduling failed (type={}): {}",
                    reply.failure_type, reply.scheduling_failure_message
                )));
            }

            // If worker_address is present, lease granted
            if reply.worker_address.is_some() {
                return Ok((reply, node_id_bytes));
            }

            // If retry_at_raylet_address is present, spillback
            if let Some(ref retry_addr) = reply.retry_at_raylet_address {
                addr = format!("{}:{}", retry_addr.ip_address, retry_addr.port);
                node_id_bytes = retry_addr.node_id.clone();
                tracing::debug!(
                    attempt,
                    spillback_addr = %addr,
                    "Spillback: retrying lease at different raylet"
                );
                continue;
            }

            // No worker, no spillback, no error — shouldn't happen
            return Err(Status::internal(
                "raylet returned empty lease reply with no spillback address",
            ));
        }

        Err(Status::deadline_exceeded(format!(
            "exceeded maximum spillback retries ({})",
            max_retries
        )))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;
    use crate::table_storage::GcsTableStorage;
    use std::collections::VecDeque;
    use parking_lot::Mutex;

    /// Mock RayletClient that stores requests and returns configured replies.
    pub struct MockRayletClient {
        pub replies: Mutex<VecDeque<Result<rpc::RequestWorkerLeaseReply, Status>>>,
        pub requests: Mutex<Vec<(String, rpc::RequestWorkerLeaseRequest)>>,
    }

    impl MockRayletClient {
        pub fn new() -> Self {
            Self {
                replies: Mutex::new(VecDeque::new()),
                requests: Mutex::new(Vec::new()),
            }
        }

        pub fn push_reply(&self, reply: Result<rpc::RequestWorkerLeaseReply, Status>) {
            self.replies.lock().push_back(reply);
        }
    }

    #[async_trait::async_trait]
    impl RayletClient for MockRayletClient {
        async fn request_worker_lease(
            &self,
            addr: &str,
            request: rpc::RequestWorkerLeaseRequest,
        ) -> Result<rpc::RequestWorkerLeaseReply, Status> {
            self.requests.lock().push((addr.to_string(), request));
            self.replies
                .lock()
                .pop_front()
                .unwrap_or(Err(Status::internal("no mock reply configured")))
        }
    }

    /// Mock CoreWorkerClient that stores requests and returns configured replies.
    pub struct MockCoreWorkerClient {
        pub replies: Mutex<VecDeque<Result<rpc::PushTaskReply, Status>>>,
        pub requests: Mutex<Vec<(String, rpc::PushTaskRequest)>>,
    }

    impl MockCoreWorkerClient {
        pub fn new() -> Self {
            Self {
                replies: Mutex::new(VecDeque::new()),
                requests: Mutex::new(Vec::new()),
            }
        }

        pub fn push_reply(&self, reply: Result<rpc::PushTaskReply, Status>) {
            self.replies.lock().push_back(reply);
        }
    }

    #[async_trait::async_trait]
    impl CoreWorkerClient for MockCoreWorkerClient {
        async fn push_task(
            &self,
            addr: &str,
            request: rpc::PushTaskRequest,
        ) -> Result<rpc::PushTaskReply, Status> {
            self.requests.lock().push((addr.to_string(), request));
            self.replies
                .lock()
                .pop_front()
                .unwrap_or(Err(Status::internal("no mock reply configured")))
        }
    }

    fn make_node_manager() -> Arc<GcsNodeManager> {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        Arc::new(GcsNodeManager::new(storage))
    }

    fn make_node_info(id: u8) -> rpc::GcsNodeInfo {
        let mut node_id = vec![0u8; 28];
        node_id[0] = id;
        rpc::GcsNodeInfo {
            node_id,
            node_name: format!("node-{}", id),
            node_manager_address: "127.0.0.1".to_string(),
            node_manager_port: 10000 + id as i32,
            state: 0,
            ..Default::default()
        }
    }

    fn make_task_spec_for_scheduling(actor_id: u8) -> rpc::TaskSpec {
        let mut aid = vec![0u8; 16];
        aid[0] = actor_id;
        rpc::TaskSpec {
            task_id: vec![1, 2, 3],
            job_id: vec![1, 0, 0, 0],
            caller_address: Some(rpc::Address {
                node_id: {
                    let mut nid = vec![0u8; 28];
                    nid[0] = 1; // owner is on node 1
                    nid
                },
                ip_address: "127.0.0.1".to_string(),
                port: 9999,
                worker_id: vec![0u8; 28],
            }),
            actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                actor_id: aid,
                name: "test_actor".to_string(),
                ray_namespace: "default".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn make_worker_address(node_id: u8) -> rpc::Address {
        let mut nid = vec![0u8; 28];
        nid[0] = node_id;
        rpc::Address {
            node_id: nid,
            ip_address: "127.0.0.1".to_string(),
            port: 20000 + node_id as i32,
            worker_id: vec![42u8; 28],
        }
    }

    #[tokio::test]
    async fn test_schedule_no_nodes() {
        let node_mgr = make_node_manager();
        let raylet = Arc::new(MockRayletClient::new());
        let worker = Arc::new(MockCoreWorkerClient::new());
        let scheduler = GcsActorScheduler::new(node_mgr, raylet, worker);

        let task_spec = make_task_spec_for_scheduling(1);
        let result = scheduler.schedule(&task_spec).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message()
            .contains("no alive nodes"));
    }

    #[tokio::test]
    async fn test_schedule_success() {
        let node_mgr = make_node_manager();
        node_mgr
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            worker_address: Some(make_worker_address(1)),
            worker_pid: 12345,
            resource_mapping: vec![rpc::ResourceMapEntry {
                name: "CPU".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }));

        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(rpc::PushTaskReply::default()));

        let scheduler = GcsActorScheduler::new(node_mgr, raylet.clone(), worker.clone());
        let task_spec = make_task_spec_for_scheduling(1);
        let result = scheduler.schedule(&task_spec).await.unwrap();

        assert_eq!(result.worker_pid, 12345);
        assert_eq!(result.worker_address.ip_address, "127.0.0.1");
        assert_eq!(result.resource_mapping.len(), 1);

        // Verify lease was sent to owner's node (node 1)
        let requests = raylet.requests.lock();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "127.0.0.1:10001");

        // Verify PushTask was sent
        let push_requests = worker.requests.lock();
        assert_eq!(push_requests.len(), 1);
    }

    #[tokio::test]
    async fn test_schedule_lease_spillback() {
        let node_mgr = make_node_manager();
        node_mgr
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        node_mgr
            .handle_register_node(make_node_info(2))
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        // First reply: spillback to node 2
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            retry_at_raylet_address: Some(rpc::Address {
                node_id: {
                    let mut nid = vec![0u8; 28];
                    nid[0] = 2;
                    nid
                },
                ip_address: "127.0.0.1".to_string(),
                port: 10002,
                ..Default::default()
            }),
            ..Default::default()
        }));
        // Second reply: grant worker
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            worker_address: Some(make_worker_address(2)),
            worker_pid: 54321,
            ..Default::default()
        }));

        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(rpc::PushTaskReply::default()));

        let scheduler = GcsActorScheduler::new(node_mgr, raylet.clone(), worker);
        let task_spec = make_task_spec_for_scheduling(1);
        let result = scheduler.schedule(&task_spec).await.unwrap();

        assert_eq!(result.worker_pid, 54321);

        // Verify two lease requests were made (initial + spillback)
        let requests = raylet.requests.lock();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].0, "127.0.0.1:10001");
        assert_eq!(requests[1].0, "127.0.0.1:10002");
    }

    #[tokio::test]
    async fn test_schedule_push_task_fails() {
        let node_mgr = make_node_manager();
        node_mgr
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            worker_address: Some(make_worker_address(1)),
            worker_pid: 12345,
            ..Default::default()
        }));

        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Err(Status::internal("worker crashed")));

        let scheduler = GcsActorScheduler::new(node_mgr, raylet, worker);
        let task_spec = make_task_spec_for_scheduling(1);
        let result = scheduler.schedule(&task_spec).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("PushTask"));
    }

    #[tokio::test]
    async fn test_schedule_prefers_owner_node() {
        let node_mgr = make_node_manager();
        // Register two nodes; node 1 is the owner's node
        node_mgr
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        node_mgr
            .handle_register_node(make_node_info(2))
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            worker_address: Some(make_worker_address(1)),
            worker_pid: 111,
            ..Default::default()
        }));

        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(rpc::PushTaskReply::default()));

        let scheduler = GcsActorScheduler::new(node_mgr, raylet.clone(), worker);
        let task_spec = make_task_spec_for_scheduling(1);
        scheduler.schedule(&task_spec).await.unwrap();

        // Should always go to node 1 (owner node)
        let requests = raylet.requests.lock();
        assert_eq!(requests[0].0, "127.0.0.1:10001");
    }

    #[tokio::test]
    async fn test_schedule_lease_canceled() {
        let node_mgr = make_node_manager();
        node_mgr
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            canceled: true,
            ..Default::default()
        }));

        let worker = Arc::new(MockCoreWorkerClient::new());
        let scheduler = GcsActorScheduler::new(node_mgr, raylet, worker);
        let task_spec = make_task_spec_for_scheduling(1);
        let result = scheduler.schedule(&task_spec).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Cancelled);
    }
}
