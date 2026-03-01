// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Real GCS RPC client backed by tonic gRPC stubs.

use parking_lot::Mutex;
use tonic::transport::Channel;
use tonic::Status;

use ray_proto::ray::rpc;
use ray_rpc::client::{RetryConfig, RetryableGrpcClient};

use crate::traits::GcsClient;

type JobClient = rpc::job_info_gcs_service_client::JobInfoGcsServiceClient<Channel>;
type NodeClient = rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient<Channel>;
type ActorClient = rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient<Channel>;
type WorkerClient = rpc::worker_info_gcs_service_client::WorkerInfoGcsServiceClient<Channel>;
type ResourceClient =
    rpc::node_resource_info_gcs_service_client::NodeResourceInfoGcsServiceClient<Channel>;
type PgClient =
    rpc::placement_group_info_gcs_service_client::PlacementGroupInfoGcsServiceClient<Channel>;
type KvClient = rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient<Channel>;
type TaskClient = rpc::task_info_gcs_service_client::TaskInfoGcsServiceClient<Channel>;

/// Real GCS RPC client wrapping tonic-generated stubs with retry logic.
pub struct GcsRpcClient {
    retry_client: RetryableGrpcClient,
    job: Mutex<JobClient>,
    node: Mutex<NodeClient>,
    actor: Mutex<ActorClient>,
    worker: Mutex<WorkerClient>,
    resource: Mutex<ResourceClient>,
    pg: Mutex<PgClient>,
    kv: Mutex<KvClient>,
    task: Mutex<TaskClient>,
}

impl GcsRpcClient {
    /// Connect to a GCS server.
    pub async fn connect(
        gcs_address: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = RetryableGrpcClient::connect(gcs_address).await?;
        Ok(Self::from_channel(channel, retry_config))
    }

    /// Create from an existing channel (useful for testing).
    pub fn from_channel(channel: Channel, retry_config: RetryConfig) -> Self {
        let retry_client = RetryableGrpcClient::new(channel.clone(), retry_config);
        Self {
            retry_client,
            job: Mutex::new(JobClient::new(channel.clone())),
            node: Mutex::new(NodeClient::new(channel.clone())),
            actor: Mutex::new(ActorClient::new(channel.clone())),
            worker: Mutex::new(WorkerClient::new(channel.clone())),
            resource: Mutex::new(ResourceClient::new(channel.clone())),
            pg: Mutex::new(PgClient::new(channel.clone())),
            kv: Mutex::new(KvClient::new(channel.clone())),
            task: Mutex::new(TaskClient::new(channel)),
        }
    }

    /// Access the underlying retry client for connection state inspection.
    pub fn retry_client(&self) -> &RetryableGrpcClient {
        &self.retry_client
    }
}

/// Macro to implement a GCS RPC method with retry.
///
/// Each generated stub method takes `&mut self`, so we lock the Mutex,
/// clone it, and call within the retry loop.
macro_rules! impl_gcs_rpc {
    ($self:ident, $stub_field:ident, $method:ident, $req:expr) => {{
        let retry = &$self.retry_client;
        let stub_mutex = &$self.$stub_field;
        retry
            .call_with_retry(0, None, || {
                let mut stub = stub_mutex.lock().clone();
                let req = $req.clone();
                async move {
                    stub.$method(tonic::Request::new(req))
                        .await
                        .map(|resp| resp.into_inner())
                }
            })
            .await
    }};
}

#[async_trait::async_trait]
impl GcsClient for GcsRpcClient {
    // ── Job RPCs ──────────────────────────────────────────────────

    async fn add_job(&self, req: rpc::AddJobRequest) -> Result<rpc::AddJobReply, Status> {
        impl_gcs_rpc!(self, job, add_job, req)
    }

    async fn mark_job_finished(
        &self,
        req: rpc::MarkJobFinishedRequest,
    ) -> Result<rpc::MarkJobFinishedReply, Status> {
        impl_gcs_rpc!(self, job, mark_job_finished, req)
    }

    async fn get_all_job_info(
        &self,
        req: rpc::GetAllJobInfoRequest,
    ) -> Result<rpc::GetAllJobInfoReply, Status> {
        impl_gcs_rpc!(self, job, get_all_job_info, req)
    }

    async fn get_next_job_id(
        &self,
        req: rpc::GetNextJobIdRequest,
    ) -> Result<rpc::GetNextJobIdReply, Status> {
        impl_gcs_rpc!(self, job, get_next_job_id, req)
    }

    async fn report_job_error(
        &self,
        req: rpc::ReportJobErrorRequest,
    ) -> Result<rpc::ReportJobErrorReply, Status> {
        impl_gcs_rpc!(self, job, report_job_error, req)
    }

    // ── Node RPCs ─────────────────────────────────────────────────

    async fn register_node(
        &self,
        req: rpc::RegisterNodeRequest,
    ) -> Result<rpc::RegisterNodeReply, Status> {
        impl_gcs_rpc!(self, node, register_node, req)
    }

    async fn unregister_node(
        &self,
        req: rpc::UnregisterNodeRequest,
    ) -> Result<rpc::UnregisterNodeReply, Status> {
        impl_gcs_rpc!(self, node, unregister_node, req)
    }

    async fn get_all_node_info(
        &self,
        req: rpc::GetAllNodeInfoRequest,
    ) -> Result<rpc::GetAllNodeInfoReply, Status> {
        impl_gcs_rpc!(self, node, get_all_node_info, req)
    }

    async fn get_cluster_id(
        &self,
        req: rpc::GetClusterIdRequest,
    ) -> Result<rpc::GetClusterIdReply, Status> {
        impl_gcs_rpc!(self, node, get_cluster_id, req)
    }

    async fn check_alive(
        &self,
        req: rpc::CheckAliveRequest,
    ) -> Result<rpc::CheckAliveReply, Status> {
        impl_gcs_rpc!(self, node, check_alive, req)
    }

    // ── Actor RPCs ────────────────────────────────────────────────

    async fn register_actor(
        &self,
        req: rpc::RegisterActorRequest,
    ) -> Result<rpc::RegisterActorReply, Status> {
        impl_gcs_rpc!(self, actor, register_actor, req)
    }

    async fn create_actor(
        &self,
        req: rpc::CreateActorRequest,
    ) -> Result<rpc::CreateActorReply, Status> {
        impl_gcs_rpc!(self, actor, create_actor, req)
    }

    async fn get_actor_info(
        &self,
        req: rpc::GetActorInfoRequest,
    ) -> Result<rpc::GetActorInfoReply, Status> {
        impl_gcs_rpc!(self, actor, get_actor_info, req)
    }

    async fn get_named_actor_info(
        &self,
        req: rpc::GetNamedActorInfoRequest,
    ) -> Result<rpc::GetNamedActorInfoReply, Status> {
        impl_gcs_rpc!(self, actor, get_named_actor_info, req)
    }

    async fn get_all_actor_info(
        &self,
        req: rpc::GetAllActorInfoRequest,
    ) -> Result<rpc::GetAllActorInfoReply, Status> {
        impl_gcs_rpc!(self, actor, get_all_actor_info, req)
    }

    async fn kill_actor_via_gcs(
        &self,
        req: rpc::KillActorViaGcsRequest,
    ) -> Result<rpc::KillActorViaGcsReply, Status> {
        impl_gcs_rpc!(self, actor, kill_actor_via_gcs, req)
    }

    // ── Worker RPCs ───────────────────────────────────────────────

    async fn report_worker_failure(
        &self,
        req: rpc::ReportWorkerFailureRequest,
    ) -> Result<rpc::ReportWorkerFailureReply, Status> {
        impl_gcs_rpc!(self, worker, report_worker_failure, req)
    }

    async fn add_worker_info(
        &self,
        req: rpc::AddWorkerInfoRequest,
    ) -> Result<rpc::AddWorkerInfoReply, Status> {
        impl_gcs_rpc!(self, worker, add_worker_info, req)
    }

    async fn get_all_worker_info(
        &self,
        req: rpc::GetAllWorkerInfoRequest,
    ) -> Result<rpc::GetAllWorkerInfoReply, Status> {
        impl_gcs_rpc!(self, worker, get_all_worker_info, req)
    }

    // ── Resource RPCs ─────────────────────────────────────────────

    async fn get_all_resource_usage(
        &self,
        req: rpc::GetAllResourceUsageRequest,
    ) -> Result<rpc::GetAllResourceUsageReply, Status> {
        impl_gcs_rpc!(self, resource, get_all_resource_usage, req)
    }

    // ── Placement Group RPCs ──────────────────────────────────────

    async fn create_placement_group(
        &self,
        req: rpc::CreatePlacementGroupRequest,
    ) -> Result<rpc::CreatePlacementGroupReply, Status> {
        impl_gcs_rpc!(self, pg, create_placement_group, req)
    }

    async fn remove_placement_group(
        &self,
        req: rpc::RemovePlacementGroupRequest,
    ) -> Result<rpc::RemovePlacementGroupReply, Status> {
        impl_gcs_rpc!(self, pg, remove_placement_group, req)
    }

    async fn get_all_placement_group(
        &self,
        req: rpc::GetAllPlacementGroupRequest,
    ) -> Result<rpc::GetAllPlacementGroupReply, Status> {
        impl_gcs_rpc!(self, pg, get_all_placement_group, req)
    }

    // ── Internal KV RPCs ──────────────────────────────────────────

    async fn internal_kv_get(
        &self,
        req: rpc::InternalKvGetRequest,
    ) -> Result<rpc::InternalKvGetReply, Status> {
        impl_gcs_rpc!(self, kv, internal_kv_get, req)
    }

    async fn internal_kv_put(
        &self,
        req: rpc::InternalKvPutRequest,
    ) -> Result<rpc::InternalKvPutReply, Status> {
        impl_gcs_rpc!(self, kv, internal_kv_put, req)
    }

    async fn internal_kv_del(
        &self,
        req: rpc::InternalKvDelRequest,
    ) -> Result<rpc::InternalKvDelReply, Status> {
        impl_gcs_rpc!(self, kv, internal_kv_del, req)
    }

    async fn internal_kv_keys(
        &self,
        req: rpc::InternalKvKeysRequest,
    ) -> Result<rpc::InternalKvKeysReply, Status> {
        impl_gcs_rpc!(self, kv, internal_kv_keys, req)
    }

    // ── Task Event RPCs ───────────────────────────────────────────

    async fn add_task_event_data(
        &self,
        req: rpc::AddTaskEventDataRequest,
    ) -> Result<rpc::AddTaskEventDataReply, Status> {
        impl_gcs_rpc!(self, task, add_task_event_data, req)
    }

    async fn get_task_events(
        &self,
        req: rpc::GetTaskEventsRequest,
    ) -> Result<rpc::GetTaskEventsReply, Status> {
        impl_gcs_rpc!(self, task, get_task_events, req)
    }
}

/// A fake GCS client for testing that records requests and returns canned responses.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// Records all requests and returns default responses.
    pub struct FakeGcsClient {
        job_requests: Mutex<VecDeque<String>>,
        node_requests: Mutex<VecDeque<String>>,
        actor_requests: Mutex<VecDeque<String>>,
        all_requests: Arc<Mutex<VecDeque<String>>>,
    }

    impl FakeGcsClient {
        pub fn new() -> Self {
            Self {
                job_requests: Mutex::new(VecDeque::new()),
                node_requests: Mutex::new(VecDeque::new()),
                actor_requests: Mutex::new(VecDeque::new()),
                all_requests: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        pub fn num_requests(&self) -> usize {
            self.all_requests.lock().len()
        }

        pub fn pop_request(&self) -> Option<String> {
            self.all_requests.lock().pop_front()
        }

        pub fn num_job_requests(&self) -> usize {
            self.job_requests.lock().len()
        }

        pub fn num_node_requests(&self) -> usize {
            self.node_requests.lock().len()
        }

        pub fn num_actor_requests(&self) -> usize {
            self.actor_requests.lock().len()
        }

        fn record(&self, category: &Mutex<VecDeque<String>>, name: &str) {
            let name = name.to_string();
            category.lock().push_back(name.clone());
            self.all_requests.lock().push_back(name);
        }

        fn record_all(&self, name: &str) {
            self.all_requests.lock().push_back(name.to_string());
        }
    }

    impl Default for FakeGcsClient {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait]
    impl GcsClient for FakeGcsClient {
        async fn add_job(
            &self,
            _req: rpc::AddJobRequest,
        ) -> Result<rpc::AddJobReply, Status> {
            self.record(&self.job_requests, "add_job");
            Ok(rpc::AddJobReply::default())
        }
        async fn mark_job_finished(
            &self,
            _req: rpc::MarkJobFinishedRequest,
        ) -> Result<rpc::MarkJobFinishedReply, Status> {
            self.record(&self.job_requests, "mark_job_finished");
            Ok(rpc::MarkJobFinishedReply::default())
        }
        async fn get_all_job_info(
            &self,
            _req: rpc::GetAllJobInfoRequest,
        ) -> Result<rpc::GetAllJobInfoReply, Status> {
            self.record(&self.job_requests, "get_all_job_info");
            Ok(rpc::GetAllJobInfoReply::default())
        }
        async fn get_next_job_id(
            &self,
            _req: rpc::GetNextJobIdRequest,
        ) -> Result<rpc::GetNextJobIdReply, Status> {
            self.record(&self.job_requests, "get_next_job_id");
            Ok(rpc::GetNextJobIdReply {
                job_id: 1,
                status: None,
            })
        }
        async fn report_job_error(
            &self,
            _req: rpc::ReportJobErrorRequest,
        ) -> Result<rpc::ReportJobErrorReply, Status> {
            self.record(&self.job_requests, "report_job_error");
            Ok(rpc::ReportJobErrorReply::default())
        }
        async fn register_node(
            &self,
            _req: rpc::RegisterNodeRequest,
        ) -> Result<rpc::RegisterNodeReply, Status> {
            self.record(&self.node_requests, "register_node");
            Ok(rpc::RegisterNodeReply::default())
        }
        async fn unregister_node(
            &self,
            _req: rpc::UnregisterNodeRequest,
        ) -> Result<rpc::UnregisterNodeReply, Status> {
            self.record(&self.node_requests, "unregister_node");
            Ok(rpc::UnregisterNodeReply::default())
        }
        async fn get_all_node_info(
            &self,
            _req: rpc::GetAllNodeInfoRequest,
        ) -> Result<rpc::GetAllNodeInfoReply, Status> {
            self.record(&self.node_requests, "get_all_node_info");
            Ok(rpc::GetAllNodeInfoReply::default())
        }
        async fn get_cluster_id(
            &self,
            _req: rpc::GetClusterIdRequest,
        ) -> Result<rpc::GetClusterIdReply, Status> {
            self.record(&self.node_requests, "get_cluster_id");
            Ok(rpc::GetClusterIdReply::default())
        }
        async fn check_alive(
            &self,
            _req: rpc::CheckAliveRequest,
        ) -> Result<rpc::CheckAliveReply, Status> {
            self.record(&self.node_requests, "check_alive");
            Ok(rpc::CheckAliveReply::default())
        }
        async fn register_actor(
            &self,
            _req: rpc::RegisterActorRequest,
        ) -> Result<rpc::RegisterActorReply, Status> {
            self.record(&self.actor_requests, "register_actor");
            Ok(rpc::RegisterActorReply::default())
        }
        async fn create_actor(
            &self,
            _req: rpc::CreateActorRequest,
        ) -> Result<rpc::CreateActorReply, Status> {
            self.record(&self.actor_requests, "create_actor");
            Ok(rpc::CreateActorReply::default())
        }
        async fn get_actor_info(
            &self,
            _req: rpc::GetActorInfoRequest,
        ) -> Result<rpc::GetActorInfoReply, Status> {
            self.record(&self.actor_requests, "get_actor_info");
            Ok(rpc::GetActorInfoReply::default())
        }
        async fn get_named_actor_info(
            &self,
            _req: rpc::GetNamedActorInfoRequest,
        ) -> Result<rpc::GetNamedActorInfoReply, Status> {
            self.record(&self.actor_requests, "get_named_actor_info");
            Ok(rpc::GetNamedActorInfoReply::default())
        }
        async fn get_all_actor_info(
            &self,
            _req: rpc::GetAllActorInfoRequest,
        ) -> Result<rpc::GetAllActorInfoReply, Status> {
            self.record(&self.actor_requests, "get_all_actor_info");
            Ok(rpc::GetAllActorInfoReply::default())
        }
        async fn kill_actor_via_gcs(
            &self,
            _req: rpc::KillActorViaGcsRequest,
        ) -> Result<rpc::KillActorViaGcsReply, Status> {
            self.record(&self.actor_requests, "kill_actor_via_gcs");
            Ok(rpc::KillActorViaGcsReply::default())
        }
        async fn report_worker_failure(
            &self,
            _req: rpc::ReportWorkerFailureRequest,
        ) -> Result<rpc::ReportWorkerFailureReply, Status> {
            self.record_all("report_worker_failure");
            Ok(rpc::ReportWorkerFailureReply::default())
        }
        async fn add_worker_info(
            &self,
            _req: rpc::AddWorkerInfoRequest,
        ) -> Result<rpc::AddWorkerInfoReply, Status> {
            self.record_all("add_worker_info");
            Ok(rpc::AddWorkerInfoReply::default())
        }
        async fn get_all_worker_info(
            &self,
            _req: rpc::GetAllWorkerInfoRequest,
        ) -> Result<rpc::GetAllWorkerInfoReply, Status> {
            self.record_all("get_all_worker_info");
            Ok(rpc::GetAllWorkerInfoReply::default())
        }
        async fn get_all_resource_usage(
            &self,
            _req: rpc::GetAllResourceUsageRequest,
        ) -> Result<rpc::GetAllResourceUsageReply, Status> {
            self.record_all("get_all_resource_usage");
            Ok(rpc::GetAllResourceUsageReply::default())
        }
        async fn create_placement_group(
            &self,
            _req: rpc::CreatePlacementGroupRequest,
        ) -> Result<rpc::CreatePlacementGroupReply, Status> {
            self.record_all("create_placement_group");
            Ok(rpc::CreatePlacementGroupReply::default())
        }
        async fn remove_placement_group(
            &self,
            _req: rpc::RemovePlacementGroupRequest,
        ) -> Result<rpc::RemovePlacementGroupReply, Status> {
            self.record_all("remove_placement_group");
            Ok(rpc::RemovePlacementGroupReply::default())
        }
        async fn get_all_placement_group(
            &self,
            _req: rpc::GetAllPlacementGroupRequest,
        ) -> Result<rpc::GetAllPlacementGroupReply, Status> {
            self.record_all("get_all_placement_group");
            Ok(rpc::GetAllPlacementGroupReply::default())
        }
        async fn internal_kv_get(
            &self,
            _req: rpc::InternalKvGetRequest,
        ) -> Result<rpc::InternalKvGetReply, Status> {
            self.record_all("internal_kv_get");
            Ok(rpc::InternalKvGetReply::default())
        }
        async fn internal_kv_put(
            &self,
            _req: rpc::InternalKvPutRequest,
        ) -> Result<rpc::InternalKvPutReply, Status> {
            self.record_all("internal_kv_put");
            Ok(rpc::InternalKvPutReply::default())
        }
        async fn internal_kv_del(
            &self,
            _req: rpc::InternalKvDelRequest,
        ) -> Result<rpc::InternalKvDelReply, Status> {
            self.record_all("internal_kv_del");
            Ok(rpc::InternalKvDelReply::default())
        }
        async fn internal_kv_keys(
            &self,
            _req: rpc::InternalKvKeysRequest,
        ) -> Result<rpc::InternalKvKeysReply, Status> {
            self.record_all("internal_kv_keys");
            Ok(rpc::InternalKvKeysReply::default())
        }
        async fn add_task_event_data(
            &self,
            _req: rpc::AddTaskEventDataRequest,
        ) -> Result<rpc::AddTaskEventDataReply, Status> {
            self.record_all("add_task_event_data");
            Ok(rpc::AddTaskEventDataReply::default())
        }
        async fn get_task_events(
            &self,
            _req: rpc::GetTaskEventsRequest,
        ) -> Result<rpc::GetTaskEventsReply, Status> {
            self.record_all("get_task_events");
            Ok(rpc::GetTaskEventsReply::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::fake::FakeGcsClient;

    // ── FakeGcsClient tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_fake_add_job() {
        let client = FakeGcsClient::new();
        let reply = client.add_job(rpc::AddJobRequest::default()).await.unwrap();
        assert_eq!(client.num_requests(), 1);
        assert_eq!(client.num_job_requests(), 1);
        assert_eq!(client.pop_request(), Some("add_job".to_string()));
        let _ = reply;
    }

    #[tokio::test]
    async fn test_fake_multiple_services() {
        let client = FakeGcsClient::new();
        client.add_job(rpc::AddJobRequest::default()).await.unwrap();
        client
            .register_node(rpc::RegisterNodeRequest::default())
            .await
            .unwrap();
        client
            .create_actor(rpc::CreateActorRequest::default())
            .await
            .unwrap();

        assert_eq!(client.num_requests(), 3);
        assert_eq!(client.num_job_requests(), 1);
        assert_eq!(client.num_node_requests(), 1);
        assert_eq!(client.num_actor_requests(), 1);
    }

    #[tokio::test]
    async fn test_fake_get_next_job_id() {
        let client = FakeGcsClient::new();
        let reply = client
            .get_next_job_id(rpc::GetNextJobIdRequest {})
            .await
            .unwrap();
        assert_eq!(reply.job_id, 1);
    }

    #[tokio::test]
    async fn test_fake_mark_job_finished() {
        let client = FakeGcsClient::new();
        client
            .mark_job_finished(rpc::MarkJobFinishedRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_job_requests(), 1);
    }

    #[tokio::test]
    async fn test_fake_node_rpcs() {
        let client = FakeGcsClient::new();
        client
            .get_all_node_info(rpc::GetAllNodeInfoRequest::default())
            .await
            .unwrap();
        client
            .get_cluster_id(rpc::GetClusterIdRequest {})
            .await
            .unwrap();
        client
            .check_alive(rpc::CheckAliveRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_node_requests(), 3);
    }

    #[tokio::test]
    async fn test_fake_actor_rpcs() {
        let client = FakeGcsClient::new();
        client
            .register_actor(rpc::RegisterActorRequest::default())
            .await
            .unwrap();
        client
            .get_actor_info(rpc::GetActorInfoRequest::default())
            .await
            .unwrap();
        client
            .get_all_actor_info(rpc::GetAllActorInfoRequest::default())
            .await
            .unwrap();
        client
            .kill_actor_via_gcs(rpc::KillActorViaGcsRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_actor_requests(), 4);
    }

    #[tokio::test]
    async fn test_fake_kv_rpcs() {
        let client = FakeGcsClient::new();
        client
            .internal_kv_put(rpc::InternalKvPutRequest::default())
            .await
            .unwrap();
        client
            .internal_kv_get(rpc::InternalKvGetRequest::default())
            .await
            .unwrap();
        client
            .internal_kv_keys(rpc::InternalKvKeysRequest::default())
            .await
            .unwrap();
        client
            .internal_kv_del(rpc::InternalKvDelRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 4);
    }

    #[tokio::test]
    async fn test_fake_pop_request_order() {
        let client = FakeGcsClient::new();
        client.add_job(rpc::AddJobRequest::default()).await.unwrap();
        client
            .register_node(rpc::RegisterNodeRequest::default())
            .await
            .unwrap();
        assert_eq!(client.pop_request(), Some("add_job".to_string()));
        assert_eq!(client.pop_request(), Some("register_node".to_string()));
        assert_eq!(client.pop_request(), None);
    }

    #[tokio::test]
    async fn test_fake_default() {
        let client = FakeGcsClient::default();
        assert_eq!(client.num_requests(), 0);
    }

    #[tokio::test]
    async fn test_fake_task_event_rpcs() {
        let client = FakeGcsClient::new();
        client
            .add_task_event_data(rpc::AddTaskEventDataRequest::default())
            .await
            .unwrap();
        client
            .get_task_events(rpc::GetTaskEventsRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 2);
    }

    #[tokio::test]
    async fn test_fake_placement_group_rpcs() {
        let client = FakeGcsClient::new();
        client
            .create_placement_group(rpc::CreatePlacementGroupRequest::default())
            .await
            .unwrap();
        client
            .remove_placement_group(rpc::RemovePlacementGroupRequest::default())
            .await
            .unwrap();
        client
            .get_all_placement_group(rpc::GetAllPlacementGroupRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 3);
    }

    #[tokio::test]
    async fn test_fake_worker_rpcs() {
        let client = FakeGcsClient::new();
        client
            .report_worker_failure(rpc::ReportWorkerFailureRequest::default())
            .await
            .unwrap();
        client
            .add_worker_info(rpc::AddWorkerInfoRequest::default())
            .await
            .unwrap();
        client
            .get_all_worker_info(rpc::GetAllWorkerInfoRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 3);
    }

    // ── GcsRpcClient construction test ────────────────────────────

    #[tokio::test]
    async fn test_real_client_from_lazy_channel() {
        let channel = tonic::transport::Channel::from_static("http://[::1]:1").connect_lazy();
        let client = GcsRpcClient::from_channel(channel, RetryConfig::default());
        assert!(client.retry_client().is_connected());
    }

    // ── Trait object test ─────────────────────────────────────────

    #[tokio::test]
    async fn test_trait_object_usage() {
        let client: Box<dyn GcsClient> = Box::new(FakeGcsClient::new());
        client.add_job(rpc::AddJobRequest::default()).await.unwrap();
        client
            .register_node(rpc::RegisterNodeRequest::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_fake_resource_usage_rpc() {
        let client = FakeGcsClient::new();
        client
            .get_all_resource_usage(rpc::GetAllResourceUsageRequest {})
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 1);
    }
}
