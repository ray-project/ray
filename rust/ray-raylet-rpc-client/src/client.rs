// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Real Raylet RPC client backed by tonic gRPC stubs.

use parking_lot::Mutex;
use tonic::transport::Channel;
use tonic::Status;

use ray_proto::ray::rpc;
use ray_rpc::client::{RetryConfig, RetryableGrpcClient};

use crate::traits::RayletClient;

type NmClient = rpc::node_manager_service_client::NodeManagerServiceClient<Channel>;

/// Real Raylet RPC client wrapping NodeManagerServiceClient with retry logic.
pub struct RayletRpcClient {
    retry_client: RetryableGrpcClient,
    stub: Mutex<NmClient>,
}

impl RayletRpcClient {
    /// Connect to a raylet gRPC server.
    pub async fn connect(
        address: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = RetryableGrpcClient::connect(address).await?;
        Ok(Self::from_channel(channel, retry_config))
    }

    /// Create from an existing channel (useful for testing).
    pub fn from_channel(channel: Channel, retry_config: RetryConfig) -> Self {
        let retry_client = RetryableGrpcClient::new(channel.clone(), retry_config);
        Self {
            retry_client,
            stub: Mutex::new(NmClient::new(channel)),
        }
    }

    /// Access the underlying retry client for connection state inspection.
    pub fn retry_client(&self) -> &RetryableGrpcClient {
        &self.retry_client
    }
}

/// Macro to implement a raylet RPC method with retry.
macro_rules! impl_raylet_rpc {
    ($self:ident, $method:ident, $req:expr) => {{
        let retry = &$self.retry_client;
        let stub_mutex = &$self.stub;
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
impl RayletClient for RayletRpcClient {
    async fn request_worker_lease(
        &self,
        req: rpc::RequestWorkerLeaseRequest,
    ) -> Result<rpc::RequestWorkerLeaseReply, Status> {
        impl_raylet_rpc!(self, request_worker_lease, req)
    }

    async fn return_worker_lease(
        &self,
        req: rpc::ReturnWorkerLeaseRequest,
    ) -> Result<rpc::ReturnWorkerLeaseReply, Status> {
        impl_raylet_rpc!(self, return_worker_lease, req)
    }

    async fn cancel_worker_lease(
        &self,
        req: rpc::CancelWorkerLeaseRequest,
    ) -> Result<rpc::CancelWorkerLeaseReply, Status> {
        impl_raylet_rpc!(self, cancel_worker_lease, req)
    }

    async fn report_worker_backlog(
        &self,
        req: rpc::ReportWorkerBacklogRequest,
    ) -> Result<rpc::ReportWorkerBacklogReply, Status> {
        impl_raylet_rpc!(self, report_worker_backlog, req)
    }

    async fn prestart_workers(
        &self,
        req: rpc::PrestartWorkersRequest,
    ) -> Result<rpc::PrestartWorkersReply, Status> {
        impl_raylet_rpc!(self, prestart_workers, req)
    }

    async fn prepare_bundle_resources(
        &self,
        req: rpc::PrepareBundleResourcesRequest,
    ) -> Result<rpc::PrepareBundleResourcesReply, Status> {
        impl_raylet_rpc!(self, prepare_bundle_resources, req)
    }

    async fn commit_bundle_resources(
        &self,
        req: rpc::CommitBundleResourcesRequest,
    ) -> Result<rpc::CommitBundleResourcesReply, Status> {
        impl_raylet_rpc!(self, commit_bundle_resources, req)
    }

    async fn cancel_resource_reserve(
        &self,
        req: rpc::CancelResourceReserveRequest,
    ) -> Result<rpc::CancelResourceReserveReply, Status> {
        impl_raylet_rpc!(self, cancel_resource_reserve, req)
    }

    async fn pin_object_ids(
        &self,
        req: rpc::PinObjectIDsRequest,
    ) -> Result<rpc::PinObjectIDsReply, Status> {
        impl_raylet_rpc!(self, pin_object_i_ds, req)
    }

    async fn get_resource_load(
        &self,
        req: rpc::GetResourceLoadRequest,
    ) -> Result<rpc::GetResourceLoadReply, Status> {
        impl_raylet_rpc!(self, get_resource_load, req)
    }

    async fn shutdown_raylet(
        &self,
        req: rpc::ShutdownRayletRequest,
    ) -> Result<rpc::ShutdownRayletReply, Status> {
        impl_raylet_rpc!(self, shutdown_raylet, req)
    }

    async fn drain_raylet(
        &self,
        req: rpc::DrainRayletRequest,
    ) -> Result<rpc::DrainRayletReply, Status> {
        impl_raylet_rpc!(self, drain_raylet, req)
    }

    async fn notify_gcs_restart(
        &self,
        req: rpc::NotifyGcsRestartRequest,
    ) -> Result<rpc::NotifyGcsRestartReply, Status> {
        impl_raylet_rpc!(self, notify_gcs_restart, req)
    }

    async fn get_node_stats(
        &self,
        req: rpc::GetNodeStatsRequest,
    ) -> Result<rpc::GetNodeStatsReply, Status> {
        impl_raylet_rpc!(self, get_node_stats, req)
    }

    async fn get_system_config(
        &self,
        req: rpc::GetSystemConfigRequest,
    ) -> Result<rpc::GetSystemConfigReply, Status> {
        impl_raylet_rpc!(self, get_system_config, req)
    }

    async fn kill_local_actor(
        &self,
        req: rpc::KillLocalActorRequest,
    ) -> Result<rpc::KillLocalActorReply, Status> {
        impl_raylet_rpc!(self, kill_local_actor, req)
    }

    async fn cancel_local_task(
        &self,
        req: rpc::CancelLocalTaskRequest,
    ) -> Result<rpc::CancelLocalTaskReply, Status> {
        impl_raylet_rpc!(self, cancel_local_task, req)
    }

    async fn global_gc(
        &self,
        req: rpc::GlobalGcRequest,
    ) -> Result<rpc::GlobalGcReply, Status> {
        impl_raylet_rpc!(self, global_gc, req)
    }
}

/// A fake raylet client for testing.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::collections::VecDeque;

    /// Records all requests and returns default responses.
    pub struct FakeRayletClient {
        requests: Mutex<VecDeque<String>>,
    }

    impl FakeRayletClient {
        pub fn new() -> Self {
            Self {
                requests: Mutex::new(VecDeque::new()),
            }
        }

        pub fn num_requests(&self) -> usize {
            self.requests.lock().len()
        }

        pub fn pop_request(&self) -> Option<String> {
            self.requests.lock().pop_front()
        }

        fn record(&self, name: &str) {
            self.requests.lock().push_back(name.to_string());
        }
    }

    impl Default for FakeRayletClient {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait]
    impl RayletClient for FakeRayletClient {
        async fn request_worker_lease(
            &self,
            _req: rpc::RequestWorkerLeaseRequest,
        ) -> Result<rpc::RequestWorkerLeaseReply, Status> {
            self.record("request_worker_lease");
            Ok(rpc::RequestWorkerLeaseReply::default())
        }
        async fn return_worker_lease(
            &self,
            _req: rpc::ReturnWorkerLeaseRequest,
        ) -> Result<rpc::ReturnWorkerLeaseReply, Status> {
            self.record("return_worker_lease");
            Ok(rpc::ReturnWorkerLeaseReply::default())
        }
        async fn cancel_worker_lease(
            &self,
            _req: rpc::CancelWorkerLeaseRequest,
        ) -> Result<rpc::CancelWorkerLeaseReply, Status> {
            self.record("cancel_worker_lease");
            Ok(rpc::CancelWorkerLeaseReply::default())
        }
        async fn report_worker_backlog(
            &self,
            _req: rpc::ReportWorkerBacklogRequest,
        ) -> Result<rpc::ReportWorkerBacklogReply, Status> {
            self.record("report_worker_backlog");
            Ok(rpc::ReportWorkerBacklogReply::default())
        }
        async fn prestart_workers(
            &self,
            _req: rpc::PrestartWorkersRequest,
        ) -> Result<rpc::PrestartWorkersReply, Status> {
            self.record("prestart_workers");
            Ok(rpc::PrestartWorkersReply::default())
        }
        async fn prepare_bundle_resources(
            &self,
            _req: rpc::PrepareBundleResourcesRequest,
        ) -> Result<rpc::PrepareBundleResourcesReply, Status> {
            self.record("prepare_bundle_resources");
            Ok(rpc::PrepareBundleResourcesReply::default())
        }
        async fn commit_bundle_resources(
            &self,
            _req: rpc::CommitBundleResourcesRequest,
        ) -> Result<rpc::CommitBundleResourcesReply, Status> {
            self.record("commit_bundle_resources");
            Ok(rpc::CommitBundleResourcesReply::default())
        }
        async fn cancel_resource_reserve(
            &self,
            _req: rpc::CancelResourceReserveRequest,
        ) -> Result<rpc::CancelResourceReserveReply, Status> {
            self.record("cancel_resource_reserve");
            Ok(rpc::CancelResourceReserveReply::default())
        }
        async fn pin_object_ids(
            &self,
            _req: rpc::PinObjectIDsRequest,
        ) -> Result<rpc::PinObjectIDsReply, Status> {
            self.record("pin_object_ids");
            Ok(rpc::PinObjectIDsReply::default())
        }
        async fn get_resource_load(
            &self,
            _req: rpc::GetResourceLoadRequest,
        ) -> Result<rpc::GetResourceLoadReply, Status> {
            self.record("get_resource_load");
            Ok(rpc::GetResourceLoadReply::default())
        }
        async fn shutdown_raylet(
            &self,
            _req: rpc::ShutdownRayletRequest,
        ) -> Result<rpc::ShutdownRayletReply, Status> {
            self.record("shutdown_raylet");
            Ok(rpc::ShutdownRayletReply::default())
        }
        async fn drain_raylet(
            &self,
            _req: rpc::DrainRayletRequest,
        ) -> Result<rpc::DrainRayletReply, Status> {
            self.record("drain_raylet");
            Ok(rpc::DrainRayletReply::default())
        }
        async fn notify_gcs_restart(
            &self,
            _req: rpc::NotifyGcsRestartRequest,
        ) -> Result<rpc::NotifyGcsRestartReply, Status> {
            self.record("notify_gcs_restart");
            Ok(rpc::NotifyGcsRestartReply::default())
        }
        async fn get_node_stats(
            &self,
            _req: rpc::GetNodeStatsRequest,
        ) -> Result<rpc::GetNodeStatsReply, Status> {
            self.record("get_node_stats");
            Ok(rpc::GetNodeStatsReply::default())
        }
        async fn get_system_config(
            &self,
            _req: rpc::GetSystemConfigRequest,
        ) -> Result<rpc::GetSystemConfigReply, Status> {
            self.record("get_system_config");
            Ok(rpc::GetSystemConfigReply::default())
        }
        async fn kill_local_actor(
            &self,
            _req: rpc::KillLocalActorRequest,
        ) -> Result<rpc::KillLocalActorReply, Status> {
            self.record("kill_local_actor");
            Ok(rpc::KillLocalActorReply::default())
        }
        async fn cancel_local_task(
            &self,
            _req: rpc::CancelLocalTaskRequest,
        ) -> Result<rpc::CancelLocalTaskReply, Status> {
            self.record("cancel_local_task");
            Ok(rpc::CancelLocalTaskReply::default())
        }
        async fn global_gc(
            &self,
            _req: rpc::GlobalGcRequest,
        ) -> Result<rpc::GlobalGcReply, Status> {
            self.record("global_gc");
            Ok(rpc::GlobalGcReply::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::fake::FakeRayletClient;

    #[tokio::test]
    async fn test_fake_lease_rpcs() {
        let client = FakeRayletClient::new();
        client
            .request_worker_lease(rpc::RequestWorkerLeaseRequest::default())
            .await
            .unwrap();
        client
            .return_worker_lease(rpc::ReturnWorkerLeaseRequest::default())
            .await
            .unwrap();
        client
            .cancel_worker_lease(rpc::CancelWorkerLeaseRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 3);
        assert_eq!(
            client.pop_request(),
            Some("request_worker_lease".to_string())
        );
        assert_eq!(
            client.pop_request(),
            Some("return_worker_lease".to_string())
        );
        assert_eq!(
            client.pop_request(),
            Some("cancel_worker_lease".to_string())
        );
    }

    #[tokio::test]
    async fn test_fake_placement_group_rpcs() {
        let client = FakeRayletClient::new();
        client
            .prepare_bundle_resources(rpc::PrepareBundleResourcesRequest::default())
            .await
            .unwrap();
        client
            .commit_bundle_resources(rpc::CommitBundleResourcesRequest::default())
            .await
            .unwrap();
        client
            .cancel_resource_reserve(rpc::CancelResourceReserveRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 3);
    }

    #[tokio::test]
    async fn test_fake_object_and_resource_rpcs() {
        let client = FakeRayletClient::new();
        client
            .pin_object_ids(rpc::PinObjectIDsRequest::default())
            .await
            .unwrap();
        client
            .get_resource_load(rpc::GetResourceLoadRequest {})
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 2);
    }

    #[tokio::test]
    async fn test_fake_lifecycle_rpcs() {
        let client = FakeRayletClient::new();
        client
            .shutdown_raylet(rpc::ShutdownRayletRequest::default())
            .await
            .unwrap();
        client
            .drain_raylet(rpc::DrainRayletRequest::default())
            .await
            .unwrap();
        client
            .notify_gcs_restart(rpc::NotifyGcsRestartRequest {})
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 3);
    }

    #[tokio::test]
    async fn test_fake_monitoring_rpcs() {
        let client = FakeRayletClient::new();
        client
            .get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
            .unwrap();
        client
            .get_system_config(rpc::GetSystemConfigRequest {})
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 2);
    }

    #[tokio::test]
    async fn test_fake_worker_task_rpcs() {
        let client = FakeRayletClient::new();
        client
            .kill_local_actor(rpc::KillLocalActorRequest::default())
            .await
            .unwrap();
        client
            .cancel_local_task(rpc::CancelLocalTaskRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 2);
    }

    #[tokio::test]
    async fn test_fake_gc_rpc() {
        let client = FakeRayletClient::new();
        client.global_gc(rpc::GlobalGcRequest {}).await.unwrap();
        assert_eq!(client.num_requests(), 1);
        assert_eq!(client.pop_request(), Some("global_gc".to_string()));
    }

    #[tokio::test]
    async fn test_fake_backlog_and_prestart() {
        let client = FakeRayletClient::new();
        client
            .report_worker_backlog(rpc::ReportWorkerBacklogRequest::default())
            .await
            .unwrap();
        client
            .prestart_workers(rpc::PrestartWorkersRequest::default())
            .await
            .unwrap();
        assert_eq!(client.num_requests(), 2);
    }

    #[tokio::test]
    async fn test_fake_pop_order() {
        let client = FakeRayletClient::new();
        client
            .request_worker_lease(rpc::RequestWorkerLeaseRequest::default())
            .await
            .unwrap();
        client.global_gc(rpc::GlobalGcRequest {}).await.unwrap();
        assert_eq!(
            client.pop_request(),
            Some("request_worker_lease".to_string())
        );
        assert_eq!(client.pop_request(), Some("global_gc".to_string()));
        assert_eq!(client.pop_request(), None);
    }

    #[tokio::test]
    async fn test_fake_default() {
        let client = FakeRayletClient::default();
        assert_eq!(client.num_requests(), 0);
    }

    #[tokio::test]
    async fn test_trait_object_usage() {
        let client: Box<dyn RayletClient> = Box::new(FakeRayletClient::new());
        client
            .request_worker_lease(rpc::RequestWorkerLeaseRequest::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_real_client_from_lazy_channel() {
        let channel = tonic::transport::Channel::from_static("http://[::1]:1").connect_lazy();
        let client = RayletRpcClient::from_channel(channel, RetryConfig::default());
        assert!(client.retry_client().is_connected());
    }
}
