// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! CoreWorker RPC client — typed wrapper around tonic-generated stubs.
//!
//! Replaces `src/ray/rpc/worker/core_worker_client.h`.
//! Provides a retryable client for worker-to-worker and raylet-to-worker RPCs.

use parking_lot::Mutex;
use tonic::transport::Channel;
use tonic::Status;

use ray_proto::ray::rpc;

use crate::client::{RetryConfig, RetryableGrpcClient};

type StubClient = rpc::core_worker_service_client::CoreWorkerServiceClient<Channel>;

/// Macro to implement a CoreWorker RPC method with retry.
macro_rules! impl_cw_rpc {
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

/// CoreWorker RPC client wrapping the tonic-generated stub with retry logic.
pub struct CoreWorkerClient {
    retry_client: RetryableGrpcClient,
    stub: Mutex<StubClient>,
    /// Remote worker address (for logging/debugging).
    address: String,
}

impl CoreWorkerClient {
    /// Connect to a CoreWorker service.
    pub async fn connect(
        addr: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = RetryableGrpcClient::connect(addr).await?;
        Ok(Self::from_channel(channel, retry_config, addr.to_string()))
    }

    /// Create from an existing channel.
    pub fn from_channel(channel: Channel, retry_config: RetryConfig, address: String) -> Self {
        let retry_client = RetryableGrpcClient::new(channel.clone(), retry_config);
        Self {
            retry_client,
            stub: Mutex::new(StubClient::new(channel)),
            address,
        }
    }

    /// Create a lazily-connecting client.
    pub fn connect_lazy(addr: &str, retry_config: RetryConfig) -> Self {
        let channel = RetryableGrpcClient::connect_lazy(addr);
        Self::from_channel(channel, retry_config, addr.to_string())
    }

    /// Get the remote address.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        self.retry_client.is_connected()
    }

    // ── Task submission RPCs ────────────────────────────────────────

    /// Push a normal task to a worker for execution.
    pub async fn push_task(
        &self,
        req: rpc::PushTaskRequest,
    ) -> Result<rpc::PushTaskReply, Status> {
        impl_cw_rpc!(self, push_task, req)
    }

    /// Cancel a running task on this worker.
    pub async fn cancel_task(
        &self,
        req: rpc::CancelTaskRequest,
    ) -> Result<rpc::CancelTaskReply, Status> {
        impl_cw_rpc!(self, cancel_task, req)
    }

    /// Request the owner to cancel a task.
    pub async fn request_owner_to_cancel_task(
        &self,
        req: rpc::RequestOwnerToCancelTaskRequest,
    ) -> Result<rpc::RequestOwnerToCancelTaskReply, Status> {
        impl_cw_rpc!(self, request_owner_to_cancel_task, req)
    }

    // ── Object RPCs ────────────────────────────────────────────────

    /// Get the status of an object from its owner.
    pub async fn get_object_status(
        &self,
        req: rpc::GetObjectStatusRequest,
    ) -> Result<rpc::GetObjectStatusReply, Status> {
        impl_cw_rpc!(self, get_object_status, req)
    }

    /// Get object locations from the owner.
    pub async fn get_object_locations_owner(
        &self,
        req: rpc::GetObjectLocationsOwnerRequest,
    ) -> Result<rpc::GetObjectLocationsOwnerReply, Status> {
        impl_cw_rpc!(self, get_object_locations_owner, req)
    }

    /// Notify that a Plasma object is ready (sealed).
    pub async fn plasma_object_ready(
        &self,
        req: rpc::PlasmaObjectReadyRequest,
    ) -> Result<rpc::PlasmaObjectReadyReply, Status> {
        impl_cw_rpc!(self, plasma_object_ready, req)
    }

    /// Delete objects on this worker.
    pub async fn delete_objects(
        &self,
        req: rpc::DeleteObjectsRequest,
    ) -> Result<rpc::DeleteObjectsReply, Status> {
        impl_cw_rpc!(self, delete_objects, req)
    }

    /// Assign ownership of an object.
    pub async fn assign_object_owner(
        &self,
        req: rpc::AssignObjectOwnerRequest,
    ) -> Result<rpc::AssignObjectOwnerReply, Status> {
        impl_cw_rpc!(self, assign_object_owner, req)
    }

    // ── Memory management RPCs ──────────────────────────────────────

    /// Trigger local garbage collection.
    pub async fn local_gc(
        &self,
        req: rpc::LocalGcRequest,
    ) -> Result<rpc::LocalGcReply, Status> {
        impl_cw_rpc!(self, local_gc, req)
    }

    /// Request the worker to spill objects to external storage.
    pub async fn spill_objects(
        &self,
        req: rpc::SpillObjectsRequest,
    ) -> Result<rpc::SpillObjectsReply, Status> {
        impl_cw_rpc!(self, spill_objects, req)
    }

    /// Request the worker to restore spilled objects.
    pub async fn restore_spilled_objects(
        &self,
        req: rpc::RestoreSpilledObjectsRequest,
    ) -> Result<rpc::RestoreSpilledObjectsReply, Status> {
        impl_cw_rpc!(self, restore_spilled_objects, req)
    }

    /// Delete spilled objects from external storage.
    pub async fn delete_spilled_objects(
        &self,
        req: rpc::DeleteSpilledObjectsRequest,
    ) -> Result<rpc::DeleteSpilledObjectsReply, Status> {
        impl_cw_rpc!(self, delete_spilled_objects, req)
    }

    // ── Object location batch RPCs ──────────────────────────────────

    /// Update object locations in batch.
    pub async fn update_object_location_batch(
        &self,
        req: rpc::UpdateObjectLocationBatchRequest,
    ) -> Result<rpc::UpdateObjectLocationBatchReply, Status> {
        impl_cw_rpc!(self, update_object_location_batch, req)
    }

    // ── Actor RPCs ──────────────────────────────────────────────────

    /// Kill an actor on this worker.
    pub async fn kill_actor(
        &self,
        req: rpc::KillActorRequest,
    ) -> Result<rpc::KillActorReply, Status> {
        impl_cw_rpc!(self, kill_actor, req)
    }

    /// Wait for an actor ref to be deleted.
    pub async fn wait_for_actor_ref_deleted(
        &self,
        req: rpc::WaitForActorRefDeletedRequest,
    ) -> Result<rpc::WaitForActorRefDeletedReply, Status> {
        impl_cw_rpc!(self, wait_for_actor_ref_deleted, req)
    }

    // ── Worker lifecycle RPCs ───────────────────────────────────────

    /// Exit (shut down) the worker.
    pub async fn exit(
        &self,
        req: rpc::ExitRequest,
    ) -> Result<rpc::ExitReply, Status> {
        impl_cw_rpc!(self, exit, req)
    }

    /// Get the number of pending tasks on this worker.
    pub async fn num_pending_tasks(
        &self,
        req: rpc::NumPendingTasksRequest,
    ) -> Result<rpc::NumPendingTasksReply, Status> {
        impl_cw_rpc!(self, num_pending_tasks, req)
    }

    /// Get stats about this CoreWorker.
    pub async fn get_core_worker_stats(
        &self,
        req: rpc::GetCoreWorkerStatsRequest,
    ) -> Result<rpc::GetCoreWorkerStatsReply, Status> {
        impl_cw_rpc!(self, get_core_worker_stats, req)
    }

    // ── Generator RPCs ──────────────────────────────────────────────

    /// Report generator item returns.
    pub async fn report_generator_item_returns(
        &self,
        req: rpc::ReportGeneratorItemReturnsRequest,
    ) -> Result<rpc::ReportGeneratorItemReturnsReply, Status> {
        impl_cw_rpc!(self, report_generator_item_returns, req)
    }

    // ── Notification RPCs ───────────────────────────────────────────

    /// Notify the worker that GCS has restarted.
    pub async fn raylet_notify_gcs_restart(
        &self,
        req: rpc::RayletNotifyGcsRestartRequest,
    ) -> Result<rpc::RayletNotifyGcsRestartReply, Status> {
        impl_cw_rpc!(self, raylet_notify_gcs_restart, req)
    }

    /// Notify that actor task arg wait is complete.
    pub async fn actor_call_arg_wait_complete(
        &self,
        req: rpc::ActorCallArgWaitCompleteRequest,
    ) -> Result<rpc::ActorCallArgWaitCompleteReply, Status> {
        impl_cw_rpc!(self, actor_call_arg_wait_complete, req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_lazy_creates_client() {
        let client =
            CoreWorkerClient::connect_lazy("http://127.0.0.1:50000", RetryConfig::default());
        assert_eq!(client.address(), "http://127.0.0.1:50000");
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_from_channel() {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        let client = CoreWorkerClient::from_channel(
            channel,
            RetryConfig::default(),
            "http://[::1]:1".to_string(),
        );
        assert_eq!(client.address(), "http://[::1]:1");
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_custom_retry_config() {
        let config = RetryConfig {
            max_retries: 10,
            ..RetryConfig::default()
        };
        let client =
            CoreWorkerClient::connect_lazy("http://127.0.0.1:50001", config);
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_connect_to_unreachable_fails() {
        // connect() tries to actually establish a connection, which should fail
        // for an unreachable address.
        let result =
            CoreWorkerClient::connect("http://192.0.2.1:1", RetryConfig::default()).await;
        assert!(result.is_err());
    }
}
