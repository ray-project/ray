// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Async trait for raylet (NodeManager) client.

use ray_proto::ray::rpc;
use tonic::Status;

/// Async trait for Raylet (NodeManager) RPC operations.
#[async_trait::async_trait]
pub trait RayletClient: Send + Sync {
    // ── Lease Management ──────────────────────────────────────────
    async fn request_worker_lease(
        &self,
        req: rpc::RequestWorkerLeaseRequest,
    ) -> Result<rpc::RequestWorkerLeaseReply, Status>;
    async fn return_worker_lease(
        &self,
        req: rpc::ReturnWorkerLeaseRequest,
    ) -> Result<rpc::ReturnWorkerLeaseReply, Status>;
    async fn cancel_worker_lease(
        &self,
        req: rpc::CancelWorkerLeaseRequest,
    ) -> Result<rpc::CancelWorkerLeaseReply, Status>;
    async fn report_worker_backlog(
        &self,
        req: rpc::ReportWorkerBacklogRequest,
    ) -> Result<rpc::ReportWorkerBacklogReply, Status>;
    async fn prestart_workers(
        &self,
        req: rpc::PrestartWorkersRequest,
    ) -> Result<rpc::PrestartWorkersReply, Status>;

    // ── Placement Groups ──────────────────────────────────────────
    async fn prepare_bundle_resources(
        &self,
        req: rpc::PrepareBundleResourcesRequest,
    ) -> Result<rpc::PrepareBundleResourcesReply, Status>;
    async fn commit_bundle_resources(
        &self,
        req: rpc::CommitBundleResourcesRequest,
    ) -> Result<rpc::CommitBundleResourcesReply, Status>;
    async fn cancel_resource_reserve(
        &self,
        req: rpc::CancelResourceReserveRequest,
    ) -> Result<rpc::CancelResourceReserveReply, Status>;

    // ── Object & Resource ─────────────────────────────────────────
    async fn pin_object_ids(
        &self,
        req: rpc::PinObjectIDsRequest,
    ) -> Result<rpc::PinObjectIDsReply, Status>;
    async fn get_resource_load(
        &self,
        req: rpc::GetResourceLoadRequest,
    ) -> Result<rpc::GetResourceLoadReply, Status>;

    // ── Lifecycle & Monitoring ────────────────────────────────────
    async fn shutdown_raylet(
        &self,
        req: rpc::ShutdownRayletRequest,
    ) -> Result<rpc::ShutdownRayletReply, Status>;
    async fn drain_raylet(
        &self,
        req: rpc::DrainRayletRequest,
    ) -> Result<rpc::DrainRayletReply, Status>;
    async fn notify_gcs_restart(
        &self,
        req: rpc::NotifyGcsRestartRequest,
    ) -> Result<rpc::NotifyGcsRestartReply, Status>;
    async fn get_node_stats(
        &self,
        req: rpc::GetNodeStatsRequest,
    ) -> Result<rpc::GetNodeStatsReply, Status>;
    async fn get_system_config(
        &self,
        req: rpc::GetSystemConfigRequest,
    ) -> Result<rpc::GetSystemConfigReply, Status>;

    // ── Worker/Task ───────────────────────────────────────────────
    async fn kill_local_actor(
        &self,
        req: rpc::KillLocalActorRequest,
    ) -> Result<rpc::KillLocalActorReply, Status>;
    async fn cancel_local_task(
        &self,
        req: rpc::CancelLocalTaskRequest,
    ) -> Result<rpc::CancelLocalTaskReply, Status>;

    // ── GC & Misc ─────────────────────────────────────────────────
    async fn global_gc(
        &self,
        req: rpc::GlobalGcRequest,
    ) -> Result<rpc::GlobalGcReply, Status>;
}
