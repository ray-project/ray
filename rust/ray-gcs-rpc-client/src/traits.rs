// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Async trait for GCS client, enabling both real and mock implementations.

use ray_proto::ray::rpc;
use tonic::Status;

/// Async trait for GCS RPC operations.
///
/// Implement this trait for real gRPC clients (`GcsRpcClient`) and
/// test mocks (`FakeGcsClient`). All methods return `Result<Response, Status>`.
#[async_trait::async_trait]
pub trait GcsClient: Send + Sync {
    // ── Job RPCs ──────────────────────────────────────────────────
    async fn add_job(&self, req: rpc::AddJobRequest) -> Result<rpc::AddJobReply, Status>;
    async fn mark_job_finished(
        &self,
        req: rpc::MarkJobFinishedRequest,
    ) -> Result<rpc::MarkJobFinishedReply, Status>;
    async fn get_all_job_info(
        &self,
        req: rpc::GetAllJobInfoRequest,
    ) -> Result<rpc::GetAllJobInfoReply, Status>;
    async fn get_next_job_id(
        &self,
        req: rpc::GetNextJobIdRequest,
    ) -> Result<rpc::GetNextJobIdReply, Status>;
    async fn report_job_error(
        &self,
        req: rpc::ReportJobErrorRequest,
    ) -> Result<rpc::ReportJobErrorReply, Status>;

    // ── Node RPCs ─────────────────────────────────────────────────
    async fn register_node(
        &self,
        req: rpc::RegisterNodeRequest,
    ) -> Result<rpc::RegisterNodeReply, Status>;
    async fn unregister_node(
        &self,
        req: rpc::UnregisterNodeRequest,
    ) -> Result<rpc::UnregisterNodeReply, Status>;
    async fn get_all_node_info(
        &self,
        req: rpc::GetAllNodeInfoRequest,
    ) -> Result<rpc::GetAllNodeInfoReply, Status>;
    async fn get_cluster_id(
        &self,
        req: rpc::GetClusterIdRequest,
    ) -> Result<rpc::GetClusterIdReply, Status>;
    async fn check_alive(
        &self,
        req: rpc::CheckAliveRequest,
    ) -> Result<rpc::CheckAliveReply, Status>;

    // ── Actor RPCs ────────────────────────────────────────────────
    async fn register_actor(
        &self,
        req: rpc::RegisterActorRequest,
    ) -> Result<rpc::RegisterActorReply, Status>;
    async fn create_actor(
        &self,
        req: rpc::CreateActorRequest,
    ) -> Result<rpc::CreateActorReply, Status>;
    async fn get_actor_info(
        &self,
        req: rpc::GetActorInfoRequest,
    ) -> Result<rpc::GetActorInfoReply, Status>;
    async fn get_named_actor_info(
        &self,
        req: rpc::GetNamedActorInfoRequest,
    ) -> Result<rpc::GetNamedActorInfoReply, Status>;
    async fn get_all_actor_info(
        &self,
        req: rpc::GetAllActorInfoRequest,
    ) -> Result<rpc::GetAllActorInfoReply, Status>;
    async fn kill_actor_via_gcs(
        &self,
        req: rpc::KillActorViaGcsRequest,
    ) -> Result<rpc::KillActorViaGcsReply, Status>;

    // ── Worker RPCs ───────────────────────────────────────────────
    async fn report_worker_failure(
        &self,
        req: rpc::ReportWorkerFailureRequest,
    ) -> Result<rpc::ReportWorkerFailureReply, Status>;
    async fn add_worker_info(
        &self,
        req: rpc::AddWorkerInfoRequest,
    ) -> Result<rpc::AddWorkerInfoReply, Status>;
    async fn get_all_worker_info(
        &self,
        req: rpc::GetAllWorkerInfoRequest,
    ) -> Result<rpc::GetAllWorkerInfoReply, Status>;

    // ── Resource RPCs ─────────────────────────────────────────────
    async fn get_all_resource_usage(
        &self,
        req: rpc::GetAllResourceUsageRequest,
    ) -> Result<rpc::GetAllResourceUsageReply, Status>;

    // ── Placement Group RPCs ──────────────────────────────────────
    async fn create_placement_group(
        &self,
        req: rpc::CreatePlacementGroupRequest,
    ) -> Result<rpc::CreatePlacementGroupReply, Status>;
    async fn remove_placement_group(
        &self,
        req: rpc::RemovePlacementGroupRequest,
    ) -> Result<rpc::RemovePlacementGroupReply, Status>;
    async fn get_all_placement_group(
        &self,
        req: rpc::GetAllPlacementGroupRequest,
    ) -> Result<rpc::GetAllPlacementGroupReply, Status>;

    // ── Internal KV RPCs ──────────────────────────────────────────
    async fn internal_kv_get(
        &self,
        req: rpc::InternalKvGetRequest,
    ) -> Result<rpc::InternalKvGetReply, Status>;
    async fn internal_kv_put(
        &self,
        req: rpc::InternalKvPutRequest,
    ) -> Result<rpc::InternalKvPutReply, Status>;
    async fn internal_kv_del(
        &self,
        req: rpc::InternalKvDelRequest,
    ) -> Result<rpc::InternalKvDelReply, Status>;
    async fn internal_kv_keys(
        &self,
        req: rpc::InternalKvKeysRequest,
    ) -> Result<rpc::InternalKvKeysReply, Status>;

    // ── Task Event RPCs ───────────────────────────────────────────
    async fn add_task_event_data(
        &self,
        req: rpc::AddTaskEventDataRequest,
    ) -> Result<rpc::AddTaskEventDataReply, Status>;
    async fn get_task_events(
        &self,
        req: rpc::GetTaskEventsRequest,
    ) -> Result<rpc::GetTaskEventsReply, Status>;
}
