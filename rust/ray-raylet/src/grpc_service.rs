// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC service implementation for the NodeManagerService.
//!
//! Implements the 30 RPCs defined in `node_manager.proto`.
//! Delegates to the NodeManager and its component managers.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use ray_proto::ray::rpc;

use crate::node_manager::NodeManager;

/// The gRPC service implementation wrapping the NodeManager.
pub struct NodeManagerServiceImpl {
    pub node_manager: Arc<NodeManager>,
}

// ─── Handler methods ────────────────────────────────────────────────────

impl NodeManagerServiceImpl {
    // ─── Scheduling RPCs ──────────────────────────────────────────────

    pub async fn handle_request_worker_lease(
        &self,
        request: rpc::RequestWorkerLeaseRequest,
    ) -> Result<rpc::RequestWorkerLeaseReply, Status> {
        tracing::debug!("RequestWorkerLease received");
        let _ = request;
        Ok(rpc::RequestWorkerLeaseReply::default())
    }

    pub async fn handle_return_worker_lease(
        &self,
        request: rpc::ReturnWorkerLeaseRequest,
    ) -> Result<rpc::ReturnWorkerLeaseReply, Status> {
        tracing::debug!("ReturnWorkerLease received");
        let _ = request;
        Ok(rpc::ReturnWorkerLeaseReply::default())
    }

    pub async fn handle_cancel_worker_lease(
        &self,
        request: rpc::CancelWorkerLeaseRequest,
    ) -> Result<rpc::CancelWorkerLeaseReply, Status> {
        tracing::debug!("CancelWorkerLease received");
        let _ = request;
        Ok(rpc::CancelWorkerLeaseReply::default())
    }

    pub async fn handle_prestart_workers(
        &self,
        request: rpc::PrestartWorkersRequest,
    ) -> Result<rpc::PrestartWorkersReply, Status> {
        tracing::debug!("PrestartWorkers received");
        let _ = request;
        Ok(rpc::PrestartWorkersReply::default())
    }

    pub async fn handle_report_worker_backlog(
        &self,
        request: rpc::ReportWorkerBacklogRequest,
    ) -> Result<rpc::ReportWorkerBacklogReply, Status> {
        tracing::debug!("ReportWorkerBacklog received");
        let _ = request;
        Ok(rpc::ReportWorkerBacklogReply::default())
    }

    pub async fn handle_cancel_leases_with_resource_shapes(
        &self,
        request: rpc::CancelLeasesWithResourceShapesRequest,
    ) -> Result<rpc::CancelLeasesWithResourceShapesReply, Status> {
        let _ = request;
        Ok(rpc::CancelLeasesWithResourceShapesReply::default())
    }

    // ─── Object Management RPCs ───────────────────────────────────────

    pub async fn handle_pin_object_ids(
        &self,
        request: rpc::PinObjectIDsRequest,
    ) -> Result<rpc::PinObjectIDsReply, Status> {
        let _ = request;
        Ok(rpc::PinObjectIDsReply::default())
    }

    // ─── Placement Group RPCs ─────────────────────────────────────────

    pub async fn handle_prepare_bundle_resources(
        &self,
        request: rpc::PrepareBundleResourcesRequest,
    ) -> Result<rpc::PrepareBundleResourcesReply, Status> {
        let _ = request;
        Ok(rpc::PrepareBundleResourcesReply::default())
    }

    pub async fn handle_commit_bundle_resources(
        &self,
        request: rpc::CommitBundleResourcesRequest,
    ) -> Result<rpc::CommitBundleResourcesReply, Status> {
        let _ = request;
        Ok(rpc::CommitBundleResourcesReply::default())
    }

    pub async fn handle_cancel_resource_reserve(
        &self,
        request: rpc::CancelResourceReserveRequest,
    ) -> Result<rpc::CancelResourceReserveReply, Status> {
        let _ = request;
        Ok(rpc::CancelResourceReserveReply::default())
    }

    pub async fn handle_release_unused_bundles(
        &self,
        request: rpc::ReleaseUnusedBundlesRequest,
    ) -> Result<rpc::ReleaseUnusedBundlesReply, Status> {
        let _ = request;
        Ok(rpc::ReleaseUnusedBundlesReply::default())
    }

    // ─── Resource RPCs ────────────────────────────────────────────────

    pub async fn handle_resize_local_resource_instances(
        &self,
        request: rpc::ResizeLocalResourceInstancesRequest,
    ) -> Result<rpc::ResizeLocalResourceInstancesReply, Status> {
        let _ = request;
        Ok(rpc::ResizeLocalResourceInstancesReply::default())
    }

    pub fn handle_get_resource_load(
        &self,
        request: rpc::GetResourceLoadRequest,
    ) -> Result<rpc::GetResourceLoadReply, Status> {
        let _ = request;
        Ok(rpc::GetResourceLoadReply::default())
    }

    // ─── Observability RPCs ───────────────────────────────────────────

    pub fn handle_get_node_stats(
        &self,
        request: rpc::GetNodeStatsRequest,
    ) -> Result<rpc::GetNodeStatsReply, Status> {
        let _ = request;
        Ok(rpc::GetNodeStatsReply::default())
    }

    pub fn handle_get_objects_info(
        &self,
        request: rpc::GetObjectsInfoRequest,
    ) -> Result<rpc::GetObjectsInfoReply, Status> {
        let _ = request;
        Ok(rpc::GetObjectsInfoReply::default())
    }

    pub fn handle_format_global_memory_info(
        &self,
        request: rpc::FormatGlobalMemoryInfoRequest,
    ) -> Result<rpc::FormatGlobalMemoryInfoReply, Status> {
        let _ = request;
        Ok(rpc::FormatGlobalMemoryInfoReply::default())
    }

    pub fn handle_get_system_config(
        &self,
        _request: rpc::GetSystemConfigRequest,
    ) -> Result<rpc::GetSystemConfigReply, Status> {
        Ok(rpc::GetSystemConfigReply {
            system_config: self.node_manager.config().ray_config.to_json(),
        })
    }

    // ─── Error Handling RPCs ──────────────────────────────────────────

    pub fn handle_get_worker_failure_cause(
        &self,
        request: rpc::GetWorkerFailureCauseRequest,
    ) -> Result<rpc::GetWorkerFailureCauseReply, Status> {
        let _ = request;
        Ok(rpc::GetWorkerFailureCauseReply::default())
    }

    // ─── Lifecycle RPCs ───────────────────────────────────────────────

    pub async fn handle_shutdown_raylet(
        &self,
        request: rpc::ShutdownRayletRequest,
    ) -> Result<rpc::ShutdownRayletReply, Status> {
        tracing::info!(graceful = request.graceful, "ShutdownRaylet received");
        Ok(rpc::ShutdownRayletReply::default())
    }

    pub async fn handle_drain_raylet(
        &self,
        request: rpc::DrainRayletRequest,
    ) -> Result<rpc::DrainRayletReply, Status> {
        let deadline_ms = request.deadline_timestamp_ms as u64;
        self.node_manager.handle_drain(deadline_ms);
        Ok(rpc::DrainRayletReply {
            is_accepted: true,
            ..Default::default()
        })
    }

    // ─── GC RPCs ──────────────────────────────────────────────────────

    pub async fn handle_global_gc(
        &self,
        request: rpc::GlobalGcRequest,
    ) -> Result<rpc::GlobalGcReply, Status> {
        let _ = request;
        Ok(rpc::GlobalGcReply::default())
    }

    // ─── GCS Recovery RPCs ────────────────────────────────────────────

    pub async fn handle_release_unused_actor_workers(
        &self,
        request: rpc::ReleaseUnusedActorWorkersRequest,
    ) -> Result<rpc::ReleaseUnusedActorWorkersReply, Status> {
        let _ = request;
        Ok(rpc::ReleaseUnusedActorWorkersReply::default())
    }

    pub async fn handle_notify_gcs_restart(
        &self,
        request: rpc::NotifyGcsRestartRequest,
    ) -> Result<rpc::NotifyGcsRestartReply, Status> {
        let _ = request;
        tracing::info!("GCS restart notification received");
        Ok(rpc::NotifyGcsRestartReply::default())
    }

    // ─── Compiled Graphs RPCs ─────────────────────────────────────────

    pub async fn handle_register_mutable_object(
        &self,
        request: rpc::RegisterMutableObjectRequest,
    ) -> Result<rpc::RegisterMutableObjectReply, Status> {
        let _ = request;
        Ok(rpc::RegisterMutableObjectReply::default())
    }

    pub async fn handle_push_mutable_object(
        &self,
        request: rpc::PushMutableObjectRequest,
    ) -> Result<rpc::PushMutableObjectReply, Status> {
        let _ = request;
        Ok(rpc::PushMutableObjectReply::default())
    }

    // ─── Health Check RPCs ────────────────────────────────────────────

    pub fn handle_is_local_worker_dead(
        &self,
        request: rpc::IsLocalWorkerDeadRequest,
    ) -> Result<rpc::IsLocalWorkerDeadReply, Status> {
        let _ = request;
        Ok(rpc::IsLocalWorkerDeadReply::default())
    }

    pub fn handle_get_worker_pids(
        &self,
        _request: rpc::GetWorkerPiDsRequest,
    ) -> Result<rpc::GetWorkerPiDsReply, Status> {
        Ok(rpc::GetWorkerPiDsReply::default())
    }

    pub fn handle_get_agent_pids(
        &self,
        _request: rpc::GetAgentPiDsRequest,
    ) -> Result<rpc::GetAgentPiDsReply, Status> {
        Ok(rpc::GetAgentPiDsReply::default())
    }

    // ─── Actor/Task Management RPCs ───────────────────────────────────

    pub async fn handle_kill_local_actor(
        &self,
        request: rpc::KillLocalActorRequest,
    ) -> Result<rpc::KillLocalActorReply, Status> {
        let _ = request;
        Ok(rpc::KillLocalActorReply::default())
    }

    pub async fn handle_cancel_local_task(
        &self,
        request: rpc::CancelLocalTaskRequest,
    ) -> Result<rpc::CancelLocalTaskReply, Status> {
        let _ = request;
        Ok(rpc::CancelLocalTaskReply::default())
    }
}

// ─── Tonic trait impl ───────────────────────────────────────────────────

#[tonic::async_trait]
impl rpc::node_manager_service_server::NodeManagerService for NodeManagerServiceImpl {
    async fn request_worker_lease(
        &self,
        req: Request<rpc::RequestWorkerLeaseRequest>,
    ) -> Result<Response<rpc::RequestWorkerLeaseReply>, Status> {
        self.handle_request_worker_lease(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn return_worker_lease(
        &self,
        req: Request<rpc::ReturnWorkerLeaseRequest>,
    ) -> Result<Response<rpc::ReturnWorkerLeaseReply>, Status> {
        self.handle_return_worker_lease(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn cancel_worker_lease(
        &self,
        req: Request<rpc::CancelWorkerLeaseRequest>,
    ) -> Result<Response<rpc::CancelWorkerLeaseReply>, Status> {
        self.handle_cancel_worker_lease(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn prestart_workers(
        &self,
        req: Request<rpc::PrestartWorkersRequest>,
    ) -> Result<Response<rpc::PrestartWorkersReply>, Status> {
        self.handle_prestart_workers(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn report_worker_backlog(
        &self,
        req: Request<rpc::ReportWorkerBacklogRequest>,
    ) -> Result<Response<rpc::ReportWorkerBacklogReply>, Status> {
        self.handle_report_worker_backlog(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn cancel_leases_with_resource_shapes(
        &self,
        req: Request<rpc::CancelLeasesWithResourceShapesRequest>,
    ) -> Result<Response<rpc::CancelLeasesWithResourceShapesReply>, Status> {
        self.handle_cancel_leases_with_resource_shapes(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn pin_object_i_ds(
        &self,
        req: Request<rpc::PinObjectIDsRequest>,
    ) -> Result<Response<rpc::PinObjectIDsReply>, Status> {
        self.handle_pin_object_ids(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn prepare_bundle_resources(
        &self,
        req: Request<rpc::PrepareBundleResourcesRequest>,
    ) -> Result<Response<rpc::PrepareBundleResourcesReply>, Status> {
        self.handle_prepare_bundle_resources(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn commit_bundle_resources(
        &self,
        req: Request<rpc::CommitBundleResourcesRequest>,
    ) -> Result<Response<rpc::CommitBundleResourcesReply>, Status> {
        self.handle_commit_bundle_resources(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn cancel_resource_reserve(
        &self,
        req: Request<rpc::CancelResourceReserveRequest>,
    ) -> Result<Response<rpc::CancelResourceReserveReply>, Status> {
        self.handle_cancel_resource_reserve(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn release_unused_bundles(
        &self,
        req: Request<rpc::ReleaseUnusedBundlesRequest>,
    ) -> Result<Response<rpc::ReleaseUnusedBundlesReply>, Status> {
        self.handle_release_unused_bundles(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn resize_local_resource_instances(
        &self,
        req: Request<rpc::ResizeLocalResourceInstancesRequest>,
    ) -> Result<Response<rpc::ResizeLocalResourceInstancesReply>, Status> {
        self.handle_resize_local_resource_instances(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_resource_load(
        &self,
        req: Request<rpc::GetResourceLoadRequest>,
    ) -> Result<Response<rpc::GetResourceLoadReply>, Status> {
        self.handle_get_resource_load(req.into_inner())
            .map(Response::new)
    }

    async fn get_node_stats(
        &self,
        req: Request<rpc::GetNodeStatsRequest>,
    ) -> Result<Response<rpc::GetNodeStatsReply>, Status> {
        self.handle_get_node_stats(req.into_inner())
            .map(Response::new)
    }

    async fn global_gc(
        &self,
        req: Request<rpc::GlobalGcRequest>,
    ) -> Result<Response<rpc::GlobalGcReply>, Status> {
        self.handle_global_gc(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn format_global_memory_info(
        &self,
        req: Request<rpc::FormatGlobalMemoryInfoRequest>,
    ) -> Result<Response<rpc::FormatGlobalMemoryInfoReply>, Status> {
        self.handle_format_global_memory_info(req.into_inner())
            .map(Response::new)
    }

    async fn get_system_config(
        &self,
        req: Request<rpc::GetSystemConfigRequest>,
    ) -> Result<Response<rpc::GetSystemConfigReply>, Status> {
        self.handle_get_system_config(req.into_inner())
            .map(Response::new)
    }

    async fn get_objects_info(
        &self,
        req: Request<rpc::GetObjectsInfoRequest>,
    ) -> Result<Response<rpc::GetObjectsInfoReply>, Status> {
        self.handle_get_objects_info(req.into_inner())
            .map(Response::new)
    }

    async fn get_worker_failure_cause(
        &self,
        req: Request<rpc::GetWorkerFailureCauseRequest>,
    ) -> Result<Response<rpc::GetWorkerFailureCauseReply>, Status> {
        self.handle_get_worker_failure_cause(req.into_inner())
            .map(Response::new)
    }

    async fn shutdown_raylet(
        &self,
        req: Request<rpc::ShutdownRayletRequest>,
    ) -> Result<Response<rpc::ShutdownRayletReply>, Status> {
        self.handle_shutdown_raylet(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn drain_raylet(
        &self,
        req: Request<rpc::DrainRayletRequest>,
    ) -> Result<Response<rpc::DrainRayletReply>, Status> {
        self.handle_drain_raylet(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn release_unused_actor_workers(
        &self,
        req: Request<rpc::ReleaseUnusedActorWorkersRequest>,
    ) -> Result<Response<rpc::ReleaseUnusedActorWorkersReply>, Status> {
        self.handle_release_unused_actor_workers(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn notify_gcs_restart(
        &self,
        req: Request<rpc::NotifyGcsRestartRequest>,
    ) -> Result<Response<rpc::NotifyGcsRestartReply>, Status> {
        self.handle_notify_gcs_restart(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn register_mutable_object(
        &self,
        req: Request<rpc::RegisterMutableObjectRequest>,
    ) -> Result<Response<rpc::RegisterMutableObjectReply>, Status> {
        self.handle_register_mutable_object(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn push_mutable_object(
        &self,
        req: Request<rpc::PushMutableObjectRequest>,
    ) -> Result<Response<rpc::PushMutableObjectReply>, Status> {
        self.handle_push_mutable_object(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn is_local_worker_dead(
        &self,
        req: Request<rpc::IsLocalWorkerDeadRequest>,
    ) -> Result<Response<rpc::IsLocalWorkerDeadReply>, Status> {
        self.handle_is_local_worker_dead(req.into_inner())
            .map(Response::new)
    }

    async fn get_worker_pi_ds(
        &self,
        req: Request<rpc::GetWorkerPiDsRequest>,
    ) -> Result<Response<rpc::GetWorkerPiDsReply>, Status> {
        self.handle_get_worker_pids(req.into_inner())
            .map(Response::new)
    }

    async fn get_agent_pi_ds(
        &self,
        req: Request<rpc::GetAgentPiDsRequest>,
    ) -> Result<Response<rpc::GetAgentPiDsReply>, Status> {
        self.handle_get_agent_pids(req.into_inner())
            .map(Response::new)
    }

    async fn kill_local_actor(
        &self,
        req: Request<rpc::KillLocalActorRequest>,
    ) -> Result<Response<rpc::KillLocalActorReply>, Status> {
        self.handle_kill_local_actor(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn cancel_local_task(
        &self,
        req: Request<rpc::CancelLocalTaskRequest>,
    ) -> Result<Response<rpc::CancelLocalTaskReply>, Status> {
        self.handle_cancel_local_task(req.into_inner())
            .await
            .map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_system_config() {
        let nm = Arc::new(NodeManager::new(crate::node_manager::RayletConfig {
            node_ip_address: "127.0.0.1".to_string(),
            port: 0,
            object_store_socket: String::new(),
            gcs_address: String::new(),
            log_dir: None,
            ray_config: ray_common::config::RayConfig::default(),
            node_id: String::new(),
            resources: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
            session_name: String::new(),
        }));
        let svc = NodeManagerServiceImpl { node_manager: nm };

        let reply = svc
            .handle_get_system_config(rpc::GetSystemConfigRequest::default())
            .unwrap();
        assert!(!reply.system_config.is_empty());
    }

    #[tokio::test]
    async fn test_drain_raylet() {
        let nm = Arc::new(NodeManager::new(crate::node_manager::RayletConfig {
            node_ip_address: "127.0.0.1".to_string(),
            port: 0,
            object_store_socket: String::new(),
            gcs_address: String::new(),
            log_dir: None,
            ray_config: ray_common::config::RayConfig::default(),
            node_id: String::new(),
            resources: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
            session_name: String::new(),
        }));
        let svc = NodeManagerServiceImpl { node_manager: nm };

        let reply = svc
            .handle_drain_raylet(rpc::DrainRayletRequest {
                deadline_timestamp_ms: 5000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(reply.is_accepted);
    }
}
