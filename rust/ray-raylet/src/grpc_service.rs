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

use tonic::Status;

use crate::node_manager::NodeManager;

/// The gRPC service implementation wrapping the NodeManager.
pub struct NodeManagerServiceImpl {
    pub node_manager: Arc<NodeManager>,
}

// ─── Handler methods (not yet wired to tonic trait) ─────────────────────
//
// Each method corresponds to one of the 30 NodeManagerService RPCs.
// They will be wired to the tonic-generated trait once the NodeManager
// has enough components to handle real requests.

impl NodeManagerServiceImpl {
    // ─── Scheduling RPCs ──────────────────────────────────────────────

    pub async fn handle_request_worker_lease(
        &self,
        request: ray_proto::ray::rpc::RequestWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::RequestWorkerLeaseReply, Status> {
        // In production, this delegates to ClusterLeaseManager
        tracing::debug!("RequestWorkerLease received");
        let _ = request;
        Ok(ray_proto::ray::rpc::RequestWorkerLeaseReply::default())
    }

    pub async fn handle_return_worker_lease(
        &self,
        request: ray_proto::ray::rpc::ReturnWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::ReturnWorkerLeaseReply, Status> {
        tracing::debug!("ReturnWorkerLease received");
        let _ = request;
        Ok(ray_proto::ray::rpc::ReturnWorkerLeaseReply::default())
    }

    pub async fn handle_cancel_worker_lease(
        &self,
        request: ray_proto::ray::rpc::CancelWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::CancelWorkerLeaseReply, Status> {
        tracing::debug!("CancelWorkerLease received");
        let _ = request;
        Ok(ray_proto::ray::rpc::CancelWorkerLeaseReply::default())
    }

    pub async fn handle_prestart_workers(
        &self,
        request: ray_proto::ray::rpc::PrestartWorkersRequest,
    ) -> Result<ray_proto::ray::rpc::PrestartWorkersReply, Status> {
        tracing::debug!("PrestartWorkers received");
        let _ = request;
        Ok(ray_proto::ray::rpc::PrestartWorkersReply::default())
    }

    pub async fn handle_report_worker_backlog(
        &self,
        request: ray_proto::ray::rpc::ReportWorkerBacklogRequest,
    ) -> Result<ray_proto::ray::rpc::ReportWorkerBacklogReply, Status> {
        tracing::debug!("ReportWorkerBacklog received");
        let _ = request;
        Ok(ray_proto::ray::rpc::ReportWorkerBacklogReply::default())
    }

    pub async fn handle_cancel_leases_with_resource_shapes(
        &self,
        request: ray_proto::ray::rpc::CancelLeasesWithResourceShapesRequest,
    ) -> Result<ray_proto::ray::rpc::CancelLeasesWithResourceShapesReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::CancelLeasesWithResourceShapesReply::default())
    }

    // ─── Object Management RPCs ───────────────────────────────────────

    pub async fn handle_pin_object_ids(
        &self,
        request: ray_proto::ray::rpc::PinObjectIDsRequest,
    ) -> Result<ray_proto::ray::rpc::PinObjectIDsReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::PinObjectIDsReply::default())
    }

    // ─── Placement Group RPCs ─────────────────────────────────────────

    pub async fn handle_prepare_bundle_resources(
        &self,
        request: ray_proto::ray::rpc::PrepareBundleResourcesRequest,
    ) -> Result<ray_proto::ray::rpc::PrepareBundleResourcesReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::PrepareBundleResourcesReply::default())
    }

    pub async fn handle_commit_bundle_resources(
        &self,
        request: ray_proto::ray::rpc::CommitBundleResourcesRequest,
    ) -> Result<ray_proto::ray::rpc::CommitBundleResourcesReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::CommitBundleResourcesReply::default())
    }

    pub async fn handle_cancel_resource_reserve(
        &self,
        request: ray_proto::ray::rpc::CancelResourceReserveRequest,
    ) -> Result<ray_proto::ray::rpc::CancelResourceReserveReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::CancelResourceReserveReply::default())
    }

    pub async fn handle_release_unused_bundles(
        &self,
        request: ray_proto::ray::rpc::ReleaseUnusedBundlesRequest,
    ) -> Result<ray_proto::ray::rpc::ReleaseUnusedBundlesReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::ReleaseUnusedBundlesReply::default())
    }

    // ─── Resource RPCs ────────────────────────────────────────────────

    pub async fn handle_resize_local_resource_instances(
        &self,
        request: ray_proto::ray::rpc::ResizeLocalResourceInstancesRequest,
    ) -> Result<ray_proto::ray::rpc::ResizeLocalResourceInstancesReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::ResizeLocalResourceInstancesReply::default())
    }

    pub fn handle_get_resource_load(
        &self,
        request: ray_proto::ray::rpc::GetResourceLoadRequest,
    ) -> Result<ray_proto::ray::rpc::GetResourceLoadReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::GetResourceLoadReply::default())
    }

    // ─── Observability RPCs ───────────────────────────────────────────

    pub fn handle_get_node_stats(
        &self,
        request: ray_proto::ray::rpc::GetNodeStatsRequest,
    ) -> Result<ray_proto::ray::rpc::GetNodeStatsReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::GetNodeStatsReply::default())
    }

    pub fn handle_get_objects_info(
        &self,
        request: ray_proto::ray::rpc::GetObjectsInfoRequest,
    ) -> Result<ray_proto::ray::rpc::GetObjectsInfoReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::GetObjectsInfoReply::default())
    }

    pub fn handle_format_global_memory_info(
        &self,
        request: ray_proto::ray::rpc::FormatGlobalMemoryInfoRequest,
    ) -> Result<ray_proto::ray::rpc::FormatGlobalMemoryInfoReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::FormatGlobalMemoryInfoReply::default())
    }

    pub fn handle_get_system_config(
        &self,
        _request: ray_proto::ray::rpc::GetSystemConfigRequest,
    ) -> Result<ray_proto::ray::rpc::GetSystemConfigReply, Status> {
        Ok(ray_proto::ray::rpc::GetSystemConfigReply {
            system_config: self.node_manager.config().ray_config.to_json(),
        })
    }

    // ─── Error Handling RPCs ──────────────────────────────────────────

    pub fn handle_get_worker_failure_cause(
        &self,
        request: ray_proto::ray::rpc::GetWorkerFailureCauseRequest,
    ) -> Result<ray_proto::ray::rpc::GetWorkerFailureCauseReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::GetWorkerFailureCauseReply::default())
    }

    // ─── Lifecycle RPCs ───────────────────────────────────────────────

    pub async fn handle_shutdown_raylet(
        &self,
        request: ray_proto::ray::rpc::ShutdownRayletRequest,
    ) -> Result<ray_proto::ray::rpc::ShutdownRayletReply, Status> {
        tracing::info!(graceful = request.graceful, "ShutdownRaylet received");
        Ok(ray_proto::ray::rpc::ShutdownRayletReply::default())
    }

    pub async fn handle_drain_raylet(
        &self,
        request: ray_proto::ray::rpc::DrainRayletRequest,
    ) -> Result<ray_proto::ray::rpc::DrainRayletReply, Status> {
        let deadline_ms = request.deadline_timestamp_ms as u64;
        self.node_manager.handle_drain(deadline_ms);
        Ok(ray_proto::ray::rpc::DrainRayletReply {
            is_accepted: true,
            ..Default::default()
        })
    }

    // ─── GC RPCs ──────────────────────────────────────────────────────

    pub async fn handle_global_gc(
        &self,
        request: ray_proto::ray::rpc::GlobalGcRequest,
    ) -> Result<ray_proto::ray::rpc::GlobalGcReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::GlobalGcReply::default())
    }

    // ─── GCS Recovery RPCs ────────────────────────────────────────────

    pub async fn handle_release_unused_actor_workers(
        &self,
        request: ray_proto::ray::rpc::ReleaseUnusedActorWorkersRequest,
    ) -> Result<ray_proto::ray::rpc::ReleaseUnusedActorWorkersReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::ReleaseUnusedActorWorkersReply::default())
    }

    pub async fn handle_notify_gcs_restart(
        &self,
        request: ray_proto::ray::rpc::NotifyGcsRestartRequest,
    ) -> Result<ray_proto::ray::rpc::NotifyGcsRestartReply, Status> {
        let _ = request;
        tracing::info!("GCS restart notification received");
        Ok(ray_proto::ray::rpc::NotifyGcsRestartReply::default())
    }

    // ─── Compiled Graphs RPCs ─────────────────────────────────────────

    pub async fn handle_register_mutable_object(
        &self,
        request: ray_proto::ray::rpc::RegisterMutableObjectRequest,
    ) -> Result<ray_proto::ray::rpc::RegisterMutableObjectReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::RegisterMutableObjectReply::default())
    }

    pub async fn handle_push_mutable_object(
        &self,
        request: ray_proto::ray::rpc::PushMutableObjectRequest,
    ) -> Result<ray_proto::ray::rpc::PushMutableObjectReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::PushMutableObjectReply::default())
    }

    // ─── Health Check RPCs ────────────────────────────────────────────

    pub fn handle_is_local_worker_dead(
        &self,
        request: ray_proto::ray::rpc::IsLocalWorkerDeadRequest,
    ) -> Result<ray_proto::ray::rpc::IsLocalWorkerDeadReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::IsLocalWorkerDeadReply::default())
    }

    pub fn handle_get_worker_pids(
        &self,
        _request: ray_proto::ray::rpc::GetWorkerPiDsRequest,
    ) -> Result<ray_proto::ray::rpc::GetWorkerPiDsReply, Status> {
        Ok(ray_proto::ray::rpc::GetWorkerPiDsReply::default())
    }

    pub fn handle_get_agent_pids(
        &self,
        _request: ray_proto::ray::rpc::GetAgentPiDsRequest,
    ) -> Result<ray_proto::ray::rpc::GetAgentPiDsReply, Status> {
        Ok(ray_proto::ray::rpc::GetAgentPiDsReply::default())
    }

    // ─── Actor/Task Management RPCs ───────────────────────────────────

    pub async fn handle_kill_local_actor(
        &self,
        request: ray_proto::ray::rpc::KillLocalActorRequest,
    ) -> Result<ray_proto::ray::rpc::KillLocalActorReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::KillLocalActorReply::default())
    }

    pub async fn handle_cancel_local_task(
        &self,
        request: ray_proto::ray::rpc::CancelLocalTaskRequest,
    ) -> Result<ray_proto::ray::rpc::CancelLocalTaskReply, Status> {
        let _ = request;
        Ok(ray_proto::ray::rpc::CancelLocalTaskReply::default())
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
            .handle_get_system_config(ray_proto::ray::rpc::GetSystemConfigRequest::default())
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
            .handle_drain_raylet(ray_proto::ray::rpc::DrainRayletRequest {
                deadline_timestamp_ms: 5000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(reply.is_accepted);
    }
}
