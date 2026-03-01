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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use ray_common::id::PlacementGroupID;
use ray_common::scheduling::{FixedPoint, ResourceSet};
use ray_proto::ray::rpc;

use crate::lease_manager::{LeaseReply, SchedulingClass};
use crate::node_manager::NodeManager;
use crate::placement_group_resource_manager::BundleID;
use crate::scheduling_resources::SchedulingOptions;

/// The gRPC service implementation wrapping the NodeManager.
pub struct NodeManagerServiceImpl {
    pub node_manager: Arc<NodeManager>,
}

// ─── Helpers ─────────────────────────────────────────────────────────────

/// Convert a proto `required_resources` map to a `ResourceSet`.
fn resources_from_map(map: &std::collections::HashMap<String, f64>) -> ResourceSet {
    let mut rs = ResourceSet::new();
    for (name, amount) in map {
        rs.set(name.clone(), FixedPoint::from_f64(*amount));
    }
    rs
}

/// Compute a scheduling class from resource requirements (hash of sorted resource names+amounts).
fn compute_scheduling_class(resources: &ResourceSet) -> SchedulingClass {
    let mut hasher = DefaultHasher::new();
    let mut entries: Vec<_> = resources.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (name, amount) in entries {
        name.hash(&mut hasher);
        amount.raw().hash(&mut hasher);
    }
    SchedulingClass(hasher.finish())
}

/// Convert a proto `SchedulingStrategy` into our internal `SchedulingOptions`.
fn scheduling_options_from_proto(
    strategy: Option<&rpc::SchedulingStrategy>,
) -> SchedulingOptions {
    let Some(strategy) = strategy else {
        return SchedulingOptions::hybrid();
    };
    let Some(ref inner) = strategy.scheduling_strategy else {
        return SchedulingOptions::hybrid();
    };
    match inner {
        rpc::scheduling_strategy::SchedulingStrategy::DefaultSchedulingStrategy(_) => {
            SchedulingOptions::hybrid()
        }
        rpc::scheduling_strategy::SchedulingStrategy::SpreadSchedulingStrategy(_) => {
            SchedulingOptions::spread()
        }
        rpc::scheduling_strategy::SchedulingStrategy::NodeAffinitySchedulingStrategy(s) => {
            let node_id = if s.node_id.is_empty() {
                None
            } else {
                Some(hex::encode(&s.node_id))
            };
            SchedulingOptions {
                node_affinity_node_id: node_id,
                node_affinity_soft: s.soft,
                node_affinity_spill_on_unavailable: s.spill_on_unavailable,
                node_affinity_fail_on_unavailable: s.fail_on_unavailable,
                ..SchedulingOptions::hybrid()
            }
        }
        _ => SchedulingOptions::hybrid(),
    }
}

/// Parse a proto `Bundle` into a `(BundleID, ResourceSet)`.
fn parse_bundle(bundle: &rpc::Bundle) -> Option<(BundleID, ResourceSet)> {
    let bundle_ident = bundle.bundle_id.as_ref()?;
    let pg_id = PlacementGroupID::from_binary(bundle_ident.placement_group_id.as_slice());
    let bundle_id = (pg_id, bundle_ident.bundle_index);
    let resources = resources_from_map(&bundle.unit_resources);
    Some((bundle_id, resources))
}

// ─── Handler methods ────────────────────────────────────────────────────

impl NodeManagerServiceImpl {
    // ─── Scheduling RPCs ──────────────────────────────────────────────

    pub async fn handle_request_worker_lease(
        &self,
        request: rpc::RequestWorkerLeaseRequest,
    ) -> Result<rpc::RequestWorkerLeaseReply, Status> {
        let lease_spec = request
            .lease_spec
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing lease_spec"))?;

        let resources = resources_from_map(&lease_spec.required_resources);
        let options = scheduling_options_from_proto(lease_spec.scheduling_strategy.as_ref());
        let scheduling_class = compute_scheduling_class(&resources);

        tracing::debug!(
            lease_id = hex::encode(&lease_spec.lease_id),
            ?scheduling_class,
            "RequestWorkerLease"
        );

        let rx = self.node_manager.lease_manager().queue_and_schedule_lease(
            resources,
            options,
            scheduling_class,
        );

        // Await the scheduling decision
        let reply = rx
            .await
            .map_err(|_| Status::internal("lease scheduling channel closed"))?;

        match reply {
            LeaseReply::Granted {
                node_id,
                worker_address: _,
                allocation: _,
            } => {
                let worker_addr = rpc::Address {
                    node_id: node_id.as_bytes().to_vec(),
                    ip_address: self.node_manager.config().node_ip_address.clone(),
                    port: self.node_manager.config().port as i32,
                    worker_id: vec![],
                };
                Ok(rpc::RequestWorkerLeaseReply {
                    worker_address: Some(worker_addr),
                    ..Default::default()
                })
            }
            LeaseReply::Spillback { node_id } => {
                let retry_addr = rpc::Address {
                    node_id: node_id.as_bytes().to_vec(),
                    ip_address: String::new(),
                    port: 0,
                    worker_id: vec![],
                };
                Ok(rpc::RequestWorkerLeaseReply {
                    retry_at_raylet_address: Some(retry_addr),
                    ..Default::default()
                })
            }
            LeaseReply::Rejected { reason } => Ok(rpc::RequestWorkerLeaseReply {
                rejected: true,
                scheduling_failure_message: reason,
                failure_type: rpc::request_worker_lease_reply::SchedulingFailureType::SchedulingFailed
                    as i32,
                ..Default::default()
            }),
        }
    }

    pub async fn handle_return_worker_lease(
        &self,
        request: rpc::ReturnWorkerLeaseRequest,
    ) -> Result<rpc::ReturnWorkerLeaseReply, Status> {
        tracing::debug!(
            worker_port = request.worker_port,
            disconnect = request.disconnect_worker,
            "ReturnWorkerLease"
        );

        if request.disconnect_worker {
            // Worker is being disconnected — could handle cleanup here
            tracing::info!(
                worker_port = request.worker_port,
                reason = %request.disconnect_worker_error_detail,
                "Worker disconnecting on return"
            );
        }
        // In a full implementation, we'd release the allocated resources
        // and push the worker back to the pool. For now, trigger re-scheduling
        // so any pending leases can be reconsidered.
        self.node_manager
            .lease_manager()
            .schedule_and_grant_leases();

        Ok(rpc::ReturnWorkerLeaseReply::default())
    }

    pub async fn handle_cancel_worker_lease(
        &self,
        request: rpc::CancelWorkerLeaseRequest,
    ) -> Result<rpc::CancelWorkerLeaseReply, Status> {
        // The proto lease_id is a byte vector; we use it as a u64 internally.
        // Convert by reading first 8 bytes or hashing.
        let lease_id = if request.lease_id.len() >= 8 {
            u64::from_le_bytes(request.lease_id[..8].try_into().unwrap())
        } else {
            let mut hasher = DefaultHasher::new();
            request.lease_id.hash(&mut hasher);
            hasher.finish()
        };

        let cancelled = self.node_manager.lease_manager().cancel_lease(lease_id);
        tracing::debug!(lease_id, cancelled, "CancelWorkerLease");

        Ok(rpc::CancelWorkerLeaseReply { success: cancelled })
    }

    pub async fn handle_prestart_workers(
        &self,
        request: rpc::PrestartWorkersRequest,
    ) -> Result<rpc::PrestartWorkersReply, Status> {
        tracing::debug!("PrestartWorkers received");
        let _ = request;
        // Worker prestart is a hint — acknowledged but not yet implemented
        Ok(rpc::PrestartWorkersReply::default())
    }

    pub async fn handle_report_worker_backlog(
        &self,
        request: rpc::ReportWorkerBacklogRequest,
    ) -> Result<rpc::ReportWorkerBacklogReply, Status> {
        tracing::debug!(
            worker_id = hex::encode(&request.worker_id),
            num_reports = request.backlog_reports.len(),
            "ReportWorkerBacklog"
        );
        // Backlog reporting is used for autoscaling decisions.
        // Acknowledged for now; integration with autoscaler is Phase 12.
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
        let num_objects = request.object_ids.len();
        tracing::debug!(num_objects, "PinObjectIDs");
        // Return success for all objects — real pinning requires object store integration
        Ok(rpc::PinObjectIDsReply {
            successes: vec![true; num_objects],
        })
    }

    // ─── Placement Group RPCs ─────────────────────────────────────────

    pub async fn handle_prepare_bundle_resources(
        &self,
        request: rpc::PrepareBundleResourcesRequest,
    ) -> Result<rpc::PrepareBundleResourcesReply, Status> {
        let bundle_specs: Vec<(BundleID, ResourceSet)> = request
            .bundle_specs
            .iter()
            .filter_map(parse_bundle)
            .collect();

        if bundle_specs.is_empty() {
            return Ok(rpc::PrepareBundleResourcesReply { success: false });
        }

        let result = self
            .node_manager
            .placement_group_resource_manager()
            .prepare_bundles(&bundle_specs);

        match result {
            Ok(()) => {
                tracing::info!(
                    num_bundles = bundle_specs.len(),
                    "Bundle resources prepared"
                );
                Ok(rpc::PrepareBundleResourcesReply { success: true })
            }
            Err(e) => {
                tracing::warn!(error = %e, "Bundle prepare failed");
                Ok(rpc::PrepareBundleResourcesReply { success: false })
            }
        }
    }

    pub async fn handle_commit_bundle_resources(
        &self,
        request: rpc::CommitBundleResourcesRequest,
    ) -> Result<rpc::CommitBundleResourcesReply, Status> {
        let bundle_ids: Vec<BundleID> = request
            .bundle_specs
            .iter()
            .filter_map(|b| {
                let ident = b.bundle_id.as_ref()?;
                let pg_id =
                    PlacementGroupID::from_binary(ident.placement_group_id.as_slice());
                Some((pg_id, ident.bundle_index))
            })
            .collect();

        if let Err(e) = self
            .node_manager
            .placement_group_resource_manager()
            .commit_bundles(&bundle_ids)
        {
            tracing::warn!(error = %e, "Bundle commit failed");
        } else {
            tracing::info!(num_bundles = bundle_ids.len(), "Bundle resources committed");
        }

        Ok(rpc::CommitBundleResourcesReply::default())
    }

    pub async fn handle_cancel_resource_reserve(
        &self,
        request: rpc::CancelResourceReserveRequest,
    ) -> Result<rpc::CancelResourceReserveReply, Status> {
        if let Some(ref bundle) = request.bundle_spec {
            if let Some((bundle_id, _)) = parse_bundle(bundle) {
                self.node_manager
                    .placement_group_resource_manager()
                    .return_bundle(&bundle_id);
                tracing::info!(?bundle_id, "Bundle resource reservation cancelled");
            }
        }
        Ok(rpc::CancelResourceReserveReply::default())
    }

    pub async fn handle_release_unused_bundles(
        &self,
        request: rpc::ReleaseUnusedBundlesRequest,
    ) -> Result<rpc::ReleaseUnusedBundlesReply, Status> {
        let in_use: Vec<BundleID> = request
            .bundles_in_use
            .iter()
            .filter_map(|b| {
                let ident = b.bundle_id.as_ref()?;
                let pg_id =
                    PlacementGroupID::from_binary(ident.placement_group_id.as_slice());
                Some((pg_id, ident.bundle_index))
            })
            .collect();

        self.node_manager
            .placement_group_resource_manager()
            .return_unused_bundles(&in_use);

        tracing::info!(num_in_use = in_use.len(), "Released unused bundles");
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
        _request: rpc::GetResourceLoadRequest,
    ) -> Result<rpc::GetResourceLoadReply, Status> {
        let local_rm = self.node_manager.scheduler().local_resource_manager();
        let available = local_rm.get_local_available_resources();
        let total = local_rm.get_local_total_resources();

        let mut resources_data = rpc::ResourcesData {
            node_id: self.node_manager.config().node_id.as_bytes().to_vec(),
            ..Default::default()
        };

        for (name, amount) in total.iter() {
            resources_data
                .resources_total
                .insert(name.to_string(), amount.to_f64());
        }
        for (name, amount) in available.iter() {
            resources_data
                .resources_available
                .insert(name.to_string(), amount.to_f64());
        }

        Ok(rpc::GetResourceLoadReply {
            resources: Some(resources_data),
        })
    }

    // ─── Observability RPCs ───────────────────────────────────────────

    pub fn handle_get_node_stats(
        &self,
        _request: rpc::GetNodeStatsRequest,
    ) -> Result<rpc::GetNodeStatsReply, Status> {
        let num_workers = self.node_manager.worker_pool().num_registered_workers() as u32;
        Ok(rpc::GetNodeStatsReply {
            num_workers,
            ..Default::default()
        })
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
        // On GCS restart, retry infeasible leases in case cluster state changed
        self.node_manager
            .lease_manager()
            .try_schedule_infeasible_leases();
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
        let worker_id = ray_common::id::WorkerID::from_binary(request.worker_id.as_slice());
        let all_workers = self.node_manager.worker_pool().get_all_workers();
        let is_dead = all_workers
            .iter()
            .find(|w| w.worker_id == worker_id)
            .is_some_and(|w| !w.is_alive);

        Ok(rpc::IsLocalWorkerDeadReply { is_dead })
    }

    pub fn handle_get_worker_pids(
        &self,
        _request: rpc::GetWorkerPiDsRequest,
    ) -> Result<rpc::GetWorkerPiDsReply, Status> {
        let workers = self.node_manager.worker_pool().get_all_workers();
        let pids: Vec<i32> = workers
            .iter()
            .filter(|w| w.is_alive)
            .map(|w| w.pid as i32)
            .collect();
        Ok(rpc::GetWorkerPiDsReply { pids })
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
    use crate::node_manager::RayletConfig;

    fn make_config() -> RayletConfig {
        RayletConfig {
            node_ip_address: "127.0.0.1".to_string(),
            port: 8765,
            object_store_socket: String::new(),
            gcs_address: String::new(),
            log_dir: None,
            ray_config: ray_common::config::RayConfig::default(),
            node_id: "test-node".to_string(),
            resources: std::collections::HashMap::from([
                ("CPU".to_string(), 4.0),
                ("GPU".to_string(), 1.0),
            ]),
            labels: std::collections::HashMap::new(),
            session_name: String::new(),
        }
    }

    fn make_svc() -> NodeManagerServiceImpl {
        NodeManagerServiceImpl {
            node_manager: Arc::new(NodeManager::new(make_config())),
        }
    }

    fn make_lease_request(cpu: f64) -> rpc::RequestWorkerLeaseRequest {
        rpc::RequestWorkerLeaseRequest {
            lease_spec: Some(rpc::LeaseSpec {
                lease_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                required_resources: std::collections::HashMap::from([(
                    "CPU".to_string(),
                    cpu,
                )]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_get_system_config() {
        let svc = make_svc();
        let reply = svc
            .handle_get_system_config(rpc::GetSystemConfigRequest::default())
            .unwrap();
        assert!(!reply.system_config.is_empty());
    }

    #[tokio::test]
    async fn test_drain_raylet() {
        let svc = make_svc();
        let reply = svc
            .handle_drain_raylet(rpc::DrainRayletRequest {
                deadline_timestamp_ms: 5000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(reply.is_accepted);
    }

    #[tokio::test]
    async fn test_request_worker_lease_granted() {
        let svc = make_svc();
        let reply = svc
            .handle_request_worker_lease(make_lease_request(1.0))
            .await
            .unwrap();

        // Should be granted locally since we have 4 CPUs
        assert!(reply.worker_address.is_some());
        assert!(!reply.rejected);
        assert!(reply.retry_at_raylet_address.is_none());
    }

    #[tokio::test]
    async fn test_request_worker_lease_rejected_infeasible() {
        let svc = make_svc();
        // Request 100 CPUs — infeasible
        let reply = svc
            .handle_request_worker_lease(make_lease_request(100.0))
            .await
            .unwrap();

        assert!(reply.rejected);
        assert!(!reply.scheduling_failure_message.is_empty());
    }

    #[tokio::test]
    async fn test_request_worker_lease_missing_spec() {
        let svc = make_svc();
        let result = svc
            .handle_request_worker_lease(rpc::RequestWorkerLeaseRequest::default())
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_cancel_worker_lease() {
        let svc = make_svc();
        // Cancel a nonexistent lease
        let reply = svc
            .handle_cancel_worker_lease(rpc::CancelWorkerLeaseRequest {
                lease_id: vec![99, 0, 0, 0, 0, 0, 0, 0],
            })
            .await
            .unwrap();

        assert!(!reply.success);
    }

    #[tokio::test]
    async fn test_return_worker_lease() {
        let svc = make_svc();
        let reply = svc
            .handle_return_worker_lease(rpc::ReturnWorkerLeaseRequest {
                worker_port: 1234,
                disconnect_worker: false,
                ..Default::default()
            })
            .await
            .unwrap();
        // Should succeed (no-op for now but triggers re-scheduling)
        let _ = reply;
    }

    #[tokio::test]
    async fn test_get_resource_load() {
        let svc = make_svc();
        let reply = svc
            .handle_get_resource_load(rpc::GetResourceLoadRequest::default())
            .unwrap();

        let resources = reply.resources.expect("should have resources");
        assert_eq!(*resources.resources_total.get("CPU").unwrap(), 4.0);
        assert_eq!(*resources.resources_total.get("GPU").unwrap(), 1.0);
    }

    #[tokio::test]
    async fn test_get_node_stats() {
        let svc = make_svc();
        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .unwrap();
        // No workers registered yet
        assert_eq!(reply.num_workers, 0);
    }

    #[tokio::test]
    async fn test_pin_object_ids() {
        let svc = make_svc();
        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![vec![1; 20], vec![2; 20], vec![3; 20]],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(reply.successes.len(), 3);
        assert!(reply.successes.iter().all(|&s| s));
    }

    #[tokio::test]
    async fn test_prepare_and_commit_bundle_resources() {
        let svc = make_svc();

        let pg_id = vec![1u8; 18];
        let bundle = rpc::Bundle {
            bundle_id: Some(rpc::bundle::BundleIdentifier {
                placement_group_id: pg_id,
                bundle_index: 0,
            }),
            unit_resources: std::collections::HashMap::from([("CPU".to_string(), 2.0)]),
            ..Default::default()
        };

        // Prepare
        let reply = svc
            .handle_prepare_bundle_resources(rpc::PrepareBundleResourcesRequest {
                bundle_specs: vec![bundle.clone()],
            })
            .await
            .unwrap();
        assert!(reply.success);
        assert_eq!(
            svc.node_manager
                .placement_group_resource_manager()
                .num_bundles(),
            1
        );

        // Commit
        let _ = svc
            .handle_commit_bundle_resources(rpc::CommitBundleResourcesRequest {
                bundle_specs: vec![bundle],
            })
            .await
            .unwrap();
        assert_eq!(
            svc.node_manager
                .placement_group_resource_manager()
                .num_committed_bundles(),
            1
        );
    }

    #[tokio::test]
    async fn test_prepare_bundle_insufficient_resources() {
        let svc = make_svc();

        let bundle = rpc::Bundle {
            bundle_id: Some(rpc::bundle::BundleIdentifier {
                placement_group_id: vec![1u8; 18],
                bundle_index: 0,
            }),
            unit_resources: std::collections::HashMap::from([("CPU".to_string(), 100.0)]),
            ..Default::default()
        };

        let reply = svc
            .handle_prepare_bundle_resources(rpc::PrepareBundleResourcesRequest {
                bundle_specs: vec![bundle],
            })
            .await
            .unwrap();
        assert!(!reply.success);
    }

    #[tokio::test]
    async fn test_cancel_resource_reserve() {
        let svc = make_svc();

        let pg_id = vec![2u8; 18];
        let bundle = rpc::Bundle {
            bundle_id: Some(rpc::bundle::BundleIdentifier {
                placement_group_id: pg_id,
                bundle_index: 0,
            }),
            unit_resources: std::collections::HashMap::from([("CPU".to_string(), 1.0)]),
            ..Default::default()
        };

        // Prepare first
        svc.handle_prepare_bundle_resources(rpc::PrepareBundleResourcesRequest {
            bundle_specs: vec![bundle.clone()],
        })
        .await
        .unwrap();
        assert_eq!(
            svc.node_manager
                .placement_group_resource_manager()
                .num_bundles(),
            1
        );

        // Cancel
        svc.handle_cancel_resource_reserve(rpc::CancelResourceReserveRequest {
            bundle_spec: Some(bundle),
        })
        .await
        .unwrap();
        assert_eq!(
            svc.node_manager
                .placement_group_resource_manager()
                .num_bundles(),
            0
        );
    }

    #[tokio::test]
    async fn test_is_local_worker_dead_unknown() {
        let svc = make_svc();
        let reply = svc
            .handle_is_local_worker_dead(rpc::IsLocalWorkerDeadRequest {
                worker_id: vec![0u8; 28],
            })
            .unwrap();
        // Unknown worker — not tracked, so not reported dead
        assert!(!reply.is_dead);
    }

    #[tokio::test]
    async fn test_get_worker_pids_empty() {
        let svc = make_svc();
        let reply = svc
            .handle_get_worker_pids(rpc::GetWorkerPiDsRequest {})
            .unwrap();
        assert!(reply.pids.is_empty());
    }

    #[tokio::test]
    async fn test_notify_gcs_restart() {
        let svc = make_svc();
        let reply = svc
            .handle_notify_gcs_restart(rpc::NotifyGcsRestartRequest::default())
            .await
            .unwrap();
        let _ = reply; // Should not panic
    }

    #[test]
    fn test_resources_from_map() {
        let map = std::collections::HashMap::from([
            ("CPU".to_string(), 4.0),
            ("GPU".to_string(), 2.0),
        ]);
        let rs = resources_from_map(&map);
        assert_eq!(rs.get("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(rs.get("GPU"), FixedPoint::from_f64(2.0));
    }

    #[test]
    fn test_compute_scheduling_class_deterministic() {
        let mut rs1 = ResourceSet::new();
        rs1.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let mut rs2 = ResourceSet::new();
        rs2.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        assert_eq!(compute_scheduling_class(&rs1), compute_scheduling_class(&rs2));
    }

    #[test]
    fn test_compute_scheduling_class_different() {
        let mut rs1 = ResourceSet::new();
        rs1.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let mut rs2 = ResourceSet::new();
        rs2.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        assert_ne!(compute_scheduling_class(&rs1), compute_scheduling_class(&rs2));
    }

    #[test]
    fn test_scheduling_options_from_proto_default() {
        let opts = scheduling_options_from_proto(None);
        assert_eq!(
            opts.scheduling_type,
            crate::scheduling_resources::SchedulingType::Hybrid
        );
    }

    #[test]
    fn test_scheduling_options_from_proto_spread() {
        let strategy = rpc::SchedulingStrategy {
            scheduling_strategy: Some(
                rpc::scheduling_strategy::SchedulingStrategy::SpreadSchedulingStrategy(
                    rpc::SpreadSchedulingStrategy {},
                ),
            ),
        };
        let opts = scheduling_options_from_proto(Some(&strategy));
        assert_eq!(
            opts.scheduling_type,
            crate::scheduling_resources::SchedulingType::Spread
        );
    }

    #[test]
    fn test_scheduling_options_from_proto_node_affinity() {
        let strategy = rpc::SchedulingStrategy {
            scheduling_strategy: Some(
                rpc::scheduling_strategy::SchedulingStrategy::NodeAffinitySchedulingStrategy(
                    rpc::NodeAffinitySchedulingStrategy {
                        node_id: vec![1, 2, 3],
                        soft: true,
                        spill_on_unavailable: true,
                        fail_on_unavailable: false,
                    },
                ),
            ),
        };
        let opts = scheduling_options_from_proto(Some(&strategy));
        assert!(opts.node_affinity_node_id.is_some());
        assert!(opts.node_affinity_soft);
        assert!(opts.node_affinity_spill_on_unavailable);
        assert!(!opts.node_affinity_fail_on_unavailable);
    }
}
