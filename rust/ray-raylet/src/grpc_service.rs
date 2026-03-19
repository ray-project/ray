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

use ray_common::id::{JobID, PlacementGroupID};
use ray_common::scheduling::{FixedPoint, ResourceSet};
use ray_proto::ray::rpc;

use crate::lease_manager::{LeaseReply, SchedulingClass};
use crate::node_manager::NodeManager;
use crate::placement_group_resource_manager::BundleID;
use crate::scheduling_resources::SchedulingOptions;
use crate::worker_pool::Language;

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
fn scheduling_options_from_proto(strategy: Option<&rpc::SchedulingStrategy>) -> SchedulingOptions {
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
                failure_type:
                    rpc::request_worker_lease_reply::SchedulingFailureType::SchedulingFailed as i32,
                ..Default::default()
            }),
        }
    }

    pub async fn handle_return_worker_lease(
        &self,
        request: rpc::ReturnWorkerLeaseRequest,
    ) -> Result<rpc::ReturnWorkerLeaseReply, Status> {
        let worker_id = if request.lease_id.len() >= 28 {
            Some(ray_common::id::WorkerID::from_binary(&request.lease_id[..28]))
        } else {
            // Try to find the worker by port.
            let port = request.worker_port as u16;
            self.node_manager
                .worker_pool()
                .get_all_workers()
                .into_iter()
                .find(|w| w.port == port)
                .map(|w| w.worker_id)
        };

        tracing::debug!(
            worker_port = request.worker_port,
            disconnect = request.disconnect_worker,
            worker_exiting = request.worker_exiting,
            ?worker_id,
            "ReturnWorkerLease"
        );

        // Release resources from the lease tracker back to the resource pool.
        if let Some(wid) = worker_id {
            if let Some(lease) = self.node_manager.worker_lease_tracker().return_lease(&wid) {
                // Convert the ResourceSet to TaskResourceInstances for the release API.
                let mut alloc =
                    crate::scheduling_resources::TaskResourceInstances::new();
                for (name, amount) in lease.allocated_resources.iter() {
                    alloc
                        .resources
                        .insert(name.to_string(), vec![amount]);
                }
                self.node_manager
                    .scheduler()
                    .release_worker_resources(&alloc);
                tracing::info!(
                    ?wid,
                    "Released resources from returned lease"
                );
            }
        }

        if request.disconnect_worker || request.worker_exiting {
            // Worker is being disconnected or exiting — do not re-grant.
            tracing::info!(
                worker_port = request.worker_port,
                reason = %request.disconnect_worker_error_detail,
                exiting = request.worker_exiting,
                "Worker disconnecting on return, will not re-grant"
            );
            if let Some(wid) = worker_id {
                self.node_manager.worker_pool().disconnect_worker(&wid);
            }
        }

        // Trigger re-scheduling so any pending leases can be reconsidered
        // now that resources have been freed.
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
        let language = match request.language {
            x if x == rpc::Language::Python as i32 => Language::Python,
            x if x == rpc::Language::Java as i32 => Language::Java,
            x if x == rpc::Language::Cpp as i32 => Language::Cpp,
            _ => Language::Python,
        };
        let job_id = request
            .job_id
            .as_ref()
            .map(|b| JobID::from_binary(b))
            .unwrap_or_else(|| JobID::from_int(0));

        tracing::debug!(?language, job_id = %job_id.hex(), "PrestartWorkers");

        // Ensure the job is active so pop_worker can succeed later.
        self.node_manager.worker_pool().handle_job_started(job_id);

        // Start one worker process for the requested language.
        let worker_id = self
            .node_manager
            .worker_pool()
            .start_worker_process(language, &job_id);
        if let Some(wid) = worker_id {
            tracing::info!(worker_id = %wid.hex(), "Prestarted worker");
        }
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

        for report in &request.backlog_reports {
            if let Some(ref lease_spec) = report.lease_spec {
                let resources = resources_from_map(&lease_spec.required_resources);
                let scheduling_class = compute_scheduling_class(&resources);
                self.node_manager
                    .backlog_tracker()
                    .update(scheduling_class, report.backlog_size);
            }
        }

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

        let obj_mgr = self.node_manager.local_object_manager();
        let locked = obj_mgr.lock();
        let successes: Vec<bool> = request
            .object_ids
            .iter()
            .map(|oid_bytes| {
                if oid_bytes.len() >= ray_common::id::ObjectID::SIZE {
                    let oid = ray_common::id::ObjectID::from_binary(oid_bytes);
                    locked.is_pinned(&oid)
                } else {
                    false
                }
            })
            .collect();

        Ok(rpc::PinObjectIDsReply { successes })
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
                let pg_id = PlacementGroupID::from_binary(ident.placement_group_id.as_slice());
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
                let pg_id = PlacementGroupID::from_binary(ident.placement_group_id.as_slice());
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
        let local_rm = self.node_manager.scheduler().local_resource_manager();

        for (name, amount) in &request.resources {
            let new_total = FixedPoint::from_f64(if *amount < 0.0 { 0.0 } else { *amount });
            if !local_rm.resize_local_resource(name, new_total) {
                return Err(Status::invalid_argument(format!(
                    "resource '{}' does not exist on this node",
                    name
                )));
            }
        }

        // Return updated total resources.
        let total = local_rm.get_local_total_resources();
        let mut total_resources = std::collections::HashMap::new();
        for (name, amount) in total.iter() {
            total_resources.insert(name.to_string(), amount.to_f64());
        }

        Ok(rpc::ResizeLocalResourceInstancesReply { total_resources })
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

        // Object store stats from the local object manager.
        let obj_stats = {
            let obj_mgr = self.node_manager.local_object_manager();
            let locked = obj_mgr.lock();
            let stats = locked.stats();
            rpc::ObjectStoreStats {
                num_local_objects: stats.num_pinned as i64,
                object_store_bytes_used: stats.pinned_bytes,
                spilled_bytes_total: stats.total_bytes_spilled as i64,
                spilled_objects_total: stats.total_objects_spilled as i64,
                ..Default::default()
            }
        };

        Ok(rpc::GetNodeStatsReply {
            num_workers,
            store_stats: Some(obj_stats),
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
            system_config: self.node_manager.config().raw_config_json.clone(),
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
        if !request.graceful {
            // Non-graceful shutdown: drain with deadline 0 (immediate)
            self.node_manager.handle_drain(0);
        } else {
            // Graceful: drain with a reasonable deadline
            let deadline = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
                + 30_000;
            self.node_manager.handle_drain(deadline);
        }
        Ok(rpc::ShutdownRayletReply::default())
    }

    pub async fn handle_drain_raylet(
        &self,
        request: rpc::DrainRayletRequest,
    ) -> Result<rpc::DrainRayletReply, Status> {
        let deadline_ms = request.deadline_timestamp_ms as u64;

        // Reject drain if there are active (non-idle) worker leases — the node is busy.
        let active_leases = self.node_manager.worker_lease_tracker().num_active_leases();
        if active_leases > 0 {
            let reason = format!(
                "Node has {} active worker lease(s); cannot drain a busy node",
                active_leases
            );
            tracing::info!(active_leases, "DrainRaylet rejected: node is busy");
            return Ok(rpc::DrainRayletReply {
                is_accepted: false,
                rejection_reason_message: reason,
            });
        }

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
        let in_use: std::collections::HashSet<ray_common::id::WorkerID> = request
            .worker_ids_in_use
            .iter()
            .filter(|b| b.len() >= 28)
            .map(|b| ray_common::id::WorkerID::from_binary(&b[..28]))
            .collect();

        // Disconnect any registered worker not in the in-use set that is idle.
        let idle_ids = self.node_manager.worker_pool().idle_worker_ids();
        let mut released = 0u32;
        for wid in &idle_ids {
            if !in_use.contains(wid) {
                // Release lease resources if any.
                if let Some(lease) = self.node_manager.worker_lease_tracker().return_lease(wid) {
                    let mut alloc = crate::scheduling_resources::TaskResourceInstances::new();
                    for (name, amount) in lease.allocated_resources.iter() {
                        alloc.resources.insert(name.to_string(), vec![amount]);
                    }
                    self.node_manager.scheduler().release_worker_resources(&alloc);
                }
                self.node_manager.worker_pool().disconnect_worker(wid);
                released += 1;
            }
        }
        if released > 0 {
            tracing::info!(released, "Released unused actor workers");
        }
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
        tracing::info!(
            actor_id = hex::encode(&request.intended_actor_id),
            force_kill = request.force_kill,
            "KillLocalActor received"
        );

        // If the request contains a worker_id, use it to find and disconnect
        // the worker running the actor, and release its resources.
        if request.worker_id.len() >= 28 {
            let wid = ray_common::id::WorkerID::from_binary(&request.worker_id[..28]);
            if let Some(lease) = self.node_manager.worker_lease_tracker().return_lease(&wid) {
                let mut alloc = crate::scheduling_resources::TaskResourceInstances::new();
                for (name, amount) in lease.allocated_resources.iter() {
                    alloc.resources.insert(name.to_string(), vec![amount]);
                }
                self.node_manager.scheduler().release_worker_resources(&alloc);
                tracing::info!(?wid, "Released resources for killed actor");
            }
            self.node_manager.worker_pool().disconnect_worker(&wid);

            // Trigger re-scheduling now that resources are freed.
            self.node_manager.lease_manager().schedule_and_grant_leases();
        }

        Ok(rpc::KillLocalActorReply::default())
    }

    pub async fn handle_cancel_local_task(
        &self,
        request: rpc::CancelLocalTaskRequest,
    ) -> Result<rpc::CancelLocalTaskReply, Status> {
        tracing::info!(
            task_id = hex::encode(&request.intended_task_id),
            force_kill = request.force_kill,
            "CancelLocalTask received"
        );

        // Try to cancel the pending lease for this task if it hasn't been granted yet.
        // The lease_id is derived from the task_id bytes.
        let lease_id = if request.intended_task_id.len() >= 8 {
            u64::from_le_bytes(request.intended_task_id[..8].try_into().unwrap())
        } else {
            let mut hasher = DefaultHasher::new();
            request.intended_task_id.hash(&mut hasher);
            hasher.finish()
        };

        let cancelled = self.node_manager.lease_manager().cancel_lease(lease_id);
        if cancelled {
            tracing::info!(lease_id, "Cancelled pending lease for task");
            return Ok(rpc::CancelLocalTaskReply {
                requested_task_running: false,
                attempt_succeeded: true,
            });
        }

        // If the task is already running on a worker, try to disconnect the worker
        // if force_kill is set.
        if request.force_kill && request.executor_worker_id.len() >= 28 {
            let wid =
                ray_common::id::WorkerID::from_binary(&request.executor_worker_id[..28]);
            if let Some(lease) = self.node_manager.worker_lease_tracker().return_lease(&wid) {
                let mut alloc = crate::scheduling_resources::TaskResourceInstances::new();
                for (name, amount) in lease.allocated_resources.iter() {
                    alloc.resources.insert(name.to_string(), vec![amount]);
                }
                self.node_manager
                    .scheduler()
                    .release_worker_resources(&alloc);
            }
            self.node_manager.worker_pool().disconnect_worker(&wid);
            self.node_manager
                .lease_manager()
                .schedule_and_grant_leases();
            return Ok(rpc::CancelLocalTaskReply {
                requested_task_running: true,
                attempt_succeeded: true,
            });
        }

        Ok(rpc::CancelLocalTaskReply {
            requested_task_running: false,
            attempt_succeeded: false,
        })
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
            auth_token: None,
            python_worker_command: None,
            raw_config_json: "{}".to_string(),
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
                required_resources: std::collections::HashMap::from([("CPU".to_string(), cpu)]),
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

        // Pin objects first so they can be found
        let oid1 = ray_common::id::ObjectID::from_binary(&[1u8; 28]);
        let oid2 = ray_common::id::ObjectID::from_binary(&[2u8; 28]);
        let oid3 = ray_common::id::ObjectID::from_binary(&[3u8; 28]);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid1, 50, 0);
            obj_mgr.pin_object(oid2, 50, 0);
            obj_mgr.pin_object(oid3, 50, 0);
        }

        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![vec![1; 28], vec![2; 28], vec![3; 28]],
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
        let map =
            std::collections::HashMap::from([("CPU".to_string(), 4.0), ("GPU".to_string(), 2.0)]);
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

        assert_eq!(
            compute_scheduling_class(&rs1),
            compute_scheduling_class(&rs2)
        );
    }

    #[test]
    fn test_compute_scheduling_class_different() {
        let mut rs1 = ResourceSet::new();
        rs1.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let mut rs2 = ResourceSet::new();
        rs2.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        assert_ne!(
            compute_scheduling_class(&rs1),
            compute_scheduling_class(&rs2)
        );
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

    // ─── RAYLET-1: ReturnWorkerLease resource release ───────────────

    #[tokio::test]
    async fn test_return_worker_lease_releases_resources() {
        let svc = make_svc();

        // Allocate 2 CPUs via a lease
        let reply = svc
            .handle_request_worker_lease(make_lease_request(2.0))
            .await
            .unwrap();
        assert!(reply.worker_address.is_some(), "lease should be granted");

        // Verify resources are consumed
        let load = svc
            .handle_get_resource_load(rpc::GetResourceLoadRequest::default())
            .unwrap();
        let avail_before = *load
            .resources
            .as_ref()
            .unwrap()
            .resources_available
            .get("CPU")
            .unwrap();
        assert!(
            avail_before < 4.0,
            "available CPU should be less than total after lease"
        );

        // Now simulate a lease tracker grant so ReturnWorkerLease can find it.
        let mut wid_bytes = [0u8; 28];
        wid_bytes[0] = 42;
        let wid = ray_common::id::WorkerID::from_binary(&wid_bytes);
        let tid_bytes = [0u8; 24];
        let tid = ray_common::id::TaskID::from_binary(&tid_bytes);
        let mut lease_resources = ResourceSet::new();
        lease_resources.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        svc.node_manager
            .worker_lease_tracker()
            .grant_lease(wid, tid, lease_resources, false);

        // Register the worker so it can be found by port
        let worker = crate::worker_pool::WorkerInfo {
            worker_id: wid,
            language: crate::worker_pool::Language::Python,
            worker_type: crate::worker_pool::WorkerType::Worker,
            job_id: ray_common::id::JobID::from_int(1),
            pid: 9999,
            port: 5555,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        };
        svc.node_manager
            .worker_pool()
            .handle_job_started(ray_common::id::JobID::from_int(1));
        svc.node_manager
            .worker_pool()
            .register_worker(worker)
            .unwrap();

        // Return the lease (not exiting)
        let _ = svc
            .handle_return_worker_lease(rpc::ReturnWorkerLeaseRequest {
                worker_port: 5555,
                disconnect_worker: false,
                worker_exiting: false,
                ..Default::default()
            })
            .await
            .unwrap();

        // Verify resources are restored
        assert_eq!(
            svc.node_manager
                .worker_lease_tracker()
                .num_active_leases(),
            0,
            "lease should be returned"
        );
    }

    #[tokio::test]
    async fn test_return_worker_lease_exiting_worker_not_regranted() {
        let svc = make_svc();

        let mut wid_bytes = [0u8; 28];
        wid_bytes[0] = 50;
        let wid = ray_common::id::WorkerID::from_binary(&wid_bytes);
        let worker = crate::worker_pool::WorkerInfo {
            worker_id: wid,
            language: crate::worker_pool::Language::Python,
            worker_type: crate::worker_pool::WorkerType::Worker,
            job_id: ray_common::id::JobID::from_int(1),
            pid: 10000,
            port: 6666,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        };
        svc.node_manager
            .worker_pool()
            .handle_job_started(ray_common::id::JobID::from_int(1));
        svc.node_manager
            .worker_pool()
            .register_worker(worker)
            .unwrap();

        // Return with worker_exiting = true
        let _ = svc
            .handle_return_worker_lease(rpc::ReturnWorkerLeaseRequest {
                worker_port: 6666,
                disconnect_worker: false,
                worker_exiting: true,
                ..Default::default()
            })
            .await
            .unwrap();

        // Worker should be disconnected
        assert!(
            svc.node_manager.worker_pool().is_worker_dead(&wid),
            "exiting worker should be disconnected"
        );
    }

    // ─── RAYLET-7: DrainRaylet rejection logic ──────────────────────

    #[tokio::test]
    async fn test_drain_raylet_rejected_busy_node() {
        let svc = make_svc();

        // Grant a lease to make the node busy
        let wid_bytes = [0u8; 28];
        let wid = ray_common::id::WorkerID::from_binary(&wid_bytes);
        let tid_bytes = [0u8; 24];
        let tid = ray_common::id::TaskID::from_binary(&tid_bytes);
        svc.node_manager
            .worker_lease_tracker()
            .grant_lease(wid, tid, ResourceSet::new(), false);
        assert_eq!(
            svc.node_manager
                .worker_lease_tracker()
                .num_active_leases(),
            1
        );

        // Drain should be rejected
        let reply = svc
            .handle_drain_raylet(rpc::DrainRayletRequest {
                deadline_timestamp_ms: 5000,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(!reply.is_accepted, "drain should be rejected on busy node");
        assert!(
            reply.rejection_reason_message.contains("active worker lease"),
            "rejection message should mention active leases, got: {}",
            reply.rejection_reason_message
        );
    }

    #[tokio::test]
    async fn test_drain_raylet_accepted_idle_node() {
        let svc = make_svc();

        // No active leases — node is idle
        assert_eq!(
            svc.node_manager
                .worker_lease_tracker()
                .num_active_leases(),
            0
        );

        let reply = svc
            .handle_drain_raylet(rpc::DrainRayletRequest {
                deadline_timestamp_ms: 5000,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(reply.is_accepted, "drain should be accepted on idle node");
        assert!(reply.rejection_reason_message.is_empty());
    }

    // ─── RAYLET-9: Cleanup RPCs ─────────────────────────────────────

    #[tokio::test]
    async fn test_kill_local_actor_disconnects_worker_and_releases_resources() {
        let svc = make_svc();

        let mut wid_bytes = [0u8; 28];
        wid_bytes[0] = 77;
        let wid = ray_common::id::WorkerID::from_binary(&wid_bytes);
        let tid_bytes = [0u8; 24];
        let tid = ray_common::id::TaskID::from_binary(&tid_bytes);

        // Grant a lease with resources
        let mut resources = ResourceSet::new();
        resources.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        svc.node_manager
            .worker_lease_tracker()
            .grant_lease(wid, tid, resources, true);

        // Register the worker
        let worker = crate::worker_pool::WorkerInfo {
            worker_id: wid,
            language: crate::worker_pool::Language::Python,
            worker_type: crate::worker_pool::WorkerType::Worker,
            job_id: ray_common::id::JobID::from_int(1),
            pid: 11111,
            port: 7777,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        };
        svc.node_manager
            .worker_pool()
            .handle_job_started(ray_common::id::JobID::from_int(1));
        svc.node_manager
            .worker_pool()
            .register_worker(worker)
            .unwrap();

        // Kill the actor
        let _ = svc
            .handle_kill_local_actor(rpc::KillLocalActorRequest {
                intended_actor_id: vec![1u8; 16],
                worker_id: wid_bytes.to_vec(),
                force_kill: true,
                death_cause: None,
            })
            .await
            .unwrap();

        // Worker should be disconnected and lease returned
        assert!(svc.node_manager.worker_pool().is_worker_dead(&wid));
        assert_eq!(
            svc.node_manager
                .worker_lease_tracker()
                .num_active_leases(),
            0
        );
    }

    #[tokio::test]
    async fn test_cancel_local_task_cancels_pending_lease() {
        let svc = make_svc();

        // Exhaust all CPUs so the next lease stays pending
        let _ = svc
            .handle_request_worker_lease(make_lease_request(4.0))
            .await
            .unwrap();

        // Queue a 1 CPU lease that will stay pending
        let _rx = svc
            .node_manager
            .lease_manager()
            .queue_and_schedule_lease(
                {
                    let mut rs = ResourceSet::new();
                    rs.set("CPU".to_string(), FixedPoint::from_f64(1.0));
                    rs
                },
                crate::scheduling_resources::SchedulingOptions::hybrid(),
                crate::lease_manager::SchedulingClass(99),
            );

        assert_eq!(svc.node_manager.lease_manager().num_pending_leases(), 1);

        // Cancel using the lease ID (which is 2, since first was ID 1)
        let task_id = 2u64.to_le_bytes().to_vec();
        let reply = svc
            .handle_cancel_local_task(rpc::CancelLocalTaskRequest {
                intended_task_id: task_id,
                force_kill: false,
                recursive: false,
                caller_worker_id: vec![],
                executor_worker_id: vec![],
            })
            .await
            .unwrap();

        assert!(reply.attempt_succeeded);
        assert!(!reply.requested_task_running);
    }

    #[tokio::test]
    async fn test_cancel_local_task_not_found() {
        let svc = make_svc();

        // Cancel a task that doesn't exist
        let reply = svc
            .handle_cancel_local_task(rpc::CancelLocalTaskRequest {
                intended_task_id: vec![99, 0, 0, 0, 0, 0, 0, 0],
                force_kill: false,
                recursive: false,
                caller_worker_id: vec![],
                executor_worker_id: vec![],
            })
            .await
            .unwrap();

        assert!(!reply.attempt_succeeded);
    }

    #[tokio::test]
    async fn test_release_unused_actor_workers() {
        let svc = make_svc();

        // Register two workers, push them to idle
        let job = ray_common::id::JobID::from_int(1);
        svc.node_manager.worker_pool().handle_job_started(job);

        for i in 1..=2u8 {
            let mut wid_bytes = [0u8; 28];
            wid_bytes[0] = i;
            let wid = ray_common::id::WorkerID::from_binary(&wid_bytes);
            let worker = crate::worker_pool::WorkerInfo {
                worker_id: wid,
                language: crate::worker_pool::Language::Python,
                worker_type: crate::worker_pool::WorkerType::Worker,
                job_id: job,
                pid: 1000 + i as u32,
                port: 8000 + i as u16,
                ip_address: "127.0.0.1".to_string(),
                is_alive: true,
            };
            svc.node_manager
                .worker_pool()
                .register_worker(worker)
                .unwrap();
            svc.node_manager
                .worker_pool()
                .push_worker(wid, crate::worker_pool::Language::Python);
        }
        assert_eq!(svc.node_manager.worker_pool().num_idle_workers(), 2);

        // Only worker 1 is in use
        let mut wid1_bytes = [0u8; 28];
        wid1_bytes[0] = 1;

        let _ = svc
            .handle_release_unused_actor_workers(rpc::ReleaseUnusedActorWorkersRequest {
                worker_ids_in_use: vec![wid1_bytes.to_vec()],
            })
            .await
            .unwrap();

        // Worker 2 should be disconnected, worker 1 should remain idle
        let mut wid2_bytes = [0u8; 28];
        wid2_bytes[0] = 2;
        let wid2 = ray_common::id::WorkerID::from_binary(&wid2_bytes);
        assert!(svc.node_manager.worker_pool().is_worker_dead(&wid2));

        let wid1 = ray_common::id::WorkerID::from_binary(&wid1_bytes);
        assert!(!svc.node_manager.worker_pool().is_worker_dead(&wid1));
    }

    // ─── RAYLET-3: Worker Backlog Reporting ──────────────────────────

    #[tokio::test]
    async fn test_report_worker_backlog_updates_state() {
        let svc = make_svc();

        let request = rpc::ReportWorkerBacklogRequest {
            worker_id: vec![1u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(rpc::LeaseSpec {
                    required_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 1.0),
                    ]),
                    ..Default::default()
                }),
                backlog_size: 42,
            }],
        };

        let reply = svc.handle_report_worker_backlog(request).await.unwrap();
        let _ = reply; // Should succeed

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert_eq!(backlog.len(), 1);
        assert!(backlog.values().any(|&v| v == 42));
    }

    #[tokio::test]
    async fn test_report_worker_backlog_updates_existing() {
        let svc = make_svc();

        let lease_spec = rpc::LeaseSpec {
            required_resources: std::collections::HashMap::from([
                ("CPU".to_string(), 2.0),
            ]),
            ..Default::default()
        };

        // First report
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![1u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(lease_spec.clone()),
                backlog_size: 10,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert!(backlog.values().any(|&v| v == 10));

        // Second report — latest wins
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![1u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(lease_spec),
                backlog_size: 99,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert_eq!(backlog.len(), 1);
        assert!(backlog.values().any(|&v| v == 99));
    }

    // ─── RAYLET-4: Object Pinning Semantics ─────────────────────────

    #[tokio::test]
    async fn test_pin_object_ids_fails_for_missing() {
        let svc = make_svc();

        // Pin non-existent objects
        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![vec![1u8; 28], vec![2u8; 28]],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(reply.successes.len(), 2);
        assert!(reply.successes.iter().all(|&s| !s));
    }

    #[tokio::test]
    async fn test_pin_object_ids_succeeds_for_local() {
        let svc = make_svc();

        // Add objects to the local object manager first
        let oid_bytes = vec![42u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 10);
        }

        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![oid_bytes.clone(), vec![99u8; 28]],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(reply.successes.len(), 2);
        assert!(reply.successes[0], "pinned object should succeed");
        assert!(!reply.successes[1], "missing object should fail");
    }

    // ─── RAYLET-5: ResizeLocalResourceInstances ─────────────────────

    #[tokio::test]
    async fn test_resize_cpu_resources() {
        let svc = make_svc();

        // Initial CPU is 4.0
        let load = svc
            .handle_get_resource_load(rpc::GetResourceLoadRequest::default())
            .unwrap();
        let initial_cpu = *load
            .resources
            .as_ref()
            .unwrap()
            .resources_total
            .get("CPU")
            .unwrap();
        assert_eq!(initial_cpu, 4.0);

        // Resize CPU to 8.0
        let reply = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 8.0),
                    ]),
                },
            )
            .await
            .unwrap();

        assert_eq!(*reply.total_resources.get("CPU").unwrap(), 8.0);

        // Verify via get_resource_load
        let load = svc
            .handle_get_resource_load(rpc::GetResourceLoadRequest::default())
            .unwrap();
        let new_cpu = *load
            .resources
            .as_ref()
            .unwrap()
            .resources_total
            .get("CPU")
            .unwrap();
        assert_eq!(new_cpu, 8.0);
    }

    #[tokio::test]
    async fn test_resize_invalid_resource_rejected() {
        let svc = make_svc();

        let result = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([
                        ("TPU".to_string(), 4.0),
                    ]),
                },
            )
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    // ─── RAYLET-6: GetNodeStats Parity ──────────────────────────────

    #[tokio::test]
    async fn test_get_node_stats_includes_store_and_worker_detail() {
        let svc = make_svc();

        // Pin some objects so store_stats is non-trivial
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            let oid1 = ray_common::id::ObjectID::from_binary(&[1u8; 28]);
            let oid2 = ray_common::id::ObjectID::from_binary(&[2u8; 28]);
            obj_mgr.pin_object(oid1, 100, 10);
            obj_mgr.pin_object(oid2, 200, 20);
        }

        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .unwrap();

        assert_eq!(reply.num_workers, 0);

        let store_stats = reply.store_stats.expect("should have store_stats");
        assert_eq!(store_stats.num_local_objects, 2);
        assert_eq!(store_stats.object_store_bytes_used, 330); // (100+10) + (200+20)
    }

    #[tokio::test]
    async fn test_get_node_stats_reflects_draining_state() {
        let svc = make_svc();

        // Initially not draining
        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .unwrap();
        assert_eq!(reply.num_workers, 0);
        // store_stats should be present even when empty
        let store_stats = reply.store_stats.as_ref().expect("should have store_stats");
        assert_eq!(store_stats.num_local_objects, 0);

        // Drain the node
        svc.node_manager.handle_drain(5000);
        assert!(
            svc.node_manager
                .scheduler()
                .local_resource_manager()
                .is_local_node_draining()
        );

        // Stats still returned after draining
        let reply2 = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .unwrap();
        assert!(reply2.store_stats.is_some());
    }
}
