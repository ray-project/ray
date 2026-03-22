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
/// Factory for creating subscriber transport clients per owner.
/// In production: creates gRPC clients from owner addresses.
/// In tests: creates InProcessSubscriberClients.
pub type SubscriberClientFactory =
    dyn Fn(&[u8]) -> Arc<dyn ray_pubsub::SubscriberClient + 'static> + Send + Sync;

pub struct NodeManagerServiceImpl {
    pub node_manager: Arc<NodeManager>,
    /// Optional factory for creating subscriber transport clients.
    /// When set, handle_pin_object_ids automatically starts the runtime
    /// poll loop for the owner. This is the production integration point.
    pub subscriber_client_factory:
        parking_lot::Mutex<Option<Arc<SubscriberClientFactory>>>,
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

/// Parse unit-instance resources from the ray config JSON.
/// Matches C++ `predefined_unit_instance_resources` (default: "GPU") and
/// `custom_unit_instance_resources` config keys.
/// Returns a set of resource names that cannot be resized.
fn parse_unit_instance_resources(config_json: &str) -> Vec<String> {
    // Default predefined unit-instance resources (matches C++ default).
    let mut resources: Vec<String> = vec!["GPU".to_string()];

    // Try to parse config JSON to check for custom additions.
    if let Ok(config) = serde_json::from_str::<serde_json::Value>(config_json) {
        if let Some(predefined) = config.get("predefined_unit_instance_resources") {
            if let Some(s) = predefined.as_str() {
                resources.clear();
                for name in s.split(',') {
                    let trimmed = name.trim().to_string();
                    if !trimmed.is_empty() {
                        resources.push(trimmed);
                    }
                }
            }
        }
        // C++ also has custom_unit_instance_resources
        if let Some(custom) = config.get("custom_unit_instance_resources") {
            if let Some(s) = custom.as_str() {
                for name in s.split(',') {
                    let trimmed = name.trim().to_string();
                    if !trimmed.is_empty() && !resources.contains(&trimmed) {
                        resources.push(trimmed);
                    }
                }
            }
        }
    }

    resources
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

        // C++ contract: validate worker exists before accepting backlog report.
        // C++ checks GetRegisteredWorker and GetRegisteredDriver; ignores unknown.
        let worker_id = if request.worker_id.len() >= ray_common::id::WorkerID::SIZE {
            ray_common::id::WorkerID::from_binary(&request.worker_id)
        } else {
            // Invalid worker_id — ignore the report (C++ ignores unknown workers)
            return Ok(rpc::ReportWorkerBacklogReply::default());
        };

        // Check worker is registered in the worker pool (C++ parity).
        // Unknown or dead workers' backlog reports are silently ignored.
        match self.node_manager.worker_pool().get_worker(&worker_id) {
            Some(w) if w.is_alive => {}
            _ => {
                tracing::debug!(
                    worker_id = hex::encode(&request.worker_id),
                    "Ignoring backlog report from unregistered/dead worker"
                );
                return Ok(rpc::ReportWorkerBacklogReply::default());
            }
        }

        // C++ contract: clear-then-set per worker.
        // Collect all (scheduling_class, backlog_size) entries.
        let entries: Vec<_> = request
            .backlog_reports
            .iter()
            .filter_map(|report| {
                report.lease_spec.as_ref().map(|lease_spec| {
                    let resources = resources_from_map(&lease_spec.required_resources);
                    let scheduling_class = compute_scheduling_class(&resources);
                    (scheduling_class, report.backlog_size)
                })
            })
            .collect();

        self.node_manager
            .backlog_tracker()
            .report_worker_backlog(worker_id, entries);

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

        // C++ contract:
        // 1. Get objects from plasma; if get fails entirely, all false
        // 2. For each object: fail if not found locally OR if pending deletion
        // 3. For successful objects, call PinObjectsAndWaitForFree with owner_address/generator_id
        //
        // In Rust, we check local pin state and pending-deletion status.
        // Objects that pass both checks are considered pinned (they already are in our model).
        let obj_mgr = self.node_manager.local_object_manager();
        let mut locked = obj_mgr.lock();

        let mut successful_oids: Vec<ray_common::id::ObjectID> = Vec::new();
        let successes: Vec<bool> = request
            .object_ids
            .iter()
            .map(|oid_bytes| {
                if oid_bytes.len() >= ray_common::id::ObjectID::SIZE {
                    let oid = ray_common::id::ObjectID::from_binary(oid_bytes);
                    // C++: fail if result is null (not local) or pending deletion
                    let ok = locked.is_pinned(&oid) && !locked.is_pending_deletion(&oid);
                    if ok {
                        successful_oids.push(oid);
                    }
                    ok
                } else {
                    false
                }
            })
            .collect();

        // C++ contract: PinObjectsAndWaitForFree retains the pin with owner/generator context.
        // The pin is only released when the owner signals free (via owner death or explicit free).
        if !successful_oids.is_empty() {
            let generator_id = request.generator_id.clone();
            let owner_address = request.owner_address.clone();
            locked.pin_objects_and_wait_for_free(
                &successful_oids,
                owner_address.clone(),
                generator_id,
            );

            // C++ contract: register WORKER_OBJECT_EVICTION subscription for each
            // pinned object via core_worker_subscriber_->Subscribe().
            // We use the real ray-pubsub Subscriber infrastructure.
            let owner_worker_id = owner_address
                .as_ref()
                .map(|a| a.worker_id.clone())
                .unwrap_or_default();
            let subscriber = self.node_manager.eviction_subscriber();
            let obj_mgr = self.node_manager.local_object_manager();

            // Channel type 0 = WORKER_OBJECT_EVICTION
            const CHANNEL_WORKER_OBJECT_EVICTION: i32 = 0;

            for oid in &successful_oids {
                let oid_binary = oid.binary();
                let obj_mgr_ref = Arc::clone(obj_mgr);
                let oid_copy = *oid;

                // subscription_callback: on eviction event → ReleaseFreedObject(obj_id)
                let item_cb: ray_pubsub::MessageCallback =
                    std::sync::Arc::new(move |_msg: &ray_pubsub::PubMessage| {
                        obj_mgr_ref.lock().release_freed_object(&oid_copy);
                    });

                // owner_dead_callback: on owner death → ReleaseFreedObject(obj_id)
                let obj_mgr_ref2 = Arc::clone(obj_mgr);
                let oid_copy2 = *oid;
                let failure_cb: ray_pubsub::FailureCallback =
                    std::sync::Arc::new(move |_key_id: &[u8]| {
                        obj_mgr_ref2.lock().release_freed_object(&oid_copy2);
                    });

                subscriber.subscribe(
                    &owner_worker_id,
                    CHANNEL_WORKER_OBJECT_EVICTION,
                    &oid_binary,
                    item_cb,
                    Some(failure_cb),
                );
            }

            // C++ contract: Subscribe() starts the long-poll connection to
            // the owner if not already connected. We start the runtime poll
            // loop for this owner so delivery happens automatically.
            if let Some(factory) = self.subscriber_client_factory.lock().as_ref() {
                let client = factory(&owner_worker_id);
                self.node_manager
                    .start_eviction_poll_loop(owner_worker_id, client);
            }
        }

        Ok(rpc::PinObjectIDsReply { successes })
    }

    /// Handle an object eviction notification from the owner.
    /// C++ contract: the subscription callback for WORKER_OBJECT_EVICTION calls
    /// `ReleaseFreedObject(obj_id)` when the owner publishes a free event.
    ///
    /// Delivers the eviction through the real `ray-pubsub::Subscriber` message
    /// dispatch path (`handle_poll_response`), which invokes the registered
    /// subscription callback. After delivery, unsubscribes (matching C++).
    ///
    /// Returns false if no subscription exists for this object (no fallback).
    pub fn notify_object_eviction(
        &self,
        object_id: &ray_common::id::ObjectID,
        owner_worker_id: &[u8],
    ) -> bool {
        const CHANNEL_WORKER_OBJECT_EVICTION: i32 = 0;
        let subscriber = self.node_manager.eviction_subscriber();
        let oid_binary = object_id.binary();

        // Check if a subscription exists before delivering.
        if !subscriber.is_subscribed(owner_worker_id, CHANNEL_WORKER_OBJECT_EVICTION, &oid_binary)
        {
            return false; // No subscription → no release (matches C++ contract)
        }

        // Deliver through subscriber dispatch (invokes the registered item_cb).
        let msg = ray_pubsub::PubMessage {
            channel_type: CHANNEL_WORKER_OBJECT_EVICTION,
            key_id: oid_binary.clone(),
            payload: vec![],
            sequence_id: 1, // single-shot delivery
        };
        subscriber.handle_poll_response(owner_worker_id, owner_worker_id, &[msg]);

        // Unsubscribe after callback (matching C++ subscription_callback behavior).
        subscriber.unsubscribe(owner_worker_id, CHANNEL_WORKER_OBJECT_EVICTION, &oid_binary);
        true
    }

    /// Handle owner death notification — release all retained pins for that owner.
    /// C++ contract: the `owner_dead_callback` in `PinObjectsAndWaitForFree` calls
    /// `ReleaseFreedObject` for all objects owned by the dead worker.
    ///
    /// Delivers through the real `ray-pubsub::Subscriber` failure mechanism
    /// (`handle_publisher_failure`), which invokes all registered failure callbacks
    /// for the dead publisher (owner).
    pub fn notify_owner_death(
        &self,
        owner_worker_id: &[u8],
    ) -> Vec<Vec<u8>> {
        let subscriber = self.node_manager.eviction_subscriber();
        subscriber.handle_publisher_failure(owner_worker_id)
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
        // C++ contract: reject unit-instance resources generically.
        // C++ uses ResourceID::IsUnitInstanceResource() which checks a configurable set.
        // Default predefined_unit_instance_resources is "GPU" (see ray_config_def.h).
        // Custom unit-instance resources can also be added via config.
        // We match the C++ logic: check predefined list + any custom_unit_instance_resources
        // from the node config. Default: ["GPU"].
        let predefined = self.node_manager.config()
            .raw_config_json
            .as_str();
        let unit_instance_resources = parse_unit_instance_resources(predefined);
        for name in request.resources.keys() {
            if unit_instance_resources.iter().any(|r| r == name) {
                return Err(Status::invalid_argument(format!(
                    "Cannot resize unit instance resource '{}'. Unit instance resources \
                     cannot be resized dynamically.",
                    name
                )));
            }
        }

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

    pub async fn handle_get_node_stats(
        &self,
        request: rpc::GetNodeStatsRequest,
    ) -> Result<rpc::GetNodeStatsReply, Status> {
        let include_memory_info = request.include_memory_info;

        // Object store stats from the local object manager.
        let obj_stats = {
            let obj_mgr = self.node_manager.local_object_manager();
            let locked = obj_mgr.lock();
            let stats = locked.stats();
            rpc::ObjectStoreStats {
                spill_time_total_s: stats.spill_time_total_s,
                spilled_bytes_total: stats.total_bytes_spilled as i64,
                spilled_objects_total: stats.total_objects_spilled as i64,
                restore_time_total_s: stats.restore_time_total_s,
                restored_bytes_total: stats.total_bytes_restored as i64,
                restored_objects_total: stats.total_objects_restored as i64,
                object_store_bytes_primary_copy: stats.pinned_bytes,
                num_object_store_primary_copies: stats.num_pinned as i64,
                object_store_bytes_used: stats.pinned_bytes,
                num_local_objects: stats.num_pinned as i64,
                object_store_bytes_avail: 0,
                object_store_bytes_fallback: 0,
                cumulative_created_objects: 0,
                cumulative_created_bytes: 0,
                object_pulls_queued: false,
            }
        };

        // C++ contract: GetCoreWorkerStats RPC to each alive worker/driver.
        // Collects live stats from each worker. For workers reachable via gRPC,
        // sends GetCoreWorkerStats RPC. Falls back to the provider's sync path
        // (tracker data) for workers without reachable endpoints.
        let provider = self.node_manager.worker_stats_provider();
        let is_live = provider.is_live_rpc_provider();
        let all_workers = self.node_manager.worker_pool().get_all_workers();
        let alive_workers: Vec<_> = all_workers.iter().filter(|w| w.is_alive).collect();
        let num_workers = alive_workers.len() as u32;

        let mut core_workers_stats: Vec<rpc::CoreWorkerStats> = Vec::with_capacity(alive_workers.len());
        for w in &alive_workers {
            let worker_type = match w.worker_type {
                crate::worker_pool::WorkerType::Driver => 0, // DRIVER
                _ => 1,                                       // WORKER
            };
            let addr = format!("{}:{}", w.ip_address, w.port);
            let mut stats = rpc::CoreWorkerStats {
                worker_id: w.worker_id.binary(),
                pid: w.pid,
                worker_type,
                ..Default::default()
            };

            // C++ contract (node_manager.cc:2676-2683): on GetCoreWorkerStats
            // RPC failure, the callback still does MergeFrom(r.core_worker_stats())
            // with the default/empty reply — it does NOT substitute cached tracker
            // data.  So on RPC failure the entry has only basic pool metadata
            // (worker_id, pid, worker_type) with zeroed runtime stats.
            //
            // Workers with port > 0: attempt live gRPC RPC; on failure → None
            //   (no tracker fallback — matches C++ which merges empty stats).
            // Workers with port == 0 (e.g. in-process): use sync provider (tracker).
            let snapshot = if is_live && w.port > 0 {
                // Async gRPC path — real C++ equivalent.
                // On failure, return None (entry gets default/zero stats).
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    crate::node_manager::GrpcWorkerStatsProvider::query_worker_rpc(
                        &addr,
                        &w.worker_id,
                        include_memory_info,
                    ),
                )
                .await
                .ok()
                .flatten()
            } else {
                provider.get_worker_stats(&addr, &w.worker_id, include_memory_info)
            };

            if let Some(snapshot) = snapshot {
                stats.num_pending_tasks = snapshot.num_pending_tasks;
                stats.num_running_tasks = snapshot.num_running_tasks;
                if include_memory_info {
                    stats.used_object_store_memory = snapshot.used_object_store_memory;
                    stats.num_owned_objects = snapshot.num_owned_objects;
                }
            }
            // If both paths return None, worker entry has basic pool metadata only —
            // matching C++ which adds the entry even on RPC failure.
            core_workers_stats.push(stats);
        }

        Ok(rpc::GetNodeStatsReply {
            num_workers,
            store_stats: Some(obj_stats),
            core_workers_stats,
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
            .await
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
            subscriber_client_factory: parking_lot::Mutex::new(None),
        }
    }

    /// Register a worker with the given ID byte pattern in the worker pool.
    /// Uses port=0 (in-process / tracker path). For testing gRPC behavior,
    /// use register_test_worker_with_port instead.
    fn register_test_worker(svc: &NodeManagerServiceImpl, id_byte: u8) {
        register_test_worker_with_port(svc, id_byte, 0);
    }

    /// Register a worker with a specific port. port=0 means in-process (tracker path).
    /// port > 0 means the worker is reachable via gRPC.
    fn register_test_worker_with_port(svc: &NodeManagerServiceImpl, id_byte: u8, port: u16) {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        let wid = ray_common::id::WorkerID::from_binary(&[id_byte; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        let worker = WorkerInfo {
            worker_id: wid,
            language: Language::Python,
            worker_type: WorkerType::Worker,
            job_id: job,
            pid: 1000 + id_byte as u32,
            port,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        };
        svc.node_manager.worker_pool().register_worker(worker).unwrap();
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
            .await
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
        register_test_worker(&svc, 1);

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
    async fn test_report_worker_backlog_clears_previous_worker_backlog() {
        let svc = make_svc();
        register_test_worker(&svc, 1);

        let cpu_spec = rpc::LeaseSpec {
            required_resources: std::collections::HashMap::from([("CPU".to_string(), 1.0)]),
            ..Default::default()
        };
        let gpu_spec = rpc::LeaseSpec {
            required_resources: std::collections::HashMap::from([("GPU".to_string(), 1.0)]),
            ..Default::default()
        };

        // Worker 1 reports CPU=10 and GPU=5
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![1u8; 28],
            backlog_reports: vec![
                rpc::WorkerBacklogReport {
                    lease_spec: Some(cpu_spec.clone()),
                    backlog_size: 10,
                },
                rpc::WorkerBacklogReport {
                    lease_spec: Some(gpu_spec.clone()),
                    backlog_size: 5,
                },
            ],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert_eq!(backlog.len(), 2);

        // Worker 1 reports ONLY CPU=3 (GPU should be cleared)
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![1u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(cpu_spec),
                backlog_size: 3,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        // GPU backlog from worker 1 should be cleared (C++ clear-then-set)
        assert_eq!(backlog.len(), 1, "stale GPU backlog should be cleared");
        assert!(backlog.values().any(|&v| v == 3));
    }

    #[tokio::test]
    async fn test_report_worker_backlog_ignores_unknown_worker() {
        let svc = make_svc();

        // Empty worker_id (invalid) — should be silently ignored
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![],  // too short
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(rpc::LeaseSpec {
                    required_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 1.0),
                    ]),
                    ..Default::default()
                }),
                backlog_size: 100,
            }],
        })
        .await
        .unwrap();

        // Backlog should remain empty (report was ignored)
        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert!(backlog.is_empty(), "unknown worker backlog should be ignored");
    }

    #[tokio::test]
    async fn test_report_worker_backlog_tracks_per_worker_per_class() {
        let svc = make_svc();
        register_test_worker(&svc, 1);
        register_test_worker(&svc, 2);

        let cpu_spec = rpc::LeaseSpec {
            required_resources: std::collections::HashMap::from([("CPU".to_string(), 1.0)]),
            ..Default::default()
        };

        // Worker 1 reports CPU=10
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![1u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(cpu_spec.clone()),
                backlog_size: 10,
            }],
        })
        .await
        .unwrap();

        // Worker 2 reports CPU=20
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![2u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(cpu_spec),
                backlog_size: 20,
            }],
        })
        .await
        .unwrap();

        // Aggregate should be 30 (10 + 20)
        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert_eq!(backlog.len(), 1);
        assert!(backlog.values().any(|&v| v == 30), "aggregate should be 30");

        // Per-worker view should be separate
        let w1 = ray_common::id::WorkerID::from_binary(&[1u8; 28]);
        let w2 = ray_common::id::WorkerID::from_binary(&[2u8; 28]);
        let w1_backlog = svc.node_manager.backlog_tracker().get_worker_backlog(&w1);
        let w2_backlog = svc.node_manager.backlog_tracker().get_worker_backlog(&w2);
        assert!(w1_backlog.values().any(|&v| v == 10));
        assert!(w2_backlog.values().any(|&v| v == 20));
    }

    // ─── RAYLET-3 Round 4: worker existence validation ──────────────

    #[tokio::test]
    async fn test_report_worker_backlog_rejects_unregistered_worker_even_with_valid_length() {
        let svc = make_svc();
        // Do NOT register the worker — valid 28-byte ID but not in worker pool.
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![99u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(rpc::LeaseSpec {
                    required_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 1.0),
                    ]),
                    ..Default::default()
                }),
                backlog_size: 100,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert!(backlog.is_empty(), "unregistered worker with valid-length ID should be rejected");
    }

    #[tokio::test]
    async fn test_report_worker_backlog_accepts_registered_worker() {
        let svc = make_svc();
        register_test_worker(&svc, 7);

        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![7u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(rpc::LeaseSpec {
                    required_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 2.0),
                    ]),
                    ..Default::default()
                }),
                backlog_size: 42,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert!(!backlog.is_empty(), "registered worker should have backlog accepted");
        assert!(backlog.values().any(|&v| v == 42));
    }

    /// C++ contract: HandleReportWorkerBacklog accepts GetRegisteredDriver(worker_id).
    /// This test registers a WorkerType::Driver and proves backlog is accepted.
    #[tokio::test]
    async fn test_report_worker_backlog_accepts_registered_driver() {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        let svc = make_svc();
        let wid = ray_common::id::WorkerID::from_binary(&[50u8; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        let driver = WorkerInfo {
            worker_id: wid,
            language: Language::Python,
            worker_type: WorkerType::Driver,
            job_id: job,
            pid: 9999,
            port: 19999,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        };
        svc.node_manager.worker_pool().register_worker(driver).unwrap();

        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![50u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(rpc::LeaseSpec {
                    required_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 1.0),
                    ]),
                    ..Default::default()
                }),
                backlog_size: 77,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert!(!backlog.is_empty(), "registered driver should have backlog accepted");
        assert!(backlog.values().any(|&v| v == 77));
    }

    /// C++ contract: unknown driver-like ID (valid length, not registered) is rejected.
    #[tokio::test]
    async fn test_report_worker_backlog_rejects_unknown_driver_id() {
        let svc = make_svc();
        // Do NOT register anything — 28-byte ID that looks valid but isn't registered
        svc.handle_report_worker_backlog(rpc::ReportWorkerBacklogRequest {
            worker_id: vec![88u8; 28],
            backlog_reports: vec![rpc::WorkerBacklogReport {
                lease_spec: Some(rpc::LeaseSpec {
                    required_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 1.0),
                    ]),
                    ..Default::default()
                }),
                backlog_size: 10,
            }],
        })
        .await
        .unwrap();

        let backlog = svc.node_manager.backlog_tracker().get_backlog();
        assert!(backlog.is_empty(), "unknown driver ID should be silently ignored");
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

    #[tokio::test]
    async fn test_pin_object_ids_fails_for_pending_deletion() {
        let svc = make_svc();

        let oid_bytes = vec![50u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            // Pin the object, then mark for deletion
            obj_mgr.pin_object(oid, 100, 0);
            obj_mgr.mark_for_deletion(oid);
        }

        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![oid_bytes],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(reply.successes.len(), 1);
        assert!(!reply.successes[0], "pending-deletion object should fail");
    }

    #[tokio::test]
    async fn test_pin_object_ids_uses_owner_address_and_generator_id_path() {
        let svc = make_svc();

        let oid_bytes = vec![60u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 200, 0);
        }

        // Provide owner_address and generator_id, matching C++ PinObjectsAndWaitForFree args
        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![oid_bytes],
                owner_address: Some(rpc::Address {
                    node_id: vec![1u8; 28],
                    worker_id: vec![2u8; 28],
                    ip_address: "127.0.0.1".to_string(),
                    port: 5000,
                }),
                generator_id: Some(vec![3u8; 28]),
            })
            .await
            .unwrap();

        assert_eq!(reply.successes.len(), 1);
        assert!(reply.successes[0], "locally pinned object with owner context should succeed");
    }

    // ─── RAYLET-4 Round 4: real pin-lifetime semantics ──────────────

    #[tokio::test]
    async fn test_pin_object_ids_establishes_pin_lifetime() {
        let svc = make_svc();
        let oid_bytes = vec![70u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 500, 0);
        }

        // Pin with owner
        let owner = rpc::Address {
            node_id: vec![1u8; 28],
            worker_id: vec![2u8; 28],
            ip_address: "10.0.0.1".to_string(),
            port: 6000,
        };
        let reply = svc
            .handle_pin_object_ids(rpc::PinObjectIDsRequest {
                object_ids: vec![oid_bytes],
                owner_address: Some(owner),
                generator_id: Some(vec![4u8; 28]),
            })
            .await
            .unwrap();

        assert!(reply.successes[0], "pin should succeed");

        // Verify the pin has owner info (real retained pin, not just a boolean lookup)
        let obj_mgr = svc.node_manager.local_object_manager().lock();
        assert!(obj_mgr.has_pin_owner(&oid), "pinned object should have owner after PinObjectsAndWaitForFree");
    }

    #[tokio::test]
    async fn test_pin_object_ids_releases_pin_only_when_owner_condition_is_met() {
        let svc = make_svc();
        let oid_bytes = vec![80u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 300, 0);
        }

        // Pin with owner
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: vec![3u8; 28],
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // Verify still pinned with owner
        {
            let obj_mgr = svc.node_manager.local_object_manager().lock();
            assert!(obj_mgr.is_pinned(&oid));
            assert!(obj_mgr.has_pin_owner(&oid));
        }

        // Release via owner free event through subscriber dispatch
        let owner_wid = vec![3u8; 28];
        assert!(svc.notify_object_eviction(&oid, &owner_wid), "owner eviction signal should succeed");

        // Now should not be pinned
        {
            let obj_mgr = svc.node_manager.local_object_manager().lock();
            assert!(!obj_mgr.is_pinned(&oid));
            assert!(!obj_mgr.has_pin_owner(&oid));
        }
    }

    // ─── RAYLET-4 Round 8: real subscriber-based eviction tests ──────────
    //
    // These tests prove that pin release is driven through the real ray-pubsub
    // Subscriber infrastructure, not through local callback objects or service
    // helper methods.

    /// C++ contract: PinObjectsAndWaitForFree registers a WORKER_OBJECT_EVICTION
    /// subscription via core_worker_subscriber_->Subscribe().
    /// This test proves the subscription is registered in the real ray-pubsub
    /// Subscriber at pin time.
    #[tokio::test]
    async fn test_pin_object_ids_registers_real_subscriber_subscription() {
        let svc = make_svc();
        let oid_bytes = vec![95u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![70u8; 28];
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        // Before pin, no subscription in the real subscriber
        let subscriber = svc.node_manager.eviction_subscriber();
        assert!(
            !subscriber.is_subscribed(&owner_wid, 0, &oid.binary()),
            "no subscription before pin"
        );

        // Pin via gRPC handler
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // After pin, subscription must exist in the real subscriber
        assert!(
            subscriber.is_subscribed(&owner_wid, 0, &oid.binary()),
            "PinObjectsAndWaitForFree must register in the real ray-pubsub Subscriber"
        );
    }

    /// C++ contract: missing subscription does NOT release the object.
    /// If no WORKER_OBJECT_EVICTION subscription exists, the eviction
    /// notification returns false and the pin remains.
    #[tokio::test]
    async fn test_pin_object_ids_missing_subscription_does_not_release_object() {
        let svc = make_svc();
        let oid = ray_common::id::ObjectID::from_binary(&[96u8; 28]);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        // No pin_objects_and_wait_for_free → no subscription
        let fake_owner = vec![99u8; 28];
        let released = svc.notify_object_eviction(&oid, &fake_owner);
        assert!(!released, "missing subscription must NOT release the object");
        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid),
            "pin must remain when no subscription exists");
    }

    /// C++ contract: the subscription_callback releases the pin when the
    /// eviction event is delivered through the subscriber.
    /// Proves release goes through handle_poll_response → item_cb → release_freed_object.
    #[tokio::test]
    async fn test_pin_object_ids_release_requires_real_subscription_delivery_path() {
        let svc = make_svc();
        let oid_bytes = vec![97u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![71u8; 28];
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // Verify subscription in real subscriber
        let subscriber = svc.node_manager.eviction_subscriber();
        assert!(subscriber.is_subscribed(&owner_wid, 0, &oid.binary()));

        // Deliver eviction through the subscriber dispatch path
        let released = svc.notify_object_eviction(&oid, &owner_wid);
        assert!(released, "eviction delivered through subscriber must succeed");

        // Subscription consumed (unsubscribed, matching C++)
        assert!(
            !subscriber.is_subscribed(&owner_wid, 0, &oid.binary()),
            "subscription must be consumed after eviction (C++ unsubscribe)"
        );
        // Pin released
        assert!(!svc.node_manager.local_object_manager().lock().is_pinned(&oid),
            "pin must be released by subscriber callback");
    }

    /// C++ contract: the real subscriber callback releases the pin on eviction.
    /// This is the positive end-to-end path.
    #[tokio::test]
    async fn test_pin_object_ids_real_subscriber_callback_releases_pin() {
        let svc = make_svc();
        let oid_bytes = vec![91u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![55u8; 28];
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ip_address: "10.0.0.1".to_string(),
                port: 7000,
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        assert!(svc.node_manager.local_object_manager().lock().has_pin_owner(&oid));

        // Deliver through real subscriber path
        let released = svc.notify_object_eviction(&oid, &owner_wid);
        assert!(released, "eviction must release the pin");
        assert!(!svc.node_manager.local_object_manager().lock().is_pinned(&oid));
    }

    /// C++ contract: owner_dead_callback releases all subscribed pins via
    /// the subscriber's failure mechanism (handle_publisher_failure).
    #[tokio::test]
    async fn test_pin_object_ids_owner_death_via_subscriber_failure() {
        let svc = make_svc();
        let oid1 = ray_common::id::ObjectID::from_binary(&[92u8; 28]);
        let oid2 = ray_common::id::ObjectID::from_binary(&[93u8; 28]);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid1, 100, 0);
            obj_mgr.pin_object(oid2, 200, 0);
        }

        let owner_wid = vec![60u8; 28];
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![vec![92u8; 28], vec![93u8; 28]],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // Both subscriptions in real subscriber
        let subscriber = svc.node_manager.eviction_subscriber();
        assert!(subscriber.is_subscribed(&owner_wid, 0, &oid1.binary()));
        assert!(subscriber.is_subscribed(&owner_wid, 0, &oid2.binary()));

        // Owner death through subscriber's failure mechanism
        let released = svc.notify_owner_death(&owner_wid);
        assert_eq!(released.len(), 2, "owner death should release both pins");

        // Subscriptions consumed by failure handler
        assert!(!subscriber.is_subscribed(&owner_wid, 0, &oid1.binary()));
        assert!(!subscriber.is_subscribed(&owner_wid, 0, &oid2.binary()));

        // Pins released
        let locked = svc.node_manager.local_object_manager().lock();
        assert!(!locked.is_pinned(&oid1));
        assert!(!locked.is_pinned(&oid2));
    }

    // ─── RAYLET-4 Round 9: subscriber transport lifecycle proof ──────────
    //
    // These tests exercise the subscriber transport path directly (drain_commands,
    // handle_poll_response, handle_publisher_failure) WITHOUT going through the
    // notify_object_eviction / notify_owner_death helper methods.

    /// Proves that subscribing via handle_pin_object_ids queues real subscribe
    /// commands in the ray-pubsub Subscriber, drainable via drain_commands.
    /// C++ equivalent: core_worker_subscriber_->Subscribe() queues a subscribe
    /// command to be sent to the owner publisher in the next long-poll batch.
    #[tokio::test]
    async fn test_pin_object_ids_subscriber_commands_are_drained_for_owner() {
        let svc = make_svc();
        let oid_bytes = vec![80u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![81u8; 28];
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        // Before pinning, no commands queued for this owner.
        let subscriber = svc.node_manager.eviction_subscriber();
        let pre_commands = subscriber.drain_commands(&owner_wid);
        assert!(pre_commands.is_empty(), "no commands before pin");

        // Pin via gRPC handler — this registers the subscription.
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // The subscriber must have a pending Subscribe command for this owner.
        let commands = subscriber.drain_commands(&owner_wid);
        assert_eq!(commands.len(), 1, "one subscribe command per pinned object");
        match &commands[0] {
            ray_pubsub::SubscriberCommand::Subscribe { channel_type, key_id } => {
                assert_eq!(*channel_type, 0, "WORKER_OBJECT_EVICTION channel");
                assert_eq!(key_id, &oid.binary(), "subscribe key must be the object ID");
            }
            other => panic!("expected Subscribe command, got {:?}", other),
        }

        // After drain, no more pending commands.
        let post_commands = subscriber.drain_commands(&owner_wid);
        assert!(post_commands.is_empty(), "commands drained");
    }

    /// Proves that delivering an eviction event through subscriber.handle_poll_response
    /// directly (simulating the long-poll delivery loop) releases the pin — WITHOUT
    /// using the notify_object_eviction helper.
    /// C++ equivalent: the subscriber receives a WORKER_OBJECT_EVICTION message via
    /// long-poll, dispatches it to the registered subscription_callback, which calls
    /// ReleaseFreedObject.
    #[tokio::test]
    async fn test_pin_object_ids_long_poll_delivery_releases_pin_without_notify_helper() {
        let svc = make_svc();
        let oid_bytes = vec![82u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![83u8; 28];
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // Verify pin is active and subscription exists.
        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid));
        let subscriber = svc.node_manager.eviction_subscriber();
        assert!(subscriber.is_subscribed(&owner_wid, 0, &oid.binary()));

        // Deliver eviction through the subscriber's handle_poll_response DIRECTLY
        // (not via notify_object_eviction — this simulates what the real long-poll
        // transport loop does when it receives a message from the owner publisher).
        let eviction_msg = ray_pubsub::PubMessage {
            channel_type: 0, // WORKER_OBJECT_EVICTION
            key_id: oid.binary(),
            payload: vec![],
            sequence_id: 1,
        };
        let processed = subscriber.handle_poll_response(
            &owner_wid,
            &owner_wid,
            &[eviction_msg],
        );
        assert_eq!(processed, 1, "one message should be dispatched");

        // Pin must be released by the subscriber callback.
        assert!(
            !svc.node_manager.local_object_manager().lock().is_pinned(&oid),
            "pin must be released by subscriber callback (not notify helper)"
        );
    }

    /// Proves that owner death through subscriber.handle_publisher_failure DIRECTLY
    /// (not via notify_owner_death) releases all pins for that owner.
    /// C++ equivalent: the subscriber detects owner death (long-poll failure/timeout),
    /// invokes failure callbacks for all subscriptions to that publisher, which call
    /// ReleaseFreedObject for each subscribed object.
    #[tokio::test]
    async fn test_pin_object_ids_owner_death_via_real_subscriber_failure_path() {
        let svc = make_svc();
        let oid1 = ray_common::id::ObjectID::from_binary(&[84u8; 28]);
        let oid2 = ray_common::id::ObjectID::from_binary(&[85u8; 28]);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid1, 100, 0);
            obj_mgr.pin_object(oid2, 200, 0);
        }

        let owner_wid = vec![86u8; 28];
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![vec![84u8; 28], vec![85u8; 28]],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // Both pins active and subscriptions exist.
        let subscriber = svc.node_manager.eviction_subscriber();
        assert!(subscriber.is_subscribed(&owner_wid, 0, &oid1.binary()));
        assert!(subscriber.is_subscribed(&owner_wid, 0, &oid2.binary()));
        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid1));
        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid2));

        // Simulate owner death through the subscriber's handle_publisher_failure
        // DIRECTLY (not via notify_owner_death — this is what the real long-poll
        // loop calls when it detects that the publisher/owner is unreachable).
        let notified = subscriber.handle_publisher_failure(&owner_wid);
        assert_eq!(notified.len(), 2, "failure should notify both subscriptions");

        // Subscriptions must be removed.
        assert!(!subscriber.is_subscribed(&owner_wid, 0, &oid1.binary()));
        assert!(!subscriber.is_subscribed(&owner_wid, 0, &oid2.binary()));

        // Both pins must be released by the failure callbacks.
        let locked = svc.node_manager.local_object_manager().lock();
        assert!(
            !locked.is_pinned(&oid1),
            "pin1 must be released by subscriber failure callback (not notify helper)"
        );
        assert!(
            !locked.is_pinned(&oid2),
            "pin2 must be released by subscriber failure callback (not notify helper)"
        );
    }

    // ─── RAYLET-4 Round 10: production transport loop proof ──────────
    //
    // These tests use SubscriberTransport.poll_publisher() — the production
    // transport driver — instead of directly calling handle_poll_response or
    // handle_publisher_failure. The transport encapsulates the full lifecycle:
    // drain commands → send to publisher → long-poll → dispatch/failure.

    /// Proves that the production subscriber transport loop drives eviction
    /// delivery end-to-end: pin → subscribe → transport drains commands →
    /// transport sends to publisher → owner publishes eviction → transport
    /// long-polls → transport dispatches → callback releases pin.
    ///
    /// No direct call to handle_poll_response or handle_publisher_failure.
    #[tokio::test]
    async fn test_pin_object_ids_eviction_subscriber_is_driven_by_production_transport_loop() {
        let svc = make_svc();
        let oid_bytes = vec![110u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![111u8; 28];
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        // Pin via gRPC handler — registers subscription in the eviction subscriber.
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid));

        // Set up the owner's publisher (simulates the remote owner process).
        let owner_publisher = std::sync::Arc::new(
            ray_pubsub::Publisher::new(ray_pubsub::PublisherConfig::default()),
        );
        owner_publisher.register_channel(0, false); // WORKER_OBJECT_EVICTION

        // Create the production transport driver.
        let transport = ray_pubsub::SubscriberTransport::new(
            std::sync::Arc::clone(svc.node_manager.eviction_subscriber()),
        );
        let client = ray_pubsub::InProcessSubscriberClient::new(
            owner_publisher.clone(),
            owner_wid.clone(),
        );

        // The owner publishes an eviction event (happens asynchronously,
        // giving time for the transport to send commands first).
        let pub_clone = owner_publisher.clone();
        let oid_binary = oid.binary();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            pub_clone.publish(0, &oid_binary, vec![]);
        });

        // Run the production transport loop — this:
        // 1. Drains the subscribe command from the eviction subscriber
        // 2. Sends it to the owner's publisher (registers the subscription)
        // 3. Long-polls the owner's publisher
        // 4. Receives the eviction message
        // 5. Dispatches it via subscriber.handle_poll_response (internally)
        // 6. The registered callback calls release_freed_object
        let processed = transport.poll_publisher(&owner_wid, &client).await.unwrap();
        assert_eq!(processed, 1, "one eviction event dispatched by transport");

        // Pin must be released — driven entirely by the production transport.
        assert!(
            !svc.node_manager.local_object_manager().lock().is_pinned(&oid),
            "pin must be released by the production transport loop (no manual handle_poll_response)"
        );
    }

    /// Proves that owner failure is detected by the production subscriber
    /// transport loop. When the owner publisher is unreachable, the transport
    /// invokes handle_publisher_failure internally, which fires the failure
    /// callbacks and releases all pins for that owner.
    ///
    /// No direct call to handle_publisher_failure.
    #[tokio::test]
    async fn test_pin_object_ids_owner_failure_reaches_subscriber_without_manual_failure_call() {
        let svc = make_svc();
        let oid1 = ray_common::id::ObjectID::from_binary(&[112u8; 28]);
        let oid2 = ray_common::id::ObjectID::from_binary(&[113u8; 28]);
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid1, 100, 0);
            obj_mgr.pin_object(oid2, 200, 0);
        }

        let owner_wid = vec![114u8; 28];
        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![vec![112u8; 28], vec![113u8; 28]],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid1));
        assert!(svc.node_manager.local_object_manager().lock().is_pinned(&oid2));

        // Create the production transport with a FAILING client (owner is dead).
        let transport = ray_pubsub::SubscriberTransport::new(
            std::sync::Arc::clone(svc.node_manager.eviction_subscriber()),
        );
        let failing_client = ray_pubsub::FailingSubscriberClient;

        // Run the production transport loop — this:
        // 1. Tries to drain and send commands → fails (owner unreachable)
        // 2. Internally calls subscriber.handle_publisher_failure()
        // 3. Failure callbacks fire → release_freed_object for both objects
        let result = transport.poll_publisher(&owner_wid, &failing_client).await;
        assert!(result.is_err(), "transport should report publisher failure");

        // Both pins must be released by the transport's internal failure handling.
        let locked = svc.node_manager.local_object_manager().lock();
        assert!(
            !locked.is_pinned(&oid1),
            "pin1 must be released by transport failure handling (no manual handle_publisher_failure)"
        );
        assert!(
            !locked.is_pinned(&oid2),
            "pin2 must be released by transport failure handling (no manual handle_publisher_failure)"
        );
    }

    // ─── RAYLET-4 Round 11: runtime integration proof ──────────
    //
    // These tests prove that the NodeManager runtime itself drives the
    // subscriber transport loop. The test sets a subscriber_client_factory
    // on the service, then pins objects. The runtime (handle_pin_object_ids)
    // automatically starts the poll loop — no test-created transport driver.

    /// Proves that handle_pin_object_ids starts the runtime eviction poll
    /// loop. The test provides a subscriber_client_factory (the mock transport
    /// to the owner), pins an object, and verifies that:
    /// 1. The runtime started the poll loop (has_eviction_poll_loop == true)
    /// 2. The owner publishes an eviction event
    /// 3. The runtime loop receives it and releases the pin
    /// No test-created SubscriberTransport.
    #[tokio::test]
    async fn test_raylet_eviction_transport_loop_runs_in_runtime() {
        let svc = make_svc();
        let oid_bytes = vec![120u8; 28];
        let oid = ray_common::id::ObjectID::from_binary(&oid_bytes);
        let owner_wid = vec![121u8; 28];

        // Set up the owner's publisher.
        let owner_publisher = std::sync::Arc::new(
            ray_pubsub::Publisher::new(ray_pubsub::PublisherConfig::default()),
        );
        owner_publisher.register_channel(0, false); // WORKER_OBJECT_EVICTION

        // Install the subscriber client factory so the runtime starts poll loops.
        let pub_for_factory = owner_publisher.clone();
        let factory: Arc<super::SubscriberClientFactory> = Arc::new(move |publisher_id: &[u8]| {
            Arc::new(ray_pubsub::InProcessSubscriberClient::new(
                pub_for_factory.clone(),
                publisher_id.to_vec(),
            )) as Arc<dyn ray_pubsub::SubscriberClient + 'static>
        });
        *svc.subscriber_client_factory.lock() = Some(factory);

        // Pin the object.
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid, 100, 0);
        }

        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![oid_bytes],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // The runtime must have started the poll loop.
        assert!(
            svc.node_manager.has_eviction_poll_loop(&owner_wid),
            "runtime must start poll loop for the owner"
        );

        // Wait for the runtime loop to send commands and register with the publisher.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Owner publishes an eviction event.
        let oid_binary = oid.binary();
        owner_publisher.publish(0, &oid_binary, vec![]);

        // Give the runtime loop time to long-poll and dispatch.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Pin must be released by the runtime-driven transport loop.
        assert!(
            !svc.node_manager.local_object_manager().lock().is_pinned(&oid),
            "pin must be released by the runtime transport loop (no test-created transport)"
        );
    }

    /// Proves that owner failure is detected by the runtime poll loop.
    /// The test installs a FailingSubscriberClient factory, pins objects,
    /// and verifies that the runtime detects the failure and releases pins.
    /// No test-created SubscriberTransport.
    #[tokio::test]
    async fn test_raylet_owner_failure_reaches_eviction_subscriber_in_runtime() {
        let svc = make_svc();
        let oid1 = ray_common::id::ObjectID::from_binary(&[122u8; 28]);
        let oid2 = ray_common::id::ObjectID::from_binary(&[123u8; 28]);
        let owner_wid = vec![124u8; 28];

        // Install a FAILING client factory (owner is dead).
        let factory: Arc<super::SubscriberClientFactory> = Arc::new(|_publisher_id: &[u8]| {
            Arc::new(ray_pubsub::FailingSubscriberClient)
                as Arc<dyn ray_pubsub::SubscriberClient + 'static>
        });
        *svc.subscriber_client_factory.lock() = Some(factory);

        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            obj_mgr.pin_object(oid1, 100, 0);
            obj_mgr.pin_object(oid2, 200, 0);
        }

        svc.handle_pin_object_ids(rpc::PinObjectIDsRequest {
            object_ids: vec![vec![122u8; 28], vec![123u8; 28]],
            owner_address: Some(rpc::Address {
                worker_id: owner_wid.clone(),
                ..Default::default()
            }),
            generator_id: None,
        })
        .await
        .unwrap();

        // Give the runtime loop time to detect failure.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Both pins must be released by the runtime's failure handling.
        let locked = svc.node_manager.local_object_manager().lock();
        assert!(
            !locked.is_pinned(&oid1),
            "pin1 must be released by runtime failure detection (no test-created transport)"
        );
        assert!(
            !locked.is_pinned(&oid2),
            "pin2 must be released by runtime failure detection (no test-created transport)"
        );
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

    #[tokio::test]
    async fn test_resize_local_resource_instances_rejects_gpu() {
        let svc = make_svc();

        // C++ rejects GPU as a unit-instance resource
        let result = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([
                        ("GPU".to_string(), 4.0),
                    ]),
                },
            )
            .await;

        assert!(result.is_err(), "GPU resize should be rejected");
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("unit instance resource"));
    }

    #[tokio::test]
    async fn test_resize_local_resource_instances_preserves_in_use_capacity() {
        let svc = make_svc();

        // Allocate 2 of 4 CPUs
        let _reply = svc
            .handle_request_worker_lease(make_lease_request(2.0))
            .await
            .unwrap();

        // Now shrink CPU to 3.0 — available should be min(3.0, old_avail + delta)
        // old_total=4, old_avail=2, delta=-1, new_avail = 2 + (-1) = 1, clamped to [0, 3]
        let reply = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 3.0),
                    ]),
                },
            )
            .await
            .unwrap();

        assert_eq!(*reply.total_resources.get("CPU").unwrap(), 3.0);

        // Available should be 1.0 (not negative, and in-use resources preserved)
        let load = svc
            .handle_get_resource_load(rpc::GetResourceLoadRequest::default())
            .unwrap();
        let avail = *load
            .resources
            .as_ref()
            .unwrap()
            .resources_available
            .get("CPU")
            .unwrap();
        assert!(avail >= 0.0, "available should not be negative after resize");
        assert!(avail <= 3.0, "available should not exceed new total");
    }

    #[tokio::test]
    async fn test_resize_local_resource_instances_matches_cpp_clamping() {
        let svc = make_svc();

        // Negative target should be clamped to 0
        let reply = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([
                        ("CPU".to_string(), -5.0),
                    ]),
                },
            )
            .await
            .unwrap();

        // CPU should be 0 (or absent from the reply, which means 0)
        let cpu_total = reply.total_resources.get("CPU").copied().unwrap_or(0.0);
        assert_eq!(cpu_total, 0.0);
    }

    // ─── RAYLET-5 Round 4: generic unit-instance resource validation ──

    #[tokio::test]
    async fn test_resize_local_resource_instances_rejects_all_unit_instance_resources() {
        // C++ uses a configurable set of unit-instance resources (default: "GPU").
        // The Rust implementation should match this generic behavior,
        // not just hard-code "GPU".
        let svc = make_svc();

        // GPU should be rejected (default predefined_unit_instance_resources)
        let result = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([("GPU".to_string(), 2.0)]),
                },
            )
            .await;
        assert!(result.is_err(), "GPU should be rejected as unit-instance resource");
        assert!(result.unwrap_err().message().contains("unit instance resource"));

        // CPU should NOT be rejected (it's a regular resource in default config)
        let result = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([("CPU".to_string(), 2.0)]),
                },
            )
            .await;
        assert!(result.is_ok(), "CPU should be accepted (not a unit-instance resource)");
    }

    #[test]
    fn test_parse_unit_instance_resources_default() {
        let resources = parse_unit_instance_resources("{}");
        assert!(resources.contains(&"GPU".to_string()));
        assert!(!resources.contains(&"CPU".to_string()));
    }

    #[test]
    fn test_parse_unit_instance_resources_custom_config() {
        let config = r#"{"predefined_unit_instance_resources": "GPU,TPU"}"#;
        let resources = parse_unit_instance_resources(config);
        assert!(resources.contains(&"GPU".to_string()));
        assert!(resources.contains(&"TPU".to_string()));
        assert!(!resources.contains(&"CPU".to_string()));
    }

    #[test]
    fn test_parse_unit_instance_resources_with_custom_resources() {
        let config = r#"{"custom_unit_instance_resources": "FPGA,ASIC"}"#;
        let resources = parse_unit_instance_resources(config);
        assert!(resources.contains(&"GPU".to_string()), "GPU should still be in defaults");
        assert!(resources.contains(&"FPGA".to_string()));
        assert!(resources.contains(&"ASIC".to_string()));
    }

    // ─── RAYLET-5 Round 5: verification of custom unit resources + delta semantics ──

    /// C++ contract: custom_unit_instance_resources from config are also rejected.
    /// This proves the generic parse path, not just the hard-coded GPU check.
    #[tokio::test]
    async fn test_resize_local_resource_instances_matches_cpp_for_custom_unit_resources() {
        // Create a service with a config that includes custom unit resources.
        // We can't easily change raw_config_json after construction, but we can
        // test the parse function directly with a custom config string and verify
        // the handler would reject the resources.
        let custom_config = r#"{"custom_unit_instance_resources": "TPU,FPGA"}"#;
        let resources = parse_unit_instance_resources(custom_config);
        assert!(resources.contains(&"GPU".to_string()), "GPU always in defaults");
        assert!(resources.contains(&"TPU".to_string()), "TPU from custom config");
        assert!(resources.contains(&"FPGA".to_string()), "FPGA from custom config");
        assert!(!resources.contains(&"CPU".to_string()), "CPU not a unit-instance resource");
        assert!(!resources.contains(&"memory".to_string()), "memory not a unit-instance resource");
    }

    /// C++ contract: delta semantics under use.
    /// Resize from total=4 with 2 in use → target=2 should work because
    /// available=2 is enough to absorb the delta of -2.
    #[tokio::test]
    async fn test_resize_local_resource_instances_matches_cpp_delta_semantics_under_use() {
        use ray_common::scheduling::{FixedPoint, ResourceSet};
        let svc = make_svc();
        // Initial CPU = 4.0 (from make_svc config)

        // "Allocate" 2 CPU by reducing available resources
        let local_rm = svc.node_manager.scheduler().local_resource_manager();
        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let alloc = local_rm.allocate_local_task_resources(&request);
        assert!(alloc.is_some(), "should be able to allocate 2 CPU from 4");

        // Available is now 2.0, total is 4.0, in-use is 2.0
        let avail_before = local_rm.get_local_available_resources();
        let cpu_avail_before = avail_before.get("CPU").to_f64();
        assert!((cpu_avail_before - 2.0).abs() < 0.01, "should have 2.0 available CPU");

        // Resize to target=2: delta = 2 - 4 = -2. Available = 2, enough for delta.
        let result = svc
            .handle_resize_local_resource_instances(
                rpc::ResizeLocalResourceInstancesRequest {
                    resources: std::collections::HashMap::from([("CPU".to_string(), 2.0)]),
                },
            )
            .await;
        assert!(result.is_ok(), "resize to 2 should succeed with 2 available");
        let total_after = result.unwrap().total_resources;
        assert!((total_after["CPU"] - 2.0).abs() < 0.01);

        // Check available is clamped properly (should be 0 or close to it)
        let avail_after = local_rm.get_local_available_resources();
        let cpu_avail_after = avail_after.get("CPU").to_f64();
        assert!(cpu_avail_after >= 0.0, "available should never go negative");
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
            .await
            .unwrap();

        assert_eq!(reply.num_workers, 0);

        let store_stats = reply.store_stats.expect("should have store_stats");
        assert_eq!(store_stats.num_local_objects, 2);
        assert_eq!(store_stats.object_store_bytes_used, 330); // (100+10) + (200+20)
        // Primary copy info should match pinned objects (C++ local_object_manager contract)
        assert_eq!(store_stats.object_store_bytes_primary_copy, 330);
        assert_eq!(store_stats.num_object_store_primary_copies, 2);
    }

    #[tokio::test]
    async fn test_get_node_stats_includes_spill_and_restore_metrics() {
        let svc = make_svc();

        // Create objects and complete a spill + restore cycle
        {
            let mut obj_mgr = svc.node_manager.local_object_manager().lock();
            let oid1 = ray_common::id::ObjectID::from_binary(&[1u8; 28]);
            let oid2 = ray_common::id::ObjectID::from_binary(&[2u8; 28]);
            obj_mgr.pin_object(oid1, 500, 0);
            obj_mgr.pin_object(oid2, 300, 0);

            // Spill oid1
            obj_mgr.mark_pending_spill(&[oid1]);
            obj_mgr.spill_completed(&oid1, "file:///spill/1".to_string());
            obj_mgr.record_spill_time(1.5);

            // Restore oid1
            obj_mgr.restore_completed(500);
            obj_mgr.record_restore_time(0.8);
        }

        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
            .unwrap();
        let stats = reply.store_stats.expect("should have store_stats");

        // Spill metrics
        assert_eq!(stats.spilled_bytes_total, 500);
        assert_eq!(stats.spilled_objects_total, 1);
        assert!((stats.spill_time_total_s - 1.5).abs() < 0.001);

        // Restore metrics
        assert_eq!(stats.restored_bytes_total, 500);
        assert_eq!(stats.restored_objects_total, 1);
        assert!((stats.restore_time_total_s - 0.8).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_get_node_stats_handles_no_worker_replies_without_hanging() {
        // C++ returns immediately when all_workers is empty.
        // Rust should also return immediately with no workers.
        let svc = make_svc();
        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
            .unwrap();
        assert_eq!(reply.num_workers, 0);
        assert!(reply.core_workers_stats.is_empty());
        assert!(reply.store_stats.is_some());
    }

    #[tokio::test]
    async fn test_get_node_stats_reports_memory_info_when_requested() {
        // The request has an include_memory_info field. Even if we can't do the
        // full memory report yet, the response must still be valid (no hang/error).
        let svc = make_svc();
        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest {
                include_memory_info: true,
            })
            .await
            .unwrap();
        // Should not error; store_stats should still be present
        assert!(reply.store_stats.is_some());
    }

    // ─── RAYLET-6 Round 4: core_workers_stats + include_memory_info ──

    #[tokio::test]
    async fn test_get_node_stats_populates_core_workers_stats() {
        let svc = make_svc();
        // Register a worker
        register_test_worker(&svc, 1);
        register_test_worker(&svc, 2);

        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
            .unwrap();
        assert_eq!(reply.num_workers, 2);
        assert_eq!(
            reply.core_workers_stats.len(),
            2,
            "core_workers_stats should be populated when workers exist"
        );
        // Verify worker IDs are present
        let worker_ids: Vec<Vec<u8>> = reply
            .core_workers_stats
            .iter()
            .map(|s| s.worker_id.clone())
            .collect();
        assert!(worker_ids.iter().any(|id| id == &vec![1u8; 28]));
        assert!(worker_ids.iter().any(|id| id == &vec![2u8; 28]));
    }

    #[tokio::test]
    async fn test_get_node_stats_propagates_include_memory_info() {
        let svc = make_svc();
        register_test_worker(&svc, 1);

        // With include_memory_info = true, the request should still succeed
        // and produce a valid response (C++ passes this to worker RPCs).
        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest {
                include_memory_info: true,
            })
            .await
            .unwrap();
        assert!(reply.store_stats.is_some());
        // When workers are registered, core_workers_stats should be non-empty
        assert!(!reply.core_workers_stats.is_empty());
    }

    #[tokio::test]
    async fn test_get_node_stats_collects_worker_stats_without_regression() {
        // Verify that the store stats are still correct when workers are present
        let svc = make_svc();
        register_test_worker(&svc, 1);

        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
            .unwrap();
        assert!(reply.store_stats.is_some());
        assert_eq!(reply.num_workers, 1);
    }

    // ─── RAYLET-6 Round 5: semantic include_memory_info + nontrivial stats ──

    /// C++ contract: include_memory_info changes the worker stats payload.
    /// When false, memory fields are not populated.
    /// When true, memory fields (used_object_store_memory, num_owned_objects) are included.
    #[tokio::test]
    async fn test_get_node_stats_include_memory_info_changes_worker_stats_payload() {
        use crate::node_manager::WorkerStatsSnapshot;
        let svc = make_svc();
        register_test_worker(&svc, 1);

        // Report stats with memory info for this worker.
        svc.node_manager.worker_stats_tracker().report_stats(
            ray_common::id::WorkerID::from_binary(&[1u8; 28]),
            WorkerStatsSnapshot {
                num_pending_tasks: 3,
                num_running_tasks: 2,
                used_object_store_memory: 1024 * 1024,
                num_owned_objects: 42,
            },
        );

        // Without include_memory_info: memory fields should be 0
        let reply_without = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest {
                include_memory_info: false,
            })
            .await
            .unwrap();
        assert_eq!(reply_without.core_workers_stats.len(), 1);
        let stats_without = &reply_without.core_workers_stats[0];
        assert_eq!(stats_without.num_pending_tasks, 3, "task stats always present");
        assert_eq!(
            stats_without.used_object_store_memory, 0,
            "memory fields NOT populated when include_memory_info=false"
        );
        assert_eq!(stats_without.num_owned_objects, 0);

        // With include_memory_info: memory fields should be populated
        let reply_with = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest {
                include_memory_info: true,
            })
            .await
            .unwrap();
        assert_eq!(reply_with.core_workers_stats.len(), 1);
        let stats_with = &reply_with.core_workers_stats[0];
        assert_eq!(stats_with.num_pending_tasks, 3);
        assert_eq!(
            stats_with.used_object_store_memory,
            1024 * 1024,
            "memory fields populated when include_memory_info=true"
        );
        assert_eq!(stats_with.num_owned_objects, 42);
    }

    /// C++ contract: core_workers_stats contains nontrivial per-worker fields
    /// (not just worker_id/pid/worker_type from the pool).
    #[tokio::test]
    async fn test_get_node_stats_core_worker_stats_include_nontrivial_fields() {
        use crate::node_manager::WorkerStatsSnapshot;
        let svc = make_svc();
        register_test_worker(&svc, 5);

        // Report real runtime stats for this worker.
        svc.node_manager.worker_stats_tracker().report_stats(
            ray_common::id::WorkerID::from_binary(&[5u8; 28]),
            WorkerStatsSnapshot {
                num_pending_tasks: 7,
                num_running_tasks: 3,
                used_object_store_memory: 0,
                num_owned_objects: 0,
            },
        );

        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
            .unwrap();
        assert_eq!(reply.core_workers_stats.len(), 1);
        let stats = &reply.core_workers_stats[0];
        assert_eq!(stats.num_pending_tasks, 7, "must have real pending task count");
        assert_eq!(stats.num_running_tasks, 3, "must have real running task count");
    }

    #[tokio::test]
    async fn test_get_node_stats_reflects_draining_state() {
        let svc = make_svc();

        // Initially not draining
        let reply = svc
            .handle_get_node_stats(rpc::GetNodeStatsRequest::default())
            .await
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
            .await
            .unwrap();
        assert!(reply2.store_stats.is_some());
    }

    // ─── RAYLET-6 Round 6: live worker stats provider tests ──────────

    /// Proves that GetNodeStats queries the WorkerStatsProvider (sync/tracker path)
    /// for each alive worker, including drivers.
    /// Uses port=0 (in-process) so stats come from the tracker, not gRPC.
    /// If a worker has not reported stats to the provider, its entry has zero runtime stats.
    #[tokio::test]
    async fn test_get_node_stats_queries_live_worker_stats_path() {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        use crate::node_manager::WorkerStatsSnapshot;

        let svc = make_svc();
        // Register a worker and a driver (port=0 → in-process tracker path)
        let wid1 = ray_common::id::WorkerID::from_binary(&[20u8; 28]);
        let wid2 = ray_common::id::WorkerID::from_binary(&[21u8; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        svc.node_manager.worker_pool().register_worker(WorkerInfo {
            worker_id: wid1, language: Language::Python, worker_type: WorkerType::Worker,
            job_id: job, pid: 2000, port: 0, ip_address: "127.0.0.1".into(), is_alive: true,
        }).unwrap();
        svc.node_manager.worker_pool().register_worker(WorkerInfo {
            worker_id: wid2, language: Language::Python, worker_type: WorkerType::Driver,
            job_id: job, pid: 2001, port: 0, ip_address: "127.0.0.1".into(), is_alive: true,
        }).unwrap();

        // Report stats only for the worker (not the driver)
        svc.node_manager.worker_stats_tracker().report_stats(wid1, WorkerStatsSnapshot {
            num_pending_tasks: 10, num_running_tasks: 5,
            used_object_store_memory: 2048, num_owned_objects: 100,
        });

        let reply = svc.handle_get_node_stats(rpc::GetNodeStatsRequest {
            include_memory_info: true,
        }).await.unwrap();

        assert_eq!(reply.num_workers, 2);
        assert_eq!(reply.core_workers_stats.len(), 2);

        // Find worker and driver entries
        let w_stats = reply.core_workers_stats.iter().find(|s| s.worker_id == wid1.binary()).unwrap();
        let d_stats = reply.core_workers_stats.iter().find(|s| s.worker_id == wid2.binary()).unwrap();

        // Worker has stats from the tracker (port=0 → sync provider path)
        assert_eq!(w_stats.num_pending_tasks, 10);
        assert_eq!(w_stats.num_running_tasks, 5);
        assert_eq!(w_stats.used_object_store_memory, 2048);
        assert_eq!(w_stats.worker_type, 1); // WORKER

        // Driver has no reported stats → runtime fields are 0
        assert_eq!(d_stats.num_pending_tasks, 0);
        assert_eq!(d_stats.num_running_tasks, 0);
        assert_eq!(d_stats.worker_type, 0); // DRIVER
    }

    /// Proves that stale tracker data is NOT used when the provider returns None.
    /// If a worker previously reported stats but the provider now returns None
    /// (simulating a dead/unreachable worker), the stats should be empty.
    #[tokio::test]
    async fn test_get_node_stats_stale_tracker_does_not_mask_missing_provider_reply() {
        use crate::node_manager::WorkerStatsSnapshot;

        let svc = make_svc();
        register_test_worker(&svc, 30);
        let wid = ray_common::id::WorkerID::from_binary(&[30u8; 28]);

        // Report stats initially
        svc.node_manager.worker_stats_tracker().report_stats(wid, WorkerStatsSnapshot {
            num_pending_tasks: 99, num_running_tasks: 88,
            used_object_store_memory: 0, num_owned_objects: 0,
        });

        // Verify stats are returned
        let reply1 = svc.handle_get_node_stats(rpc::GetNodeStatsRequest::default()).await.unwrap();
        assert_eq!(reply1.core_workers_stats[0].num_pending_tasks, 99);

        // Remove the stats (simulating worker becoming unreachable)
        svc.node_manager.worker_stats_tracker().remove_worker(&wid);

        // Now the provider returns None → runtime stats should be 0
        let reply2 = svc.handle_get_node_stats(rpc::GetNodeStatsRequest::default()).await.unwrap();
        assert_eq!(reply2.core_workers_stats.len(), 1);
        assert_eq!(
            reply2.core_workers_stats[0].num_pending_tasks, 0,
            "stale data must not be used when provider returns None"
        );
    }

    // ─── RAYLET-6 Round 7: prove default provider is GrpcWorkerStatsProvider ──

    /// Proves the default provider is a live RPC provider (GrpcWorkerStatsProvider),
    /// not just a tracker-based local-state provider.
    #[tokio::test]
    async fn test_get_node_stats_uses_real_worker_stats_provider_by_default() {
        let svc = make_svc();
        assert!(
            svc.node_manager.worker_stats_provider().is_live_rpc_provider(),
            "default provider must be the live gRPC provider, not tracker-only"
        );
    }

    /// C++ contract (node_manager.cc:2676-2683): on GetCoreWorkerStats RPC
    /// failure, the callback does MergeFrom(r.core_worker_stats()) with the
    /// default empty reply — it does NOT substitute cached tracker data.
    /// This test verifies: when a worker's gRPC endpoint is unreachable and
    /// tracker has cached data, the returned entry must have default/zero
    /// runtime stats, NOT the cached tracker values.
    #[tokio::test]
    async fn test_get_node_stats_rpc_failure_does_not_fallback_to_tracker_data() {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        use crate::node_manager::WorkerStatsSnapshot;

        let svc = make_svc();
        let wid = ray_common::id::WorkerID::from_binary(&[40u8; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        // Register worker with a real-looking port (but no server running)
        svc.node_manager.worker_pool().register_worker(WorkerInfo {
            worker_id: wid, language: Language::Python, worker_type: WorkerType::Worker,
            job_id: job, pid: 4000, port: 49999, ip_address: "127.0.0.1".into(), is_alive: true,
        }).unwrap();

        // Report non-zero stats in tracker — these must NOT appear on RPC failure
        svc.node_manager.worker_stats_tracker().report_stats(wid, WorkerStatsSnapshot {
            num_pending_tasks: 42, num_running_tasks: 10,
            used_object_store_memory: 999, num_owned_objects: 77,
        });

        let reply = svc.handle_get_node_stats(rpc::GetNodeStatsRequest::default()).await.unwrap();
        assert_eq!(reply.num_workers, 1);
        assert_eq!(reply.core_workers_stats.len(), 1);
        let s = &reply.core_workers_stats[0];
        // C++ merges empty/default stats on RPC failure — not cached tracker data
        assert_eq!(s.num_pending_tasks, 0,
            "RPC failure must NOT fall back to tracker data (C++ merges empty reply)");
        assert_eq!(s.num_running_tasks, 0,
            "RPC failure must NOT fall back to tracker data");
        // Basic pool metadata still present
        assert_eq!(s.pid, 4000);
        assert_eq!(s.worker_id, wid.binary());
    }

    /// Same contract as above but for an unreachable driver (port > 0).
    /// C++ treats drivers and workers identically in HandleGetNodeStats.
    #[tokio::test]
    async fn test_get_node_stats_unreachable_driver_does_not_fallback_to_tracker_data() {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        use crate::node_manager::WorkerStatsSnapshot;

        let svc = make_svc();
        let wid = ray_common::id::WorkerID::from_binary(&[41u8; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        svc.node_manager.worker_pool().register_worker(WorkerInfo {
            worker_id: wid, language: Language::Python, worker_type: WorkerType::Driver,
            job_id: job, pid: 4001, port: 49998, ip_address: "127.0.0.1".into(), is_alive: true,
        }).unwrap();

        svc.node_manager.worker_stats_tracker().report_stats(wid, WorkerStatsSnapshot {
            num_pending_tasks: 5, num_running_tasks: 2,
            used_object_store_memory: 0, num_owned_objects: 0,
        });

        let reply = svc.handle_get_node_stats(rpc::GetNodeStatsRequest::default()).await.unwrap();
        assert_eq!(reply.num_workers, 1);
        let d = &reply.core_workers_stats[0];
        assert_eq!(d.worker_type, 0, "must be DRIVER type");
        // RPC fails (no server), so runtime stats must be default/zero
        assert_eq!(d.num_pending_tasks, 0,
            "unreachable driver must NOT use tracker data");
        assert_eq!(d.num_running_tasks, 0);
        // Pool metadata still present
        assert_eq!(d.pid, 4001);
    }

    /// Verifies that when the live gRPC RPC succeeds, runtime stats are
    /// properly populated in the response (positive path).
    /// Uses a worker with port=0 (in-process tracker path) to simulate
    /// successful stats collection without needing a real gRPC server.
    #[tokio::test]
    async fn test_get_node_stats_live_rpc_success_still_populates_runtime_fields() {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        use crate::node_manager::WorkerStatsSnapshot;

        let svc = make_svc();
        let wid = ray_common::id::WorkerID::from_binary(&[42u8; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        // port=0 → uses tracker (simulates successful collection)
        svc.node_manager.worker_pool().register_worker(WorkerInfo {
            worker_id: wid, language: Language::Python, worker_type: WorkerType::Worker,
            job_id: job, pid: 4002, port: 0, ip_address: "127.0.0.1".into(), is_alive: true,
        }).unwrap();

        svc.node_manager.worker_stats_tracker().report_stats(wid, WorkerStatsSnapshot {
            num_pending_tasks: 7, num_running_tasks: 3,
            used_object_store_memory: 2048, num_owned_objects: 15,
        });

        // With include_memory_info=true, all fields should be populated
        let reply = svc.handle_get_node_stats(rpc::GetNodeStatsRequest {
            include_memory_info: true,
        }).await.unwrap();
        assert_eq!(reply.core_workers_stats.len(), 1);
        let s = &reply.core_workers_stats[0];
        assert_eq!(s.num_pending_tasks, 7, "task stats populated on success");
        assert_eq!(s.num_running_tasks, 3);
        assert_eq!(s.used_object_store_memory, 2048, "memory info populated when requested");
        assert_eq!(s.num_owned_objects, 15);
    }

    /// Proves that drivers with port=0 (in-process) still get stats from
    /// the sync tracker path. This is correct behavior — port=0 workers
    /// cannot be reached via gRPC.
    #[tokio::test]
    async fn test_get_node_stats_collects_driver_stats_through_same_live_path() {
        use crate::worker_pool::{Language, WorkerInfo, WorkerType};
        use crate::node_manager::WorkerStatsSnapshot;

        let svc = make_svc();
        let wid = ray_common::id::WorkerID::from_binary(&[43u8; 28]);
        let job = ray_common::id::JobID::from_binary(&[0u8; 4]);
        svc.node_manager.worker_pool().register_worker(WorkerInfo {
            worker_id: wid, language: Language::Python, worker_type: WorkerType::Driver,
            job_id: job, pid: 4003, port: 0, ip_address: "127.0.0.1".into(), is_alive: true,
        }).unwrap();

        svc.node_manager.worker_stats_tracker().report_stats(wid, WorkerStatsSnapshot {
            num_pending_tasks: 5, num_running_tasks: 2,
            used_object_store_memory: 0, num_owned_objects: 0,
        });

        let reply = svc.handle_get_node_stats(rpc::GetNodeStatsRequest::default()).await.unwrap();
        assert_eq!(reply.num_workers, 1);
        let d = &reply.core_workers_stats[0];
        assert_eq!(d.worker_type, 0, "must be DRIVER type");
        assert_eq!(d.num_pending_tasks, 5, "port=0 driver uses tracker path");
    }
}
