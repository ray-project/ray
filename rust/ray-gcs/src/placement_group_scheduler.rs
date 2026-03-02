// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Placement Group Scheduler — orchestrates bundle-to-node assignment.
//!
//! Replaces `src/ray/gcs/gcs_placement_group_scheduler.h/cc`.
//!
//! Coordinates the scheduling of placement group bundles across the cluster:
//! 1. Converts placement strategy to scheduling options
//! 2. Invokes bundle scheduling policies to assign bundles to nodes
//! 3. Tracks 2-phase commit status (prepare → commit) per placement group
//! 4. Handles resource acquisition, commitment, and rollback

use std::collections::HashMap;

use parking_lot::RwLock;
use ray_common::id::PlacementGroupID;
use ray_common::scheduling::{FixedPoint, ResourceSet};

/// A bundle identifier: (placement_group_id, bundle_index).
pub type BundleID = (PlacementGroupID, i32);

/// Maps bundle IDs to their assigned node IDs.
pub type ScheduleMap = HashMap<BundleID, String>;

/// Placement strategy constants matching protobuf values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PlacementStrategy {
    Pack = 0,
    Spread = 1,
    StrictPack = 2,
    StrictSpread = 3,
}

impl From<i32> for PlacementStrategy {
    fn from(v: i32) -> Self {
        match v {
            0 => PlacementStrategy::Pack,
            1 => PlacementStrategy::Spread,
            2 => PlacementStrategy::StrictPack,
            3 => PlacementStrategy::StrictSpread,
            _ => PlacementStrategy::Pack,
        }
    }
}

/// Status of the 2-phase commit for a placement group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePhase {
    /// Resources are being prepared (phase 1).
    Preparing,
    /// Resources are being committed (phase 2).
    Committing,
    /// Scheduling completed successfully.
    Committed,
    /// Scheduling was cancelled or failed.
    Cancelled,
}

/// Tracks the 2-phase commit status for a placement group.
#[derive(Debug)]
pub struct LeaseStatusTracker {
    pub pg_id: PlacementGroupID,
    pub phase: LeasePhase,
    /// Bundle locations: bundle_id → (node_id, resource_requirements).
    pub bundle_locations: HashMap<BundleID, (String, ResourceSet)>,
    /// Bundles that have been prepared.
    pub prepared_bundles: Vec<BundleID>,
    /// Bundles that have been committed.
    pub committed_bundles: Vec<BundleID>,
    /// Number of outstanding prepare requests.
    pub prepare_requests_remaining: usize,
    /// Number of outstanding commit requests.
    pub commit_requests_remaining: usize,
    /// Whether any prepare request failed.
    pub prepare_failed: bool,
}

impl LeaseStatusTracker {
    pub fn new(
        pg_id: PlacementGroupID,
        bundle_locations: HashMap<BundleID, (String, ResourceSet)>,
    ) -> Self {
        let num_bundles = bundle_locations.len();
        Self {
            pg_id,
            phase: LeasePhase::Preparing,
            bundle_locations,
            prepared_bundles: Vec::new(),
            committed_bundles: Vec::new(),
            prepare_requests_remaining: num_bundles,
            commit_requests_remaining: num_bundles,
            prepare_failed: false,
        }
    }

    /// Record a successful prepare for a bundle.
    pub fn mark_prepared(&mut self, bundle_id: &BundleID) {
        self.prepared_bundles.push(*bundle_id);
        self.prepare_requests_remaining = self.prepare_requests_remaining.saturating_sub(1);
    }

    /// Record a failed prepare for a bundle.
    pub fn mark_prepare_failed(&mut self, _bundle_id: &BundleID) {
        self.prepare_failed = true;
        self.prepare_requests_remaining = self.prepare_requests_remaining.saturating_sub(1);
    }

    /// Record a successful commit for a bundle.
    pub fn mark_committed(&mut self, bundle_id: &BundleID) {
        self.committed_bundles.push(*bundle_id);
        self.commit_requests_remaining = self.commit_requests_remaining.saturating_sub(1);
    }

    /// Check if all prepare requests have returned.
    pub fn all_prepared(&self) -> bool {
        self.prepare_requests_remaining == 0
    }

    /// Check if all commit requests have returned.
    pub fn all_committed(&self) -> bool {
        self.commit_requests_remaining == 0
    }

    /// Check if the prepare phase succeeded (all prepared, none failed).
    pub fn prepare_succeeded(&self) -> bool {
        self.all_prepared() && !self.prepare_failed
    }
}

/// Simplified cluster resource view for GCS-side scheduling.
#[derive(Default)]
pub struct ClusterResourceView {
    nodes: HashMap<String, NodeResourceState>,
}

/// Resource state for a single node.
#[derive(Debug, Clone)]
pub struct NodeResourceState {
    pub total: ResourceSet,
    pub available: ResourceSet,
    pub is_alive: bool,
}

/// Sum of all resource values in a ResourceSet (for scoring nodes).
fn resource_score(rs: &ResourceSet) -> FixedPoint {
    let mut sum = FixedPoint::ZERO;
    for (_, amount) in rs.iter() {
        sum += amount;
    }
    sum
}

impl ClusterResourceView {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a node's resource state.
    pub fn update_node(&mut self, node_id: String, state: NodeResourceState) {
        self.nodes.insert(node_id, state);
    }

    /// Remove a node (on death).
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.remove(node_id);
    }

    /// Get all alive node IDs.
    pub fn alive_node_ids(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|(_, s)| s.is_alive)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check if a node can satisfy the resource request (available).
    pub fn has_available_resources(&self, node_id: &str, request: &ResourceSet) -> bool {
        if let Some(state) = self.nodes.get(node_id) {
            state.is_alive && state.available.is_superset_of(request)
        } else {
            false
        }
    }

    /// Check if a node could ever satisfy the request (total capacity).
    pub fn has_feasible_resources(&self, node_id: &str, request: &ResourceSet) -> bool {
        if let Some(state) = self.nodes.get(node_id) {
            state.total.is_superset_of(request)
        } else {
            false
        }
    }

    /// Subtract resources from a node's available pool.
    pub fn subtract_available(&mut self, node_id: &str, request: &ResourceSet) -> bool {
        if let Some(state) = self.nodes.get_mut(node_id) {
            if !state.available.is_superset_of(request) {
                return false;
            }
            state.available.subtract(request);
            true
        } else {
            false
        }
    }

    /// Add resources back to a node's available pool.
    pub fn add_available(&mut self, node_id: &str, request: &ResourceSet) {
        if let Some(state) = self.nodes.get_mut(node_id) {
            state.available.add(request);
        }
    }

    /// Get a node's state.
    pub fn get_node(&self, node_id: &str) -> Option<&NodeResourceState> {
        self.nodes.get(node_id)
    }

    /// Number of alive nodes.
    pub fn num_alive_nodes(&self) -> usize {
        self.nodes.values().filter(|s| s.is_alive).count()
    }
}

/// Result of a scheduling attempt.
#[derive(Debug)]
pub enum SchedulingResult {
    /// Successfully assigned all bundles to nodes.
    Success(ScheduleMap),
    /// Failed but retryable (some resources are temporarily unavailable).
    Failed,
    /// Will never succeed with current cluster (infeasible resources).
    Infeasible,
}

/// The GCS Placement Group Scheduler.
///
/// Orchestrates the scheduling of placement group bundles by:
/// 1. Converting the PG strategy to a scheduling algorithm
/// 2. Assigning bundles to nodes using the cluster resource view
/// 3. Managing the 2-phase commit lifecycle
pub struct GcsPlacementGroupScheduler {
    /// Current cluster resource state.
    resource_view: RwLock<ClusterResourceView>,
    /// Active lease trackers for placement groups being scheduled.
    lease_trackers: RwLock<HashMap<PlacementGroupID, LeaseStatusTracker>>,
    /// Index: node_id → set of (pg_id, bundle_index) on that node.
    bundle_location_index: RwLock<HashMap<String, Vec<BundleID>>>,
}

impl Default for GcsPlacementGroupScheduler {
    fn default() -> Self {
        Self {
            resource_view: RwLock::new(ClusterResourceView::new()),
            lease_trackers: RwLock::new(HashMap::new()),
            bundle_location_index: RwLock::new(HashMap::new()),
        }
    }
}

impl GcsPlacementGroupScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update a node's resource state in the cluster view.
    pub fn update_node_resources(&self, node_id: String, state: NodeResourceState) {
        self.resource_view.write().update_node(node_id, state);
    }

    /// Remove a node from the cluster view.
    pub fn remove_node(&self, node_id: &str) {
        self.resource_view.write().remove_node(node_id);
        let _bundles_on_node = self
            .bundle_location_index
            .write()
            .remove(node_id)
            .unwrap_or_default();
    }

    /// Schedule unplaced bundles for a placement group.
    pub fn schedule_placement_group(
        &self,
        pg_id: PlacementGroupID,
        bundles: &[BundleSpec],
        strategy: PlacementStrategy,
        soft_target_node_id: Option<&str>,
        existing_bundle_locations: &HashMap<BundleID, String>,
    ) -> SchedulingResult {
        let unplaced: Vec<(usize, &BundleSpec)> = bundles
            .iter()
            .enumerate()
            .filter(|(idx, _)| {
                let bid = (pg_id, *idx as i32);
                !existing_bundle_locations.contains_key(&bid)
            })
            .collect();

        if unplaced.is_empty() {
            return SchedulingResult::Success(HashMap::new());
        }

        let view = self.resource_view.read();

        match strategy {
            PlacementStrategy::Pack => self.schedule_pack(&view, pg_id, &unplaced),
            PlacementStrategy::Spread => {
                self.schedule_spread(&view, pg_id, &unplaced, existing_bundle_locations)
            }
            PlacementStrategy::StrictPack => {
                self.schedule_strict_pack(&view, pg_id, &unplaced, soft_target_node_id)
            }
            PlacementStrategy::StrictSpread => {
                self.schedule_strict_spread(&view, pg_id, &unplaced, existing_bundle_locations)
            }
        }
    }

    /// PACK strategy: pack bundles onto as few nodes as possible.
    fn schedule_pack(
        &self,
        view: &ClusterResourceView,
        pg_id: PlacementGroupID,
        unplaced: &[(usize, &BundleSpec)],
    ) -> SchedulingResult {
        let alive_nodes = view.alive_node_ids();
        if alive_nodes.is_empty() {
            return SchedulingResult::Failed;
        }

        // Check feasibility
        for (_, bundle) in unplaced {
            let feasible = alive_nodes
                .iter()
                .any(|nid| view.has_feasible_resources(nid, &bundle.resources));
            if !feasible {
                return SchedulingResult::Infeasible;
            }
        }

        // Track remaining available resources per node
        let mut remaining: HashMap<String, ResourceSet> = alive_nodes
            .iter()
            .filter_map(|nid| {
                view.get_node(nid)
                    .map(|s| (nid.clone(), s.available.clone()))
            })
            .collect();

        let mut schedule = ScheduleMap::new();

        for &(idx, bundle) in unplaced {
            let bid = (pg_id, idx as i32);

            // Find node with the most remaining resources that can fit this bundle
            let best_node = remaining
                .iter()
                .filter(|(_, avail)| avail.is_superset_of(&bundle.resources))
                .max_by_key(|(_, avail)| resource_score(avail))
                .map(|(nid, _)| nid.clone());

            match best_node {
                Some(node_id) => {
                    if let Some(avail) = remaining.get_mut(&node_id) {
                        avail.subtract(&bundle.resources);
                    }
                    schedule.insert(bid, node_id);
                }
                None => {
                    return SchedulingResult::Failed;
                }
            }
        }

        SchedulingResult::Success(schedule)
    }

    /// SPREAD strategy: spread bundles across distinct nodes.
    fn schedule_spread(
        &self,
        view: &ClusterResourceView,
        pg_id: PlacementGroupID,
        unplaced: &[(usize, &BundleSpec)],
        existing_locations: &HashMap<BundleID, String>,
    ) -> SchedulingResult {
        let alive_nodes = view.alive_node_ids();
        if alive_nodes.is_empty() {
            return SchedulingResult::Failed;
        }

        // Check feasibility
        for (_, bundle) in unplaced {
            let feasible = alive_nodes
                .iter()
                .any(|nid| view.has_feasible_resources(nid, &bundle.resources));
            if !feasible {
                return SchedulingResult::Infeasible;
            }
        }

        let mut remaining: HashMap<String, ResourceSet> = alive_nodes
            .iter()
            .filter_map(|nid| {
                view.get_node(nid)
                    .map(|s| (nid.clone(), s.available.clone()))
            })
            .collect();

        let mut used_nodes: Vec<String> = existing_locations.values().cloned().collect();
        used_nodes.sort();
        used_nodes.dedup();

        let mut schedule = ScheduleMap::new();

        for &(idx, bundle) in unplaced {
            let bid = (pg_id, idx as i32);

            // First try: unused node with available resources
            let new_node = remaining
                .iter()
                .filter(|(nid, avail)| {
                    !used_nodes.contains(nid)
                        && !schedule.values().any(|v| v == *nid)
                        && avail.is_superset_of(&bundle.resources)
                })
                .max_by_key(|(_, avail)| resource_score(avail))
                .map(|(nid, _)| nid.clone());

            // Fallback: any node with available resources
            let chosen = new_node.or_else(|| {
                remaining
                    .iter()
                    .filter(|(_, avail)| avail.is_superset_of(&bundle.resources))
                    .max_by_key(|(_, avail)| resource_score(avail))
                    .map(|(nid, _)| nid.clone())
            });

            match chosen {
                Some(node_id) => {
                    if let Some(avail) = remaining.get_mut(&node_id) {
                        avail.subtract(&bundle.resources);
                    }
                    schedule.insert(bid, node_id);
                }
                None => {
                    return SchedulingResult::Failed;
                }
            }
        }

        SchedulingResult::Success(schedule)
    }

    /// STRICT_PACK strategy: all bundles on a single node.
    fn schedule_strict_pack(
        &self,
        view: &ClusterResourceView,
        pg_id: PlacementGroupID,
        unplaced: &[(usize, &BundleSpec)],
        soft_target_node_id: Option<&str>,
    ) -> SchedulingResult {
        let alive_nodes = view.alive_node_ids();
        if alive_nodes.is_empty() {
            return SchedulingResult::Failed;
        }

        // Aggregate all bundle requirements
        let mut aggregated = ResourceSet::new();
        for (_, bundle) in unplaced {
            aggregated.add(&bundle.resources);
        }

        // Check feasibility on any node
        let has_feasible = alive_nodes
            .iter()
            .any(|nid| view.has_feasible_resources(nid, &aggregated));

        if !has_feasible {
            return SchedulingResult::Infeasible;
        }

        // Prefer soft target node if it has available resources
        if let Some(target) = soft_target_node_id {
            if view.has_available_resources(target, &aggregated) {
                let mut schedule = ScheduleMap::new();
                for &(idx, _) in unplaced {
                    schedule.insert((pg_id, idx as i32), target.to_string());
                }
                return SchedulingResult::Success(schedule);
            }
        }

        // Find best available node
        let available_node = alive_nodes
            .iter()
            .filter(|nid| view.has_available_resources(nid, &aggregated))
            .max_by_key(|nid| {
                view.get_node(nid)
                    .map(|s| resource_score(&s.available))
                    .unwrap_or(FixedPoint::ZERO)
            });

        match available_node {
            Some(node_id) => {
                let mut schedule = ScheduleMap::new();
                for &(idx, _) in unplaced {
                    schedule.insert((pg_id, idx as i32), node_id.clone());
                }
                SchedulingResult::Success(schedule)
            }
            None => SchedulingResult::Failed,
        }
    }

    /// STRICT_SPREAD strategy: each bundle on a different node.
    fn schedule_strict_spread(
        &self,
        view: &ClusterResourceView,
        pg_id: PlacementGroupID,
        unplaced: &[(usize, &BundleSpec)],
        existing_locations: &HashMap<BundleID, String>,
    ) -> SchedulingResult {
        let alive_nodes = view.alive_node_ids();

        // Nodes already used by this placement group
        let mut excluded_nodes: Vec<String> = existing_locations.values().cloned().collect();
        excluded_nodes.sort();
        excluded_nodes.dedup();

        let available_nodes: Vec<String> = alive_nodes
            .into_iter()
            .filter(|nid| !excluded_nodes.contains(nid))
            .collect();

        // Must have enough distinct nodes for all unplaced bundles
        if available_nodes.len() < unplaced.len() {
            let total_potential = view.num_alive_nodes() - excluded_nodes.len();
            if total_potential < unplaced.len() {
                return SchedulingResult::Infeasible;
            }
            return SchedulingResult::Failed;
        }

        // Check feasibility
        for (_, bundle) in unplaced {
            let feasible = available_nodes
                .iter()
                .any(|nid| view.has_feasible_resources(nid, &bundle.resources));
            if !feasible {
                return SchedulingResult::Infeasible;
            }
        }

        let mut remaining: HashMap<String, ResourceSet> = available_nodes
            .iter()
            .filter_map(|nid| {
                view.get_node(nid)
                    .map(|s| (nid.clone(), s.available.clone()))
            })
            .collect();

        let mut schedule = ScheduleMap::new();
        let mut assigned_nodes: Vec<String> = Vec::new();

        for &(idx, bundle) in unplaced {
            let bid = (pg_id, idx as i32);

            let best_node = remaining
                .iter()
                .filter(|(nid, avail)| {
                    !assigned_nodes.contains(nid) && avail.is_superset_of(&bundle.resources)
                })
                .max_by_key(|(_, avail)| resource_score(avail))
                .map(|(nid, _)| nid.clone());

            match best_node {
                Some(node_id) => {
                    if let Some(avail) = remaining.get_mut(&node_id) {
                        avail.subtract(&bundle.resources);
                    }
                    assigned_nodes.push(node_id.clone());
                    schedule.insert(bid, node_id);
                }
                None => {
                    return SchedulingResult::Failed;
                }
            }
        }

        SchedulingResult::Success(schedule)
    }

    /// Acquire (reserve) resources for the scheduled bundles.
    pub fn acquire_bundle_resources(
        &self,
        schedule: &ScheduleMap,
        bundles: &[BundleSpec],
    ) -> bool {
        let mut view = self.resource_view.write();

        // Verify all resources are still available
        for ((_, idx), node_id) in schedule {
            let bundle = &bundles[*idx as usize];
            if !view.has_available_resources(node_id, &bundle.resources) {
                return false;
            }
        }

        // Subtract all resources
        for ((_, idx), node_id) in schedule {
            let bundle = &bundles[*idx as usize];
            view.subtract_available(node_id, &bundle.resources);
        }

        true
    }

    /// Start tracking the 2-phase commit for a placement group.
    pub fn start_lease_tracking(
        &self,
        pg_id: PlacementGroupID,
        schedule: &ScheduleMap,
        bundles: &[BundleSpec],
    ) {
        let bundle_locations: HashMap<BundleID, (String, ResourceSet)> = schedule
            .iter()
            .map(|(&bid, node_id)| {
                let resources = bundles[bid.1 as usize].resources.clone();
                (bid, (node_id.clone(), resources))
            })
            .collect();

        let tracker = LeaseStatusTracker::new(pg_id, bundle_locations);
        self.lease_trackers.write().insert(pg_id, tracker);
    }

    /// Handle a prepare response for a bundle.
    pub fn on_bundle_prepared(
        &self,
        pg_id: &PlacementGroupID,
        bundle_id: &BundleID,
        success: bool,
    ) {
        let mut trackers = self.lease_trackers.write();
        if let Some(tracker) = trackers.get_mut(pg_id) {
            if success {
                tracker.mark_prepared(bundle_id);
            } else {
                tracker.mark_prepare_failed(bundle_id);
            }
        }
    }

    /// Check if all prepare requests for a PG have returned.
    /// Returns (all_done, all_succeeded).
    pub fn check_prepare_status(&self, pg_id: &PlacementGroupID) -> Option<(bool, bool)> {
        let trackers = self.lease_trackers.read();
        trackers
            .get(pg_id)
            .map(|t| (t.all_prepared(), t.prepare_succeeded()))
    }

    /// Handle a commit response for a bundle.
    pub fn on_bundle_committed(&self, pg_id: &PlacementGroupID, bundle_id: &BundleID) {
        let mut trackers = self.lease_trackers.write();
        if let Some(tracker) = trackers.get_mut(pg_id) {
            tracker.mark_committed(bundle_id);
        }
    }

    /// Check if all commit requests for a PG have returned.
    pub fn check_commit_status(&self, pg_id: &PlacementGroupID) -> Option<bool> {
        let trackers = self.lease_trackers.read();
        trackers.get(pg_id).map(|t| t.all_committed())
    }

    /// Commit bundle resources: update the bundle location index.
    pub fn commit_bundle_resources(&self, pg_id: PlacementGroupID, schedule: &ScheduleMap) {
        let mut index = self.bundle_location_index.write();
        for (&bid, node_id) in schedule {
            index.entry(node_id.clone()).or_default().push(bid);
        }

        let mut trackers = self.lease_trackers.write();
        if let Some(tracker) = trackers.get_mut(&pg_id) {
            tracker.phase = LeasePhase::Committed;
        }
    }

    /// Return (rollback) resources for a failed scheduling attempt.
    pub fn return_bundle_resources(&self, schedule: &ScheduleMap, bundles: &[BundleSpec]) {
        let mut view = self.resource_view.write();
        for ((_, idx), node_id) in schedule {
            let bundle = &bundles[*idx as usize];
            view.add_available(node_id, &bundle.resources);
        }
    }

    /// Cancel an in-progress scheduling and return resources.
    pub fn cancel_scheduling(&self, pg_id: &PlacementGroupID) {
        let tracker = self.lease_trackers.write().remove(pg_id);
        if let Some(tracker) = tracker {
            let mut view = self.resource_view.write();
            for (node_id, resources) in tracker.bundle_locations.values() {
                view.add_available(node_id, resources);
            }

            let mut index = self.bundle_location_index.write();
            for (node_id, _) in tracker.bundle_locations.values() {
                if let Some(bundles_on_node) = index.get_mut(node_id) {
                    bundles_on_node.retain(|bid| bid.0 != *pg_id);
                }
            }
        }
    }

    /// Destroy all resources for a placement group (on removal).
    pub fn destroy_placement_group_resources(
        &self,
        pg_id: &PlacementGroupID,
        bundles: &[BundleSpec],
    ) {
        self.lease_trackers.write().remove(pg_id);

        let mut index = self.bundle_location_index.write();
        let mut view = self.resource_view.write();

        // Find all nodes with bundles from this PG
        let nodes_to_clean: Vec<String> = index
            .iter()
            .filter(|(_, bids)| bids.iter().any(|bid| bid.0 == *pg_id))
            .map(|(nid, _)| nid.clone())
            .collect();

        for node_id in &nodes_to_clean {
            if let Some(bundle_ids) = index.get_mut(node_id) {
                let pg_bundles: Vec<BundleID> = bundle_ids
                    .iter()
                    .filter(|bid| bid.0 == *pg_id)
                    .copied()
                    .collect();

                for bid in &pg_bundles {
                    if let Some(bundle) = bundles.get(bid.1 as usize) {
                        view.add_available(node_id, &bundle.resources);
                    }
                }

                bundle_ids.retain(|bid| bid.0 != *pg_id);
            }
        }
    }

    /// Get bundles located on a specific node.
    pub fn get_bundles_on_node(&self, node_id: &str) -> Vec<BundleID> {
        self.bundle_location_index
            .read()
            .get(node_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the current phase for a placement group.
    pub fn get_lease_phase(&self, pg_id: &PlacementGroupID) -> Option<LeasePhase> {
        self.lease_trackers.read().get(pg_id).map(|t| t.phase)
    }

    /// Number of active lease trackers.
    pub fn num_active_leases(&self) -> usize {
        self.lease_trackers.read().len()
    }

    /// Get the number of alive nodes in the cluster view.
    pub fn num_alive_nodes(&self) -> usize {
        self.resource_view.read().num_alive_nodes()
    }
}

/// Specification for a single bundle in a placement group.
#[derive(Debug, Clone)]
pub struct BundleSpec {
    pub resources: ResourceSet,
}

impl BundleSpec {
    pub fn new(resources: ResourceSet) -> Self {
        Self { resources }
    }

    /// Create a BundleSpec from a proto Bundle's unit_resources.
    pub fn from_unit_resources(unit_resources: &HashMap<String, f64>) -> Self {
        let mut resources = ResourceSet::new();
        for (name, &amount) in unit_resources {
            resources.set(name.clone(), FixedPoint::from_f64(amount));
        }
        Self { resources }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pg_id(val: u8) -> PlacementGroupID {
        let mut data = [0u8; 18];
        data[0] = val;
        PlacementGroupID::from_binary(&data)
    }

    fn make_node(cpu: f64) -> NodeResourceState {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(cpu));
        NodeResourceState {
            total: total.clone(),
            available: total,
            is_alive: true,
        }
    }

    fn make_bundle(cpu: f64) -> BundleSpec {
        let mut resources = ResourceSet::new();
        resources.set("CPU".to_string(), FixedPoint::from_f64(cpu));
        BundleSpec::new(resources)
    }

    fn setup_scheduler(nodes: Vec<(&str, NodeResourceState)>) -> GcsPlacementGroupScheduler {
        let scheduler = GcsPlacementGroupScheduler::new();
        for (id, state) in nodes {
            scheduler.update_node_resources(id.to_string(), state);
        }
        scheduler
    }

    // ---- Pack strategy tests ----

    #[test]
    fn test_pack_single_bundle() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(8.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(2.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::Pack,
            None,
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 1);
                assert_eq!(schedule[&(pg_id, 0)], "node2");
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_pack_multiple_bundles_same_node() {
        // node1 has 8 CPU (enough for all 3 bundles), node2 only has 2 CPU
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(8.0)),
            ("node2", make_node(2.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(2.0), make_bundle(2.0), make_bundle(2.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::Pack,
            None,
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 3);
                // All should be on node1 (only node that can fit all 3)
                assert_eq!(schedule[&(pg_id, 0)], "node1");
                assert_eq!(schedule[&(pg_id, 1)], "node1");
                assert_eq!(schedule[&(pg_id, 2)], "node1");
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_pack_infeasible() {
        let scheduler = setup_scheduler(vec![("node1", make_node(4.0))]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(100.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::Pack,
            None,
            &HashMap::new(),
        );

        assert!(matches!(result, SchedulingResult::Infeasible));
    }

    // ---- Spread strategy tests ----

    #[test]
    fn test_spread_places_on_different_nodes() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
            ("node3", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(1.0), make_bundle(1.0), make_bundle(1.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::Spread,
            None,
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 3);
                let mut nodes: Vec<String> = schedule.values().cloned().collect();
                nodes.sort();
                nodes.dedup();
                assert_eq!(nodes.len(), 3);
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_spread_fallback_when_not_enough_nodes() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(1.0), make_bundle(1.0), make_bundle(1.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::Spread,
            None,
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 3);
            }
            _ => panic!("expected success"),
        }
    }

    // ---- Strict Pack tests ----

    #[test]
    fn test_strict_pack_all_on_one_node() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(8.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(2.0), make_bundle(2.0), make_bundle(2.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::StrictPack,
            None,
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 3);
                let node = &schedule[&(pg_id, 0)];
                assert_eq!(node, "node2");
                for i in 0..3 {
                    assert_eq!(&schedule[&(pg_id, i)], node);
                }
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_strict_pack_with_soft_target() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(8.0)),
            ("node2", make_node(8.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(2.0), make_bundle(2.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::StrictPack,
            Some("node1"),
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 2);
                assert_eq!(schedule[&(pg_id, 0)], "node1");
                assert_eq!(schedule[&(pg_id, 1)], "node1");
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_strict_pack_infeasible_no_single_node_fits() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(3.0), make_bundle(3.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::StrictPack,
            None,
            &HashMap::new(),
        );

        assert!(matches!(result, SchedulingResult::Infeasible));
    }

    // ---- Strict Spread tests ----

    #[test]
    fn test_strict_spread_each_on_different_node() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
            ("node3", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(1.0), make_bundle(1.0), make_bundle(1.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::StrictSpread,
            None,
            &HashMap::new(),
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 3);
                let mut nodes: Vec<String> = schedule.values().cloned().collect();
                nodes.sort();
                nodes.dedup();
                assert_eq!(nodes.len(), 3);
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_strict_spread_fails_not_enough_nodes() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(1.0), make_bundle(1.0), make_bundle(1.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::StrictSpread,
            None,
            &HashMap::new(),
        );

        assert!(matches!(result, SchedulingResult::Infeasible));
    }

    #[test]
    fn test_strict_spread_excludes_existing_locations() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
            ("node3", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let mut existing = HashMap::new();
        existing.insert((pg_id, 0), "node1".to_string());

        let bundles = vec![make_bundle(1.0), make_bundle(1.0)];
        let result = scheduler.schedule_placement_group(
            pg_id,
            &bundles,
            PlacementStrategy::StrictSpread,
            None,
            &existing,
        );

        match result {
            SchedulingResult::Success(schedule) => {
                assert_eq!(schedule.len(), 1);
                let node = &schedule[&(pg_id, 1)];
                assert_ne!(node, "node1");
            }
            _ => panic!("expected success"),
        }
    }

    // ---- Resource management tests ----

    #[test]
    fn test_acquire_and_return_resources() {
        let scheduler = setup_scheduler(vec![("node1", make_node(4.0))]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(2.0)];

        let mut schedule = ScheduleMap::new();
        schedule.insert((pg_id, 0), "node1".to_string());

        assert!(scheduler.acquire_bundle_resources(&schedule, &bundles));

        {
            let view = scheduler.resource_view.read();
            let node = view.get_node("node1").unwrap();
            assert_eq!(node.available.get("CPU"), FixedPoint::from_f64(2.0));
        }

        scheduler.return_bundle_resources(&schedule, &bundles);

        {
            let view = scheduler.resource_view.read();
            let node = view.get_node("node1").unwrap();
            assert_eq!(node.available.get("CPU"), FixedPoint::from_f64(4.0));
        }
    }

    #[test]
    fn test_acquire_fails_insufficient_resources() {
        let scheduler = setup_scheduler(vec![("node1", make_node(4.0))]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(10.0)];

        let mut schedule = ScheduleMap::new();
        schedule.insert((pg_id, 0), "node1".to_string());

        assert!(!scheduler.acquire_bundle_resources(&schedule, &bundles));
    }

    // ---- Lease tracking tests ----

    #[test]
    fn test_lease_status_tracker() {
        let pg_id = make_pg_id(1);
        let mut locations = HashMap::new();
        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        locations.insert((pg_id, 0), ("node1".to_string(), r1.clone()));
        locations.insert((pg_id, 1), ("node2".to_string(), r1));

        let mut tracker = LeaseStatusTracker::new(pg_id, locations);

        assert!(!tracker.all_prepared());
        assert!(!tracker.prepare_succeeded());

        tracker.mark_prepared(&(pg_id, 0));
        assert!(!tracker.all_prepared());

        tracker.mark_prepared(&(pg_id, 1));
        assert!(tracker.all_prepared());
        assert!(tracker.prepare_succeeded());
    }

    #[test]
    fn test_lease_tracker_prepare_failure() {
        let pg_id = make_pg_id(1);
        let mut locations = HashMap::new();
        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        locations.insert((pg_id, 0), ("node1".to_string(), r1.clone()));
        locations.insert((pg_id, 1), ("node2".to_string(), r1));

        let mut tracker = LeaseStatusTracker::new(pg_id, locations);

        tracker.mark_prepared(&(pg_id, 0));
        tracker.mark_prepare_failed(&(pg_id, 1));

        assert!(tracker.all_prepared());
        assert!(!tracker.prepare_succeeded());
    }

    #[test]
    fn test_commit_bundle_resources_updates_index() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let mut schedule = ScheduleMap::new();
        schedule.insert((pg_id, 0), "node1".to_string());
        schedule.insert((pg_id, 1), "node2".to_string());

        scheduler.commit_bundle_resources(pg_id, &schedule);

        assert_eq!(scheduler.get_bundles_on_node("node1"), vec![(pg_id, 0)]);
        assert_eq!(scheduler.get_bundles_on_node("node2"), vec![(pg_id, 1)]);
    }

    #[test]
    fn test_destroy_placement_group_resources() {
        let scheduler = setup_scheduler(vec![("node1", make_node(4.0))]);

        let pg_id = make_pg_id(1);
        let bundles = vec![make_bundle(2.0)];

        let mut schedule = ScheduleMap::new();
        schedule.insert((pg_id, 0), "node1".to_string());

        scheduler.acquire_bundle_resources(&schedule, &bundles);
        scheduler.commit_bundle_resources(pg_id, &schedule);

        {
            let view = scheduler.resource_view.read();
            assert_eq!(
                view.get_node("node1").unwrap().available.get("CPU"),
                FixedPoint::from_f64(2.0)
            );
        }

        scheduler.destroy_placement_group_resources(&pg_id, &bundles);

        {
            let view = scheduler.resource_view.read();
            assert_eq!(
                view.get_node("node1").unwrap().available.get("CPU"),
                FixedPoint::from_f64(4.0)
            );
        }
        assert!(scheduler.get_bundles_on_node("node1").is_empty());
    }

    #[test]
    fn test_bundle_spec_from_unit_resources() {
        let mut unit = HashMap::new();
        unit.insert("CPU".to_string(), 4.0);
        unit.insert("GPU".to_string(), 1.0);

        let spec = BundleSpec::from_unit_resources(&unit);
        assert_eq!(spec.resources.get("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(spec.resources.get("GPU"), FixedPoint::from_f64(1.0));
    }

    #[test]
    fn test_remove_dead_node() {
        let scheduler = setup_scheduler(vec![
            ("node1", make_node(4.0)),
            ("node2", make_node(4.0)),
        ]);

        let pg_id = make_pg_id(1);
        let mut schedule = ScheduleMap::new();
        schedule.insert((pg_id, 0), "node1".to_string());
        scheduler.commit_bundle_resources(pg_id, &schedule);

        assert_eq!(scheduler.num_alive_nodes(), 2);
        scheduler.remove_node("node1");
        assert_eq!(scheduler.num_alive_nodes(), 1);
        assert!(scheduler.get_bundles_on_node("node1").is_empty());
    }
}
