// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Pull manager — fetches objects from remote nodes with priority queuing.
//!
//! Replaces `src/ray/object_manager/pull_manager.h/cc`.
//!
//! Objects are fetched with three priority levels:
//! 1. GET_REQUEST (highest) — ray.get() calls
//! 2. WAIT_REQUEST — ray.wait() calls
//! 3. TASK_ARGS (lowest) — task argument dependencies

use std::collections::HashMap;

use ray_common::id::{NodeID, ObjectID};

/// Priority level for pull requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BundlePriority {
    /// ray.get() — highest priority.
    GetRequest = 0,
    /// ray.wait() — medium priority.
    WaitRequest = 1,
    /// Task arguments — lowest priority.
    TaskArgs = 2,
}

/// Unique identifier for a bundle pull request.
type BundleRequestId = u64;

/// Tracking info for an individual object being pulled.
#[derive(Debug)]
pub struct ObjectPullRequest {
    /// Nodes that have this object.
    pub client_locations: Vec<NodeID>,
    /// URL if the object was spilled to external storage.
    pub spilled_url: String,
    /// Node where the object was spilled.
    pub spilled_node_id: NodeID,
    /// Whether the object is being reconstructed.
    pub pending_object_creation: bool,
    /// Next time to retry pulling this object (monotonic ms).
    pub next_pull_time: f64,
    /// When this pull was activated (epoch ms).
    pub activate_time_ms: i64,
    /// Number of retry attempts.
    pub num_retries: u8,
    /// Whether the object size is known.
    pub object_size_set: bool,
    /// Size of the object in bytes.
    pub object_size: usize,
    /// Bundle requests that need this object.
    pub bundle_request_ids: Vec<BundleRequestId>,
}

impl Default for ObjectPullRequest {
    fn default() -> Self {
        Self {
            client_locations: Vec::new(),
            spilled_url: String::new(),
            spilled_node_id: NodeID::nil(),
            pending_object_creation: false,
            next_pull_time: 0.0,
            activate_time_ms: 0,
            num_retries: 0,
            object_size_set: false,
            object_size: 0,
            bundle_request_ids: Vec::new(),
        }
    }
}

/// A bundle of objects to pull together.
#[derive(Debug)]
pub struct BundlePullRequest {
    /// Unique ID for this bundle.
    pub id: BundleRequestId,
    /// All objects in this bundle.
    pub objects: Vec<ObjectID>,
    /// Objects whose sizes are known (pullable).
    pub pullable_objects: Vec<ObjectID>,
}

/// The pull manager coordinates fetching objects from remote nodes.
///
/// Maintains three priority queues for pull requests.
#[allow(dead_code)]
pub struct PullManager {
    /// Next bundle request ID.
    next_bundle_id: BundleRequestId,
    /// Individual object pull tracking.
    object_pull_requests: HashMap<ObjectID, ObjectPullRequest>,
    /// Active bundles by priority.
    get_request_bundles: Vec<BundlePullRequest>,
    wait_request_bundles: Vec<BundlePullRequest>,
    task_argument_bundles: Vec<BundlePullRequest>,
    /// Total bytes being pulled.
    num_bytes_being_pulled: i64,
    /// Available bytes (quota for concurrent pulls).
    num_bytes_available: i64,
    /// Number of active pull requests.
    num_active_pulls: usize,
}

impl PullManager {
    pub fn new(available_bytes: i64) -> Self {
        Self {
            next_bundle_id: 0,
            object_pull_requests: HashMap::new(),
            get_request_bundles: Vec::new(),
            wait_request_bundles: Vec::new(),
            task_argument_bundles: Vec::new(),
            num_bytes_being_pulled: 0,
            num_bytes_available: available_bytes,
            num_active_pulls: 0,
        }
    }

    /// Submit a pull request for a bundle of objects.
    /// Returns the bundle request ID.
    pub fn pull(&mut self, objects: Vec<ObjectID>, priority: BundlePriority) -> BundleRequestId {
        let id = self.next_bundle_id;
        self.next_bundle_id += 1;

        // Register individual object requests
        for &oid in &objects {
            let req = self.object_pull_requests.entry(oid).or_default();
            req.bundle_request_ids.push(id);
        }

        let bundle = BundlePullRequest {
            id,
            objects: objects.clone(),
            pullable_objects: Vec::new(),
        };

        match priority {
            BundlePriority::GetRequest => self.get_request_bundles.push(bundle),
            BundlePriority::WaitRequest => self.wait_request_bundles.push(bundle),
            BundlePriority::TaskArgs => self.task_argument_bundles.push(bundle),
        }

        id
    }

    /// Cancel a bundle pull request.
    pub fn cancel_pull(&mut self, bundle_id: BundleRequestId) {
        // Remove from all queues
        for queue in [
            &mut self.get_request_bundles,
            &mut self.wait_request_bundles,
            &mut self.task_argument_bundles,
        ] {
            queue.retain(|b| b.id != bundle_id);
        }

        // Clean up individual object requests
        self.object_pull_requests.retain(|_, req| {
            req.bundle_request_ids.retain(|&id| id != bundle_id);
            !req.bundle_request_ids.is_empty()
        });
    }

    /// Update the location of an object (node where it can be fetched from).
    pub fn update_object_location(&mut self, object_id: &ObjectID, node_id: NodeID) {
        if let Some(req) = self.object_pull_requests.get_mut(object_id) {
            if !req.client_locations.contains(&node_id) {
                req.client_locations.push(node_id);
            }
        }
    }

    /// Notify that an object is now available locally.
    pub fn object_available_locally(&mut self, object_id: &ObjectID) {
        self.object_pull_requests.remove(object_id);

        // Remove from all bundle pullable lists
        for queue in [
            &mut self.get_request_bundles,
            &mut self.wait_request_bundles,
            &mut self.task_argument_bundles,
        ] {
            for bundle in queue.iter_mut() {
                bundle.objects.retain(|oid| oid != object_id);
            }
        }

        // Remove completed bundles
        self.get_request_bundles.retain(|b| !b.objects.is_empty());
        self.wait_request_bundles.retain(|b| !b.objects.is_empty());
        self.task_argument_bundles.retain(|b| !b.objects.is_empty());
    }

    /// Get the next objects to pull, respecting priority and byte quota.
    pub fn get_next_pull_targets(&self) -> Vec<(ObjectID, NodeID)> {
        let mut targets = Vec::new();

        // Iterate queues in priority order
        for queue in [
            &self.get_request_bundles,
            &self.wait_request_bundles,
            &self.task_argument_bundles,
        ] {
            for bundle in queue {
                for oid in &bundle.objects {
                    if let Some(req) = self.object_pull_requests.get(oid) {
                        if let Some(&node) = req.client_locations.first() {
                            targets.push((*oid, node));
                        }
                    }
                }
            }
        }

        targets
    }

    /// Record the retry attempt for an object, applying exponential backoff.
    ///
    /// Returns the next retry time offset in milliseconds.
    pub fn record_retry(&mut self, object_id: &ObjectID, now_ms: f64) -> f64 {
        if let Some(req) = self.object_pull_requests.get_mut(object_id) {
            req.num_retries = req.num_retries.saturating_add(1);
            // Exponential backoff: base * 2^retries, capped at 60 seconds.
            let backoff_ms = (1000.0 * (1u64 << req.num_retries.min(6)) as f64).min(60_000.0);
            req.next_pull_time = now_ms + backoff_ms;
            backoff_ms
        } else {
            0.0
        }
    }

    /// Check if an object is ready for retry at the given time.
    pub fn is_ready_for_retry(&self, object_id: &ObjectID, now_ms: f64) -> bool {
        self.object_pull_requests
            .get(object_id)
            .map(|req| now_ms >= req.next_pull_time)
            .unwrap_or(false)
    }

    /// Set the object size, marking it as pullable.
    pub fn set_object_size(&mut self, object_id: &ObjectID, size: usize) {
        if let Some(req) = self.object_pull_requests.get_mut(object_id) {
            req.object_size = size;
            req.object_size_set = true;
        }
    }

    /// Set the spilled URL for an object.
    pub fn set_spilled_url(
        &mut self,
        object_id: &ObjectID,
        url: String,
        spill_node_id: NodeID,
    ) {
        if let Some(req) = self.object_pull_requests.get_mut(object_id) {
            req.spilled_url = url;
            req.spilled_node_id = spill_node_id;
        }
    }

    /// Get objects that need to be restored from external storage.
    pub fn get_spilled_pull_targets(&self) -> Vec<(ObjectID, String, NodeID)> {
        let mut targets = Vec::new();
        for (oid, req) in &self.object_pull_requests {
            if !req.spilled_url.is_empty() {
                targets.push((*oid, req.spilled_url.clone(), req.spilled_node_id));
            }
        }
        targets
    }

    /// Mark an object as being actively pulled, deducting from quota.
    pub fn start_pulling(&mut self, object_id: &ObjectID) -> bool {
        if let Some(req) = self.object_pull_requests.get(object_id) {
            if req.object_size_set {
                let size = req.object_size as i64;
                if self.num_bytes_being_pulled + size <= self.num_bytes_available {
                    self.num_bytes_being_pulled += size;
                    return true;
                }
                return false; // Over quota
            }
        }
        true // Unknown size, allow pull
    }

    /// Release quota when a pull completes or fails.
    pub fn finish_pulling(&mut self, object_id: &ObjectID) {
        if let Some(req) = self.object_pull_requests.get(object_id) {
            if req.object_size_set {
                self.num_bytes_being_pulled -= req.object_size as i64;
                self.num_bytes_being_pulled = self.num_bytes_being_pulled.max(0);
            }
        }
    }

    /// Get pull targets that are ready for retry and within quota.
    pub fn get_ready_pull_targets(&self, now_ms: f64) -> Vec<(ObjectID, NodeID)> {
        let mut targets = Vec::new();
        let mut bytes_added: i64 = 0;

        for queue in [
            &self.get_request_bundles,
            &self.wait_request_bundles,
            &self.task_argument_bundles,
        ] {
            for bundle in queue {
                for oid in &bundle.objects {
                    if let Some(req) = self.object_pull_requests.get(oid) {
                        // Check retry time
                        if now_ms < req.next_pull_time {
                            continue;
                        }
                        // Check quota
                        if req.object_size_set {
                            let size = req.object_size as i64;
                            if self.num_bytes_being_pulled + bytes_added + size
                                > self.num_bytes_available
                            {
                                continue;
                            }
                            bytes_added += size;
                        }
                        // Need at least one known location
                        if let Some(&node) = req.client_locations.first() {
                            targets.push((*oid, node));
                        }
                    }
                }
            }
        }

        targets
    }

    /// Remove a specific node from all object locations (e.g., node died).
    pub fn remove_node_location(&mut self, node_id: &NodeID) {
        for req in self.object_pull_requests.values_mut() {
            req.client_locations.retain(|n| n != node_id);
        }
    }

    /// Get the number of retries for an object.
    pub fn num_retries(&self, object_id: &ObjectID) -> u8 {
        self.object_pull_requests
            .get(object_id)
            .map(|r| r.num_retries)
            .unwrap_or(0)
    }

    /// Total number of bundles across all priority queues.
    pub fn num_bundles(&self) -> usize {
        self.get_request_bundles.len()
            + self.wait_request_bundles.len()
            + self.task_argument_bundles.len()
    }

    pub fn num_active_pulls(&self) -> usize {
        self.object_pull_requests.len()
    }

    pub fn num_bytes_being_pulled(&self) -> i64 {
        self.num_bytes_being_pulled
    }

    pub fn num_bytes_available(&self) -> i64 {
        self.num_bytes_available
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    #[test]
    fn test_pull_and_cancel() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let o2 = make_oid(2);

        let id = pm.pull(vec![o1, o2], BundlePriority::GetRequest);
        assert_eq!(pm.num_active_pulls(), 2);

        pm.cancel_pull(id);
        assert_eq!(pm.num_active_pulls(), 0);
    }

    #[test]
    fn test_object_available_locally() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let o2 = make_oid(2);

        pm.pull(vec![o1, o2], BundlePriority::TaskArgs);
        pm.object_available_locally(&o1);
        assert_eq!(pm.num_active_pulls(), 1);
    }

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    #[test]
    fn test_priority_ordering() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let o2 = make_oid(2);
        let o3 = make_oid(3);
        let node = make_nid(1);

        // Add in reverse priority order
        pm.pull(vec![o3], BundlePriority::TaskArgs);
        pm.pull(vec![o2], BundlePriority::WaitRequest);
        pm.pull(vec![o1], BundlePriority::GetRequest);

        // Set locations so get_next_pull_targets works
        pm.update_object_location(&o1, node);
        pm.update_object_location(&o2, node);
        pm.update_object_location(&o3, node);

        let targets = pm.get_next_pull_targets();
        // GetRequest (o1) should come first
        assert_eq!(targets[0].0, o1);
        // WaitRequest (o2) second
        assert_eq!(targets[1].0, o2);
        // TaskArgs (o3) last
        assert_eq!(targets[2].0, o3);
    }

    #[test]
    fn test_update_location_deduplicates() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let node = make_nid(1);

        pm.pull(vec![o1], BundlePriority::GetRequest);
        pm.update_object_location(&o1, node);
        pm.update_object_location(&o1, node); // Duplicate

        let targets = pm.get_next_pull_targets();
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn test_cancel_cleans_object_requests() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);

        let id = pm.pull(vec![o1], BundlePriority::GetRequest);
        assert_eq!(pm.num_active_pulls(), 1);

        pm.cancel_pull(id);
        assert_eq!(pm.num_active_pulls(), 0);
    }

    #[test]
    fn test_shared_object_across_bundles() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);

        // Same object in two bundles
        let id1 = pm.pull(vec![o1], BundlePriority::GetRequest);
        let _id2 = pm.pull(vec![o1], BundlePriority::TaskArgs);

        // Object still active (shared by two bundles)
        assert_eq!(pm.num_active_pulls(), 1);

        // Cancel first bundle — object still needed by second
        pm.cancel_pull(id1);
        assert_eq!(pm.num_active_pulls(), 1);
    }

    #[test]
    fn test_object_available_removes_completed_bundles() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);

        pm.pull(vec![o1], BundlePriority::GetRequest);
        pm.object_available_locally(&o1);

        // Bundle should be removed (its only object is available)
        assert_eq!(pm.num_active_pulls(), 0);
    }

    #[test]
    fn test_get_targets_skips_objects_without_locations() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let o2 = make_oid(2);
        let node = make_nid(1);

        pm.pull(vec![o1, o2], BundlePriority::GetRequest);
        // Only set location for o1
        pm.update_object_location(&o1, node);

        let targets = pm.get_next_pull_targets();
        // Only o1 has a location
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, o1);
    }

    #[test]
    fn test_bundle_id_increments() {
        let mut pm = PullManager::new(1024 * 1024);
        let id1 = pm.pull(vec![make_oid(1)], BundlePriority::GetRequest);
        let id2 = pm.pull(vec![make_oid(2)], BundlePriority::GetRequest);
        assert_eq!(id2, id1 + 1);
    }

    #[test]
    fn test_retry_backoff() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        pm.pull(vec![o1], BundlePriority::GetRequest);

        let backoff1 = pm.record_retry(&o1, 1000.0);
        assert_eq!(backoff1, 2000.0); // 1000 * 2^1

        let backoff2 = pm.record_retry(&o1, 3000.0);
        assert_eq!(backoff2, 4000.0); // 1000 * 2^2

        assert_eq!(pm.num_retries(&o1), 2);
    }

    #[test]
    fn test_retry_backoff_capped() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        pm.pull(vec![o1], BundlePriority::GetRequest);

        // Run many retries
        for i in 0..20 {
            let backoff = pm.record_retry(&o1, i as f64 * 100_000.0);
            assert!(backoff <= 60_000.0, "backoff exceeded cap: {}", backoff);
        }
    }

    #[test]
    fn test_is_ready_for_retry() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        pm.pull(vec![o1], BundlePriority::GetRequest);

        // Initially ready (next_pull_time = 0)
        assert!(pm.is_ready_for_retry(&o1, 0.0));

        pm.record_retry(&o1, 1000.0);
        // next_pull_time = 1000 + 2000 = 3000
        assert!(!pm.is_ready_for_retry(&o1, 2000.0));
        assert!(pm.is_ready_for_retry(&o1, 3000.0));
    }

    #[test]
    fn test_set_object_size() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        pm.pull(vec![o1], BundlePriority::GetRequest);

        pm.set_object_size(&o1, 5000);
        let req = pm.object_pull_requests.get(&o1).unwrap();
        assert!(req.object_size_set);
        assert_eq!(req.object_size, 5000);
    }

    #[test]
    fn test_set_spilled_url() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let spill_node = make_nid(5);
        pm.pull(vec![o1], BundlePriority::GetRequest);

        pm.set_spilled_url(&o1, "s3://bucket/obj1".into(), spill_node);

        let spilled = pm.get_spilled_pull_targets();
        assert_eq!(spilled.len(), 1);
        assert_eq!(spilled[0].0, o1);
        assert_eq!(spilled[0].1, "s3://bucket/obj1");
        assert_eq!(spilled[0].2, spill_node);
    }

    #[test]
    fn test_quota_enforcement() {
        let mut pm = PullManager::new(10_000); // 10KB quota
        let o1 = make_oid(1);
        let o2 = make_oid(2);
        pm.pull(vec![o1, o2], BundlePriority::GetRequest);

        pm.set_object_size(&o1, 6000);
        pm.set_object_size(&o2, 6000);

        // First pull fits in quota
        assert!(pm.start_pulling(&o1));
        assert_eq!(pm.num_bytes_being_pulled(), 6000);

        // Second pull exceeds quota
        assert!(!pm.start_pulling(&o2));

        // Finish first, second now fits
        pm.finish_pulling(&o1);
        assert_eq!(pm.num_bytes_being_pulled(), 0);
        assert!(pm.start_pulling(&o2));
    }

    #[test]
    fn test_get_ready_pull_targets_respects_quota() {
        let mut pm = PullManager::new(5000);
        let o1 = make_oid(1);
        let o2 = make_oid(2);
        let node = make_nid(1);

        pm.pull(vec![o1, o2], BundlePriority::GetRequest);
        pm.update_object_location(&o1, node);
        pm.update_object_location(&o2, node);
        pm.set_object_size(&o1, 3000);
        pm.set_object_size(&o2, 3000);

        let targets = pm.get_ready_pull_targets(0.0);
        // Only one fits in 5000 quota
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn test_get_ready_pull_targets_respects_retry_time() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let node = make_nid(1);

        pm.pull(vec![o1], BundlePriority::GetRequest);
        pm.update_object_location(&o1, node);
        pm.record_retry(&o1, 1000.0); // next_pull_time = 3000

        // Not ready yet
        let targets = pm.get_ready_pull_targets(2000.0);
        assert!(targets.is_empty());

        // Now ready
        let targets = pm.get_ready_pull_targets(3000.0);
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn test_remove_node_location() {
        let mut pm = PullManager::new(1024 * 1024);
        let o1 = make_oid(1);
        let node1 = make_nid(1);
        let node2 = make_nid(2);

        pm.pull(vec![o1], BundlePriority::GetRequest);
        pm.update_object_location(&o1, node1);
        pm.update_object_location(&o1, node2);

        pm.remove_node_location(&node1);
        let targets = pm.get_next_pull_targets();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].1, node2); // Only node2 remains
    }

    #[test]
    fn test_num_bundles() {
        let mut pm = PullManager::new(1024 * 1024);
        pm.pull(vec![make_oid(1)], BundlePriority::GetRequest);
        pm.pull(vec![make_oid(2)], BundlePriority::WaitRequest);
        pm.pull(vec![make_oid(3)], BundlePriority::TaskArgs);
        assert_eq!(pm.num_bundles(), 3);
    }
}
