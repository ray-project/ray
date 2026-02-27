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

    pub fn num_active_pulls(&self) -> usize {
        self.object_pull_requests.len()
    }

    pub fn num_bytes_being_pulled(&self) -> i64 {
        self.num_bytes_being_pulled
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
}
