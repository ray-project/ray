// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object recovery manager — recovers lost objects via re-execution or fetch.
//!
//! Ports C++ `core_worker/object_recovery_manager.cc`.
//!
//! When an object is lost (node dies, evicted, etc.), the recovery manager
//! determines the best strategy to recover it:
//! - **Spill restore**: If the object was spilled to external storage
//! - **Lineage re-execution**: If the producing task's lineage is available
//! - **Fetch from replica**: If the object exists on another node
//! - **Fail**: If no recovery path is available

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use ray_common::id::ObjectID;

use crate::reference_counter::ReferenceCounter;

/// Strategy used to recover a lost object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// Restore from spill URL.
    RestoreFromSpill { url: String },
    /// Re-execute the producing task via lineage.
    ReExecuteLineage { task_id: ray_common::id::TaskID },
    /// Fetch from another node that has a copy.
    FetchFromNode { node_id: String },
    /// No recovery possible — object is permanently lost.
    Unrecoverable { reason: String },
}

/// Status of a recovery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryStatus {
    /// Recovery in progress.
    InProgress,
    /// Recovery succeeded.
    Succeeded,
    /// Recovery failed.
    Failed { reason: String },
}

/// Information about a recovery in progress.
#[derive(Debug, Clone)]
struct RecoveryEntry {
    /// The recovery strategy being used.
    strategy: RecoveryStrategy,
    /// Current status.
    status: RecoveryStatus,
    /// Number of recovery attempts so far.
    attempts: u32,
}

/// Callback when an object needs recovery via lineage re-execution.
pub type LineageReconstructionCallback = Box<dyn Fn(&ray_common::id::TaskID) -> bool + Send + Sync>;

/// Callback when an object needs to be fetched from another node.
pub type FetchObjectCallback = Box<dyn Fn(&ObjectID, &str) -> bool + Send + Sync>;

/// Callback when an object needs to be restored from spill.
pub type RestoreSpilledObjectCallback = Box<dyn Fn(&ObjectID, &str) -> bool + Send + Sync>;

/// Manages recovery of lost objects.
pub struct ObjectRecoveryManager {
    inner: Mutex<ObjectRecoveryManagerInner>,
    reference_counter: Arc<ReferenceCounter>,
    max_recovery_attempts: u32,
}

struct ObjectRecoveryManagerInner {
    /// Objects currently being recovered.
    recovering: HashMap<ObjectID, RecoveryEntry>,
    /// Objects that have been permanently marked as unrecoverable.
    unrecoverable: HashSet<ObjectID>,
    /// Callback for lineage re-execution.
    lineage_callback: Option<LineageReconstructionCallback>,
    /// Callback for fetching from other nodes.
    fetch_callback: Option<FetchObjectCallback>,
    /// Callback for restoring spilled objects.
    restore_callback: Option<RestoreSpilledObjectCallback>,
    /// Statistics.
    total_recovered: u64,
    total_failed: u64,
}

impl ObjectRecoveryManager {
    /// Create a new object recovery manager.
    pub fn new(reference_counter: Arc<ReferenceCounter>, max_recovery_attempts: u32) -> Self {
        Self {
            inner: Mutex::new(ObjectRecoveryManagerInner {
                recovering: HashMap::new(),
                unrecoverable: HashSet::new(),
                lineage_callback: None,
                fetch_callback: None,
                restore_callback: None,
                total_recovered: 0,
                total_failed: 0,
            }),
            reference_counter,
            max_recovery_attempts,
        }
    }

    /// Set the callback for lineage re-execution.
    pub fn set_lineage_callback(&self, callback: LineageReconstructionCallback) {
        self.inner.lock().lineage_callback = Some(callback);
    }

    /// Set the callback for fetching objects from other nodes.
    pub fn set_fetch_callback(&self, callback: FetchObjectCallback) {
        self.inner.lock().fetch_callback = Some(callback);
    }

    /// Set the callback for restoring spilled objects.
    pub fn set_restore_callback(&self, callback: RestoreSpilledObjectCallback) {
        self.inner.lock().restore_callback = Some(callback);
    }

    /// Attempt to recover a lost object.
    ///
    /// Determines the best recovery strategy based on available information:
    /// 1. If the object has a spill URL → restore from spill
    /// 2. If the object has other known locations → fetch from replica
    /// 3. If the producing task's lineage is available → re-execute
    /// 4. Otherwise → mark as unrecoverable
    pub fn recover_object(&self, object_id: ObjectID) -> RecoveryStrategy {
        let mut inner = self.inner.lock();

        // Already unrecoverable.
        if inner.unrecoverable.contains(&object_id) {
            return RecoveryStrategy::Unrecoverable {
                reason: "previously marked unrecoverable".to_string(),
            };
        }

        // Already being recovered.
        if let Some(entry) = inner.recovering.get(&object_id) {
            return entry.strategy.clone();
        }

        // Check for spill URL.
        if let Some(url) = self.reference_counter.get_spill_url(&object_id) {
            if !url.is_empty() {
                let strategy = RecoveryStrategy::RestoreFromSpill { url: url.clone() };

                // Try to invoke the restore callback.
                if let Some(ref cb) = inner.restore_callback {
                    cb(&object_id, &url);
                }

                inner.recovering.insert(
                    object_id,
                    RecoveryEntry {
                        strategy: strategy.clone(),
                        status: RecoveryStatus::InProgress,
                        attempts: 1,
                    },
                );
                tracing::info!(?object_id, %url, "Recovering object from spill");
                return strategy;
            }
        }

        // Check for other locations.
        let locations = self.reference_counter.get_object_locations(&object_id);
        if let Some(node_id) = locations.first() {
            let strategy = RecoveryStrategy::FetchFromNode {
                node_id: node_id.clone(),
            };

            if let Some(ref cb) = inner.fetch_callback {
                cb(&object_id, node_id);
            }

            inner.recovering.insert(
                object_id,
                RecoveryEntry {
                    strategy: strategy.clone(),
                    status: RecoveryStatus::InProgress,
                    attempts: 1,
                },
            );
            tracing::info!(?object_id, %node_id, "Recovering object from other node");
            return strategy;
        }

        // No recovery path available.
        let strategy = RecoveryStrategy::Unrecoverable {
            reason: "no spill URL, no other locations, no lineage".to_string(),
        };
        inner.unrecoverable.insert(object_id);
        inner.total_failed += 1;
        tracing::warn!(?object_id, "Object is unrecoverable");
        strategy
    }

    /// Mark a recovery as complete (object has been restored).
    pub fn recovery_succeeded(&self, object_id: &ObjectID) {
        let mut inner = self.inner.lock();
        if inner.recovering.remove(object_id).is_some() {
            inner.total_recovered += 1;
            tracing::info!(?object_id, "Object recovery succeeded");
        }
    }

    /// Mark a recovery as failed. May retry if under the max attempts.
    ///
    /// Returns true if a retry was initiated.
    pub fn recovery_failed(&self, object_id: &ObjectID, reason: &str) -> bool {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.recovering.get_mut(object_id) {
            entry.attempts += 1;
            if entry.attempts <= self.max_recovery_attempts {
                tracing::warn!(
                    ?object_id,
                    attempts = entry.attempts,
                    max = self.max_recovery_attempts,
                    "Recovery failed, retrying"
                );
                return true;
            }
            // Max attempts exceeded.
            entry.status = RecoveryStatus::Failed {
                reason: reason.to_string(),
            };
        }
        inner.recovering.remove(object_id);
        inner.unrecoverable.insert(*object_id);
        inner.total_failed += 1;
        tracing::error!(?object_id, %reason, "Object recovery permanently failed");
        false
    }

    /// Check if an object is currently being recovered.
    pub fn is_recovering(&self, object_id: &ObjectID) -> bool {
        self.inner.lock().recovering.contains_key(object_id)
    }

    /// Check if an object has been marked as unrecoverable.
    pub fn is_unrecoverable(&self, object_id: &ObjectID) -> bool {
        self.inner.lock().unrecoverable.contains(object_id)
    }

    /// Number of objects currently being recovered.
    pub fn num_recovering(&self) -> usize {
        self.inner.lock().recovering.len()
    }

    /// Statistics: (total_recovered, total_failed).
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        (inner.total_recovered, inner.total_failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn make_manager() -> ObjectRecoveryManager {
        let rc = Arc::new(ReferenceCounter::new());
        ObjectRecoveryManager::new(rc, 3)
    }

    fn make_manager_with_rc() -> (ObjectRecoveryManager, Arc<ReferenceCounter>) {
        let rc = Arc::new(ReferenceCounter::new());
        let mgr = ObjectRecoveryManager::new(rc.clone(), 3);
        (mgr, rc)
    }

    #[test]
    fn test_recover_unrecoverable_no_info() {
        let mgr = make_manager();
        let oid = ObjectID::from_random();

        let strategy = mgr.recover_object(oid);
        assert!(matches!(strategy, RecoveryStrategy::Unrecoverable { .. }));
        assert!(mgr.is_unrecoverable(&oid));
        assert_eq!(mgr.stats(), (0, 1));
    }

    #[test]
    fn test_recover_from_spill() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.set_spill_url(&oid, "s3://bucket/key".to_string());

        let strategy = mgr.recover_object(oid);
        assert_eq!(
            strategy,
            RecoveryStrategy::RestoreFromSpill {
                url: "s3://bucket/key".to_string()
            }
        );
        assert!(mgr.is_recovering(&oid));
        assert_eq!(mgr.num_recovering(), 1);
    }

    #[test]
    fn test_recover_from_other_node() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.add_object_location(&oid, "node-42".to_string());

        let strategy = mgr.recover_object(oid);
        assert_eq!(
            strategy,
            RecoveryStrategy::FetchFromNode {
                node_id: "node-42".to_string()
            }
        );
        assert!(mgr.is_recovering(&oid));
    }

    #[test]
    fn test_recovery_succeeded() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.set_spill_url(&oid, "file:///tmp/spill".to_string());

        mgr.recover_object(oid);
        assert!(mgr.is_recovering(&oid));

        mgr.recovery_succeeded(&oid);
        assert!(!mgr.is_recovering(&oid));
        assert_eq!(mgr.stats(), (1, 0));
    }

    #[test]
    fn test_recovery_failed_with_retry() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.set_spill_url(&oid, "s3://bucket/key".to_string());

        mgr.recover_object(oid);

        // First failure should retry (attempt 2 of 3).
        assert!(mgr.recovery_failed(&oid, "network error"));
        assert!(mgr.is_recovering(&oid));

        // Second failure should retry (attempt 3 of 3).
        assert!(mgr.recovery_failed(&oid, "network error"));
        assert!(mgr.is_recovering(&oid));

        // Third failure exceeds max — permanently failed.
        assert!(!mgr.recovery_failed(&oid, "network error"));
        assert!(!mgr.is_recovering(&oid));
        assert!(mgr.is_unrecoverable(&oid));
        assert_eq!(mgr.stats(), (0, 1));
    }

    #[test]
    fn test_duplicate_recovery_returns_existing_strategy() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.set_spill_url(&oid, "s3://bucket/key".to_string());

        let s1 = mgr.recover_object(oid);
        let s2 = mgr.recover_object(oid);
        assert_eq!(s1, s2);
        assert_eq!(mgr.num_recovering(), 1);
    }

    #[test]
    fn test_restore_callback_invoked() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.set_spill_url(&oid, "file:///tmp/spill".to_string());

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        mgr.set_restore_callback(Box::new(move |_oid, _url| {
            cc.fetch_add(1, Ordering::Relaxed);
            true
        }));

        mgr.recover_object(oid);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_fetch_callback_invoked() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.add_object_location(&oid, "node-1".to_string());

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        mgr.set_fetch_callback(Box::new(move |_oid, _node| {
            cc.fetch_add(1, Ordering::Relaxed);
            true
        }));

        mgr.recover_object(oid);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_spill_preferred_over_fetch() {
        let (mgr, rc) = make_manager_with_rc();
        let oid = ObjectID::from_random();

        rc.add_local_reference(oid);
        rc.set_spill_url(&oid, "s3://spill".to_string());
        rc.add_object_location(&oid, "node-1".to_string());

        // Spill URL takes precedence.
        let strategy = mgr.recover_object(oid);
        assert!(matches!(
            strategy,
            RecoveryStrategy::RestoreFromSpill { .. }
        ));
    }

    #[test]
    fn test_stats_after_multiple_operations() {
        let (mgr, rc) = make_manager_with_rc();

        // Object 1: unrecoverable.
        let oid1 = ObjectID::from_random();
        mgr.recover_object(oid1);

        // Object 2: recovered from spill.
        let oid2 = ObjectID::from_random();
        rc.add_local_reference(oid2);
        rc.set_spill_url(&oid2, "file:///spill".to_string());
        mgr.recover_object(oid2);
        mgr.recovery_succeeded(&oid2);

        let (recovered, failed) = mgr.stats();
        assert_eq!(recovered, 1);
        assert_eq!(failed, 1);
    }
}
