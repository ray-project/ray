// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Local object manager for the raylet.
//!
//! Manages pinned objects (objects needed by running tasks), coordinates
//! spilling decisions, and batches freed-object deletions. Replaces
//! `src/ray/raylet/local_object_manager.h/cc`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use ray_common::id::ObjectID;

/// Configuration for the local object manager.
#[derive(Debug, Clone)]
pub struct LocalObjectManagerConfig {
    /// Minimum total bytes before triggering a spill batch.
    pub min_spilling_size: i64,
    /// Maximum number of objects per spill batch.
    pub max_spill_batch_count: usize,
    /// Interval (in ticks) between flush-freed-objects batches.
    pub free_objects_batch_size: usize,
    /// Object spilling threshold (fraction 0.0-1.0 of store capacity).
    pub object_spilling_threshold: f64,
}

impl Default for LocalObjectManagerConfig {
    fn default() -> Self {
        Self {
            min_spilling_size: 100 * 1024 * 1024, // 100MB
            max_spill_batch_count: 100,
            free_objects_batch_size: 100,
            object_spilling_threshold: 0.8,
        }
    }
}

/// State of a pinned object managed by this node.
#[derive(Debug, Clone)]
pub struct PinnedObject {
    pub object_id: ObjectID,
    /// Size of the object data (bytes).
    pub data_size: i64,
    /// Size of the object metadata (bytes).
    pub metadata_size: i64,
    /// Whether this object is currently being spilled.
    pub is_pending_spill: bool,
    /// The spill URL if the object has been spilled.
    pub spill_url: Option<String>,
}

impl PinnedObject {
    /// Total object size.
    pub fn total_size(&self) -> i64 {
        self.data_size + self.metadata_size
    }
}

/// Stats for monitoring.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalObjectManagerStats {
    /// Total pinned object bytes.
    pub pinned_bytes: i64,
    /// Number of pinned objects.
    pub num_pinned: usize,
    /// Total bytes pending spill.
    pub pending_spill_bytes: i64,
    /// Number of objects pending spill.
    pub num_pending_spill: usize,
    /// Total bytes that have been spilled (cumulative).
    pub total_bytes_spilled: u64,
    /// Total objects spilled (cumulative).
    pub total_objects_spilled: u64,
    /// Number of objects waiting for deletion.
    pub num_pending_deletion: usize,
}

/// Manages pinned objects on this node and coordinates spilling.
pub struct LocalObjectManager {
    config: LocalObjectManagerConfig,
    /// Objects pinned on this node (in use by tasks or recently created).
    pinned_objects: HashMap<ObjectID, PinnedObject>,
    /// Total size of all pinned objects.
    pinned_bytes: i64,
    /// Objects that are being spilled (awaiting spill completion).
    pending_spill: HashSet<ObjectID>,
    /// Total bytes currently being spilled.
    pending_spill_bytes: i64,
    /// Objects that have been freed and are waiting for batch deletion.
    pending_deletion: Vec<ObjectID>,
    /// Cumulative stats.
    total_bytes_spilled: AtomicU64,
    total_objects_spilled: AtomicU64,
}

impl LocalObjectManager {
    pub fn new(config: LocalObjectManagerConfig) -> Self {
        Self {
            config,
            pinned_objects: HashMap::new(),
            pinned_bytes: 0,
            pending_spill: HashSet::new(),
            pending_spill_bytes: 0,
            pending_deletion: Vec::new(),
            total_bytes_spilled: AtomicU64::new(0),
            total_objects_spilled: AtomicU64::new(0),
        }
    }

    /// Pin an object. Called when a new object is created on this node
    /// or when a task needs an object to stay resident.
    pub fn pin_object(&mut self, object_id: ObjectID, data_size: i64, metadata_size: i64) {
        let total = data_size + metadata_size;
        let obj = PinnedObject {
            object_id,
            data_size,
            metadata_size,
            is_pending_spill: false,
            spill_url: None,
        };
        if self.pinned_objects.insert(object_id, obj).is_none() {
            // Only add to total if this is a new pin.
            self.pinned_bytes += total;
        }
    }

    /// Release (unpin) an object. Called when a task completes and
    /// no longer needs the object, or when the object is freed.
    pub fn release_object(&mut self, object_id: &ObjectID) -> bool {
        if let Some(obj) = self.pinned_objects.remove(object_id) {
            self.pinned_bytes -= obj.total_size();
            if obj.is_pending_spill {
                self.pending_spill.remove(object_id);
                self.pending_spill_bytes -= obj.total_size();
            }
            true
        } else {
            false
        }
    }

    /// Mark an object for deletion (to be flushed in a batch).
    pub fn mark_for_deletion(&mut self, object_id: ObjectID) {
        // Remove from pinned if present.
        self.release_object(&object_id);
        self.pending_deletion.push(object_id);
    }

    /// Flush objects that are pending deletion.
    /// Returns the batch of object IDs to delete from the store.
    pub fn flush_freed_objects(&mut self) -> Vec<ObjectID> {
        let batch_size = self.config.free_objects_batch_size;
        if self.pending_deletion.len() <= batch_size {
            std::mem::take(&mut self.pending_deletion)
        } else {
            self.pending_deletion.drain(..batch_size).collect()
        }
    }

    /// Select objects to spill, prioritizing the largest unpinned-by-tasks
    /// objects. Returns a list of (object_id, size) sorted largest-first.
    ///
    /// Only selects objects that are not already being spilled and not
    /// already spilled.
    pub fn select_objects_to_spill(&self) -> Vec<(ObjectID, i64)> {
        // Collect eligible objects and sort by size descending (largest first).
        let mut candidates: Vec<(ObjectID, i64)> = self
            .pinned_objects
            .values()
            .filter(|obj| !obj.is_pending_spill && obj.spill_url.is_none())
            .map(|obj| (obj.object_id, obj.total_size()))
            .collect();

        candidates.sort_by(|a, b| b.1.cmp(&a.1));

        let mut result = Vec::new();
        let mut total_selected = 0i64;

        for (oid, size) in candidates {
            result.push((oid, size));
            total_selected += size;

            if total_selected >= self.config.min_spilling_size
                || result.len() >= self.config.max_spill_batch_count
            {
                break;
            }
        }

        result
    }

    /// Mark objects as pending spill (spill has been initiated).
    pub fn mark_pending_spill(&mut self, object_ids: &[ObjectID]) {
        for oid in object_ids {
            if let Some(obj) = self.pinned_objects.get_mut(oid) {
                if !obj.is_pending_spill {
                    obj.is_pending_spill = true;
                    self.pending_spill.insert(*oid);
                    self.pending_spill_bytes += obj.total_size();
                }
            }
        }
    }

    /// Mark a spill as completed for the given object.
    pub fn spill_completed(&mut self, object_id: &ObjectID, spill_url: String) {
        if let Some(obj) = self.pinned_objects.get_mut(object_id) {
            let size = obj.total_size();
            obj.is_pending_spill = false;
            obj.spill_url = Some(spill_url);
            self.pending_spill.remove(object_id);
            self.pending_spill_bytes -= size;
            self.total_bytes_spilled
                .fetch_add(size as u64, Ordering::Relaxed);
            self.total_objects_spilled.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Mark a spill as failed for the given object (retry later).
    pub fn spill_failed(&mut self, object_id: &ObjectID) {
        if let Some(obj) = self.pinned_objects.get_mut(object_id) {
            if obj.is_pending_spill {
                self.pending_spill_bytes -= obj.total_size();
                obj.is_pending_spill = false;
                self.pending_spill.remove(object_id);
            }
        }
    }

    /// Whether the object store is above the spilling threshold.
    pub fn should_spill(&self, used_fraction: f64) -> bool {
        used_fraction >= self.config.object_spilling_threshold
    }

    /// Get objects that have been spilled (have spill URLs) and can be
    /// evicted from local storage to free space.
    pub fn get_spilled_objects(&self) -> Vec<(ObjectID, String)> {
        self.pinned_objects
            .values()
            .filter_map(|obj| {
                obj.spill_url
                    .as_ref()
                    .map(|url| (obj.object_id, url.clone()))
            })
            .collect()
    }

    /// Get the spill URL for a specific object, if it has been spilled.
    pub fn get_spill_url(&self, object_id: &ObjectID) -> Option<String> {
        self.pinned_objects
            .get(object_id)
            .and_then(|obj| obj.spill_url.clone())
    }

    /// Check if an object is pinned.
    pub fn is_pinned(&self, object_id: &ObjectID) -> bool {
        self.pinned_objects.contains_key(object_id)
    }

    /// Get current stats.
    pub fn stats(&self) -> LocalObjectManagerStats {
        LocalObjectManagerStats {
            pinned_bytes: self.pinned_bytes,
            num_pinned: self.pinned_objects.len(),
            pending_spill_bytes: self.pending_spill_bytes,
            num_pending_spill: self.pending_spill.len(),
            total_bytes_spilled: self.total_bytes_spilled.load(Ordering::Relaxed),
            total_objects_spilled: self.total_objects_spilled.load(Ordering::Relaxed),
            num_pending_deletion: self.pending_deletion.len(),
        }
    }

    pub fn config(&self) -> &LocalObjectManagerConfig {
        &self.config
    }

    /// Total pinned bytes.
    pub fn pinned_bytes(&self) -> i64 {
        self.pinned_bytes
    }

    /// Number of pinned objects.
    pub fn num_pinned(&self) -> usize {
        self.pinned_objects.len()
    }

    /// Number of objects pending deletion.
    pub fn num_pending_deletion(&self) -> usize {
        self.pending_deletion.len()
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

    fn make_config() -> LocalObjectManagerConfig {
        LocalObjectManagerConfig {
            min_spilling_size: 100,
            max_spill_batch_count: 10,
            free_objects_batch_size: 5,
            object_spilling_threshold: 0.8,
        }
    }

    #[test]
    fn test_pin_and_release() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 10);
        assert!(mgr.is_pinned(&oid));
        assert_eq!(mgr.pinned_bytes(), 110);
        assert_eq!(mgr.num_pinned(), 1);

        assert!(mgr.release_object(&oid));
        assert!(!mgr.is_pinned(&oid));
        assert_eq!(mgr.pinned_bytes(), 0);
    }

    #[test]
    fn test_release_nonexistent_returns_false() {
        let mut mgr = LocalObjectManager::new(make_config());
        assert!(!mgr.release_object(&make_oid(99)));
    }

    #[test]
    fn test_double_pin_no_double_count() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 0);
        mgr.pin_object(oid, 100, 0);
        // Second pin should replace, not double-count.
        assert_eq!(mgr.pinned_bytes(), 100);
        assert_eq!(mgr.num_pinned(), 1);
    }

    #[test]
    fn test_mark_for_deletion() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 0);
        mgr.mark_for_deletion(oid);

        assert!(!mgr.is_pinned(&oid));
        assert_eq!(mgr.pinned_bytes(), 0);
        assert_eq!(mgr.num_pending_deletion(), 1);
    }

    #[test]
    fn test_flush_freed_objects() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            free_objects_batch_size: 2,
            ..make_config()
        });

        for i in 0..5u8 {
            mgr.mark_for_deletion(make_oid(i));
        }
        assert_eq!(mgr.num_pending_deletion(), 5);

        let batch1 = mgr.flush_freed_objects();
        assert_eq!(batch1.len(), 2);
        assert_eq!(mgr.num_pending_deletion(), 3);

        let batch2 = mgr.flush_freed_objects();
        assert_eq!(batch2.len(), 2);
        assert_eq!(mgr.num_pending_deletion(), 1);

        let batch3 = mgr.flush_freed_objects();
        assert_eq!(batch3.len(), 1);
        assert_eq!(mgr.num_pending_deletion(), 0);
    }

    #[test]
    fn test_select_objects_to_spill_largest_first() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 10, // low threshold to trigger
            ..make_config()
        });

        mgr.pin_object(make_oid(1), 50, 0);
        mgr.pin_object(make_oid(2), 200, 0);
        mgr.pin_object(make_oid(3), 100, 0);

        let to_spill = mgr.select_objects_to_spill();
        assert!(!to_spill.is_empty());
        // Should be sorted largest first.
        assert_eq!(to_spill[0].1, 200);
    }

    #[test]
    fn test_select_skips_pending_spill() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 10,
            ..make_config()
        });

        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        mgr.pin_object(oid1, 100, 0);
        mgr.pin_object(oid2, 200, 0);
        mgr.mark_pending_spill(&[oid2]);

        let to_spill = mgr.select_objects_to_spill();
        assert_eq!(to_spill.len(), 1);
        assert_eq!(to_spill[0].0, oid1);
    }

    #[test]
    fn test_select_skips_already_spilled() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 10,
            ..make_config()
        });

        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        mgr.pin_object(oid1, 100, 0);
        mgr.pin_object(oid2, 200, 0);
        mgr.mark_pending_spill(&[oid2]);
        mgr.spill_completed(&oid2, "file:///tmp/spill".to_string());

        let to_spill = mgr.select_objects_to_spill();
        assert_eq!(to_spill.len(), 1);
        assert_eq!(to_spill[0].0, oid1);
    }

    #[test]
    fn test_spill_lifecycle() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 10);

        // Mark pending spill.
        mgr.mark_pending_spill(&[oid]);
        assert_eq!(mgr.stats().num_pending_spill, 1);
        assert_eq!(mgr.stats().pending_spill_bytes, 110);

        // Complete spill.
        mgr.spill_completed(&oid, "file:///tmp/spill".to_string());
        assert_eq!(mgr.stats().num_pending_spill, 0);
        assert_eq!(mgr.stats().pending_spill_bytes, 0);
        assert_eq!(mgr.stats().total_bytes_spilled, 110);
        assert_eq!(mgr.stats().total_objects_spilled, 1);

        // Object should have a spill URL.
        assert_eq!(
            mgr.get_spill_url(&oid),
            Some("file:///tmp/spill".to_string())
        );
    }

    #[test]
    fn test_spill_failed() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 0);
        mgr.mark_pending_spill(&[oid]);
        assert_eq!(mgr.stats().num_pending_spill, 1);

        mgr.spill_failed(&oid);
        assert_eq!(mgr.stats().num_pending_spill, 0);
        assert_eq!(mgr.stats().total_objects_spilled, 0);
        // Object should still be available for re-spill.
        assert!(mgr.is_pinned(&oid));
        assert!(mgr.get_spill_url(&oid).is_none());
    }

    #[test]
    fn test_get_spilled_objects() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        let oid3 = make_oid(3);

        mgr.pin_object(oid1, 100, 0);
        mgr.pin_object(oid2, 200, 0);
        mgr.pin_object(oid3, 300, 0);

        mgr.mark_pending_spill(&[oid1, oid3]);
        mgr.spill_completed(&oid1, "url1".to_string());
        mgr.spill_completed(&oid3, "url3".to_string());

        let spilled = mgr.get_spilled_objects();
        assert_eq!(spilled.len(), 2);
        let urls: HashSet<String> = spilled.into_iter().map(|(_, u)| u).collect();
        assert!(urls.contains("url1"));
        assert!(urls.contains("url3"));
    }

    #[test]
    fn test_should_spill() {
        let mgr = LocalObjectManager::new(make_config());
        assert!(!mgr.should_spill(0.5));
        assert!(!mgr.should_spill(0.79));
        assert!(mgr.should_spill(0.8));
        assert!(mgr.should_spill(0.95));
    }

    #[test]
    fn test_stats() {
        let mut mgr = LocalObjectManager::new(make_config());
        mgr.pin_object(make_oid(1), 100, 0);
        mgr.pin_object(make_oid(2), 200, 0);
        mgr.mark_for_deletion(make_oid(3)); // not pinned, just pending delete

        let stats = mgr.stats();
        assert_eq!(stats.pinned_bytes, 300);
        assert_eq!(stats.num_pinned, 2);
        assert_eq!(stats.num_pending_deletion, 1);
    }

    #[test]
    fn test_release_pending_spill_cleans_up() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 0);
        mgr.mark_pending_spill(&[oid]);
        assert_eq!(mgr.stats().num_pending_spill, 1);

        // Releasing a pending-spill object should clean up spill tracking.
        mgr.release_object(&oid);
        assert_eq!(mgr.stats().num_pending_spill, 0);
        assert_eq!(mgr.stats().pending_spill_bytes, 0);
    }

    #[test]
    fn test_select_respects_batch_count() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: i64::MAX, // very high so count limit triggers
            max_spill_batch_count: 2,
            ..make_config()
        });

        for i in 0..5u8 {
            mgr.pin_object(make_oid(i), 100, 0);
        }

        let to_spill = mgr.select_objects_to_spill();
        assert_eq!(to_spill.len(), 2);
    }

    // --- Additional tests ported from C++ local_object_manager_test.cc ---

    #[test]
    fn test_spill_multiple_objects_in_batch() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 200, // need at least 200 bytes
            max_spill_batch_count: 10,
            ..make_config()
        });

        // Pin 3 objects of different sizes
        mgr.pin_object(make_oid(1), 50, 0);
        mgr.pin_object(make_oid(2), 80, 0);
        mgr.pin_object(make_oid(3), 120, 0);

        let to_spill = mgr.select_objects_to_spill();
        // Sorted largest first: 120, 80, 50
        // 120 + 80 = 200 >= min_spilling_size, so we stop at 2
        assert_eq!(to_spill.len(), 2);
        assert_eq!(to_spill[0].1, 120);
        assert_eq!(to_spill[1].1, 80);
    }

    #[test]
    fn test_spill_completed_then_select_excludes_spilled() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 10,
            ..make_config()
        });

        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        let oid3 = make_oid(3);

        mgr.pin_object(oid1, 100, 0);
        mgr.pin_object(oid2, 200, 0);
        mgr.pin_object(oid3, 300, 0);

        // Spill oid3 completely
        mgr.mark_pending_spill(&[oid3]);
        mgr.spill_completed(&oid3, "file:///spill/3".to_string());

        // Spill oid2 completely
        mgr.mark_pending_spill(&[oid2]);
        mgr.spill_completed(&oid2, "file:///spill/2".to_string());

        // Only oid1 should be eligible for spilling
        let to_spill = mgr.select_objects_to_spill();
        assert_eq!(to_spill.len(), 1);
        assert_eq!(to_spill[0].0, oid1);
    }

    #[test]
    fn test_mark_for_deletion_unpinned_object() {
        let mut mgr = LocalObjectManager::new(make_config());
        // Mark a non-pinned object for deletion — should still queue it
        let oid = make_oid(42);
        mgr.mark_for_deletion(oid);
        assert_eq!(mgr.num_pending_deletion(), 1);
        assert!(!mgr.is_pinned(&oid));
    }

    #[test]
    fn test_flush_empty() {
        let mut mgr = LocalObjectManager::new(make_config());
        let batch = mgr.flush_freed_objects();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_pin_multiple_objects_stats() {
        let mut mgr = LocalObjectManager::new(make_config());

        mgr.pin_object(make_oid(1), 100, 10);
        mgr.pin_object(make_oid(2), 200, 20);
        mgr.pin_object(make_oid(3), 300, 30);

        let stats = mgr.stats();
        assert_eq!(stats.num_pinned, 3);
        assert_eq!(stats.pinned_bytes, 660); // (100+10)+(200+20)+(300+30)
        assert_eq!(stats.num_pending_spill, 0);
        assert_eq!(stats.total_bytes_spilled, 0);
        assert_eq!(stats.total_objects_spilled, 0);
    }

    #[test]
    fn test_spill_and_release_interleaved() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);

        mgr.pin_object(oid, 100, 0);
        mgr.mark_pending_spill(&[oid]);
        assert_eq!(mgr.stats().pending_spill_bytes, 100);

        // Release while spill is pending — should clean up both
        mgr.release_object(&oid);
        assert_eq!(mgr.stats().pending_spill_bytes, 0);
        assert_eq!(mgr.stats().num_pending_spill, 0);
        assert_eq!(mgr.num_pinned(), 0);
    }

    #[test]
    fn test_mark_pending_spill_idempotent() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);

        mgr.pin_object(oid, 100, 0);
        mgr.mark_pending_spill(&[oid]);
        mgr.mark_pending_spill(&[oid]); // second call should be no-op
        assert_eq!(mgr.stats().pending_spill_bytes, 100);
        assert_eq!(mgr.stats().num_pending_spill, 1);
    }

    #[test]
    fn test_spill_failed_then_reattempt() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 10,
            ..make_config()
        });
        let oid = make_oid(1);

        mgr.pin_object(oid, 100, 0);
        mgr.mark_pending_spill(&[oid]);
        mgr.spill_failed(&oid);

        // Object should still be pinned and eligible for re-spill
        assert!(mgr.is_pinned(&oid));
        let to_spill = mgr.select_objects_to_spill();
        assert_eq!(to_spill.len(), 1);
        assert_eq!(to_spill[0].0, oid);
    }

    #[test]
    fn test_cumulative_spill_stats() {
        let mut mgr = LocalObjectManager::new(make_config());

        for i in 1..=5 {
            let oid = make_oid(i);
            mgr.pin_object(oid, 100 * i as i64, 0);
            mgr.mark_pending_spill(&[oid]);
            mgr.spill_completed(&oid, format!("file:///spill/{}", i));
        }

        let stats = mgr.stats();
        assert_eq!(stats.total_objects_spilled, 5);
        // 100 + 200 + 300 + 400 + 500 = 1500
        assert_eq!(stats.total_bytes_spilled, 1500);
    }

    #[test]
    fn test_delete_then_flush_all() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            free_objects_batch_size: 100, // large batch size
            ..make_config()
        });

        for i in 0..10u8 {
            mgr.pin_object(make_oid(i), 50, 0);
            mgr.mark_for_deletion(make_oid(i));
        }

        assert_eq!(mgr.num_pending_deletion(), 10);
        let batch = mgr.flush_freed_objects();
        assert_eq!(batch.len(), 10);
        assert_eq!(mgr.num_pending_deletion(), 0);
    }

    #[test]
    fn test_get_spill_url_none_when_not_spilled() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(1);
        mgr.pin_object(oid, 100, 0);
        assert!(mgr.get_spill_url(&oid).is_none());

        // Mark pending but not completed
        mgr.mark_pending_spill(&[oid]);
        assert!(mgr.get_spill_url(&oid).is_none());
    }
}
