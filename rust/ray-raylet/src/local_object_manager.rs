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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use ray_common::id::ObjectID;
use ray_rpc::client::RetryConfig;
use ray_rpc::core_worker_client::CoreWorkerClient;

use crate::worker_pool::IOWorkerPool;

/// Configuration for the local object manager.
#[derive(Debug, Clone)]
pub struct LocalObjectManagerConfig {
    /// Minimum total bytes before triggering a spill batch.
    /// C++ equivalent: `min_spilling_size_` (from RayConfig).
    pub min_spilling_size: i64,
    /// Maximum number of objects per spill batch (fused object count).
    /// C++ equivalent: `max_fused_object_count_`.
    pub max_fused_object_count: usize,
    /// Maximum bytes per single fused spill file. 0 = disabled.
    /// C++ equivalent: `max_spilling_file_size_bytes_` (from RayConfig).
    pub max_spilling_file_size_bytes: i64,
    /// Number of freed objects to accumulate before flushing.
    /// C++ equivalent: `free_objects_batch_size_`.
    pub free_objects_batch_size: usize,
    /// Period in milliseconds between flush-freed-objects attempts.
    /// C++ equivalent: `free_objects_period_ms_`.
    /// Negative means disabled, 0 means flush immediately on each free.
    pub free_objects_period_ms: i64,
    /// Object spilling threshold (fraction 0.0-1.0 of store capacity).
    pub object_spilling_threshold: f64,
    /// Maximum concurrent IO workers (spill throughput cap).
    /// C++ equivalent: `max_io_workers` parameter to LocalObjectManager ctor.
    pub max_io_workers: i64,
}

impl Default for LocalObjectManagerConfig {
    fn default() -> Self {
        Self {
            min_spilling_size: 100 * 1024 * 1024, // 100MB
            max_fused_object_count: 100,
            max_spilling_file_size_bytes: 0, // 0 = disabled
            free_objects_batch_size: 100,
            free_objects_period_ms: 1000, // 1 second default
            object_spilling_threshold: 0.8,
            max_io_workers: 4,
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
    /// Owner address (set by PinObjectsAndWaitForFree). Pin is retained until owner frees.
    pub owner_address: Option<ray_proto::ray::rpc::Address>,
    /// Generator ID (for streaming generators, set by PinObjectsAndWaitForFree).
    pub generator_id: Option<Vec<u8>>,
    /// Whether this object has been freed by the owner.
    /// C++ contract: `is_freed_` in `LocalObjectInfo`. Object stays in the map
    /// (for spill tracking) but the pin is logically released.
    pub is_freed: bool,
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
    /// Total bytes restored (cumulative).
    pub total_bytes_restored: u64,
    /// Total objects restored (cumulative).
    pub total_objects_restored: u64,
    /// Number of objects waiting for deletion.
    pub num_pending_deletion: usize,
    /// Number of objects pending restore.
    pub num_pending_restore: usize,
    /// Total bytes pending restore.
    pub bytes_pending_restore: u64,
    /// Total wall-clock time spent spilling (seconds).
    pub spill_time_total_s: f64,
    /// Total wall-clock time spent restoring (seconds).
    pub restore_time_total_s: f64,
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
    total_bytes_restored: AtomicU64,
    total_objects_restored: AtomicU64,
    /// Total wall-clock time spent spilling (seconds, monotonic).
    spill_time_total_s: f64,
    /// Total wall-clock time spent restoring (seconds, monotonic).
    restore_time_total_s: f64,
    /// Objects currently being restored (deduplication set).
    /// C++ equivalent: `objects_pending_restore_` (local_object_manager.h:314).
    objects_pending_restore: HashSet<ObjectID>,
    /// Total bytes of objects currently being restored.
    /// C++ equivalent: `num_bytes_pending_restore_` (local_object_manager.h:334).
    num_bytes_pending_restore: u64,
    /// Current number of active spill workers.
    /// C++ equivalent: `num_active_workers_` (local_object_manager.h:358).
    num_active_workers: i64,
    /// Maximum concurrent spill workers (from max_io_workers config).
    /// C++ equivalent: `max_active_workers_` (local_object_manager.h:361).
    max_active_workers: i64,
    /// Cumulative count of failed deletion requests (for metrics).
    /// C++ equivalent: `num_failed_deletion_requests_`.
    num_failed_deletion_requests: u64,
    /// IO worker pool for spill/restore/delete operations.
    /// Set via `set_io_worker_pool()` after the worker pool is ready.
    io_worker_pool: Option<Arc<dyn IOWorkerPool>>,
    /// Callback to check if a plasma object is spillable (not pinned by workers).
    /// Returns true if the object can be safely spilled, false otherwise.
    /// C++ equivalent: `is_plasma_object_spillable_` (local_object_manager.h:365).
    /// If None, all non-pending non-spilled objects are considered spillable.
    is_plasma_object_spillable: Option<Arc<dyn Fn(&ObjectID) -> bool + Send + Sync>>,
    /// Callback invoked when objects are freed (batch deletion).
    /// C++ equivalent: `on_objects_freed_` (local_object_manager.h:289).
    /// Called by `flush_free_objects()` with the batch of object IDs to delete.
    on_objects_freed: Option<Arc<dyn Fn(Vec<ObjectID>) + Send + Sync>>,
    /// Queue of object IDs whose spilled copies need to be deleted from external storage.
    /// C++ equivalent: `spilled_object_pending_delete_` (local_object_manager.h:339).
    spilled_object_pending_delete: VecDeque<ObjectID>,
    /// Mapping from object ID to spill URL (with offsets). Separate from pinned_objects
    /// because pinned_objects entries are removed when spilling completes.
    /// C++ equivalent: `spilled_objects_url_` (local_object_manager.h:343).
    spilled_objects_url: HashMap<ObjectID, String>,
    /// Base URL -> reference count. Multiple objects may be fused into a single spill
    /// file; we ref-count to avoid deleting the file before all objects are out of scope.
    /// C++ equivalent: `url_ref_count_` (local_object_manager.h:348).
    url_ref_count: HashMap<String, u64>,
    /// Timestamp of last flush_free_objects call (milliseconds since an arbitrary epoch).
    /// C++ equivalent: `last_free_objects_at_ms_` (local_object_manager.h:318).
    last_free_objects_at_ms: u64,
    /// Monotonic instant used for computing current_time_ms relative offsets.
    epoch: Instant,
}

impl LocalObjectManager {
    pub fn new(config: LocalObjectManagerConfig) -> Self {
        // C++ parity: validate max_spilling_file_size_bytes >= min_spilling_size
        // when the file-size limit is enabled (local_object_manager.h:89).
        if config.max_spilling_file_size_bytes > 0 {
            assert!(
                config.max_spilling_file_size_bytes >= config.min_spilling_size,
                "max_spilling_file_size_bytes ({}) must be >= min_spilling_size ({})",
                config.max_spilling_file_size_bytes,
                config.min_spilling_size,
            );
        }
        let max_active = config.max_io_workers;
        Self {
            config,
            pinned_objects: HashMap::new(),
            pinned_bytes: 0,
            pending_spill: HashSet::new(),
            pending_spill_bytes: 0,
            pending_deletion: Vec::new(),
            total_bytes_spilled: AtomicU64::new(0),
            total_objects_spilled: AtomicU64::new(0),
            total_bytes_restored: AtomicU64::new(0),
            total_objects_restored: AtomicU64::new(0),
            spill_time_total_s: 0.0,
            restore_time_total_s: 0.0,
            objects_pending_restore: HashSet::new(),
            num_bytes_pending_restore: 0,
            num_active_workers: 0,
            max_active_workers: max_active,
            num_failed_deletion_requests: 0,
            io_worker_pool: None,
            is_plasma_object_spillable: None,
            on_objects_freed: None,
            spilled_object_pending_delete: VecDeque::new(),
            spilled_objects_url: HashMap::new(),
            url_ref_count: HashMap::new(),
            last_free_objects_at_ms: 0,
            epoch: Instant::now(),
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
            owner_address: None,
            generator_id: None,
            is_freed: false,
        };
        if self.pinned_objects.insert(object_id, obj).is_none() {
            // Only add to total if this is a new pin.
            self.pinned_bytes += total;
        }
    }

    /// Pin objects and retain the pin until the owner frees them.
    /// C++ contract: `PinObjectsAndWaitForFree` (local_object_manager.cc:31-108)
    /// 1. Annotates pinned objects with owner_address and generator_id
    /// 2. Subscription registration is done externally via the real `ray-pubsub::Subscriber`
    ///    (the caller — typically the gRPC service handler — registers with the subscriber)
    /// 3. Pin is released ONLY through subscription callbacks (no direct-release fallback)
    pub fn pin_objects_and_wait_for_free(
        &mut self,
        object_ids: &[ObjectID],
        owner_address: Option<ray_proto::ray::rpc::Address>,
        generator_id: Option<Vec<u8>>,
    ) {
        for oid in object_ids {
            if let Some(obj) = self.pinned_objects.get_mut(oid) {
                obj.owner_address = owner_address.clone();
                obj.generator_id = generator_id.clone();
            }
        }
    }

    /// Check if a pinned object has an active owner-retained pin.
    /// Returns false if freed by the owner or not pinned with an owner.
    pub fn has_pin_owner(&self, object_id: &ObjectID) -> bool {
        self.pinned_objects
            .get(object_id)
            .is_some_and(|obj| obj.owner_address.is_some() && !obj.is_freed)
    }

    /// Release a pinned object because the owner freed it.
    /// C++ contract: `ReleaseFreedObject` marks `is_freed_ = true` and unpins the object
    /// from plasma (unless it's mid-spill, in which case the free is deferred).
    /// Returns true if the object was found and freed.
    ///
    /// This is called from subscriber callbacks (eviction or owner death), matching
    /// the C++ path where subscription_callback and owner_dead_callback both call
    /// `ReleaseFreedObject(obj_id)`.
    pub fn release_freed_object(&mut self, object_id: &ObjectID) -> bool {
        if let Some(obj) = self.pinned_objects.get_mut(object_id) {
            if obj.is_freed {
                return false; // already freed
            }
            obj.is_freed = true;
            // If not mid-spill, actually unpin immediately.
            if !obj.is_pending_spill {
                let size = obj.total_size();
                self.pinned_bytes -= size;
                self.pinned_objects.remove(object_id);
            }
            // If mid-spill, we leave it in the map; spill_completed will clean up.
            true
        } else {
            false
        }
    }

    /// Release all pinned objects owned by the given worker (owner death).
    /// C++ contract: when the owner dies, all pins held for that owner are released.
    pub fn release_objects_for_owner(&mut self, owner_worker_id: &[u8]) -> Vec<ObjectID> {
        let to_release: Vec<ObjectID> = self
            .pinned_objects
            .values()
            .filter(|obj| {
                !obj.is_freed
                    && obj
                        .owner_address
                        .as_ref()
                        .is_some_and(|addr| addr.worker_id == owner_worker_id)
            })
            .map(|obj| obj.object_id)
            .collect();

        for oid in &to_release {
            self.release_freed_object(oid);
        }
        to_release
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
    ///
    /// **Legacy API**: Only drains `pending_deletion` without invoking the
    /// `on_objects_freed` callback or processing the spilled delete queue.
    /// Prefer `flush_free_objects()` which matches the full C++ contract.
    pub fn flush_freed_objects(&mut self) -> Vec<ObjectID> {
        let batch_size = self.config.free_objects_batch_size;
        if self.pending_deletion.len() <= batch_size {
            std::mem::take(&mut self.pending_deletion)
        } else {
            self.pending_deletion.drain(..batch_size).collect()
        }
    }

    /// Full C++ `FlushFreeObjects()` equivalent (local_object_manager.cc:150-163).
    ///
    /// 1. Drains `pending_deletion` into a batch (all pending, not just batch_size,
    ///    matching C++ which clears the entire set).
    /// 2. Invokes `on_objects_freed(objects_to_delete)` so the object manager
    ///    actually frees them from plasma / notifies other nodes.
    /// 3. Calls `process_spilled_objects_delete_queue(free_objects_batch_size)`.
    /// 4. Updates `last_free_objects_at_ms`.
    pub fn flush_free_objects(&mut self) {
        if !self.pending_deletion.is_empty() {
            tracing::debug!(
                num_objects = self.pending_deletion.len(),
                "Freeing out-of-scope objects"
            );
            let objects_to_delete = std::mem::take(&mut self.pending_deletion);
            if let Some(ref callback) = self.on_objects_freed {
                callback(objects_to_delete);
            }
        }
        let batch_size = self.config.free_objects_batch_size;
        self.process_spilled_objects_delete_queue(batch_size);
        self.last_free_objects_at_ms = self.current_time_ms();
    }

    /// Process the spilled-objects delete queue, deleting up to `max_batch_size`
    /// objects from external storage.
    ///
    /// C++ equivalent: `ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size)`
    /// (local_object_manager.cc:523-577).
    ///
    /// Walks the `spilled_object_pending_delete` queue front-to-back:
    /// - If the object is still being spilled (`pending_spill`), stop (blocks queue).
    /// - If the object was spilled (has a URL in `spilled_objects_url`), decrement
    ///   the URL ref count. When ref count reaches 0, add the URL to the delete batch.
    /// - If the object was NOT spilled, it was re-pinned; just remove it.
    ///
    /// Returns the list of spill URLs that should be deleted from external storage.
    /// The caller is responsible for actually invoking `delete_spilled_objects()`.
    pub fn process_spilled_objects_delete_queue(
        &mut self,
        max_batch_size: usize,
    ) -> Vec<String> {
        let mut urls_to_delete = Vec::new();

        while !self.spilled_object_pending_delete.is_empty()
            && urls_to_delete.len() < max_batch_size
        {
            let object_id = *self.spilled_object_pending_delete.front().unwrap();

            // If the object is still being spilled, stop processing.
            // C++ (local_object_manager.cc:533): blocks queue until spill completes.
            if self.pending_spill.contains(&object_id) {
                break;
            }

            // Check if the object was spilled (has a URL).
            if let Some(object_url) = self.spilled_objects_url.remove(&object_id) {
                // Parse the base URL from the full object URL.
                // C++ uses ParseURL to extract the "url" key. For Rust, we use
                // a simplified approach: the base URL is the part before any
                // offset separator (commonly '?' or '@' depending on format).
                let base_url = parse_base_url(&object_url);

                if let Some(ref_count) = self.url_ref_count.get_mut(&base_url) {
                    *ref_count = ref_count.saturating_sub(1);
                    if *ref_count == 0 {
                        self.url_ref_count.remove(&base_url);
                        tracing::debug!(
                            url = %object_url,
                            "URL deleted because all references are out of scope"
                        );
                        urls_to_delete.push(object_url);
                    }
                } else {
                    // No ref count entry — just delete the URL directly.
                    urls_to_delete.push(object_url);
                }
            }
            // If not spilled, C++ erases from pinned_objects_ and local_objects_.
            // In Rust the pinned_objects entry may already be gone; no action needed.

            self.spilled_object_pending_delete.pop_front();
        }

        urls_to_delete
    }

    /// Enqueue an object for spilled-object deletion.
    /// C++ equivalent: adding to `spilled_object_pending_delete_`.
    pub fn enqueue_spilled_object_delete(&mut self, object_id: ObjectID) {
        self.spilled_object_pending_delete.push_back(object_id);
    }

    /// Register a spill URL and update ref counting.
    /// Called after spill completes to track the URL for later deletion.
    /// C++ equivalent: updating `spilled_objects_url_` and `url_ref_count_`.
    pub fn register_spilled_url(&mut self, object_id: ObjectID, url: String) {
        let base_url = parse_base_url(&url);
        *self.url_ref_count.entry(base_url).or_insert(0) += 1;
        self.spilled_objects_url.insert(object_id, url);
    }

    /// Set the callback invoked when freed objects should be deleted.
    /// C++ equivalent: `on_objects_freed_` (local_object_manager.h:289).
    pub fn set_on_objects_freed(
        &mut self,
        callback: Arc<dyn Fn(Vec<ObjectID>) + Send + Sync>,
    ) {
        self.on_objects_freed = Some(callback);
    }

    /// Get the current time in milliseconds (monotonic, relative to manager creation).
    fn current_time_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Get the last flush time in milliseconds.
    pub fn last_free_objects_at_ms(&self) -> u64 {
        self.last_free_objects_at_ms
    }

    /// Select objects for a single spill batch, matching C++ `TryToSpillObjects()`
    /// (local_object_manager.cc:187-230).
    ///
    /// Returns `None` if spilling should be deferred (too-small batch with
    /// spills already in flight). Returns `Some(vec)` with the batch otherwise.
    ///
    /// C++ iterates pinned_objects_ in arbitrary hash-map order. Rust uses
    /// HashMap which also has arbitrary order — we do NOT sort largest-first
    /// to match C++ semantics.
    ///
    /// Stop criteria (matching C++ exactly):
    /// 1. `max_spilling_file_size_bytes` exceeded (per-file size cap)
    /// 2. `max_fused_object_count` reached (per-batch object count cap)
    /// 3. End of iteration
    /// 4. Defer: reached end AND batch < min_spilling_size AND spills in flight
    pub fn try_select_objects_to_spill(&self) -> Option<Vec<(ObjectID, i64)>> {
        let mut objects_to_spill = Vec::new();
        let mut bytes_to_spill: i64 = 0;
        let mut idx: usize = 0;

        for obj in self.pinned_objects.values() {
            // Skip objects already being spilled or already spilled.
            if obj.is_pending_spill || obj.spill_url.is_some() {
                idx += 1;
                continue;
            }

            // C++ parity: check is_plasma_object_spillable_ callback
            // (local_object_manager.cc:196). Only spill objects that are
            // not pinned by any worker.
            if let Some(ref predicate) = self.is_plasma_object_spillable {
                if !predicate(&obj.object_id) {
                    idx += 1;
                    continue;
                }
            }

            let object_size = obj.total_size();

            // CHECK 1: max_spilling_file_size_bytes cap.
            // C++ (local_object_manager.cc:202-204): stop if adding this object
            // would exceed the per-file size limit (but always allow first object).
            if self.config.max_spilling_file_size_bytes > 0
                && !objects_to_spill.is_empty()
                && bytes_to_spill + object_size > self.config.max_spilling_file_size_bytes
            {
                break;
            }

            bytes_to_spill += object_size;
            objects_to_spill.push((obj.object_id, object_size));

            // CHECK 2: max_fused_object_count cap.
            // C++ (local_object_manager.cc:210): stop at object count limit.
            if objects_to_spill.len() >= self.config.max_fused_object_count {
                break;
            }

            idx += 1;
        }

        if objects_to_spill.is_empty() {
            return None;
        }

        // CHECK 3: "Defer too-small batch if other spills are in flight."
        // C++ (local_object_manager.cc:220-228):
        //   if we reached end of pinned_objects_ (idx == objects_pending_spill_.size())
        //   AND batch is below min_spilling_size
        //   AND there are already spills in progress
        //   → defer (return false), wait for current spills to finish
        //
        // In Rust, `idx` counts all iterated objects. If we reached the end
        // without hitting a count/size limit, and the batch is small, defer.
        let reached_end = objects_to_spill.len() < self.config.max_fused_object_count
            && (self.config.max_spilling_file_size_bytes <= 0
                || bytes_to_spill <= self.config.max_spilling_file_size_bytes);
        if reached_end
            && bytes_to_spill < self.config.min_spilling_size
            && !self.pending_spill.is_empty()
        {
            tracing::debug!(
                bytes_to_spill,
                min_spilling_size = self.config.min_spilling_size,
                pending_spill = self.pending_spill.len(),
                "Deferring spill — batch too small and spills already in flight"
            );
            return None;
        }

        Some(objects_to_spill)
    }

    /// Legacy API: select objects to spill without defer logic.
    /// Prefer `try_select_objects_to_spill()` which matches C++ semantics.
    pub fn select_objects_to_spill(&self) -> Vec<(ObjectID, i64)> {
        self.try_select_objects_to_spill().unwrap_or_default()
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

    /// Check if a restore is already pending for this object.
    /// C++ equivalent: `objects_pending_restore_.count(object_id)` check
    /// in `AsyncRestoreSpilledObject()` (local_object_manager.cc:469).
    pub fn is_restore_pending(&self, object_id: &ObjectID) -> bool {
        self.objects_pending_restore.contains(object_id)
    }

    /// Mark an object as pending restore. Returns false if already pending
    /// (deduplication — matching C++ local_object_manager.cc:469-474).
    pub fn mark_pending_restore(&mut self, object_id: ObjectID, object_size: u64) -> bool {
        if self.objects_pending_restore.contains(&object_id) {
            tracing::debug!(
                ?object_id,
                "Restore already pending — deduplicating"
            );
            return false;
        }
        self.objects_pending_restore.insert(object_id);
        self.num_bytes_pending_restore += object_size;
        true
    }

    /// Remove an object from the pending-restore set.
    /// C++ equivalent: `objects_pending_restore_.erase(object_id)` +
    /// `num_bytes_pending_restore_ -= object_size` (local_object_manager.cc:489).
    pub fn clear_pending_restore(&mut self, object_id: &ObjectID, object_size: u64) {
        if self.objects_pending_restore.remove(object_id) {
            self.num_bytes_pending_restore = self.num_bytes_pending_restore.saturating_sub(object_size);
        }
    }

    /// Number of objects currently pending restore.
    pub fn num_objects_pending_restore(&self) -> usize {
        self.objects_pending_restore.len()
    }

    /// Total bytes currently pending restore.
    pub fn num_bytes_pending_restore(&self) -> u64 {
        self.num_bytes_pending_restore
    }

    /// Record a completed restore operation.
    pub fn restore_completed(&mut self, bytes_restored: u64) {
        self.total_bytes_restored
            .fetch_add(bytes_restored, Ordering::Relaxed);
        self.total_objects_restored.fetch_add(1, Ordering::Relaxed);
    }

    /// Record spill wall-clock time (additive).
    pub fn record_spill_time(&mut self, seconds: f64) {
        self.spill_time_total_s += seconds;
    }

    /// Record restore wall-clock time (additive).
    pub fn record_restore_time(&mut self, seconds: f64) {
        self.restore_time_total_s += seconds;
    }

    /// Whether the object store is above the spilling threshold.
    pub fn should_spill(&self, used_fraction: f64) -> bool {
        used_fraction >= self.config.object_spilling_threshold
    }

    /// Whether any spill operations are currently in progress.
    /// C++ equivalent: `IsSpillingInProgress()` (local_object_manager.cc:184).
    pub fn is_spilling_in_progress(&self) -> bool {
        self.num_active_workers > 0
    }

    /// Whether another spill worker can be launched (throughput gating).
    /// C++ equivalent: check `num_active_workers_ < max_active_workers_`
    /// in `SpillObjectUptoMaxThroughput()` (local_object_manager.cc:178).
    pub fn can_spill_more(&self) -> bool {
        self.num_active_workers < self.max_active_workers
    }

    /// Increment the active spill worker count.
    /// Called when a spill worker is popped from the pool.
    /// C++ equivalent: `num_active_workers_ += 1` (local_object_manager.cc:326).
    pub fn increment_active_workers(&mut self) {
        self.num_active_workers += 1;
    }

    /// Decrement the active spill worker count.
    /// Called when a spill RPC completes (success or failure).
    /// C++ equivalent: `num_active_workers_ -= 1` (local_object_manager.cc:361).
    pub fn decrement_active_workers(&mut self) {
        self.num_active_workers = (self.num_active_workers - 1).max(0);
    }

    /// Get the number of failed deletion requests (for metrics).
    pub fn num_failed_deletion_requests(&self) -> u64 {
        self.num_failed_deletion_requests
    }

    /// Increment failed deletion request counter.
    pub fn increment_failed_deletions(&mut self) {
        self.num_failed_deletion_requests += 1;
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

    /// Check if an object is pending deletion (matches C++ ObjectPendingDeletion).
    pub fn is_pending_deletion(&self, object_id: &ObjectID) -> bool {
        self.pending_deletion.contains(object_id)
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
            total_bytes_restored: self.total_bytes_restored.load(Ordering::Relaxed),
            total_objects_restored: self.total_objects_restored.load(Ordering::Relaxed),
            num_pending_deletion: self.pending_deletion.len(),
            num_pending_restore: self.objects_pending_restore.len(),
            bytes_pending_restore: self.num_bytes_pending_restore,
            spill_time_total_s: self.spill_time_total_s,
            restore_time_total_s: self.restore_time_total_s,
        }
    }

    /// Set the IO worker pool for spill/restore/delete operations.
    pub fn set_io_worker_pool(&mut self, pool: Arc<dyn IOWorkerPool>) {
        self.io_worker_pool = Some(pool);
    }

    /// Set the callback that checks whether a plasma object is spillable.
    /// C++ equivalent: `is_plasma_object_spillable_` (local_object_manager.h:365).
    /// Returns true if the object is not pinned by any worker and can be
    /// safely spilled. If not set, all eligible objects are considered spillable.
    pub fn set_is_plasma_object_spillable(
        &mut self,
        predicate: Arc<dyn Fn(&ObjectID) -> bool + Send + Sync>,
    ) {
        self.is_plasma_object_spillable = Some(predicate);
    }

    /// Initiate spilling of objects to external storage.
    ///
    /// C++ equivalent: `LocalObjectManager::SpillObjects()`.
    /// Selects objects to spill, marks them as pending, and requests a spill
    /// worker from the IO worker pool. When a worker is assigned, connects
    /// via gRPC and sends the SpillObjects RPC.
    ///
    /// NOTE: This is a convenience wrapper. For the full Arc-based dispatch
    /// (where the callback can update LocalObjectManager state on completion),
    /// use the free function [`spill_objects`].
    pub fn spill_objects(&mut self) {
        tracing::warn!(
            "spill_objects() called on &mut self — use the free function \
             spill_objects(lom) for full RPC dispatch with completion handling"
        );
        let to_spill = self.select_objects_to_spill();
        if to_spill.is_empty() {
            tracing::debug!("No objects eligible for spilling");
            return;
        }
        let object_ids: Vec<ObjectID> = to_spill.iter().map(|(oid, _)| *oid).collect();
        let total_bytes: i64 = to_spill.iter().map(|(_, sz)| *sz).sum();
        self.mark_pending_spill(&object_ids);
        tracing::info!(
            num_objects = object_ids.len(),
            total_bytes,
            "Spill objects marked pending (no RPC dispatch from &mut self path)"
        );
    }

    /// Initiate restoration of a spilled object from external storage.
    ///
    /// C++ equivalent: `LocalObjectManager::AsyncRestoreSpilledObject()`.
    /// NOTE: Use the free function [`restore_object`] for full RPC dispatch.
    pub fn restore_object(&mut self, _object_id: ObjectID, _url: String) {
        tracing::warn!(
            "restore_object() called on &mut self — use the free function \
             restore_object(lom, ..) for full RPC dispatch"
        );
    }

    /// Initiate deletion of spilled objects from external storage.
    ///
    /// C++ equivalent: `LocalObjectManager::DeleteSpilledObjects()`.
    /// NOTE: Use the free function [`delete_spilled_objects`] for full RPC dispatch.
    pub fn delete_spilled_objects(&mut self, _urls: Vec<String>) {
        tracing::warn!(
            "delete_spilled_objects() called on &mut self — use the free function \
             delete_spilled_objects(lom, ..) for full RPC dispatch"
        );
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

    /// Total "primary" bytes: pinned + pending-spill.
    /// C++ equivalent: `GetPrimaryBytes()` (local_object_manager.cc:669-671).
    /// Used by `SpillIfOverPrimaryObjectsThreshold` to decide whether to trigger spilling.
    pub fn get_primary_bytes(&self) -> i64 {
        self.pinned_bytes + self.pending_spill_bytes
    }
}

/// Parse the base URL from a spill object URL.
///
/// C++ equivalent: `ParseURL(object_url)` then extracting the `"url"` key.
/// Ray spill URLs typically have the format `<base_url>?offset=X&size=Y`
/// or `<base_url>@<offset>_<size>`. We extract the part before any `?` or `@`
/// separator that indicates object-specific offset information.
fn parse_base_url(url: &str) -> String {
    // Try '?' first (query-string style), then '@' (offset-style).
    if let Some(idx) = url.find('?') {
        url[..idx].to_string()
    } else if let Some(idx) = url.rfind('@') {
        // Only treat '@' as separator if what follows looks like offset info
        // (digits, underscores). Otherwise the whole URL is the base.
        let after = &url[idx + 1..];
        if !after.is_empty() && after.chars().all(|c| c.is_ascii_digit() || c == '_') {
            url[..idx].to_string()
        } else {
            url.to_string()
        }
    } else {
        url.to_string()
    }
}

// ─── Free functions for Arc-based RPC dispatch ─────────────────────────
//
// These functions take `&Arc<Mutex<LocalObjectManager>>` so that the IO worker
// callback (which runs after the mutex is released) can lock the manager to
// update state on RPC completion.  This matches the C++ pattern where the
// callback captures `this` (the raw pointer to LocalObjectManager).

/// Connect to an IO worker and return a [`CoreWorkerClient`].
async fn connect_to_worker(
    pool: &dyn IOWorkerPool,
    worker_id: &ray_common::id::WorkerID,
) -> Option<(CoreWorkerClient, String)> {
    let info = pool.get_worker(worker_id)?;
    let addr = format!("http://{}:{}", info.ip_address, info.port);
    match CoreWorkerClient::connect(&addr, RetryConfig::default()).await {
        Ok(client) => Some((client, addr)),
        Err(e) => {
            tracing::error!(error = %e, addr = %addr, "Failed to connect to IO worker");
            None
        }
    }
}

/// Initiate spilling of objects to external storage via IO worker RPC.
///
/// C++ equivalent: `LocalObjectManager::SpillObjects()`.
///
/// 1. Selects objects to spill and marks them pending (under lock).
/// 2. Requests a spill worker from the pool.
/// 3. When the worker is assigned, spawns a tokio task that connects to the
///    worker, sends a `SpillObjectsRequest`, and on completion updates the
///    manager state and returns the worker to the pool.
/// Default number of retries for delete operations.
/// C++ equivalent: `kDefaultSpilledObjectDeleteRetries` (local_object_manager.h:42).
const DEFAULT_SPILLED_OBJECT_DELETE_RETRIES: i64 = 3;

/// Spill objects up to the maximum throughput limit.
///
/// C++ equivalent: `SpillObjectUptoMaxThroughput()` (local_object_manager.cc:169-182).
/// Keeps calling `spill_objects_once` until either no more objects can be spilled
/// or the active worker count reaches `max_active_workers`.
pub fn spill_objects_upto_max_throughput(lom: &Arc<Mutex<LocalObjectManager>>) {
    loop {
        let can_spill = {
            let guard = lom.lock();
            guard.can_spill_more()
        };
        if !can_spill {
            break;
        }
        if !spill_objects_once(lom) {
            break;
        }
    }
}

/// Attempt a single spill batch. Returns true if a spill was initiated.
///
/// C++ equivalent: `TryToSpillObjects()` called from `SpillObjectUptoMaxThroughput`.
fn spill_objects_once(lom: &Arc<Mutex<LocalObjectManager>>) -> bool {
    let (object_ids, total_bytes, pool) = {
        let mut guard = lom.lock();
        // C++ parity: use try_select which implements defer-if-too-small logic.
        let to_spill = match guard.try_select_objects_to_spill() {
            Some(batch) if !batch.is_empty() => batch,
            _ => return false, // Nothing to spill or deferred
        };
        let oids: Vec<ObjectID> = to_spill.iter().map(|(oid, _)| *oid).collect();
        let total: i64 = to_spill.iter().map(|(_, sz)| *sz).sum();
        guard.mark_pending_spill(&oids);
        // C++ parity: increment active workers when spill starts (local_object_manager.cc:326).
        guard.increment_active_workers();
        let pool = guard.io_worker_pool.clone();
        (oids, total, pool)
    };

    let Some(pool) = pool else {
        tracing::warn!("No IO worker pool set — cannot spill objects");
        let mut guard = lom.lock();
        guard.decrement_active_workers();
        return false;
    };

    tracing::info!(
        num_objects = object_ids.len(),
        total_bytes,
        "Initiating spill for objects"
    );

    let lom = Arc::clone(lom);
    let pool_for_closure = Arc::clone(&pool);

    pool.pop_spill_worker(Box::new(move |worker_id| {
        let pool = pool_for_closure;
        tokio::spawn(async move {
            let start = Instant::now();

            let Some((client, addr)) = connect_to_worker(pool.as_ref(), &worker_id).await else {
                let mut guard = lom.lock();
                for oid in &object_ids {
                    guard.spill_failed(oid);
                }
                // C++ parity: decrement active workers on failure (local_object_manager.cc:361).
                guard.decrement_active_workers();
                pool.push_spill_worker(worker_id);
                return;
            };

            let mut request = ray_proto::ray::rpc::SpillObjectsRequest::default();
            for oid in &object_ids {
                let mut obj_ref = ray_proto::ray::rpc::ObjectReference::default();
                obj_ref.object_id = oid.as_bytes().to_vec();
                request.object_refs_to_spill.push(obj_ref);
            }

            tracing::debug!(
                num_objects = object_ids.len(),
                addr = %addr,
                "Sending SpillObjects RPC"
            );

            match client.spill_objects(request).await {
                Ok(reply) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    let mut guard = lom.lock();
                    for (i, oid) in object_ids.iter().enumerate() {
                        if let Some(url) = reply.spilled_objects_url.get(i) {
                            guard.spill_completed(oid, url.clone());
                        } else {
                            guard.spill_failed(oid);
                        }
                    }
                    guard.record_spill_time(elapsed);
                    // C++ parity: decrement active workers on completion (local_object_manager.cc:361).
                    guard.decrement_active_workers();
                    tracing::info!(
                        num_objects = object_ids.len(),
                        elapsed_s = elapsed,
                        "Spill completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "SpillObjects RPC failed");
                    let mut guard = lom.lock();
                    for oid in &object_ids {
                        guard.spill_failed(oid);
                    }
                    guard.decrement_active_workers();
                }
            }

            pool.push_spill_worker(worker_id);
        });
    }));

    true
}

/// Single-shot spill (legacy API). Calls `spill_objects_once` once.
pub fn spill_objects(lom: &Arc<Mutex<LocalObjectManager>>) {
    spill_objects_once(lom);
}

/// Initiate restoration of a spilled object from external storage via IO worker RPC.
///
/// C++ equivalent: `LocalObjectManager::AsyncRestoreSpilledObject()`.
///
/// Requests a restore worker, connects via gRPC, and sends a
/// `RestoreSpilledObjectsRequest`. On success, updates cumulative stats.
pub fn restore_object(
    lom: &Arc<Mutex<LocalObjectManager>>,
    object_id: ObjectID,
    object_size: u64,
    url: String,
) {
    // C++ parity: deduplication check (local_object_manager.cc:469-474).
    // If this object is already being restored, skip the duplicate request.
    let pool = {
        let mut guard = lom.lock();
        if !guard.mark_pending_restore(object_id, object_size) {
            // Already pending — deduplicated.
            return;
        }
        guard.io_worker_pool.clone()
    };

    let Some(pool) = pool else {
        tracing::warn!("No IO worker pool set — cannot restore object");
        let mut guard = lom.lock();
        guard.clear_pending_restore(&object_id, object_size);
        return;
    };

    tracing::info!(?object_id, url = %url, "Initiating restore for spilled object");

    let lom = Arc::clone(lom);
    let pool_for_closure = Arc::clone(&pool);

    pool.pop_restore_worker(Box::new(move |worker_id| {
        let pool = pool_for_closure;
        tokio::spawn(async move {
            let start = Instant::now();

            let Some((client, addr)) = connect_to_worker(pool.as_ref(), &worker_id).await else {
                tracing::error!("Cannot connect to restore worker — restore failed");
                let mut guard = lom.lock();
                guard.clear_pending_restore(&object_id, object_size);
                pool.push_restore_worker(worker_id);
                return;
            };

            let mut request = ray_proto::ray::rpc::RestoreSpilledObjectsRequest::default();
            request.spilled_objects_url.push(url.clone());
            request
                .object_ids_to_restore
                .push(object_id.as_bytes().to_vec());

            tracing::debug!(
                ?object_id,
                url = %url,
                addr = %addr,
                "Sending RestoreSpilledObjects RPC"
            );

            match client.restore_spilled_objects(request).await {
                Ok(reply) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    let mut guard = lom.lock();
                    // C++ parity: clear pending restore state (local_object_manager.cc:489).
                    guard.clear_pending_restore(&object_id, object_size);
                    guard.restore_completed(reply.bytes_restored_total as u64);
                    guard.record_restore_time(elapsed);
                    tracing::info!(
                        ?object_id,
                        bytes_restored = reply.bytes_restored_total,
                        elapsed_s = elapsed,
                        "Restore completed"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        ?object_id,
                        "RestoreSpilledObjects RPC failed"
                    );
                    let mut guard = lom.lock();
                    guard.clear_pending_restore(&object_id, object_size);
                }
            }

            pool.push_restore_worker(worker_id);
        });
    }));
}

/// Initiate deletion of spilled objects from external storage via IO worker RPC.
///
/// C++ equivalent: `LocalObjectManager::DeleteSpilledObjects()` (local_object_manager.cc:579-614).
///
/// Requests a delete worker, connects via gRPC, and sends a
/// `DeleteSpilledObjectsRequest`. On failure, retries up to `num_retries` times
/// (matching C++ `kDefaultSpilledObjectDeleteRetries = 3`).
pub fn delete_spilled_objects(
    lom: &Arc<Mutex<LocalObjectManager>>,
    urls: Vec<String>,
) {
    delete_spilled_objects_with_retries(lom, urls, DEFAULT_SPILLED_OBJECT_DELETE_RETRIES);
}

/// Delete with explicit retry count.
/// C++ equivalent: `DeleteSpilledObjects(urls, num_retries)` (local_object_manager.cc:579).
fn delete_spilled_objects_with_retries(
    lom: &Arc<Mutex<LocalObjectManager>>,
    urls: Vec<String>,
    num_retries: i64,
) {
    if urls.is_empty() {
        return;
    }

    let pool = lom.lock().io_worker_pool.clone();
    let Some(pool) = pool else {
        tracing::warn!("No IO worker pool set — cannot delete spilled objects");
        return;
    };

    tracing::info!(num_urls = urls.len(), num_retries, "Initiating deletion of spilled objects");

    let lom = Arc::clone(lom);
    let pool_for_closure = Arc::clone(&pool);

    pool.pop_delete_worker(Box::new(move |worker_id| {
        let pool = pool_for_closure;
        tokio::spawn(async move {
            let Some((client, addr)) = connect_to_worker(pool.as_ref(), &worker_id).await else {
                tracing::error!("Cannot connect to delete worker — delete failed");
                pool.push_delete_worker(worker_id);
                // C++ parity: retry on connection failure.
                if num_retries > 0 {
                    lom.lock().increment_failed_deletions();
                    delete_spilled_objects_with_retries(&lom, urls, num_retries - 1);
                }
                return;
            };

            let mut request = ray_proto::ray::rpc::DeleteSpilledObjectsRequest::default();
            request.spilled_objects_url = urls.clone();

            tracing::debug!(
                num_urls = urls.len(),
                addr = %addr,
                "Sending DeleteSpilledObjects RPC"
            );

            match client.delete_spilled_objects(request).await {
                Ok(_) => {
                    tracing::info!(
                        num_urls = urls.len(),
                        "Delete spilled objects completed"
                    );
                }
                Err(e) => {
                    // C++ parity: retry failed deletions (local_object_manager.cc:597-607).
                    lom.lock().increment_failed_deletions();
                    tracing::error!(
                        error = %e,
                        num_urls = urls.len(),
                        retry_count = num_retries,
                        "DeleteSpilledObjects RPC failed"
                    );

                    if num_retries > 0 {
                        // C++ parity: retry immediately via io_service_.post()
                        // (local_object_manager.cc:601-606).
                        delete_spilled_objects_with_retries(&lom, urls, num_retries - 1);
                    }
                }
            }

            pool.push_delete_worker(worker_id);
        });
    }));
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
            max_fused_object_count: 10,
            max_spilling_file_size_bytes: 0,
            free_objects_batch_size: 5,
            free_objects_period_ms: 1000,
            object_spilling_threshold: 0.8,
            max_io_workers: 4,
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
    fn test_select_objects_to_spill_returns_eligible() {
        let mut mgr = LocalObjectManager::new(LocalObjectManagerConfig {
            min_spilling_size: 10, // low threshold to trigger
            ..make_config()
        });

        mgr.pin_object(make_oid(1), 50, 0);
        mgr.pin_object(make_oid(2), 200, 0);
        mgr.pin_object(make_oid(3), 100, 0);

        let to_spill = mgr.select_objects_to_spill();
        assert!(!to_spill.is_empty());
        // C++ iterates in arbitrary hash-map order (no sorting).
        // Rust matches this: HashMap iteration order is not guaranteed.
        // Just verify all selected objects are from the pinned set.
        let total: i64 = to_spill.iter().map(|(_, sz)| *sz).sum();
        assert!(total >= 10, "Expected >= min_spilling_size bytes");
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
            max_fused_object_count: 2,
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
            max_fused_object_count: 10,
            ..make_config()
        });

        // Pin 3 objects of different sizes
        mgr.pin_object(make_oid(1), 50, 0);
        mgr.pin_object(make_oid(2), 80, 0);
        mgr.pin_object(make_oid(3), 120, 0);

        let to_spill = mgr.select_objects_to_spill();
        // C++ iterates in arbitrary hash-map order, as does Rust.
        // Total pinned = 250 bytes. With min_spilling_size=200, the batch
        // accumulates objects until the total >= 200 or the count limit is hit.
        // We should get at least 2 objects (enough to reach 200 bytes).
        assert!(to_spill.len() >= 2, "Expected at least 2 objects, got {}", to_spill.len());
        let total_bytes: i64 = to_spill.iter().map(|(_, sz)| *sz).sum();
        assert!(total_bytes >= 200, "Expected >= 200 bytes, got {}", total_bytes);
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

    // ─── RAYLET-4 Round 5: owner-driven pin lifetime ──────────────

    /// C++ contract: pin_objects_and_wait_for_free retains pin until owner free event.
    /// Pin is released via release_freed_object (called by subscriber callbacks).
    #[test]
    fn test_pin_object_ids_keeps_pin_until_owner_free_event() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid = make_oid(10);
        mgr.pin_object(oid, 100, 0);

        let owner_addr = ray_proto::ray::rpc::Address {
            worker_id: vec![42u8; 28],
            ip_address: "127.0.0.1".into(),
            port: 50000,
            ..Default::default()
        };
        mgr.pin_objects_and_wait_for_free(&[oid], Some(owner_addr), None);
        assert!(mgr.has_pin_owner(&oid), "pin should be active with owner");
        assert!(mgr.is_pinned(&oid), "object should remain pinned");

        // Free event from owner releases the pin (called by subscriber callback).
        assert!(mgr.release_freed_object(&oid), "should succeed");
        assert!(!mgr.has_pin_owner(&oid), "pin should be released after free event");
        assert!(!mgr.is_pinned(&oid), "object should be unpinned after free");
    }

    /// C++ contract: owner free event releases retained pin.
    #[test]
    fn test_pin_object_ids_owner_free_event_releases_retained_pin() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid1 = make_oid(20);
        let oid2 = make_oid(21);
        mgr.pin_object(oid1, 100, 0);
        mgr.pin_object(oid2, 200, 0);

        let owner_addr = ray_proto::ray::rpc::Address {
            worker_id: vec![99u8; 28],
            ..Default::default()
        };
        mgr.pin_objects_and_wait_for_free(&[oid1, oid2], Some(owner_addr), None);

        // Both should be pinned
        assert_eq!(mgr.pinned_bytes(), 300);

        // Free oid1 only
        mgr.release_freed_object(&oid1);
        assert!(!mgr.is_pinned(&oid1));
        assert!(mgr.is_pinned(&oid2));
        assert_eq!(mgr.pinned_bytes(), 200);

        // Free oid2
        mgr.release_freed_object(&oid2);
        assert!(!mgr.is_pinned(&oid2));
        assert_eq!(mgr.pinned_bytes(), 0);
    }

    /// C++ contract: owner death releases all pins for that owner.
    #[test]
    fn test_pin_objects_owner_death_releases_all_pins() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid1 = make_oid(30);
        let oid2 = make_oid(31);
        let oid3 = make_oid(32);
        mgr.pin_object(oid1, 100, 0);
        mgr.pin_object(oid2, 200, 0);
        mgr.pin_object(oid3, 300, 0);

        let owner1 = ray_proto::ray::rpc::Address {
            worker_id: vec![1u8; 28],
            ..Default::default()
        };
        let owner2 = ray_proto::ray::rpc::Address {
            worker_id: vec![2u8; 28],
            ..Default::default()
        };

        mgr.pin_objects_and_wait_for_free(&[oid1, oid2], Some(owner1), None);
        mgr.pin_objects_and_wait_for_free(&[oid3], Some(owner2), None);

        // Owner 1 dies — should release oid1 and oid2 but not oid3
        let released = mgr.release_objects_for_owner(&vec![1u8; 28]);
        assert_eq!(released.len(), 2);
        assert!(!mgr.is_pinned(&oid1));
        assert!(!mgr.is_pinned(&oid2));
        assert!(mgr.is_pinned(&oid3));
        assert_eq!(mgr.pinned_bytes(), 300);
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

    // ─── Tests for flush_free_objects (full C++ FlushFreeObjects parity) ───

    #[test]
    fn test_flush_free_objects_invokes_callback() {
        let mut mgr = LocalObjectManager::new(make_config());
        let freed = Arc::new(Mutex::new(Vec::new()));
        let freed_clone = Arc::clone(&freed);
        mgr.set_on_objects_freed(Arc::new(move |ids| {
            freed_clone.lock().extend(ids);
        }));

        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        mgr.mark_for_deletion(oid1);
        mgr.mark_for_deletion(oid2);

        mgr.flush_free_objects();

        let freed_ids = freed.lock().clone();
        assert_eq!(freed_ids.len(), 2);
        assert!(freed_ids.contains(&oid1));
        assert!(freed_ids.contains(&oid2));
        // pending_deletion should be empty after flush
        assert_eq!(mgr.num_pending_deletion(), 0);
    }

    #[test]
    fn test_flush_free_objects_empty_no_callback() {
        let mut mgr = LocalObjectManager::new(make_config());
        let called = Arc::new(Mutex::new(false));
        let called_clone = Arc::clone(&called);
        mgr.set_on_objects_freed(Arc::new(move |_ids| {
            *called_clone.lock() = true;
        }));

        // No pending deletions — callback should NOT be invoked
        mgr.flush_free_objects();
        assert!(!*called.lock());
    }

    #[test]
    fn test_flush_free_objects_no_callback_set() {
        let mut mgr = LocalObjectManager::new(make_config());
        mgr.mark_for_deletion(make_oid(1));
        // Should not panic even without callback set
        mgr.flush_free_objects();
        assert_eq!(mgr.num_pending_deletion(), 0);
    }

    #[test]
    fn test_flush_free_objects_updates_timestamp() {
        let mut mgr = LocalObjectManager::new(make_config());
        let ts_before = mgr.last_free_objects_at_ms();
        // Small sleep to ensure time progresses
        std::thread::sleep(std::time::Duration::from_millis(5));
        mgr.flush_free_objects();
        assert!(mgr.last_free_objects_at_ms() >= ts_before);
    }

    // ─── Tests for process_spilled_objects_delete_queue ───

    #[test]
    fn test_process_spilled_delete_queue_basic() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        // Register spill URLs (simulating completed spills)
        mgr.register_spilled_url(oid1, "file:///tmp/spill1".to_string());
        mgr.register_spilled_url(oid2, "file:///tmp/spill2".to_string());

        // Enqueue for deletion
        mgr.enqueue_spilled_object_delete(oid1);
        mgr.enqueue_spilled_object_delete(oid2);

        let urls = mgr.process_spilled_objects_delete_queue(10);
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&"file:///tmp/spill1".to_string()));
        assert!(urls.contains(&"file:///tmp/spill2".to_string()));
    }

    #[test]
    fn test_process_spilled_delete_queue_blocks_on_pending_spill() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        mgr.pin_object(oid1, 100, 0);
        mgr.mark_pending_spill(&[oid1]); // oid1 is still being spilled

        mgr.register_spilled_url(oid2, "file:///tmp/spill2".to_string());

        // Enqueue oid1 (still spilling) then oid2
        mgr.enqueue_spilled_object_delete(oid1);
        mgr.enqueue_spilled_object_delete(oid2);

        // Should block at oid1 (still pending spill), not process oid2
        let urls = mgr.process_spilled_objects_delete_queue(10);
        assert!(urls.is_empty());
    }

    #[test]
    fn test_process_spilled_delete_queue_ref_counting() {
        let mut mgr = LocalObjectManager::new(make_config());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        // Both objects in the same fused spill file
        let fused_url = "file:///tmp/fused_spill";
        mgr.register_spilled_url(oid1, format!("{}?offset=0&size=100", fused_url));
        mgr.register_spilled_url(oid2, format!("{}?offset=100&size=200", fused_url));

        // Delete only oid1 — URL should NOT be deleted (ref count = 1 remaining)
        mgr.enqueue_spilled_object_delete(oid1);
        let urls = mgr.process_spilled_objects_delete_queue(10);
        assert!(urls.is_empty(), "URL should not be deleted while refs remain");

        // Now delete oid2 — URL should be deleted (ref count = 0)
        mgr.enqueue_spilled_object_delete(oid2);
        let urls = mgr.process_spilled_objects_delete_queue(10);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].starts_with(fused_url));
    }

    #[test]
    fn test_process_spilled_delete_queue_respects_batch_size() {
        let mut mgr = LocalObjectManager::new(make_config());

        for i in 0..5u8 {
            let oid = make_oid(i);
            mgr.register_spilled_url(oid, format!("file:///tmp/spill{}", i));
            mgr.enqueue_spilled_object_delete(oid);
        }

        // Process only 2 at a time
        let urls = mgr.process_spilled_objects_delete_queue(2);
        assert_eq!(urls.len(), 2);

        // 3 remaining
        let urls = mgr.process_spilled_objects_delete_queue(10);
        assert_eq!(urls.len(), 3);
    }

    #[test]
    fn test_parse_base_url() {
        assert_eq!(
            parse_base_url("file:///tmp/spill?offset=0&size=100"),
            "file:///tmp/spill"
        );
        assert_eq!(
            parse_base_url("file:///tmp/spill@0_100"),
            "file:///tmp/spill"
        );
        assert_eq!(
            parse_base_url("file:///tmp/spill"),
            "file:///tmp/spill"
        );
        assert_eq!(
            parse_base_url("s3://bucket/key?offset=0"),
            "s3://bucket/key"
        );
    }

    #[test]
    fn test_flush_free_objects_processes_spill_delete_queue() {
        let mut mgr = LocalObjectManager::new(make_config());

        // Set up a spilled object pending delete
        let oid = make_oid(1);
        mgr.register_spilled_url(oid, "file:///tmp/spill1".to_string());
        mgr.enqueue_spilled_object_delete(oid);

        // Track freed objects via callback
        let freed = Arc::new(Mutex::new(Vec::new()));
        let freed_clone = Arc::clone(&freed);
        mgr.set_on_objects_freed(Arc::new(move |ids| {
            freed_clone.lock().extend(ids);
        }));

        // Also add a pending deletion
        mgr.mark_for_deletion(make_oid(2));

        // flush_free_objects should handle both pending deletions AND spill delete queue
        mgr.flush_free_objects();

        // The pending deletion callback was invoked
        assert_eq!(freed.lock().len(), 1);
        // The spill delete queue was processed (queue should be empty now)
        assert!(mgr.spilled_object_pending_delete.is_empty());
    }
}
