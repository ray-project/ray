// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Transport loop for the object manager.
//!
//! Drives the PullManager and PushManager by periodically:
//! 1. Querying `PullManager::get_ready_pull_targets()` → initiating fetches
//! 2. Querying `PullManager::get_spilled_pull_targets()` → restoring spills
//! 3. Querying `PushManager::get_chunks_to_send()` → sending chunks
//! 4. Detecting timed-out pushes → cancelling stale transfers

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use ray_common::id::{NodeID, ObjectID};

use crate::object_manager::ObjectManager;
use crate::spill_manager::SpillManager;

/// Configuration for the transport loop.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Tick interval in milliseconds.
    pub tick_interval_ms: u64,
    /// Push timeout in milliseconds.
    pub push_timeout_ms: f64,
    /// Maximum concurrent pull operations.
    pub max_concurrent_pulls: usize,
    /// Maximum concurrent restore operations.
    pub max_concurrent_restores: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: 100,
            push_timeout_ms: 10_000.0,
            max_concurrent_pulls: 10,
            max_concurrent_restores: 5,
        }
    }
}

/// Callback for sending a push chunk to a remote node.
/// (node_id, object_id, chunk_index, chunk_data) → success.
pub type SendChunkCallback = Arc<dyn Fn(&NodeID, &ObjectID, i64, &[u8]) -> bool + Send + Sync>;

/// Callback for initiating a pull from a remote node.
/// (object_id, node_id) → success.
pub type PullObjectCallback = Arc<dyn Fn(&ObjectID, &NodeID) -> bool + Send + Sync>;

/// Callback for restoring a spilled object.
/// (object_id, spill_url, spill_node_id) → success.
pub type RestoreObjectCallback = Arc<dyn Fn(&ObjectID, &str, &NodeID) -> bool + Send + Sync>;

/// Statistics from the transport loop.
#[derive(Debug, Clone, Default)]
pub struct TransportStats {
    pub total_ticks: u64,
    pub total_pulls_initiated: u64,
    pub total_restores_initiated: u64,
    pub total_chunks_sent: u64,
    pub total_pushes_timed_out: u64,
}

/// The transport loop periodically drives the object manager's pull and push
/// operations by calling into the PullManager and PushManager.
///
/// This is a pure data-structure approach: it provides a `tick()` method
/// that should be called periodically (e.g., from a tokio task).
pub struct TransportLoop {
    config: TransportConfig,
    object_manager: Arc<Mutex<ObjectManager>>,
    spill_manager: Option<Arc<SpillManager>>,
    // Callbacks for actual network/disk I/O.
    send_chunk_cb: Option<SendChunkCallback>,
    pull_object_cb: Option<PullObjectCallback>,
    restore_object_cb: Option<RestoreObjectCallback>,
    // Stats.
    stats: Mutex<TransportStats>,
    running: AtomicBool,
}

impl TransportLoop {
    /// Create a new transport loop.
    pub fn new(config: TransportConfig, object_manager: Arc<Mutex<ObjectManager>>) -> Self {
        Self {
            config,
            object_manager,
            spill_manager: None,
            send_chunk_cb: None,
            pull_object_cb: None,
            restore_object_cb: None,
            stats: Mutex::new(TransportStats::default()),
            running: AtomicBool::new(false),
        }
    }

    /// Set the spill manager for restore operations.
    pub fn set_spill_manager(&mut self, spill_manager: Arc<SpillManager>) {
        self.spill_manager = Some(spill_manager);
    }

    /// Set callback for sending chunks to remote nodes.
    pub fn set_send_chunk_callback(&mut self, cb: SendChunkCallback) {
        self.send_chunk_cb = Some(cb);
    }

    /// Set callback for pulling objects from remote nodes.
    pub fn set_pull_object_callback(&mut self, cb: PullObjectCallback) {
        self.pull_object_cb = Some(cb);
    }

    /// Set callback for restoring spilled objects.
    pub fn set_restore_object_callback(&mut self, cb: RestoreObjectCallback) {
        self.restore_object_cb = Some(cb);
    }

    /// Execute one tick of the transport loop.
    ///
    /// This is the main entry point, called periodically.
    pub fn tick(&self, now_ms: f64) {
        let mut stats = self.stats.lock();
        stats.total_ticks += 1;
        drop(stats);

        self.process_pulls(now_ms);
        self.process_restores();
        self.process_pushes();
        self.process_timeouts(now_ms);
    }

    /// Process pending pull operations.
    fn process_pulls(&self, now_ms: f64) {
        let pull_cb = match &self.pull_object_cb {
            Some(cb) => cb,
            None => return,
        };

        let targets = {
            let om = self.object_manager.lock();
            om.pull_manager().get_ready_pull_targets(now_ms)
        };

        let mut initiated = 0;
        for (object_id, node_id) in targets {
            if initiated >= self.config.max_concurrent_pulls {
                break;
            }
            let can_pull = {
                let mut om = self.object_manager.lock();
                om.pull_manager_mut().start_pulling(&object_id)
            };
            if can_pull {
                if pull_cb(&object_id, &node_id) {
                    initiated += 1;
                } else {
                    // Pull failed to start — release quota.
                    let mut om = self.object_manager.lock();
                    om.pull_manager_mut().finish_pulling(&object_id);
                }
            }
        }

        if initiated > 0 {
            let mut stats = self.stats.lock();
            stats.total_pulls_initiated += initiated as u64;
        }
    }

    /// Process pending restore (spilled object) operations.
    fn process_restores(&self) {
        let restore_cb = match &self.restore_object_cb {
            Some(cb) => cb,
            None => return,
        };

        let targets = {
            let om = self.object_manager.lock();
            om.pull_manager().get_spilled_pull_targets()
        };

        let mut initiated = 0;
        for (object_id, url, node_id) in targets {
            if initiated >= self.config.max_concurrent_restores {
                break;
            }
            if restore_cb(&object_id, &url, &node_id) {
                initiated += 1;
            }
        }

        if initiated > 0 {
            let mut stats = self.stats.lock();
            stats.total_restores_initiated += initiated as u64;
        }
    }

    /// Process pending push (send chunk) operations.
    fn process_pushes(&self) {
        let send_cb = match &self.send_chunk_cb {
            Some(cb) => cb,
            None => return,
        };

        let chunks = {
            let mut om = self.object_manager.lock();
            om.push_manager_mut().get_chunks_to_send()
        };

        let mut sent = 0;
        for (node_id, object_id, chunk_index) in &chunks {
            // Read the object data from the plasma store and extract the chunk.
            let chunk_data = {
                let om = self.object_manager.lock();
                let chunk_size = om.config().object_chunk_size as usize;
                match om.get_object_data(object_id) {
                    Some((data, metadata)) => {
                        let total_size = data.len() + metadata.len();
                        let start = (*chunk_index as usize) * chunk_size;
                        if start >= total_size {
                            Vec::new()
                        } else {
                            let end = (start + chunk_size).min(total_size);
                            // Slice across data + metadata boundary.
                            if start < data.len() {
                                let data_end = end.min(data.len());
                                let mut chunk = data[start..data_end].to_vec();
                                if end > data.len() {
                                    let meta_start = 0;
                                    let meta_end = end - data.len();
                                    chunk.extend_from_slice(&metadata[meta_start..meta_end]);
                                }
                                chunk
                            } else {
                                let meta_start = start - data.len();
                                let meta_end = end - data.len();
                                metadata[meta_start..meta_end].to_vec()
                            }
                        }
                    }
                    None => Vec::new(),
                }
            };
            if send_cb(node_id, object_id, *chunk_index, &chunk_data) {
                let mut om = self.object_manager.lock();
                om.push_manager_mut().chunk_sent(*node_id, *object_id);
                sent += 1;
            }
        }

        if sent > 0 {
            let mut stats = self.stats.lock();
            stats.total_chunks_sent += sent;
        }
    }

    /// Detect and cancel timed-out push operations.
    fn process_timeouts(&self, now_ms: f64) {
        let timed_out = {
            let om = self.object_manager.lock();
            om.push_manager()
                .get_timed_out_pushes(now_ms, self.config.push_timeout_ms)
        };

        if !timed_out.is_empty() {
            let mut om = self.object_manager.lock();
            let mut cancelled = 0u64;
            for (node_id, object_id) in &timed_out {
                om.push_manager_mut().cancel_push(node_id, object_id);
                cancelled += 1;
            }
            drop(om);

            let mut stats = self.stats.lock();
            stats.total_pushes_timed_out += cancelled;
        }
    }

    /// Start the transport loop as a tokio task.
    ///
    /// Returns a handle that stops the loop when dropped.
    pub fn start(self: Arc<Self>) -> TransportLoopHandle {
        self.running.store(true, Ordering::Relaxed);
        let loop_ref = Arc::clone(&self);

        let handle = tokio::spawn(async move {
            let interval = std::time::Duration::from_millis(loop_ref.config.tick_interval_ms);
            loop {
                if !loop_ref.running.load(Ordering::Relaxed) {
                    break;
                }
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64;
                loop_ref.tick(now_ms);
                tokio::time::sleep(interval).await;
            }
        });

        TransportLoopHandle {
            running: Arc::clone(&self),
            _handle: handle,
        }
    }

    /// Get transport statistics.
    pub fn stats(&self) -> TransportStats {
        self.stats.lock().clone()
    }

    /// Check if the loop is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

/// Handle for a running transport loop. Stops the loop when dropped.
pub struct TransportLoopHandle {
    running: Arc<TransportLoop>,
    _handle: tokio::task::JoinHandle<()>,
}

impl TransportLoopHandle {
    pub fn stop(&self) {
        self.running.running.store(false, Ordering::Relaxed);
    }
}

impl Drop for TransportLoopHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ObjectManagerConfig;
    use crate::object_manager::ObjectManager;
    use crate::plasma::allocator::IAllocator;
    use crate::plasma::store::{PlasmaStore, PlasmaStoreConfig};
    use crate::pull_manager::BundlePriority;
    use ray_common::id::NodeID;
    use std::sync::atomic::AtomicU64;

    struct DummyAllocator;
    impl IAllocator for DummyAllocator {
        fn allocate(&self, bytes: usize) -> Option<crate::plasma::allocator::Allocation> {
            Some(crate::plasma::allocator::Allocation {
                address: std::ptr::null_mut(),
                size: bytes as i64,
                fd: -1,
                offset: 0,
                device_num: 0,
                mmap_size: bytes as i64,
                fallback_allocated: false,
            })
        }
        fn fallback_allocate(&self, _: usize) -> Option<crate::plasma::allocator::Allocation> {
            None
        }
        fn free(&self, _: crate::plasma::allocator::Allocation) {}
        fn footprint_limit(&self) -> i64 {
            i64::MAX
        }
        fn allocated(&self) -> i64 {
            0
        }
        fn fallback_allocated(&self) -> i64 {
            0
        }
    }

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    fn make_om() -> ObjectManager {
        let store_config = PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let allocator = Arc::new(DummyAllocator);
        let store = Arc::new(PlasmaStore::new(allocator, &store_config));
        ObjectManager::new(ObjectManagerConfig::default(), make_nid(1), store)
    }

    #[test]
    fn test_transport_config_default() {
        let config = TransportConfig::default();
        assert_eq!(config.tick_interval_ms, 100);
        assert_eq!(config.push_timeout_ms, 10_000.0);
    }

    #[test]
    fn test_tick_with_no_callbacks() {
        let om = Arc::new(Mutex::new(make_om()));
        let tl = TransportLoop::new(TransportConfig::default(), om);
        // Should not panic even with no callbacks.
        tl.tick(0.0);
        assert_eq!(tl.stats().total_ticks, 1);
    }

    #[test]
    fn test_tick_counts() {
        let om = Arc::new(Mutex::new(make_om()));
        let tl = TransportLoop::new(TransportConfig::default(), om);

        for _ in 0..10 {
            tl.tick(0.0);
        }
        assert_eq!(tl.stats().total_ticks, 10);
    }

    #[test]
    fn test_process_pulls_with_callback() {
        let om = Arc::new(Mutex::new(make_om()));
        let pull_count = Arc::new(AtomicU64::new(0));
        let pc = pull_count.clone();

        // Register a pull target — pull first to create the entry, then set location.
        {
            let mut om_lock = om.lock();
            om_lock.pull(vec![make_oid(1)], BundlePriority::GetRequest);
            om_lock
                .pull_manager_mut()
                .update_object_location(&make_oid(1), make_nid(2));
        }

        let mut tl = TransportLoop::new(TransportConfig::default(), om);
        tl.set_pull_object_callback(Arc::new(move |_oid, _nid| {
            pc.fetch_add(1, Ordering::Relaxed);
            true
        }));

        tl.tick(0.0);
        assert!(pull_count.load(Ordering::Relaxed) >= 1);
        assert!(tl.stats().total_pulls_initiated >= 1);
    }

    #[test]
    fn test_process_pushes_with_callback() {
        let om = Arc::new(Mutex::new(make_om()));
        let send_count = Arc::new(AtomicU64::new(0));
        let sc = send_count.clone();

        // Start a push.
        {
            let mut om_lock = om.lock();
            om_lock
                .push_manager_mut()
                .start_push(make_nid(2), make_oid(1), 1024);
        }

        let mut tl = TransportLoop::new(TransportConfig::default(), om);
        tl.set_send_chunk_callback(Arc::new(move |_nid, _oid, _chunk, _data| {
            sc.fetch_add(1, Ordering::Relaxed);
            true
        }));

        tl.tick(0.0);
        assert!(send_count.load(Ordering::Relaxed) >= 1);
        assert!(tl.stats().total_chunks_sent >= 1);
    }

    #[test]
    fn test_timeout_detection() {
        let om = Arc::new(Mutex::new(make_om()));

        // Start a push at time 1.0 (must be > 0.0 for timeout detection).
        {
            let mut om_lock = om.lock();
            om_lock
                .push_manager_mut()
                .start_push_with_time(make_nid(2), make_oid(1), 1024, 1.0);
        }

        let tl = TransportLoop::new(
            TransportConfig {
                push_timeout_ms: 100.0,
                ..Default::default()
            },
            om.clone(),
        );

        // Tick at time 200ms — should detect timeout (200 - 1 = 199 > 100).
        tl.tick(200.0);
        assert_eq!(tl.stats().total_pushes_timed_out, 1);

        // Push should be cancelled.
        let om_lock = om.lock();
        assert!(!om_lock
            .push_manager()
            .is_pushing(&make_nid(2), &make_oid(1)));
    }

    #[test]
    fn test_push_sends_real_object_data() {
        use crate::common::ObjectInfo;

        // Create a PlasmaStore with a real allocator so we get valid memory.
        let dir = tempfile::tempdir().unwrap();
        let allocator = Arc::new(crate::plasma::allocator::PlasmaAllocator::new(
            1024 * 1024,
            dir.path().to_str().unwrap(),
            "",
            false,
        ));
        let store_config = crate::plasma::store::PlasmaStoreConfig {
            object_store_memory: 1024 * 1024,
            plasma_directory: dir.path().to_str().unwrap().to_string(),
            fallback_directory: String::new(),
            huge_pages: false,
        };
        let store = Arc::new(PlasmaStore::new(allocator.clone(), &store_config));

        // Create and seal an object. With a real allocator, mmap'd memory
        // is zero-initialized, so we can verify the callback receives
        // the correct number of zero bytes.
        let oid = make_oid(42);
        let data_size: i64 = 128;
        let info = ObjectInfo {
            object_id: oid,
            data_size,
            metadata_size: 0,
            ..Default::default()
        };
        store
            .create_object(
                info.clone(),
                crate::common::ObjectSource::CreatedByWorker,
                allocator.as_ref(),
            )
            .unwrap();
        store.seal_object(&oid).unwrap();

        // Verify PlasmaStore can read the data.
        let (obj_data, _meta) = store.get_object_data(&oid).unwrap();
        assert_eq!(obj_data.len(), data_size as usize);

        // Create ObjectManager with this store.
        let mut om = ObjectManager::new(
            crate::common::ObjectManagerConfig::default(),
            make_nid(1),
            store,
        );
        om.object_added(info);

        let om = Arc::new(Mutex::new(om));

        // Start a push for this object.
        {
            let mut om_lock = om.lock();
            om_lock
                .push_manager_mut()
                .start_push(make_nid(2), oid, data_size as u64);
        }

        // Track what data the send callback receives.
        let received_data = Arc::new(Mutex::new(Vec::new()));
        let rd = received_data.clone();

        let mut tl = TransportLoop::new(TransportConfig::default(), om);
        tl.set_send_chunk_callback(Arc::new(move |_nid, _oid, _chunk, data| {
            rd.lock().extend_from_slice(data);
            true
        }));

        tl.tick(0.0);

        let data = received_data.lock();
        // The callback should have received exactly data_size bytes (not empty).
        assert_eq!(
            data.len(),
            data_size as usize,
            "Push should send real object data, not empty chunks"
        );
    }

    #[tokio::test]
    async fn test_start_and_stop() {
        let om = Arc::new(Mutex::new(make_om()));
        let tl = Arc::new(TransportLoop::new(
            TransportConfig {
                tick_interval_ms: 10,
                ..Default::default()
            },
            om,
        ));

        let handle = tl.clone().start();
        assert!(tl.is_running());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        handle.stop();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert!(tl.stats().total_ticks >= 1);
    }
}
