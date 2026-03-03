// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object store memory monitor.
//!
//! Tracks memory usage in the object store and fires callbacks when
//! usage crosses configurable thresholds. Replaces the memory monitoring
//! logic from `src/ray/object_manager/object_manager.cc`.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// Memory pressure level, ordered by severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemoryPressureLevel {
    /// Below soft limit — no action needed.
    Normal,
    /// Above soft limit — should start spilling.
    SoftLimit,
    /// Above hard limit — block new creates.
    HardLimit,
    /// Critically full — reject all new objects.
    Critical,
}

/// Configuration for memory monitoring thresholds.
#[derive(Debug, Clone)]
pub struct MemoryMonitorConfig {
    /// Total object store capacity in bytes.
    pub object_store_capacity: i64,
    /// Fraction of capacity at which to start spilling (default: 0.8).
    pub soft_limit_fraction: f64,
    /// Fraction of capacity at which to block new creates (default: 0.95).
    pub hard_limit_fraction: f64,
    /// Fraction of capacity considered critical (default: 0.99).
    pub critical_fraction: f64,
}

impl Default for MemoryMonitorConfig {
    fn default() -> Self {
        Self {
            object_store_capacity: 1024 * 1024 * 1024, // 1GB
            soft_limit_fraction: 0.8,
            hard_limit_fraction: 0.95,
            critical_fraction: 0.99,
        }
    }
}

impl MemoryMonitorConfig {
    /// Absolute byte threshold for the soft limit.
    pub fn soft_limit_bytes(&self) -> i64 {
        (self.object_store_capacity as f64 * self.soft_limit_fraction) as i64
    }

    /// Absolute byte threshold for the hard limit.
    pub fn hard_limit_bytes(&self) -> i64 {
        (self.object_store_capacity as f64 * self.hard_limit_fraction) as i64
    }

    /// Absolute byte threshold for the critical limit.
    pub fn critical_limit_bytes(&self) -> i64 {
        (self.object_store_capacity as f64 * self.critical_fraction) as i64
    }
}

/// Snapshot of memory usage at a point in time.
#[derive(Debug, Clone, Copy)]
pub struct MemorySnapshot {
    /// Total capacity of the object store.
    pub capacity: i64,
    /// Bytes currently used.
    pub used: i64,
    /// Bytes available (capacity - used).
    pub available: i64,
    /// Bytes allocated from fallback (disk-backed) storage.
    pub fallback_allocated: i64,
    /// Number of local objects.
    pub num_objects: i64,
    /// Current pressure level.
    pub pressure_level: MemoryPressureLevel,
    /// Usage as a fraction [0.0, 1.0].
    pub usage_fraction: f64,
}

/// Callback invoked when memory pressure level changes.
pub type PressureCallback = Box<dyn Fn(MemoryPressureLevel, &MemorySnapshot) + Send + Sync>;

/// Tracks object store memory usage and fires pressure callbacks.
pub struct MemoryMonitor {
    config: MemoryMonitorConfig,
    /// Running total of bytes used (data + metadata).
    used_bytes: AtomicI64,
    /// Running total of bytes in fallback (disk) allocations.
    fallback_bytes: AtomicI64,
    /// Number of local objects.
    num_objects: AtomicI64,
    /// Current pressure level.
    current_level: Mutex<MemoryPressureLevel>,
    /// Whether spilling has been requested but not yet completed.
    spill_requested: AtomicBool,
    /// Callback for pressure level changes.
    pressure_callback: Mutex<Option<PressureCallback>>,
}

impl MemoryMonitor {
    pub fn new(config: MemoryMonitorConfig) -> Self {
        Self {
            config,
            used_bytes: AtomicI64::new(0),
            fallback_bytes: AtomicI64::new(0),
            num_objects: AtomicI64::new(0),
            current_level: Mutex::new(MemoryPressureLevel::Normal),
            spill_requested: AtomicBool::new(false),
            pressure_callback: Mutex::new(None),
        }
    }

    /// Set the callback invoked when memory pressure level changes.
    pub fn set_pressure_callback(&self, cb: PressureCallback) {
        *self.pressure_callback.lock() = Some(cb);
    }

    /// Record that an object was added to the store.
    pub fn object_added(&self, data_size: i64, metadata_size: i64, fallback: bool) {
        let total = data_size + metadata_size;
        self.used_bytes.fetch_add(total, Ordering::Relaxed);
        self.num_objects.fetch_add(1, Ordering::Relaxed);
        if fallback {
            self.fallback_bytes.fetch_add(total, Ordering::Relaxed);
        }
        self.check_pressure();
    }

    /// Record that an object was deleted from the store.
    pub fn object_deleted(&self, data_size: i64, metadata_size: i64, fallback: bool) {
        let total = data_size + metadata_size;
        self.used_bytes.fetch_sub(total, Ordering::Relaxed);
        self.num_objects.fetch_sub(1, Ordering::Relaxed);
        if fallback {
            self.fallback_bytes.fetch_sub(total, Ordering::Relaxed);
        }
        self.check_pressure();
    }

    /// Take a snapshot of current memory state.
    pub fn snapshot(&self) -> MemorySnapshot {
        let used = self.used_bytes.load(Ordering::Relaxed);
        let capacity = self.config.object_store_capacity;
        let available = (capacity - used).max(0);
        let usage_fraction = if capacity > 0 {
            used as f64 / capacity as f64
        } else {
            0.0
        };

        MemorySnapshot {
            capacity,
            used,
            available,
            fallback_allocated: self.fallback_bytes.load(Ordering::Relaxed),
            num_objects: self.num_objects.load(Ordering::Relaxed),
            pressure_level: self.compute_pressure_level(used),
            usage_fraction,
        }
    }

    /// Current used bytes.
    pub fn used_bytes(&self) -> i64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Current available bytes.
    pub fn available_bytes(&self) -> i64 {
        (self.config.object_store_capacity - self.used_bytes.load(Ordering::Relaxed)).max(0)
    }

    /// Current usage as a fraction of capacity [0.0, 1.0].
    pub fn usage_fraction(&self) -> f64 {
        let capacity = self.config.object_store_capacity;
        if capacity <= 0 {
            return 0.0;
        }
        self.used_bytes.load(Ordering::Relaxed) as f64 / capacity as f64
    }

    /// Current pressure level.
    pub fn pressure_level(&self) -> MemoryPressureLevel {
        *self.current_level.lock()
    }

    /// Whether the store is above the soft limit (should spill).
    pub fn should_spill(&self) -> bool {
        self.used_bytes.load(Ordering::Relaxed) >= self.config.soft_limit_bytes()
    }

    /// Whether the store is above the hard limit (should block creates).
    pub fn should_block_creates(&self) -> bool {
        self.used_bytes.load(Ordering::Relaxed) >= self.config.hard_limit_bytes()
    }

    /// Whether a spill is currently requested.
    pub fn is_spill_requested(&self) -> bool {
        self.spill_requested.load(Ordering::Relaxed)
    }

    /// Mark that a spill has been requested.
    pub fn request_spill(&self) {
        self.spill_requested.store(true, Ordering::Relaxed);
    }

    /// Mark that a spill has completed.
    pub fn spill_completed(&self) {
        self.spill_requested.store(false, Ordering::Relaxed);
    }

    /// Check if there's enough space for a new object of given size.
    pub fn has_space(&self, needed_bytes: i64) -> bool {
        let used = self.used_bytes.load(Ordering::Relaxed);
        used + needed_bytes <= self.config.object_store_capacity
    }

    /// The configuration.
    pub fn config(&self) -> &MemoryMonitorConfig {
        &self.config
    }

    /// Compute pressure level from current usage.
    fn compute_pressure_level(&self, used: i64) -> MemoryPressureLevel {
        if used >= self.config.critical_limit_bytes() {
            MemoryPressureLevel::Critical
        } else if used >= self.config.hard_limit_bytes() {
            MemoryPressureLevel::HardLimit
        } else if used >= self.config.soft_limit_bytes() {
            MemoryPressureLevel::SoftLimit
        } else {
            MemoryPressureLevel::Normal
        }
    }

    /// Check the current pressure level and fire callbacks if it changed.
    fn check_pressure(&self) {
        let used = self.used_bytes.load(Ordering::Relaxed);
        let new_level = self.compute_pressure_level(used);

        let mut current = self.current_level.lock();
        if *current != new_level {
            let old_level = *current;
            *current = new_level;
            drop(current);

            // Auto-request spill when we cross the soft limit.
            if new_level >= MemoryPressureLevel::SoftLimit
                && old_level < MemoryPressureLevel::SoftLimit
            {
                self.spill_requested.store(true, Ordering::Relaxed);
            }

            let snapshot = self.snapshot();
            let cb = self.pressure_callback.lock();
            if let Some(ref callback) = *cb {
                callback(new_level, &snapshot);
            }
        }
    }
}

/// Thread-safe wrapper for use across async boundaries.
impl MemoryMonitor {
    /// Create a new Arc-wrapped monitor.
    pub fn new_shared(config: MemoryMonitorConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn make_config(capacity: i64) -> MemoryMonitorConfig {
        MemoryMonitorConfig {
            object_store_capacity: capacity,
            ..Default::default()
        }
    }

    #[test]
    fn test_initial_state() {
        let monitor = MemoryMonitor::new(make_config(1000));
        assert_eq!(monitor.used_bytes(), 0);
        assert_eq!(monitor.available_bytes(), 1000);
        assert_eq!(monitor.pressure_level(), MemoryPressureLevel::Normal);
        assert!(!monitor.should_spill());
        assert!(!monitor.should_block_creates());
    }

    #[test]
    fn test_object_added_updates_usage() {
        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.object_added(100, 10, false);
        assert_eq!(monitor.used_bytes(), 110);
        assert_eq!(monitor.available_bytes(), 890);
    }

    #[test]
    fn test_object_deleted_updates_usage() {
        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.object_added(100, 10, false);
        monitor.object_deleted(100, 10, false);
        assert_eq!(monitor.used_bytes(), 0);
        assert_eq!(monitor.available_bytes(), 1000);
    }

    #[test]
    fn test_fallback_tracking() {
        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.object_added(100, 0, true);
        let snap = monitor.snapshot();
        assert_eq!(snap.fallback_allocated, 100);
        monitor.object_deleted(100, 0, true);
        let snap = monitor.snapshot();
        assert_eq!(snap.fallback_allocated, 0);
    }

    #[test]
    fn test_soft_limit_triggers_spill() {
        let monitor = MemoryMonitor::new(make_config(1000));
        // Soft limit at 80% = 800 bytes.
        assert!(!monitor.should_spill());
        monitor.object_added(800, 0, false);
        assert!(monitor.should_spill());
        assert_eq!(monitor.pressure_level(), MemoryPressureLevel::SoftLimit);
    }

    #[test]
    fn test_hard_limit_blocks_creates() {
        let monitor = MemoryMonitor::new(make_config(1000));
        // Hard limit at 95% = 950 bytes.
        monitor.object_added(950, 0, false);
        assert!(monitor.should_block_creates());
        assert_eq!(monitor.pressure_level(), MemoryPressureLevel::HardLimit);
    }

    #[test]
    fn test_critical_level() {
        let monitor = MemoryMonitor::new(make_config(1000));
        // Critical at 99% = 990 bytes.
        monitor.object_added(990, 0, false);
        assert_eq!(monitor.pressure_level(), MemoryPressureLevel::Critical);
    }

    #[test]
    fn test_pressure_callback_fires() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let count_clone = call_count.clone();

        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.set_pressure_callback(Box::new(move |level, snapshot| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            assert_eq!(level, MemoryPressureLevel::SoftLimit);
            assert!(snapshot.usage_fraction >= 0.8);
        }));

        // Cross the soft limit.
        monitor.object_added(800, 0, false);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // Stay at same level — callback should NOT fire again.
        monitor.object_added(10, 0, false);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_pressure_level_transitions_both_ways() {
        let levels = Arc::new(Mutex::new(Vec::new()));
        let levels_clone = levels.clone();

        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.set_pressure_callback(Box::new(move |level, _| {
            levels_clone.lock().push(level);
        }));

        // Normal → SoftLimit
        monitor.object_added(800, 0, false);
        // SoftLimit → HardLimit
        monitor.object_added(150, 0, false);
        // HardLimit → SoftLimit (free some)
        monitor.object_deleted(100, 0, false);
        // SoftLimit → Normal (free more)
        monitor.object_deleted(700, 0, false);

        let recorded = levels.lock().clone();
        assert_eq!(
            recorded,
            vec![
                MemoryPressureLevel::SoftLimit,
                MemoryPressureLevel::HardLimit,
                MemoryPressureLevel::SoftLimit,
                MemoryPressureLevel::Normal,
            ]
        );
    }

    #[test]
    fn test_snapshot() {
        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.object_added(500, 0, false);
        monitor.object_added(100, 0, true);

        let snap = monitor.snapshot();
        assert_eq!(snap.capacity, 1000);
        assert_eq!(snap.used, 600);
        assert_eq!(snap.available, 400);
        assert_eq!(snap.fallback_allocated, 100);
        assert_eq!(snap.num_objects, 2);
        assert!((snap.usage_fraction - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_has_space() {
        let monitor = MemoryMonitor::new(make_config(1000));
        assert!(monitor.has_space(999));
        assert!(monitor.has_space(1000));
        assert!(!monitor.has_space(1001));

        monitor.object_added(500, 0, false);
        assert!(monitor.has_space(500));
        assert!(!monitor.has_space(501));
    }

    #[test]
    fn test_spill_request_lifecycle() {
        let monitor = MemoryMonitor::new(make_config(1000));
        assert!(!monitor.is_spill_requested());

        // Crossing soft limit auto-requests spill.
        monitor.object_added(800, 0, false);
        assert!(monitor.is_spill_requested());

        // Explicit completion.
        monitor.spill_completed();
        assert!(!monitor.is_spill_requested());

        // Manual request.
        monitor.request_spill();
        assert!(monitor.is_spill_requested());
    }

    #[test]
    fn test_zero_capacity() {
        let monitor = MemoryMonitor::new(make_config(0));
        assert_eq!(monitor.usage_fraction(), 0.0);
        let snap = monitor.snapshot();
        assert_eq!(snap.usage_fraction, 0.0);
    }

    #[test]
    fn test_num_objects_tracking() {
        let monitor = MemoryMonitor::new(make_config(1000));
        monitor.object_added(10, 0, false);
        monitor.object_added(20, 0, false);
        monitor.object_added(30, 0, false);
        assert_eq!(monitor.snapshot().num_objects, 3);

        monitor.object_deleted(10, 0, false);
        assert_eq!(monitor.snapshot().num_objects, 2);
    }

    #[test]
    fn test_config_thresholds() {
        let config = MemoryMonitorConfig {
            object_store_capacity: 10000,
            soft_limit_fraction: 0.5,
            hard_limit_fraction: 0.75,
            critical_fraction: 0.9,
        };
        assert_eq!(config.soft_limit_bytes(), 5000);
        assert_eq!(config.hard_limit_bytes(), 7500);
        assert_eq!(config.critical_limit_bytes(), 9000);
    }
}
