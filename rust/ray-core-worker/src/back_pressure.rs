// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Back-pressure protocol for the core worker.
//!
//! When the object store approaches capacity, the back-pressure controller
//! blocks `ray.put()` calls until space is freed (via eviction or spilling).
//! This prevents unbounded memory growth and OOM crashes.
//!
//! Replaces the back-pressure logic from `src/ray/core_worker/core_worker.cc`.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

/// Configuration for back-pressure behavior.
#[derive(Debug, Clone)]
pub struct BackPressureConfig {
    /// Maximum time to wait for space before failing (default: 30s).
    pub max_wait_timeout: Duration,
    /// Polling interval when waiting for space (default: 100ms).
    pub poll_interval: Duration,
    /// Whether to block on memory pressure (default: true).
    /// If false, put operations fail immediately when memory is full.
    pub block_on_pressure: bool,
}

impl Default for BackPressureConfig {
    fn default() -> Self {
        Self {
            max_wait_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(100),
            block_on_pressure: true,
        }
    }
}

/// Error returned when back-pressure blocks an operation.
#[derive(Debug, thiserror::Error)]
pub enum BackPressureError {
    #[error("object store full: timed out waiting for {needed_bytes} bytes after {elapsed:?}")]
    TimedOut {
        needed_bytes: i64,
        elapsed: Duration,
    },
    #[error("object store critically full, rejecting new objects")]
    Rejected,
    #[error("back-pressure controller is shutting down")]
    Shutdown,
}

/// Statistics about back-pressure events.
#[derive(Debug, Clone, Copy)]
pub struct BackPressureStats {
    /// Number of times a put was blocked.
    pub num_blocked: u64,
    /// Number of times a put timed out waiting.
    pub num_timed_out: u64,
    /// Number of times a put was rejected immediately.
    pub num_rejected: u64,
    /// Total bytes that were blocked.
    pub total_bytes_blocked: i64,
    /// Current number of waiters.
    pub num_waiters: u64,
}

/// Controls back-pressure on `ray.put()` calls when the object store is full.
pub struct BackPressureController {
    config: BackPressureConfig,
    /// Whether the object store is in a "full" state.
    is_full: AtomicBool,
    /// Whether the controller is shutting down.
    is_shutdown: AtomicBool,
    /// Notify channel to wake up waiters when space is freed.
    space_freed: Arc<Notify>,
    // Stats
    num_blocked: AtomicU64,
    num_timed_out: AtomicU64,
    num_rejected: AtomicU64,
    total_bytes_blocked: AtomicI64,
    num_waiters: AtomicU64,
}

impl BackPressureController {
    pub fn new(config: BackPressureConfig) -> Self {
        Self {
            config,
            is_full: AtomicBool::new(false),
            is_shutdown: AtomicBool::new(false),
            space_freed: Arc::new(Notify::new()),
            num_blocked: AtomicU64::new(0),
            num_timed_out: AtomicU64::new(0),
            num_rejected: AtomicU64::new(0),
            total_bytes_blocked: AtomicI64::new(0),
            num_waiters: AtomicU64::new(0),
        }
    }

    /// Wait until the object store has space for `needed_bytes`.
    ///
    /// If `block_on_pressure` is true, blocks up to `max_wait_timeout`.
    /// If false, returns `Err(Rejected)` immediately when full.
    pub async fn wait_for_space(&self, needed_bytes: i64) -> Result<(), BackPressureError> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(BackPressureError::Shutdown);
        }

        // Fast path: not full.
        if !self.is_full.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Non-blocking mode: reject immediately.
        if !self.config.block_on_pressure {
            self.num_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(BackPressureError::Rejected);
        }

        // Blocking mode: wait for space.
        self.num_blocked.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_blocked
            .fetch_add(needed_bytes, Ordering::Relaxed);
        self.num_waiters.fetch_add(1, Ordering::Relaxed);

        let start = tokio::time::Instant::now();
        let deadline = start + self.config.max_wait_timeout;

        let result = loop {
            if self.is_shutdown.load(Ordering::Relaxed) {
                break Err(BackPressureError::Shutdown);
            }

            if !self.is_full.load(Ordering::Relaxed) {
                break Ok(());
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.num_timed_out.fetch_add(1, Ordering::Relaxed);
                break Err(BackPressureError::TimedOut {
                    needed_bytes,
                    elapsed: start.elapsed(),
                });
            }

            // Wait for notification or poll interval, whichever is shorter.
            let wait_time = remaining.min(self.config.poll_interval);
            let _ = tokio::time::timeout(wait_time, self.space_freed.notified()).await;
        };

        self.num_waiters.fetch_sub(1, Ordering::Relaxed);
        result
    }

    /// Mark the object store as full (above hard limit).
    pub fn set_full(&self) {
        self.is_full.store(true, Ordering::Relaxed);
    }

    /// Mark the object store as having space (below hard limit).
    pub fn set_not_full(&self) {
        let was_full = self.is_full.swap(false, Ordering::Relaxed);
        if was_full {
            self.space_freed.notify_waiters();
        }
    }

    /// Whether the object store is currently in a "full" state.
    pub fn is_full(&self) -> bool {
        self.is_full.load(Ordering::Relaxed)
    }

    /// Shut down the controller, waking all waiters with an error.
    pub fn shutdown(&self) {
        self.is_shutdown.store(true, Ordering::Relaxed);
        self.space_freed.notify_waiters();
    }

    /// Get current statistics.
    pub fn stats(&self) -> BackPressureStats {
        BackPressureStats {
            num_blocked: self.num_blocked.load(Ordering::Relaxed),
            num_timed_out: self.num_timed_out.load(Ordering::Relaxed),
            num_rejected: self.num_rejected.load(Ordering::Relaxed),
            total_bytes_blocked: self.total_bytes_blocked.load(Ordering::Relaxed),
            num_waiters: self.num_waiters.load(Ordering::Relaxed),
        }
    }

    pub fn config(&self) -> &BackPressureConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_not_full_passes_immediately() {
        let ctrl = BackPressureController::new(BackPressureConfig::default());
        // Not full — should pass immediately.
        ctrl.wait_for_space(1024).await.unwrap();
        assert_eq!(ctrl.stats().num_blocked, 0);
    }

    #[tokio::test]
    async fn test_full_blocks_then_unblocks() {
        let ctrl = Arc::new(BackPressureController::new(BackPressureConfig {
            max_wait_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
            block_on_pressure: true,
        }));

        ctrl.set_full();

        let ctrl2 = ctrl.clone();
        let handle = tokio::spawn(async move { ctrl2.wait_for_space(1024).await });

        // Give the waiter time to block.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(ctrl.stats().num_waiters, 1);

        // Unblock.
        ctrl.set_not_full();
        handle.await.unwrap().unwrap();
        assert_eq!(ctrl.stats().num_blocked, 1);
        assert_eq!(ctrl.stats().num_waiters, 0);
    }

    #[tokio::test]
    async fn test_full_times_out() {
        let ctrl = BackPressureController::new(BackPressureConfig {
            max_wait_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(10),
            block_on_pressure: true,
        });

        ctrl.set_full();
        let result = ctrl.wait_for_space(1024).await;
        assert!(matches!(result, Err(BackPressureError::TimedOut { .. })));
        assert_eq!(ctrl.stats().num_timed_out, 1);
    }

    #[tokio::test]
    async fn test_non_blocking_rejects() {
        let ctrl = BackPressureController::new(BackPressureConfig {
            block_on_pressure: false,
            ..Default::default()
        });

        ctrl.set_full();
        let result = ctrl.wait_for_space(1024).await;
        assert!(matches!(result, Err(BackPressureError::Rejected)));
        assert_eq!(ctrl.stats().num_rejected, 1);
    }

    #[tokio::test]
    async fn test_shutdown_wakes_waiters() {
        let ctrl = Arc::new(BackPressureController::new(BackPressureConfig {
            max_wait_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(10),
            block_on_pressure: true,
        }));

        ctrl.set_full();

        let ctrl2 = ctrl.clone();
        let handle = tokio::spawn(async move { ctrl2.wait_for_space(1024).await });

        tokio::time::sleep(Duration::from_millis(30)).await;
        ctrl.shutdown();

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(BackPressureError::Shutdown)));
    }

    #[tokio::test]
    async fn test_set_not_full_when_already_not_full() {
        let ctrl = BackPressureController::new(BackPressureConfig::default());
        // Should be a no-op.
        ctrl.set_not_full();
        assert!(!ctrl.is_full());
    }

    #[tokio::test]
    async fn test_stats_accumulate() {
        let ctrl = BackPressureController::new(BackPressureConfig {
            max_wait_timeout: Duration::from_millis(10),
            poll_interval: Duration::from_millis(5),
            block_on_pressure: true,
        });

        ctrl.set_full();
        let _ = ctrl.wait_for_space(100).await;
        let _ = ctrl.wait_for_space(200).await;

        let stats = ctrl.stats();
        assert_eq!(stats.num_blocked, 2);
        assert_eq!(stats.num_timed_out, 2);
        assert_eq!(stats.total_bytes_blocked, 300);
    }

    #[tokio::test]
    async fn test_multiple_waiters() {
        let ctrl = Arc::new(BackPressureController::new(BackPressureConfig {
            max_wait_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
            block_on_pressure: true,
        }));

        ctrl.set_full();

        let mut handles = Vec::new();
        for _ in 0..3 {
            let ctrl2 = ctrl.clone();
            handles.push(tokio::spawn(async move {
                ctrl2.wait_for_space(100).await
            }));
        }

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(ctrl.stats().num_waiters, 3);

        ctrl.set_not_full();
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(ctrl.stats().num_waiters, 0);
        assert_eq!(ctrl.stats().num_blocked, 3);
    }

    #[test]
    fn test_default_config() {
        let config = BackPressureConfig::default();
        assert_eq!(config.max_wait_timeout, Duration::from_secs(30));
        assert!(config.block_on_pressure);
    }
}
