// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Unified event loop for periodic tasks.
//!
//! Provides a configurable event loop that drives periodic tasks such as
//! heartbeats, resource reporting, metrics export, and health checks.
//!
//! Uses tokio intervals for timer management.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A periodic task that runs at a fixed interval.
pub struct PeriodicTask {
    /// Human-readable name for logging.
    pub name: String,
    /// How often to run (e.g., every 1s for heartbeat).
    pub interval: Duration,
    /// The callback to execute each tick.
    callback: Box<dyn Fn() + Send + Sync>,
}

impl PeriodicTask {
    pub fn new<F>(name: impl Into<String>, interval: Duration, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            interval,
            callback: Box::new(callback),
        }
    }

    /// Execute the task's callback.
    pub fn tick(&self) {
        (self.callback)();
    }
}

/// Configuration for the event loop.
#[derive(Debug, Clone)]
pub struct EventLoopConfig {
    /// Whether to log each tick for debugging.
    pub debug_ticks: bool,
}

impl Default for EventLoopConfig {
    fn default() -> Self {
        Self {
            debug_ticks: false,
        }
    }
}

/// An event loop manager that schedules and runs periodic tasks.
pub struct EventLoopManager {
    config: EventLoopConfig,
    running: Arc<AtomicBool>,
}

impl EventLoopManager {
    pub fn new(config: EventLoopConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if the event loop is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Stop the event loop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Spawn all periodic tasks as tokio tasks.
    ///
    /// Each task runs in its own tokio task with its own interval timer.
    /// All tasks stop when `stop()` is called or the returned handles are dropped.
    pub fn spawn_tasks(&self, tasks: Vec<Arc<PeriodicTask>>) -> Vec<tokio::task::JoinHandle<()>> {
        self.running.store(true, Ordering::SeqCst);
        let mut handles = Vec::with_capacity(tasks.len());

        for task in tasks {
            let running = self.running.clone();
            let debug = self.config.debug_ticks;

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(task.interval);
                // Skip the first immediate tick.
                interval.tick().await;

                while running.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    if debug {
                        tracing::trace!(task = %task.name, "periodic tick");
                    }
                    task.tick();
                }
            });
            handles.push(handle);
        }

        handles
    }
}

impl Default for EventLoopManager {
    fn default() -> Self {
        Self::new(EventLoopConfig::default())
    }
}

/// Commonly used periodic task intervals.
pub mod intervals {
    use std::time::Duration;

    /// Heartbeat interval (default: 1 second).
    pub const HEARTBEAT: Duration = Duration::from_secs(1);

    /// Resource reporting interval (default: 100ms).
    pub const RESOURCE_REPORT: Duration = Duration::from_millis(100);

    /// Metrics export interval (default: 10 seconds).
    pub const METRICS_EXPORT: Duration = Duration::from_secs(10);

    /// Health check interval (default: 5 seconds).
    pub const HEALTH_CHECK: Duration = Duration::from_secs(5);

    /// Object store memory check interval (default: 100ms).
    pub const MEMORY_CHECK: Duration = Duration::from_millis(100);

    /// Dead worker cleanup interval (default: 1 second).
    pub const WORKER_CLEANUP: Duration = Duration::from_secs(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn test_periodic_task_tick() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        let task = PeriodicTask::new("test", Duration::from_millis(10), move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        task.tick();
        task.tick();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_event_loop_spawn_and_stop() {
        let manager = EventLoopManager::default();
        let counter = Arc::new(AtomicU32::new(0));

        let c = counter.clone();
        let task = Arc::new(PeriodicTask::new(
            "counter",
            Duration::from_millis(10),
            move || {
                c.fetch_add(1, Ordering::SeqCst);
            },
        ));

        let handles = manager.spawn_tasks(vec![task]);
        assert!(manager.is_running());

        // Let it tick a few times.
        tokio::time::sleep(Duration::from_millis(50)).await;

        manager.stop();

        // Give tasks time to notice the stop.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let final_count = counter.load(Ordering::SeqCst);
        assert!(final_count > 0, "task should have ticked at least once");

        // Wait for handles to complete.
        for h in handles {
            let _ = tokio::time::timeout(Duration::from_millis(100), h).await;
        }
    }

    #[tokio::test]
    async fn test_event_loop_multiple_tasks() {
        let manager = EventLoopManager::default();
        let counter_a = Arc::new(AtomicU32::new(0));
        let counter_b = Arc::new(AtomicU32::new(0));

        let ca = counter_a.clone();
        let task_a = Arc::new(PeriodicTask::new(
            "fast",
            Duration::from_millis(10),
            move || {
                ca.fetch_add(1, Ordering::SeqCst);
            },
        ));

        let cb = counter_b.clone();
        let task_b = Arc::new(PeriodicTask::new(
            "slow",
            Duration::from_millis(30),
            move || {
                cb.fetch_add(1, Ordering::SeqCst);
            },
        ));

        let _handles = manager.spawn_tasks(vec![task_a, task_b]);

        tokio::time::sleep(Duration::from_millis(80)).await;
        manager.stop();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let a = counter_a.load(Ordering::SeqCst);
        let b = counter_b.load(Ordering::SeqCst);
        // Fast task should tick more often than slow task.
        assert!(a > b, "fast task ({a}) should tick more than slow ({b})");
    }

    #[test]
    fn test_intervals() {
        assert_eq!(intervals::HEARTBEAT, Duration::from_secs(1));
        assert_eq!(intervals::RESOURCE_REPORT, Duration::from_millis(100));
        assert_eq!(intervals::METRICS_EXPORT, Duration::from_secs(10));
        assert_eq!(intervals::HEALTH_CHECK, Duration::from_secs(5));
    }
}
