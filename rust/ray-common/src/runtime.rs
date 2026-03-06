// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Async runtime wrappers.
//!
//! Replaces `src/ray/common/asio/` — wraps tokio runtime with Ray-specific
//! functionality like `PeriodicalRunner`.

use std::future::Future;
use std::time::Duration;
use tokio::task::JoinHandle;

/// A wrapper around `tokio::runtime::Handle` providing Ray-specific utilities.
#[derive(Clone)]
pub struct RayRuntime {
    handle: tokio::runtime::Handle,
}

impl RayRuntime {
    /// Create from the current tokio runtime handle.
    pub fn current() -> Self {
        Self {
            handle: tokio::runtime::Handle::current(),
        }
    }

    /// Create from an explicit handle.
    pub fn from_handle(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }

    /// Spawn a future on the runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Get the underlying tokio handle.
    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
}

/// A periodical runner that executes a callback at fixed intervals.
///
/// Replaces C++ `PeriodicalRunner` (which uses boost::asio timers).
pub struct PeriodicalRunner {
    handle: Option<JoinHandle<()>>,
}

impl PeriodicalRunner {
    /// Start running `callback` every `interval`.
    pub fn start<F>(interval: Duration, callback: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                callback();
            }
        });
        Self {
            handle: Some(handle),
        }
    }

    /// Stop the periodical runner.
    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for PeriodicalRunner {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_ray_runtime_current() {
        let rt = RayRuntime::current();
        let handle = rt.spawn(async { 42 });
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_ray_runtime_from_handle() {
        let rt = RayRuntime::from_handle(tokio::runtime::Handle::current());
        let handle = rt.spawn(async { "hello" });
        assert_eq!(handle.await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_ray_runtime_clone() {
        let rt1 = RayRuntime::current();
        let rt2 = rt1.clone();
        let h1 = rt1.spawn(async { 1 });
        let h2 = rt2.spawn(async { 2 });
        assert_eq!(h1.await.unwrap() + h2.await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_periodical_runner_ticks() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let _runner = PeriodicalRunner::start(Duration::from_millis(10), move || {
            c.fetch_add(1, Ordering::Relaxed);
        });
        tokio::time::sleep(Duration::from_millis(55)).await;
        let ticks = count.load(Ordering::Relaxed);
        // Should have ticked at least 3 times in 55ms with 10ms interval.
        assert!(ticks >= 3, "expected >= 3 ticks, got {}", ticks);
    }

    #[tokio::test]
    async fn test_periodical_runner_stop() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let mut runner = PeriodicalRunner::start(Duration::from_millis(10), move || {
            c.fetch_add(1, Ordering::Relaxed);
        });
        tokio::time::sleep(Duration::from_millis(35)).await;
        runner.stop();
        let after_stop = count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let after_wait = count.load(Ordering::Relaxed);
        // Count should not increase after stop.
        assert_eq!(after_stop, after_wait);
    }

    #[tokio::test]
    async fn test_periodical_runner_drop_stops() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        {
            let _runner = PeriodicalRunner::start(Duration::from_millis(10), move || {
                c.fetch_add(1, Ordering::Relaxed);
            });
            tokio::time::sleep(Duration::from_millis(35)).await;
        } // Dropped here
        let after_drop = count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(after_drop, count.load(Ordering::Relaxed));
    }
}
