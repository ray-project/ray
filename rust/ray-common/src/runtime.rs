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
