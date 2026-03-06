// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Signal handling utilities for Ray processes.
//!
//! Provides a unified mechanism for handling OS signals (SIGTERM, SIGINT, SIGHUP)
//! and coordinating graceful shutdown across all components.
//!
//! Replaces parts of `src/ray/util/process.cc` related to signal handling.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Global shutdown flag — set when a shutdown signal is received.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Whether a fast (non-graceful) shutdown was requested (SIGINT).
static FAST_SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Check if a shutdown has been requested.
pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

/// Check if a fast (immediate) shutdown was requested.
pub fn is_fast_shutdown() -> bool {
    FAST_SHUTDOWN.load(Ordering::Relaxed)
}

/// Request a graceful shutdown.
pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Request a fast (immediate) shutdown.
pub fn request_fast_shutdown() {
    FAST_SHUTDOWN.store(true, Ordering::SeqCst);
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Reset shutdown flags (for testing).
pub fn reset_shutdown_flags() {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    FAST_SHUTDOWN.store(false, Ordering::SeqCst);
}

/// A shutdown coordinator that allows components to register callbacks
/// to be invoked during shutdown.
pub struct ShutdownCoordinator {
    /// Callbacks to run on graceful shutdown, in order.
    callbacks: parking_lot::Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    /// Whether shutdown has been executed.
    executed: AtomicBool,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            callbacks: parking_lot::Mutex::new(Vec::new()),
            executed: AtomicBool::new(false),
        }
    }

    /// Register a callback to run during shutdown.
    pub fn on_shutdown<F: FnOnce() + Send + 'static>(&self, callback: F) {
        self.callbacks.lock().push(Box::new(callback));
    }

    /// Execute all registered shutdown callbacks.
    /// Returns `true` if this was the first call (callbacks were executed).
    pub fn execute(&self) -> bool {
        if self
            .executed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }

        let callbacks: Vec<_> = self.callbacks.lock().drain(..).collect();
        for callback in callbacks {
            callback();
        }
        true
    }

    /// Check if shutdown has been executed.
    pub fn is_executed(&self) -> bool {
        self.executed.load(Ordering::Relaxed)
    }

    /// Number of registered callbacks.
    pub fn num_callbacks(&self) -> usize {
        self.callbacks.lock().len()
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT) asynchronously.
///
/// Returns `true` for graceful shutdown (SIGTERM), `false` for fast shutdown (SIGINT).
///
/// This is intended to be used with `tokio::select!` in the main event loop:
/// ```ignore
/// tokio::select! {
///     graceful = wait_for_shutdown_signal() => {
///         if graceful {
///             coordinator.execute();
///         }
///     }
///     _ = main_loop() => {}
/// }
/// ```
pub async fn wait_for_shutdown_signal() -> bool {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt())
            .expect("failed to install SIGINT handler");
        let mut sighup = signal(SignalKind::hangup())
            .expect("failed to install SIGHUP handler");

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM — initiating graceful shutdown");
                request_shutdown();
                true
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT — initiating fast shutdown");
                request_fast_shutdown();
                false
            }
            _ = sighup.recv() => {
                tracing::info!("Received SIGHUP — initiating graceful shutdown");
                request_shutdown();
                true
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("Received Ctrl+C — initiating fast shutdown");
        request_fast_shutdown();
        false
    }
}

/// Install a shutdown handler that runs the coordinator on signal.
///
/// Spawns a background tokio task that waits for signals and runs
/// the shutdown coordinator when a signal is received.
pub fn install_shutdown_handler(coordinator: Arc<ShutdownCoordinator>) {
    tokio::spawn(async move {
        let graceful = wait_for_shutdown_signal().await;
        if graceful {
            tracing::info!("Running graceful shutdown callbacks...");
        } else {
            tracing::info!("Running fast shutdown callbacks...");
        }
        coordinator.execute();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn test_shutdown_flags() {
        reset_shutdown_flags();
        assert!(!is_shutdown_requested());
        assert!(!is_fast_shutdown());

        request_shutdown();
        assert!(is_shutdown_requested());
        assert!(!is_fast_shutdown());

        reset_shutdown_flags();
        assert!(!is_shutdown_requested());

        request_fast_shutdown();
        assert!(is_shutdown_requested());
        assert!(is_fast_shutdown());

        reset_shutdown_flags();
    }

    #[test]
    fn test_shutdown_coordinator_basic() {
        let coord = ShutdownCoordinator::new();
        let counter = Arc::new(AtomicU32::new(0));

        let c = counter.clone();
        coord.on_shutdown(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        let c = counter.clone();
        coord.on_shutdown(move || {
            c.fetch_add(10, Ordering::SeqCst);
        });

        assert_eq!(coord.num_callbacks(), 2);
        assert!(!coord.is_executed());

        assert!(coord.execute()); // First call returns true.
        assert_eq!(counter.load(Ordering::SeqCst), 11);
        assert!(coord.is_executed());
    }

    #[test]
    fn test_shutdown_coordinator_executes_once() {
        let coord = ShutdownCoordinator::new();
        let counter = Arc::new(AtomicU32::new(0));

        let c = counter.clone();
        coord.on_shutdown(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        assert!(coord.execute());
        assert!(!coord.execute()); // Second call returns false.
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Callbacks run only once.
    }

    #[test]
    fn test_shutdown_coordinator_empty() {
        let coord = ShutdownCoordinator::new();
        assert_eq!(coord.num_callbacks(), 0);
        assert!(coord.execute()); // Succeeds even with no callbacks.
        assert!(coord.is_executed());
    }

    #[test]
    fn test_fast_shutdown_also_sets_shutdown() {
        reset_shutdown_flags();
        request_fast_shutdown();
        assert!(is_shutdown_requested());
        assert!(is_fast_shutdown());
        reset_shutdown_flags();
    }

    #[test]
    fn test_shutdown_coordinator_default() {
        let coord = ShutdownCoordinator::default();
        assert_eq!(coord.num_callbacks(), 0);
        assert!(!coord.is_executed());
    }

    #[test]
    fn test_shutdown_coordinator_callback_order() {
        let coord = ShutdownCoordinator::new();
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let o = order.clone();
        coord.on_shutdown(move || o.lock().push(1));
        let o = order.clone();
        coord.on_shutdown(move || o.lock().push(2));
        let o = order.clone();
        coord.on_shutdown(move || o.lock().push(3));

        coord.execute();
        assert_eq!(*order.lock(), vec![1, 2, 3]);
    }
}
