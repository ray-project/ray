// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Process management utilities.
//!
//! Replaces C++ `process.cc/h`, `process_utils.cc/h`, `subreaper.cc/h`.

use std::process::Command;

/// Check if a process with the given PID is alive.
pub fn is_process_alive(pid: u32) -> bool {
    // On Unix, sending signal 0 checks process existence without killing it.
    #[cfg(unix)]
    {
        use nix::sys::signal;
        use nix::unistd::Pid;
        signal::kill(Pid::from_raw(pid as i32), None).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Kill a process by PID.
pub fn kill_process(pid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;
        signal::kill(Pid::from_raw(pid as i32), Signal::SIGKILL)
            .map_err(std::io::Error::other)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "kill not supported on this platform",
        ))
    }
}

/// Get the current process ID.
pub fn get_pid() -> u32 {
    std::process::id()
}

/// Spawn a subprocess and return its PID.
pub fn spawn_process(
    executable: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> std::io::Result<u32> {
    let mut cmd = Command::new(executable);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.spawn()?;
    Ok(child.id())
}

/// Set the current process as a subreaper (Linux only).
/// This ensures orphaned child processes are re-parented to this process.
#[cfg(target_os = "linux")]
pub fn set_subreaper() -> std::io::Result<()> {
    use libc::{prctl, PR_SET_CHILD_SUBREAPER};
    let ret = unsafe { prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_subreaper() -> std::io::Result<()> {
    // Subreaper is Linux-specific; no-op on other platforms.
    Ok(())
}

/// Graceful shutdown coordinator.
///
/// Manages orderly shutdown of multiple components. Each component registers
/// a shutdown callback; when `shutdown()` is called, all callbacks run in
/// reverse registration order (LIFO). This matches the C++ pattern where
/// components shut down in reverse startup order.
pub struct ShutdownCoordinator {
    #[allow(clippy::type_complexity)]
    callbacks: parking_lot::Mutex<Vec<(String, Box<dyn FnOnce() + Send>)>>,
    shutdown_initiated: std::sync::atomic::AtomicBool,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            callbacks: parking_lot::Mutex::new(Vec::new()),
            shutdown_initiated: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Register a component's shutdown callback.
    pub fn register(&self, name: impl Into<String>, callback: impl FnOnce() + Send + 'static) {
        let mut cbs = self.callbacks.lock();
        cbs.push((name.into(), Box::new(callback)));
    }

    /// Run all shutdown callbacks in reverse order.
    ///
    /// Returns the number of components shut down. Calling this more than
    /// once is a no-op (returns 0 on subsequent calls).
    pub fn shutdown(&self) -> usize {
        if self
            .shutdown_initiated
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return 0;
        }

        let mut cbs = self.callbacks.lock();
        let callbacks: Vec<_> = std::mem::take(&mut *cbs);
        drop(cbs);

        let count = callbacks.len();
        for (name, cb) in callbacks.into_iter().rev() {
            tracing::info!(component = %name, "shutting down");
            cb();
        }
        count
    }

    /// Check if shutdown has been initiated.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_initiated
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Return the number of registered components.
    pub fn num_registered(&self) -> usize {
        self.callbacks.lock().len()
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker process health monitor.
///
/// Tracks a set of worker PIDs and checks liveness periodically.
/// Reports dead workers via a callback.
pub struct WorkerHealthMonitor {
    workers: parking_lot::Mutex<std::collections::HashMap<u32, WorkerInfo>>,
}

/// Info about a monitored worker process.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub pid: u32,
    pub worker_id: String,
    pub start_time_ms: u64,
}

impl WorkerHealthMonitor {
    pub fn new() -> Self {
        Self {
            workers: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Start monitoring a worker process.
    pub fn add_worker(&self, pid: u32, worker_id: impl Into<String>, start_time_ms: u64) {
        let mut workers = self.workers.lock();
        workers.insert(
            pid,
            WorkerInfo {
                pid,
                worker_id: worker_id.into(),
                start_time_ms,
            },
        );
    }

    /// Stop monitoring a worker process.
    pub fn remove_worker(&self, pid: u32) -> Option<WorkerInfo> {
        let mut workers = self.workers.lock();
        workers.remove(&pid)
    }

    /// Check all workers and return the list of dead ones (removing them).
    pub fn check_health(&self) -> Vec<WorkerInfo> {
        let mut workers = self.workers.lock();
        let dead_pids: Vec<u32> = workers
            .keys()
            .filter(|&&pid| !is_process_alive(pid))
            .copied()
            .collect();

        let mut dead = Vec::new();
        for pid in dead_pids {
            if let Some(info) = workers.remove(&pid) {
                dead.push(info);
            }
        }
        dead
    }

    /// Return the number of monitored workers.
    pub fn num_workers(&self) -> usize {
        self.workers.lock().len()
    }

    /// Check if a specific PID is being monitored.
    pub fn is_monitoring(&self, pid: u32) -> bool {
        self.workers.lock().contains_key(&pid)
    }
}

impl Default for WorkerHealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_is_process_alive_self() {
        assert!(is_process_alive(get_pid()));
    }

    #[test]
    fn test_is_process_alive_dead_process() {
        // Spawn a short-lived process, wait for it to exit, then confirm it's dead.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap(); // Reap the child
        // After reaping, the PID should no longer be alive.
        assert!(!is_process_alive(pid));
    }

    #[test]
    fn test_get_pid() {
        assert!(get_pid() > 0);
        assert_eq!(get_pid(), std::process::id());
    }

    #[test]
    fn test_set_subreaper() {
        // Should not panic on any platform.
        let _ = set_subreaper();
    }

    #[test]
    fn test_shutdown_coordinator_basic() {
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let coord = ShutdownCoordinator::new();

        let o1 = order.clone();
        coord.register("A", move || o1.lock().push("A"));
        let o2 = order.clone();
        coord.register("B", move || o2.lock().push("B"));
        let o3 = order.clone();
        coord.register("C", move || o3.lock().push("C"));

        assert_eq!(coord.num_registered(), 3);
        assert!(!coord.is_shutdown());

        let count = coord.shutdown();
        assert_eq!(count, 3);
        assert!(coord.is_shutdown());

        // LIFO order
        assert_eq!(*order.lock(), vec!["C", "B", "A"]);
    }

    #[test]
    fn test_shutdown_coordinator_idempotent() {
        let coord = ShutdownCoordinator::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        coord.register("X", move || {
            c.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(coord.shutdown(), 1);
        assert_eq!(coord.shutdown(), 0); // Second call is no-op
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_shutdown_coordinator_empty() {
        let coord = ShutdownCoordinator::new();
        assert_eq!(coord.shutdown(), 0);
    }

    #[test]
    fn test_worker_health_monitor_add_remove() {
        let monitor = WorkerHealthMonitor::new();
        assert_eq!(monitor.num_workers(), 0);

        monitor.add_worker(1234, "w-1", 100);
        monitor.add_worker(5678, "w-2", 200);
        assert_eq!(monitor.num_workers(), 2);
        assert!(monitor.is_monitoring(1234));
        assert!(monitor.is_monitoring(5678));

        let removed = monitor.remove_worker(1234);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().worker_id, "w-1");
        assert_eq!(monitor.num_workers(), 1);
        assert!(!monitor.is_monitoring(1234));
    }

    #[test]
    fn test_worker_health_monitor_check_self() {
        let monitor = WorkerHealthMonitor::new();
        // Monitor our own process — should be alive.
        monitor.add_worker(get_pid(), "self", 0);
        let dead = monitor.check_health();
        assert!(dead.is_empty());
        assert_eq!(monitor.num_workers(), 1);
    }

    #[test]
    fn test_worker_health_monitor_check_dead() {
        let monitor = WorkerHealthMonitor::new();
        // Use a very high PID that doesn't exist.
        monitor.add_worker(u32::MAX - 1, "ghost", 0);
        let dead = monitor.check_health();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].worker_id, "ghost");
        // Dead workers should be removed.
        assert_eq!(monitor.num_workers(), 0);
    }

    #[test]
    fn test_worker_health_monitor_remove_nonexistent() {
        let monitor = WorkerHealthMonitor::new();
        assert!(monitor.remove_worker(9999).is_none());
    }
}
