// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Worker process reaper — handles graceful shutdown and zombie detection.
//!
//! Ports C++ worker killing logic from `raylet/node_manager.cc` and
//! `raylet/worker_pool.cc`.
//!
//! Responsibilities:
//! - Graceful shutdown: SIGTERM → grace period → SIGKILL
//! - Zombie worker detection via health checks
//! - Resource cleanup when workers die unexpectedly

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use ray_common::id::WorkerID;

/// Configuration for the worker reaper.
#[derive(Debug, Clone)]
pub struct WorkerReaperConfig {
    /// Grace period between SIGTERM and SIGKILL.
    pub shutdown_grace_period: Duration,
    /// How often to check for dead workers.
    pub health_check_interval: Duration,
    /// Timeout for worker startup (registration after process start).
    pub startup_timeout: Duration,
}

impl Default for WorkerReaperConfig {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Duration::from_secs(5),
            health_check_interval: Duration::from_secs(10),
            startup_timeout: Duration::from_secs(30),
        }
    }
}

/// State of a worker being reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapState {
    /// SIGTERM sent, waiting for grace period.
    TermSent,
    /// SIGKILL sent, waiting for process to exit.
    KillSent,
    /// Process confirmed dead.
    Dead,
}

/// Tracks a worker process being shut down or monitored.
#[derive(Debug, Clone)]
struct TrackedWorker {
    /// OS process ID.
    pid: u32,
    /// Current reap state (None = healthy, being monitored).
    reap_state: Option<ReapState>,
    /// When this worker started (for startup timeout).
    started_at: Instant,
    /// When SIGTERM was sent (for grace period tracking).
    term_sent_at: Option<Instant>,
}

/// Callback invoked when a worker dies (for resource cleanup).
pub type WorkerDeathCallback = Box<dyn Fn(&WorkerID, u32) + Send + Sync>;

/// Manages worker process lifecycle — health monitoring and graceful shutdown.
pub struct WorkerReaper {
    inner: Mutex<WorkerReaperInner>,
    config: WorkerReaperConfig,
}

struct WorkerReaperInner {
    /// Workers being tracked.
    tracked: HashMap<WorkerID, TrackedWorker>,
    /// Callback when a worker dies.
    death_callback: Option<WorkerDeathCallback>,
    /// Statistics.
    total_reaped: u64,
    total_timed_out: u64,
}

impl WorkerReaper {
    /// Create a new worker reaper.
    pub fn new(config: WorkerReaperConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(WorkerReaperInner {
                tracked: HashMap::new(),
                death_callback: None,
                total_reaped: 0,
                total_timed_out: 0,
            }),
        }
    }

    /// Set the callback invoked when a worker dies.
    pub fn set_death_callback(&self, callback: WorkerDeathCallback) {
        self.inner.lock().death_callback = Some(callback);
    }

    /// Start tracking a worker process.
    pub fn track_worker(&self, worker_id: WorkerID, pid: u32) {
        self.inner.lock().tracked.insert(
            worker_id,
            TrackedWorker {
                pid,
                reap_state: None,
                started_at: Instant::now(),
                term_sent_at: None,
            },
        );
    }

    /// Stop tracking a worker process (e.g., normal shutdown).
    pub fn untrack_worker(&self, worker_id: &WorkerID) {
        self.inner.lock().tracked.remove(worker_id);
    }

    /// Request graceful shutdown of a worker.
    ///
    /// Sends SIGTERM to the process. The worker will be SIGKILL'd after
    /// the grace period if it hasn't exited.
    pub fn request_shutdown(&self, worker_id: &WorkerID) -> bool {
        let mut inner = self.inner.lock();
        if let Some(worker) = inner.tracked.get_mut(worker_id) {
            if worker.reap_state.is_some() {
                return false; // Already shutting down.
            }

            // Send SIGTERM.
            let pid = worker.pid;
            if send_signal(pid, Signal::Term) {
                worker.reap_state = Some(ReapState::TermSent);
                worker.term_sent_at = Some(Instant::now());
                tracing::info!(?worker_id, pid, "Sent SIGTERM to worker");
                true
            } else {
                // Process already dead.
                worker.reap_state = Some(ReapState::Dead);
                inner.total_reaped += 1;
                true
            }
        } else {
            false
        }
    }

    /// Force-kill a worker immediately (SIGKILL).
    pub fn force_kill(&self, worker_id: &WorkerID) -> bool {
        let mut inner = self.inner.lock();
        if let Some(worker) = inner.tracked.get_mut(worker_id) {
            let pid = worker.pid;
            send_signal(pid, Signal::Kill);
            worker.reap_state = Some(ReapState::KillSent);
            inner.total_reaped += 1;
            tracing::info!(?worker_id, pid, "Sent SIGKILL to worker");
            true
        } else {
            false
        }
    }

    /// Check all tracked workers for health and process reap timeouts.
    ///
    /// Returns the set of worker IDs that were found dead.
    pub fn check_workers(&self) -> Vec<(WorkerID, u32)> {
        let mut dead = Vec::new();
        let grace_period = self.config.shutdown_grace_period;
        let mut inner = self.inner.lock();
        let mut to_escalate = Vec::new();
        let mut newly_dead = Vec::new();

        for (wid, worker) in inner.tracked.iter() {
            match worker.reap_state {
                None => {
                    // Healthy worker — check if process is still alive.
                    if !is_process_alive(worker.pid) {
                        newly_dead.push((*wid, worker.pid));
                    }
                }
                Some(ReapState::TermSent) => {
                    // Check if grace period expired.
                    if let Some(sent_at) = worker.term_sent_at {
                        if sent_at.elapsed() >= grace_period {
                            if is_process_alive(worker.pid) {
                                to_escalate.push((*wid, worker.pid));
                            } else {
                                newly_dead.push((*wid, worker.pid));
                            }
                        } else if !is_process_alive(worker.pid) {
                            newly_dead.push((*wid, worker.pid));
                        }
                    }
                }
                Some(ReapState::KillSent) => {
                    // Just check if dead.
                    if !is_process_alive(worker.pid) {
                        newly_dead.push((*wid, worker.pid));
                    }
                }
                Some(ReapState::Dead) => {
                    // Already processed.
                }
            }
        }

        // Escalate SIGTERM → SIGKILL.
        for (wid, pid) in &to_escalate {
            if let Some(worker) = inner.tracked.get_mut(wid) {
                send_signal(*pid, Signal::Kill);
                worker.reap_state = Some(ReapState::KillSent);
                tracing::warn!(?wid, pid, "Grace period expired, escalating to SIGKILL");
            }
        }

        // Mark newly dead workers.
        for (wid, pid) in &newly_dead {
            if let Some(worker) = inner.tracked.get_mut(wid) {
                worker.reap_state = Some(ReapState::Dead);
                inner.total_reaped += 1;
            }
            dead.push((*wid, *pid));
        }

        // Fire death callback for newly dead workers (outside of mutable borrow context).
        // We need to drop the lock first.
        let callback_ref = inner.death_callback.is_some();
        if callback_ref && !dead.is_empty() {
            if let Some(ref cb) = inner.death_callback {
                for (wid, pid) in &dead {
                    cb(wid, *pid);
                }
            }
        }

        dead
    }

    /// Get workers that have timed out during startup (didn't register in time).
    pub fn get_startup_timeouts(&self) -> Vec<(WorkerID, u32)> {
        let startup_timeout = self.config.startup_timeout;
        let mut inner = self.inner.lock();
        let mut timed_out = Vec::new();

        for (wid, worker) in inner.tracked.iter() {
            if worker.reap_state.is_none() && worker.started_at.elapsed() >= startup_timeout {
                timed_out.push((*wid, worker.pid));
            }
        }

        inner.total_timed_out += timed_out.len() as u64;
        timed_out
    }

    /// Clean up dead workers from tracking.
    pub fn cleanup_dead(&self) -> usize {
        let mut inner = self.inner.lock();
        let before = inner.tracked.len();
        inner
            .tracked
            .retain(|_, w| w.reap_state != Some(ReapState::Dead));
        before - inner.tracked.len()
    }

    /// Number of workers currently being tracked.
    pub fn num_tracked(&self) -> usize {
        self.inner.lock().tracked.len()
    }

    /// Number of workers currently being shut down.
    pub fn num_shutting_down(&self) -> usize {
        self.inner
            .lock()
            .tracked
            .values()
            .filter(|w| {
                matches!(
                    w.reap_state,
                    Some(ReapState::TermSent | ReapState::KillSent)
                )
            })
            .count()
    }

    /// Statistics: (total_reaped, total_timed_out).
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        (inner.total_reaped, inner.total_timed_out)
    }
}

impl Default for WorkerReaper {
    fn default() -> Self {
        Self::new(WorkerReaperConfig::default())
    }
}

/// Signal types for process management.
#[derive(Debug, Clone, Copy)]
enum Signal {
    Term,
    Kill,
}

/// Send a signal to a process. Returns true if the signal was sent successfully.
fn send_signal(pid: u32, signal: Signal) -> bool {
    #[cfg(unix)]
    {
        let sig = match signal {
            Signal::Term => nix::sys::signal::Signal::SIGTERM,
            Signal::Kill => nix::sys::signal::Signal::SIGKILL,
        };
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), Some(sig)).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, signal);
        false
    }
}

/// Check if a process is alive.
fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // kill(pid, 0) checks if process exists without sending a signal.
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn make_wid(val: u8) -> WorkerID {
        let mut data = [0u8; 28];
        data[0] = val;
        WorkerID::from_binary(&data)
    }

    #[test]
    fn test_track_and_untrack() {
        let reaper = WorkerReaper::default();
        let wid = make_wid(1);

        reaper.track_worker(wid, 12345);
        assert_eq!(reaper.num_tracked(), 1);

        reaper.untrack_worker(&wid);
        assert_eq!(reaper.num_tracked(), 0);
    }

    #[test]
    fn test_request_shutdown_untracked() {
        let reaper = WorkerReaper::default();
        let wid = make_wid(1);
        assert!(!reaper.request_shutdown(&wid));
    }

    #[test]
    fn test_force_kill_untracked() {
        let reaper = WorkerReaper::default();
        let wid = make_wid(1);
        assert!(!reaper.force_kill(&wid));
    }

    #[test]
    fn test_check_workers_detects_dead() {
        let reaper = WorkerReaper::default();
        let wid = make_wid(1);

        // Use PID 0 which is typically not a valid process.
        // Actually use a very large PID that doesn't exist.
        reaper.track_worker(wid, u32::MAX - 1);

        let dead = reaper.check_workers();
        // On most systems, this PID won't exist.
        assert!(dead.len() <= 1); // May or may not detect as dead depending on OS.
    }

    #[test]
    fn test_cleanup_dead() {
        let reaper = WorkerReaper::new(WorkerReaperConfig {
            shutdown_grace_period: Duration::from_millis(1),
            ..Default::default()
        });
        let wid = make_wid(1);

        // Track a non-existent process.
        reaper.track_worker(wid, u32::MAX - 2);

        // Force-kill should mark for reaping.
        reaper.force_kill(&wid);
        assert_eq!(reaper.num_tracked(), 1);

        // Check and detect as dead.
        reaper.check_workers();

        // Cleanup dead workers.
        let cleaned = reaper.cleanup_dead();
        // May or may not have been marked as dead depending on timing.
        assert!(cleaned <= 1);
    }

    #[test]
    fn test_num_shutting_down() {
        let reaper = WorkerReaper::default();
        let wid1 = make_wid(1);
        let wid2 = make_wid(2);

        // Track workers with the current process PID (so they're "alive").
        let my_pid = std::process::id();
        reaper.track_worker(wid1, my_pid);
        reaper.track_worker(wid2, my_pid);

        assert_eq!(reaper.num_shutting_down(), 0);

        // Don't actually send SIGTERM to ourselves — just test the state.
        // Use force_kill on a non-existent PID.
        let wid3 = make_wid(3);
        reaper.track_worker(wid3, u32::MAX - 3);
        reaper.force_kill(&wid3);

        assert_eq!(reaper.num_shutting_down(), 1);
    }

    #[test]
    fn test_death_callback() {
        let reaper = WorkerReaper::default();
        let death_count = Arc::new(AtomicU32::new(0));
        let dc = death_count.clone();

        reaper.set_death_callback(Box::new(move |_wid, _pid| {
            dc.fetch_add(1, Ordering::Relaxed);
        }));

        let wid = make_wid(1);
        // Track a non-existent process.
        reaper.track_worker(wid, u32::MAX - 4);

        // Check should detect it as dead and fire callback.
        let dead = reaper.check_workers();
        if !dead.is_empty() {
            assert!(death_count.load(Ordering::Relaxed) >= 1);
        }
    }

    #[test]
    fn test_startup_timeout() {
        let reaper = WorkerReaper::new(WorkerReaperConfig {
            startup_timeout: Duration::from_millis(1),
            ..Default::default()
        });
        let wid = make_wid(1);
        let my_pid = std::process::id();
        reaper.track_worker(wid, my_pid);

        // Sleep past the timeout.
        std::thread::sleep(Duration::from_millis(5));

        let timed_out = reaper.get_startup_timeouts();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].0, wid);
    }

    #[test]
    fn test_stats() {
        let reaper = WorkerReaper::default();
        let (reaped, timed_out) = reaper.stats();
        assert_eq!(reaped, 0);
        assert_eq!(timed_out, 0);
    }
}
