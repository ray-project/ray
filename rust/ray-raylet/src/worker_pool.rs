// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Worker pool — manages worker process lifecycle.
//!
//! Replaces `src/ray/raylet/worker_pool.h/cc`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use ray_common::id::{JobID, WorkerID};

/// Worker language type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Language {
    Python,
    Java,
    Cpp,
}

/// Worker type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerType {
    Worker,
    Driver,
    SpillWorker,
    RestoreWorker,
    DeleteWorker,
}

/// Status of a PopWorker request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopWorkerStatus {
    Ok,
    JobConfigMissing,
    TooManyStartingWorkerProcesses,
    WorkerPendingRegistration,
    RuntimeEnvCreationFailed,
    JobFinished,
}

/// Information about a worker.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: WorkerID,
    pub language: Language,
    pub worker_type: WorkerType,
    pub job_id: JobID,
    pub pid: u32,
    pub port: u16,
    pub ip_address: String,
    pub is_alive: bool,
}

/// Result of requesting a worker.
pub struct PopWorkerResult {
    pub worker: Option<WorkerInfo>,
    pub status: PopWorkerStatus,
}

/// Callback invoked when a worker process needs to be started.
/// Receives language, job_id, and worker_id. Returns the PID of the started process.
pub type StartWorkerCallback = Box<dyn Fn(Language, &JobID, &WorkerID) -> Option<u32> + Send + Sync>;

/// The worker pool manages the lifecycle of worker processes.
pub struct WorkerPool {
    /// Per-language state.
    states: RwLock<HashMap<Language, LanguageState>>,
    /// All registered workers.
    all_workers: RwLock<HashMap<WorkerID, WorkerInfo>>,
    /// Dead workers (cached for queries).
    dead_workers: RwLock<HashMap<WorkerID, WorkerInfo>>,
    /// Active job configurations.
    active_jobs: RwLock<HashSet<JobID>>,
    /// Next worker ID counter.
    next_worker_counter: AtomicU64,
    /// Maximum concurrent worker starts.
    maximum_startup_concurrency: usize,
    /// Worker soft limit.
    num_workers_soft_limit: usize,
    /// Optional callback for starting worker processes.
    start_worker_callback: RwLock<Option<StartWorkerCallback>>,
}

/// Per-language pool state.
#[derive(Debug, Default)]
struct LanguageState {
    /// Idle workers available for reuse.
    idle_workers: VecDeque<WorkerID>,
    /// Workers currently starting up.
    starting_workers: HashSet<WorkerID>,
    /// Pending pop requests waiting for workers.
    #[allow(dead_code)]
    pending_requests: VecDeque<PendingPopRequest>,
}

/// A queued request waiting for a worker.
#[derive(Debug)]
#[allow(dead_code)]
struct PendingPopRequest {
    job_id: JobID,
    language: Language,
}

impl WorkerPool {
    pub fn new(maximum_startup_concurrency: usize, num_workers_soft_limit: usize) -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
            all_workers: RwLock::new(HashMap::new()),
            dead_workers: RwLock::new(HashMap::new()),
            active_jobs: RwLock::new(HashSet::new()),
            next_worker_counter: AtomicU64::new(1),
            maximum_startup_concurrency,
            num_workers_soft_limit,
            start_worker_callback: RwLock::new(None),
        }
    }

    /// Set the callback for starting worker processes.
    pub fn set_start_worker_callback(&self, callback: StartWorkerCallback) {
        *self.start_worker_callback.write() = Some(callback);
    }

    /// Register a worker that has connected.
    pub fn register_worker(&self, worker: WorkerInfo) -> Result<(), String> {
        let worker_id = worker.worker_id;
        let language = worker.language;
        self.all_workers.write().insert(worker_id, worker);

        // Remove from starting set
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&language) {
            state.starting_workers.remove(&worker_id);
        }

        tracing::info!(?worker_id, ?language, "Worker registered");
        Ok(())
    }

    /// Return an idle worker to the pool.
    pub fn push_worker(&self, worker_id: WorkerID, language: Language) {
        let mut states = self.states.write();
        let state = states.entry(language).or_default();
        state.idle_workers.push_back(worker_id);
    }

    /// Try to get an idle worker for the given language and job.
    pub fn pop_worker(&self, language: Language, job_id: &JobID) -> PopWorkerResult {
        if !self.active_jobs.read().contains(job_id) {
            return PopWorkerResult {
                worker: None,
                status: PopWorkerStatus::JobConfigMissing,
            };
        }

        let mut states = self.states.write();
        let state = states.entry(language).or_default();

        // Try to find an idle worker for this job
        if let Some(pos) = state.idle_workers.iter().position(|wid| {
            self.all_workers
                .read()
                .get(wid)
                .is_some_and(|w| w.job_id == *job_id)
        }) {
            let worker_id = state.idle_workers.remove(pos).unwrap();
            let worker = self.all_workers.read().get(&worker_id).cloned();
            return PopWorkerResult {
                worker,
                status: PopWorkerStatus::Ok,
            };
        }

        // No idle worker found, would need to start one
        if state.starting_workers.len() >= self.maximum_startup_concurrency {
            return PopWorkerResult {
                worker: None,
                status: PopWorkerStatus::TooManyStartingWorkerProcesses,
            };
        }

        PopWorkerResult {
            worker: None,
            status: PopWorkerStatus::WorkerPendingRegistration,
        }
    }

    /// Start a new worker process for the given language and job.
    /// Returns the worker ID of the starting worker, or None if startup failed.
    pub fn start_worker_process(&self, language: Language, job_id: &JobID) -> Option<WorkerID> {
        let mut states = self.states.write();
        let state = states.entry(language).or_default();

        if state.starting_workers.len() >= self.maximum_startup_concurrency {
            return None;
        }

        // Generate a new worker ID
        let counter = self.next_worker_counter.fetch_add(1, Ordering::Relaxed);
        let mut wid_bytes = [0u8; 28];
        wid_bytes[..8].copy_from_slice(&counter.to_le_bytes());
        let worker_id = WorkerID::from_binary(&wid_bytes);

        // Invoke the callback if set
        let callback = self.start_worker_callback.read();
        if let Some(ref cb) = *callback {
            let _pid = cb(language, job_id, &worker_id);
        }

        state.starting_workers.insert(worker_id);
        tracing::info!(?worker_id, ?language, "Starting worker process");
        Some(worker_id)
    }

    /// Disconnect a worker (mark as dead and remove from pools).
    pub fn disconnect_worker(&self, worker_id: &WorkerID) {
        let mut workers = self.all_workers.write();
        if let Some(worker) = workers.get_mut(worker_id) {
            worker.is_alive = false;
            self.dead_workers
                .write()
                .insert(*worker_id, worker.clone());
        }

        // Remove from idle queues
        let mut states = self.states.write();
        for state in states.values_mut() {
            state.idle_workers.retain(|id| id != worker_id);
        }
    }

    /// Get worker info by ID.
    pub fn get_worker(&self, worker_id: &WorkerID) -> Option<WorkerInfo> {
        self.all_workers.read().get(worker_id).cloned()
    }

    /// Check if a worker is dead.
    pub fn is_worker_dead(&self, worker_id: &WorkerID) -> bool {
        self.dead_workers.read().contains_key(worker_id)
    }

    /// Get the number of workers currently starting.
    pub fn num_starting_workers(&self) -> usize {
        self.states
            .read()
            .values()
            .map(|s| s.starting_workers.len())
            .sum()
    }

    /// Handle a job starting.
    pub fn handle_job_started(&self, job_id: JobID) {
        self.active_jobs.write().insert(job_id);
    }

    /// Handle a job finishing — remove job and disconnect idle workers for that job.
    pub fn handle_job_finished(&self, job_id: &JobID) {
        self.active_jobs.write().remove(job_id);

        // Disconnect idle workers that belong to this finished job
        let idle_to_disconnect: Vec<WorkerID> = {
            let workers = self.all_workers.read();
            let states = self.states.read();
            states
                .values()
                .flat_map(|s| s.idle_workers.iter())
                .filter(|wid| {
                    workers
                        .get(wid)
                        .is_some_and(|w| w.job_id == *job_id)
                })
                .copied()
                .collect()
        };

        for wid in &idle_to_disconnect {
            self.disconnect_worker(wid);
        }
    }

    /// Get the number of idle workers.
    pub fn num_idle_workers(&self) -> usize {
        self.states
            .read()
            .values()
            .map(|s| s.idle_workers.len())
            .sum()
    }

    /// Get the number of registered workers.
    pub fn num_registered_workers(&self) -> usize {
        self.all_workers.read().len()
    }

    /// Get all worker info.
    pub fn get_all_workers(&self) -> Vec<WorkerInfo> {
        self.all_workers.read().values().cloned().collect()
    }

    /// Kill idle workers to reclaim resources.
    pub fn try_killing_idle_workers(&self, max_to_kill: usize) -> Vec<WorkerID> {
        let total_workers = self.num_registered_workers();
        if total_workers <= self.num_workers_soft_limit {
            return vec![];
        }

        let mut killed = Vec::new();
        let mut states = self.states.write();

        for state in states.values_mut() {
            while !state.idle_workers.is_empty() && killed.len() < max_to_kill {
                if let Some(worker_id) = state.idle_workers.pop_front() {
                    killed.push(worker_id);
                }
            }
        }
        killed
    }

    /// Get the next unique worker counter.
    pub fn next_worker_counter(&self) -> u64 {
        self.next_worker_counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_worker(id: u8, lang: Language, job: JobID) -> WorkerInfo {
        let mut wid_bytes = [0u8; 28];
        wid_bytes[0] = id;
        WorkerInfo {
            worker_id: WorkerID::from_binary(&wid_bytes),
            language: lang,
            worker_type: WorkerType::Worker,
            job_id: job,
            pid: 1000 + id as u32,
            port: 10000 + id as u16,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        }
    }

    fn make_job_id(id: u8) -> JobID {
        let mut bytes = [0u8; 4];
        bytes[0] = id;
        JobID::from_binary(&bytes)
    }

    #[test]
    fn test_register_and_pop_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        let result = pool.pop_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert!(result.worker.is_some());
    }

    #[test]
    fn test_pop_missing_job() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        // Don't call handle_job_started

        let result = pool.pop_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::JobConfigMissing);
    }

    #[test]
    fn test_disconnect_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        pool.disconnect_worker(&wid);
        assert_eq!(pool.num_idle_workers(), 0);
    }

    #[test]
    fn test_kill_idle_workers() {
        let pool = WorkerPool::new(10, 0); // soft limit 0
        let job = make_job_id(1);
        pool.handle_job_started(job);

        for i in 1..=5 {
            let worker = make_worker(i, Language::Python, job);
            let wid = worker.worker_id;
            pool.register_worker(worker).unwrap();
            pool.push_worker(wid, Language::Python);
        }

        let killed = pool.try_killing_idle_workers(3);
        assert_eq!(killed.len(), 3);
        assert_eq!(pool.num_idle_workers(), 2);
    }

    #[test]
    fn test_start_worker_process() {
        let pool = WorkerPool::new(2, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let wid1 = pool.start_worker_process(Language::Python, &job);
        assert!(wid1.is_some());
        assert_eq!(pool.num_starting_workers(), 1);

        let wid2 = pool.start_worker_process(Language::Python, &job);
        assert!(wid2.is_some());
        assert_eq!(pool.num_starting_workers(), 2);

        // Third should fail — max concurrency is 2
        let wid3 = pool.start_worker_process(Language::Python, &job);
        assert!(wid3.is_none());
    }

    #[test]
    fn test_register_removes_from_starting() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let wid = pool.start_worker_process(Language::Python, &job).unwrap();
        assert_eq!(pool.num_starting_workers(), 1);

        // Register the worker (simulating it connecting back)
        let worker = WorkerInfo {
            worker_id: wid,
            language: Language::Python,
            worker_type: WorkerType::Worker,
            job_id: job,
            pid: 9999,
            port: 12345,
            ip_address: "127.0.0.1".to_string(),
            is_alive: true,
        };
        pool.register_worker(worker).unwrap();
        assert_eq!(pool.num_starting_workers(), 0);
        assert_eq!(pool.num_registered_workers(), 1);
    }

    #[test]
    fn test_get_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();

        let found = pool.get_worker(&wid);
        assert!(found.is_some());
        assert_eq!(found.unwrap().pid, 1001);

        let missing = pool.get_worker(&WorkerID::from_binary(&[0u8; 28]));
        assert!(missing.is_none());
    }

    #[test]
    fn test_is_worker_dead() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();

        assert!(!pool.is_worker_dead(&wid));
        pool.disconnect_worker(&wid);
        assert!(pool.is_worker_dead(&wid));
    }

    #[test]
    fn test_job_finished_disconnects_idle_workers() {
        let pool = WorkerPool::new(10, 100);
        let job1 = make_job_id(1);
        let job2 = make_job_id(2);
        pool.handle_job_started(job1);
        pool.handle_job_started(job2);

        // Register workers for both jobs
        let w1 = make_worker(1, Language::Python, job1);
        let wid1 = w1.worker_id;
        pool.register_worker(w1).unwrap();
        pool.push_worker(wid1, Language::Python);

        let w2 = make_worker(2, Language::Python, job2);
        let wid2 = w2.worker_id;
        pool.register_worker(w2).unwrap();
        pool.push_worker(wid2, Language::Python);

        assert_eq!(pool.num_idle_workers(), 2);

        // Finish job1 — only worker 1 should be disconnected
        pool.handle_job_finished(&job1);
        assert_eq!(pool.num_idle_workers(), 1);
        assert!(pool.is_worker_dead(&wid1));
        assert!(!pool.is_worker_dead(&wid2));
    }

    #[test]
    fn test_pop_worker_different_language() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        // Pop Java worker — no idle Java workers
        let result = pool.pop_worker(Language::Java, &job);
        assert_eq!(result.status, PopWorkerStatus::WorkerPendingRegistration);
        assert!(result.worker.is_none());

        // Pop Python worker — should succeed
        let result = pool.pop_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert!(result.worker.is_some());
    }

    #[test]
    fn test_start_worker_callback() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let started_count = Arc::new(AtomicU32::new(0));
        let count_clone = started_count.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Some(42) // fake PID
        }));

        pool.start_worker_process(Language::Python, &job);
        pool.start_worker_process(Language::Java, &job);

        assert_eq!(started_count.load(Ordering::Relaxed), 2);
    }
}
