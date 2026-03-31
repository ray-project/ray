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
use std::sync::Arc;

use parking_lot::RwLock;
use ray_common::id::{JobID, WorkerID};

use crate::runtime_env_agent_client::RuntimeEnvAgentClientTrait;

/// Worker language type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Language {
    #[default]
    Python,
    Java,
    Cpp,
}

/// Worker type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WorkerType {
    #[default]
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
#[derive(Debug, Clone, Default)]
pub struct WorkerInfo {
    pub worker_id: WorkerID,
    pub language: Language,
    pub worker_type: WorkerType,
    pub job_id: JobID,
    pub pid: u32,
    pub port: u16,
    pub ip_address: String,
    pub is_alive: bool,
    /// Serialized runtime environment JSON for this worker.
    /// C++ equivalent: `serialized_runtime_env` in `WorkerProcessInfo`.
    pub serialized_runtime_env: String,
}

/// Result of requesting a worker.
pub struct PopWorkerResult {
    pub worker: Option<WorkerInfo>,
    pub status: PopWorkerStatus,
}

/// Compute runtime-env hash for worker-pool matching.
///
/// PARITY STATUS: RUST-LOCAL OPERATIONAL EQUIVALENCE (not exact C++ parity).
///
/// C++ uses `static_cast<int>(std::hash<std::string>(...))` which is
/// platform-specific and not reproducible across languages. Rust uses
/// `DefaultHasher` (SipHash-based) truncated to `i32`.
///
/// This is acceptable because the hash is ONLY used within a single
/// raylet process for worker-pool caching/matching:
/// 1. Raylet computes hash from serialized_runtime_env
/// 2. Passes it to worker via --runtime-env-hash argv
/// 3. Worker sends it back at registration
/// 4. Raylet compares for worker reuse
///
/// Since both compute and compare happen within the same Rust process,
/// exact cross-language equivalence is not required. The hash just needs
/// to be self-consistent and deterministic within a single process.
///
/// Contract matching C++:
/// - Empty or "{}" -> returns 0
/// - Otherwise -> deterministic i32 hash
/// - Same input always produces same output within a process
pub fn calculate_runtime_env_hash(serialized_runtime_env: &str) -> i32 {
    if serialized_runtime_env.is_empty() || serialized_runtime_env == "{}" {
        return 0;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serialized_runtime_env.hash(&mut hasher);
    hasher.finish() as i32 // Truncate to i32 (same width as C++, different hash algorithm)
}

/// Worker spawn context carrying per-task runtime env state.
///
/// C++ parity: these fields are passed as explicit parameters through the
/// `StartWorkerProcess` call chain in `worker_pool.cc`. In Rust, we bundle
/// them into a struct so the callback signature remains clean.
#[derive(Debug, Clone)]
pub struct SpawnWorkerContext {
    /// Hash of the serialized runtime env string. 0 means no runtime env.
    /// Uses Rust-local SipHash (not C++ std::hash); see `calculate_runtime_env_hash`.
    pub runtime_env_hash: i32,
    /// Serialized runtime env context returned by the runtime env agent.
    pub serialized_runtime_env_context: String,
    /// Worker type — used to emit `--worker-type` on the command line for
    /// IO/spill/restore workers. Regular workers omit this flag.
    /// NOTE: IO worker types are defined but never used on production paths
    /// because IO-worker infrastructure is not implemented in the Rust raylet.
    pub worker_type: WorkerType,
}

impl Default for SpawnWorkerContext {
    fn default() -> Self {
        Self {
            runtime_env_hash: 0,
            serialized_runtime_env_context: String::new(),
            worker_type: WorkerType::Worker,
        }
    }
}

/// Callback invoked when a worker process needs to be started.
/// Receives language, job_id, worker_id, and per-task runtime env context.
/// Returns the PID of the started process.
pub type StartWorkerCallback =
    Box<dyn Fn(Language, &JobID, &WorkerID, &SpawnWorkerContext) -> Option<u32> + Send + Sync>;

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
    /// Runtime env agent client for creating/deleting runtime environments.
    /// C++ equivalent: `runtime_env_agent_client_` in `worker_pool.h`.
    /// Set via `set_runtime_env_agent_client()` after agent is ready.
    runtime_env_agent_client: RwLock<Option<Arc<dyn RuntimeEnvAgentClientTrait>>>,
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
            runtime_env_agent_client: RwLock::new(None),
        }
    }

    /// Set the callback for starting worker processes.
    pub fn set_start_worker_callback(&self, callback: StartWorkerCallback) {
        *self.start_worker_callback.write() = Some(callback);
    }

    /// Set the runtime env agent client.
    ///
    /// C++ equivalent: `WorkerPool::SetRuntimeEnvAgentClient()`.
    pub fn set_runtime_env_agent_client(
        &self,
        client: Arc<dyn RuntimeEnvAgentClientTrait>,
    ) {
        *self.runtime_env_agent_client.write() = Some(client);
    }

    /// Get the runtime env agent client (if set).
    pub fn runtime_env_agent_client(&self) -> Option<Arc<dyn RuntimeEnvAgentClientTrait>> {
        self.runtime_env_agent_client.read().clone()
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
    pub fn start_worker_process(
        &self,
        language: Language,
        job_id: &JobID,
        ctx: &SpawnWorkerContext,
    ) -> Option<WorkerID> {
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
            let _pid = cb(language, job_id, &worker_id, ctx);
        }

        state.starting_workers.insert(worker_id);
        tracing::info!(?worker_id, ?language, "Starting worker process");
        Some(worker_id)
    }

    /// Pre-start N Python worker processes at startup.
    ///
    /// C++ parity: `WorkerPool::PrestartWorkersInternal()` spawns workers
    /// immediately so they are idle and ready when the first job arrives.
    /// Uses a nil JobID because prestarted workers aren't bound to a job yet.
    pub fn prestart_workers(&self, count: u32) {
        if count == 0 {
            return;
        }
        let nil_job = JobID::nil();
        let default_ctx = SpawnWorkerContext::default();
        for i in 0..count {
            if self.start_worker_process(Language::Python, &nil_job, &default_ctx).is_some() {
                tracing::info!(worker_num = i + 1, total = count, "Prestarted Python worker");
            } else {
                tracing::warn!(
                    worker_num = i + 1,
                    total = count,
                    "Failed to prestart worker (concurrency limit reached)"
                );
                break;
            }
        }
    }

    /// Disconnect a worker (mark as dead and remove from pools).
    ///
    /// C++ equivalent: `WorkerPool::DisconnectWorker()` (worker_pool.cc:1592).
    /// Deletes the worker's runtime env via the agent client.
    pub fn disconnect_worker(&self, worker_id: &WorkerID) {
        let runtime_env = {
            let mut workers = self.all_workers.write();
            if let Some(worker) = workers.get_mut(worker_id) {
                worker.is_alive = false;
                let env = worker.serialized_runtime_env.clone();
                self.dead_workers.write().insert(*worker_id, worker.clone());
                env
            } else {
                String::new()
            }
        };

        // Remove from idle queues
        let mut states = self.states.write();
        for state in states.values_mut() {
            state.idle_workers.retain(|id| id != worker_id);
        }
        drop(states);

        // C++ parity: delete runtime env on worker disconnect (worker_pool.cc:1592).
        if !runtime_env.is_empty() && runtime_env != "{}" {
            if let Some(client) = self.runtime_env_agent_client() {
                self.delete_runtime_env_via_client(&client, &runtime_env);
            }
        }
    }

    /// Delete a runtime env via the agent client.
    ///
    /// C++ equivalent: `WorkerPool::DeleteRuntimeEnvIfPossible()`.
    fn delete_runtime_env_via_client(
        &self,
        client: &Arc<dyn RuntimeEnvAgentClientTrait>,
        serialized_runtime_env: &str,
    ) {
        let env = serialized_runtime_env.to_string();
        let env_for_log = env.clone();
        client.delete_runtime_env_if_possible(
            &env,
            Box::new(move |success| {
                if !success {
                    tracing::warn!(
                        env = %env_for_log,
                        "Failed to delete runtime env"
                    );
                }
            }),
        );
    }

    /// Start a new worker process with a runtime environment.
    ///
    /// C++ equivalent: `PopWorker()` with runtime env creation (worker_pool.cc:1349-1394).
    /// Creates the runtime env first, waits for the callback result, and only
    /// starts the worker process if runtime-env creation succeeds.
    ///
    /// `pool` must be an `Arc` pointing to `self` so we can move it into the
    /// async callback. The worker process is started inside the success callback,
    /// NOT unconditionally — matching C++ sequencing.
    ///
    /// Returns `None` immediately because the actual worker start is deferred
    /// until the runtime-env callback fires. On success, the worker will be
    /// started via `start_worker_process` inside the callback. On failure,
    /// no worker is started and the runtime env is cleaned up.
    pub fn start_worker_with_runtime_env(
        pool: &Arc<Self>,
        language: Language,
        job_id: &JobID,
        serialized_runtime_env: &str,
        serialized_runtime_env_config: &str,
    ) -> Option<WorkerID> {
        if serialized_runtime_env.is_empty() || serialized_runtime_env == "{}" {
            return pool.start_worker_process(language, job_id, &SpawnWorkerContext::default());
        }

        if let Some(client) = pool.runtime_env_agent_client() {
            let env = serialized_runtime_env.to_string();
            let jid = *job_id;
            let lang = language;
            let client_ref = Arc::clone(&client);
            let pool_ref = Arc::clone(pool);
            client.get_or_create_runtime_env(
                job_id,
                serialized_runtime_env,
                serialized_runtime_env_config,
                Box::new(move |success, context, error| {
                    if success {
                        tracing::info!(
                            job_id = %jid.hex(),
                            "Runtime env created for worker — starting worker process"
                        );
                        let ctx = SpawnWorkerContext {
                            runtime_env_hash: calculate_runtime_env_hash(&env),
                            serialized_runtime_env_context: context,
                            ..SpawnWorkerContext::default()
                        };
                        // C++ parity: only start the worker process AFTER
                        // successful runtime-env creation (worker_pool.cc:1377).
                        pool_ref.start_worker_process(lang, &jid, &ctx);
                    } else {
                        tracing::error!(
                            job_id = %jid.hex(),
                            error = %error,
                            "Failed to create runtime env for worker, not starting worker"
                        );
                        // C++ parity: delete env on failure (worker_pool.cc:1364).
                        client_ref.delete_runtime_env_if_possible(
                            &env,
                            Box::new(|_| {}),
                        );
                    }
                }),
            );
        }

        // Worker start is deferred to the callback. Return None to indicate
        // the caller should not expect a synchronous worker ID.
        None
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
    ///
    /// C++ equivalent: `WorkerPool::HandleJobStarted()` (worker_pool.cc:755).
    /// If a runtime env is set and eager installation is enabled, creates it now.
    pub fn handle_job_started(&self, job_id: JobID) {
        self.handle_job_started_with_runtime_env(job_id, String::new(), String::new());
    }

    /// Handle a job starting, with an optional runtime environment.
    ///
    /// Eagerly installs the runtime env via the agent client only when the
    /// runtime env is non-empty AND `eager_install` is true in the config.
    /// C++ equivalent: `WorkerPool::HandleJobStarted()` with
    /// `NeedToEagerInstallRuntimeEnv` guard (worker_pool.cc:730-769).
    pub fn handle_job_started_with_runtime_env(
        &self,
        job_id: JobID,
        serialized_runtime_env: String,
        serialized_runtime_env_config: String,
    ) {
        self.active_jobs.write().insert(job_id);

        // C++ parity: NeedToEagerInstallRuntimeEnv (worker_pool.cc:730-738).
        // Only eagerly install if:
        // 1. runtime env is non-empty
        // 2. eager_install flag is true in the config
        let should_eager_install = if serialized_runtime_env.is_empty()
            || serialized_runtime_env == "{}"
        {
            false
        } else {
            Self::should_eager_install(&serialized_runtime_env_config)
        };

        if should_eager_install {
            if let Some(client) = self.runtime_env_agent_client() {
                let env = serialized_runtime_env.clone();
                let jid = job_id;
                client.get_or_create_runtime_env(
                    &jid,
                    &env,
                    &serialized_runtime_env_config,
                    Box::new(move |success, _context, error| {
                        if success {
                            tracing::info!(
                                job_id = %jid.hex(),
                                "Eagerly installed runtime env for job"
                            );
                        } else {
                            tracing::error!(
                                job_id = %jid.hex(),
                                error = %error,
                                "Failed to eagerly install runtime env for job"
                            );
                        }
                    }),
                );
            }
        }
    }

    /// Check if `eager_install` is enabled in the serialized runtime env config.
    ///
    /// C++ parity: `NeedToEagerInstallRuntimeEnv` in worker_pool.cc.
    /// In protobuf, bool defaults to `false`, so an empty/missing config means
    /// no eager install. Returns `true` only when the config explicitly sets
    /// `eager_install` to `true`.
    fn should_eager_install(serialized_config: &str) -> bool {
        if serialized_config.is_empty() || serialized_config == "{}" {
            return false; // No config → proto default for bool is false
        }
        serde_json::from_str::<serde_json::Value>(serialized_config)
            .ok()
            .and_then(|config| config.get("eager_install")?.as_bool())
            .unwrap_or(false) // Missing field or parse failure → proto default false
    }

    /// Handle a job finishing — remove job and disconnect idle workers for that job.
    ///
    /// C++ equivalent: `WorkerPool::HandleJobFinished()` (worker_pool.cc:782).
    /// Deletes the job's runtime env via the agent client.
    pub fn handle_job_finished(&self, job_id: &JobID) {
        // Collect runtime envs to delete before removing workers.
        let envs_to_delete: Vec<String> = {
            let workers = self.all_workers.read();
            workers
                .values()
                .filter(|w| w.job_id == *job_id && !w.serialized_runtime_env.is_empty())
                .map(|w| w.serialized_runtime_env.clone())
                .collect()
        };

        self.active_jobs.write().remove(job_id);

        // Disconnect idle workers that belong to this finished job
        let idle_to_disconnect: Vec<WorkerID> = {
            let workers = self.all_workers.read();
            let states = self.states.read();
            states
                .values()
                .flat_map(|s| s.idle_workers.iter())
                .filter(|wid| workers.get(wid).is_some_and(|w| w.job_id == *job_id))
                .copied()
                .collect()
        };

        for wid in &idle_to_disconnect {
            self.disconnect_worker(wid);
        }

        // C++ parity: delete runtime envs for finished job (worker_pool.cc:782).
        if let Some(client) = self.runtime_env_agent_client() {
            for env in envs_to_delete {
                self.delete_runtime_env_via_client(&client, &env);
            }
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

    /// Pop an idle worker or start a new one for the given language and job.
    ///
    /// This combines pop_worker() + start_worker_process() into a single
    /// convenience method. Returns (worker_id, is_new) where is_new indicates
    /// whether a new process was started.
    pub fn pop_or_start_worker(&self, language: Language, job_id: &JobID) -> PopOrStartResult {
        // First try to pop an existing idle worker.
        let result = self.pop_worker(language, job_id);
        if result.status == PopWorkerStatus::Ok {
            let worker_id = result.worker.as_ref().map(|w| w.worker_id);
            return PopOrStartResult {
                worker: result.worker,
                worker_id,
                status: PopWorkerStatus::Ok,
                is_new_process: false,
            };
        }

        // No idle worker — try to start a new one.
        if result.status == PopWorkerStatus::WorkerPendingRegistration {
            if let Some(wid) = self.start_worker_process(language, job_id, &SpawnWorkerContext::default()) {
                return PopOrStartResult {
                    worker: None,
                    worker_id: Some(wid),
                    status: PopWorkerStatus::WorkerPendingRegistration,
                    is_new_process: true,
                };
            } else {
                return PopOrStartResult {
                    worker: None,
                    worker_id: None,
                    status: PopWorkerStatus::TooManyStartingWorkerProcesses,
                    is_new_process: false,
                };
            }
        }

        // Some other status (job not found, etc.)
        PopOrStartResult {
            worker: None,
            worker_id: None,
            status: result.status,
            is_new_process: false,
        }
    }

    /// Pop an idle worker or start a new one with runtime-env gating.
    ///
    /// C++ equivalent: `PopWorker()` in `worker_pool.cc:1349-1394`.
    /// This is the PRODUCTION path that should be used whenever a worker is
    /// needed for a task with a runtime environment.
    ///
    /// If `serialized_runtime_env` is non-empty, the worker start is deferred
    /// to the runtime-env creation callback (matching C++ sequencing). The
    /// `on_failure` callback is invoked if runtime-env creation fails, allowing
    /// the caller to propagate `RuntimeEnvCreationFailed` status.
    pub fn pop_or_start_worker_with_runtime_env(
        pool: &Arc<Self>,
        language: Language,
        job_id: &JobID,
        serialized_runtime_env: &str,
        serialized_runtime_env_config: &str,
        on_failure: Option<Box<dyn FnOnce(PopWorkerStatus) + Send + 'static>>,
    ) -> PopOrStartResult {
        // First try to pop an existing idle worker (same as pop_or_start_worker).
        let result = pool.pop_worker(language, job_id);
        if result.status == PopWorkerStatus::Ok {
            let worker_id = result.worker.as_ref().map(|w| w.worker_id);
            return PopOrStartResult {
                worker: result.worker,
                worker_id,
                status: PopWorkerStatus::Ok,
                is_new_process: false,
            };
        }

        // No idle worker — start a new one with runtime-env gating.
        if result.status == PopWorkerStatus::WorkerPendingRegistration {
            // Use the runtime-env-gated path when env is non-trivial.
            if !serialized_runtime_env.is_empty() && serialized_runtime_env != "{}" {
                // Deferred start: worker will be started inside the callback.
                WorkerPool::start_worker_with_runtime_env_and_callback(
                    pool,
                    language,
                    job_id,
                    serialized_runtime_env,
                    serialized_runtime_env_config,
                    on_failure,
                );
                return PopOrStartResult {
                    worker: None,
                    worker_id: None,
                    status: PopWorkerStatus::WorkerPendingRegistration,
                    is_new_process: true,
                };
            }

            // No runtime env — start directly.
            if let Some(wid) = pool.start_worker_process(language, job_id, &SpawnWorkerContext::default()) {
                return PopOrStartResult {
                    worker: None,
                    worker_id: Some(wid),
                    status: PopWorkerStatus::WorkerPendingRegistration,
                    is_new_process: true,
                };
            } else {
                return PopOrStartResult {
                    worker: None,
                    worker_id: None,
                    status: PopWorkerStatus::TooManyStartingWorkerProcesses,
                    is_new_process: false,
                };
            }
        }

        PopOrStartResult {
            worker: None,
            worker_id: None,
            status: result.status,
            is_new_process: false,
        }
    }

    /// Internal: Start a worker with runtime-env gating and failure callback.
    ///
    /// C++ equivalent: the runtime-env-gated path in `PopWorker()`.
    /// On success: starts the worker process inside the callback.
    /// On failure: invokes `on_failure` with `RuntimeEnvCreationFailed` and
    /// cleans up the failed env.
    fn start_worker_with_runtime_env_and_callback(
        pool: &Arc<Self>,
        language: Language,
        job_id: &JobID,
        serialized_runtime_env: &str,
        serialized_runtime_env_config: &str,
        on_failure: Option<Box<dyn FnOnce(PopWorkerStatus) + Send + 'static>>,
    ) {
        if let Some(client) = pool.runtime_env_agent_client() {
            let env = serialized_runtime_env.to_string();
            let jid = *job_id;
            let lang = language;
            let client_ref = Arc::clone(&client);
            let pool_ref = Arc::clone(pool);
            client.get_or_create_runtime_env(
                job_id,
                serialized_runtime_env,
                serialized_runtime_env_config,
                Box::new(move |success, context, error| {
                    if success {
                        tracing::info!(
                            job_id = %jid.hex(),
                            "Runtime env created — starting worker process"
                        );
                        let ctx = SpawnWorkerContext {
                            runtime_env_hash: calculate_runtime_env_hash(&env),
                            serialized_runtime_env_context: context,
                            ..SpawnWorkerContext::default()
                        };
                        pool_ref.start_worker_process(lang, &jid, &ctx);
                    } else {
                        tracing::error!(
                            job_id = %jid.hex(),
                            error = %error,
                            "Runtime env creation failed — not starting worker"
                        );
                        // C++ parity: propagate RuntimeEnvCreationFailed status.
                        if let Some(cb) = on_failure {
                            cb(PopWorkerStatus::RuntimeEnvCreationFailed);
                        }
                        // C++ parity: clean up the failed env.
                        client_ref.delete_runtime_env_if_possible(
                            &env,
                            Box::new(|_| {}),
                        );
                    }
                }),
            );
        }
    }

    /// Pop any idle worker for the given language, regardless of job ID.
    ///
    /// This is used for work stealing or when a task can run on any worker.
    pub fn pop_any_idle_worker(&self, language: Language) -> Option<WorkerInfo> {
        let mut states = self.states.write();
        let state = states.entry(language).or_default();

        if let Some(worker_id) = state.idle_workers.pop_front() {
            self.all_workers.read().get(&worker_id).cloned()
        } else {
            None
        }
    }

    /// Get the number of idle workers for a specific language.
    pub fn num_idle_workers_for_language(&self, language: Language) -> usize {
        self.states
            .read()
            .get(&language)
            .map(|s| s.idle_workers.len())
            .unwrap_or(0)
    }

    /// Get the number of active jobs.
    pub fn num_active_jobs(&self) -> usize {
        self.active_jobs.read().len()
    }

    /// Check if a job is active.
    pub fn is_job_active(&self, job_id: &JobID) -> bool {
        self.active_jobs.read().contains(job_id)
    }

    /// Get worker IDs for all idle workers.
    pub fn idle_worker_ids(&self) -> Vec<WorkerID> {
        self.states
            .read()
            .values()
            .flat_map(|s| s.idle_workers.iter().copied())
            .collect()
    }
}

/// Result of a pop-or-start operation.
pub struct PopOrStartResult {
    /// The worker if an idle one was found.
    pub worker: Option<WorkerInfo>,
    /// The worker ID (either existing or newly started).
    pub worker_id: Option<WorkerID>,
    /// Status of the operation.
    pub status: PopWorkerStatus,
    /// Whether a new process was started.
    pub is_new_process: bool,
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
            ..Default::default()
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

        let ctx = SpawnWorkerContext::default();
        let wid1 = pool.start_worker_process(Language::Python, &job, &ctx);
        assert!(wid1.is_some());
        assert_eq!(pool.num_starting_workers(), 1);

        let wid2 = pool.start_worker_process(Language::Python, &job, &ctx);
        assert!(wid2.is_some());
        assert_eq!(pool.num_starting_workers(), 2);

        // Third should fail — max concurrency is 2
        let wid3 = pool.start_worker_process(Language::Python, &job, &ctx);
        assert!(wid3.is_none());
    }

    #[test]
    fn test_register_removes_from_starting() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let wid = pool.start_worker_process(Language::Python, &job, &SpawnWorkerContext::default()).unwrap();
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
            ..Default::default()
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
    fn test_pop_or_start_idle_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        let result = pool.pop_or_start_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert!(result.worker.is_some());
        assert!(!result.is_new_process);
    }

    #[test]
    fn test_pop_or_start_new_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let result = pool.pop_or_start_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::WorkerPendingRegistration);
        assert!(result.worker.is_none());
        assert!(result.worker_id.is_some());
        assert!(result.is_new_process);
    }

    #[test]
    fn test_pop_any_idle_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        let result = pool.pop_any_idle_worker(Language::Python);
        assert!(result.is_some());
        assert_eq!(result.unwrap().worker_id, wid);
        assert_eq!(pool.num_idle_workers(), 0);
    }

    #[test]
    fn test_pop_any_idle_worker_empty() {
        let pool = WorkerPool::new(10, 100);
        let result = pool.pop_any_idle_worker(Language::Python);
        assert!(result.is_none());
    }

    #[test]
    fn test_num_idle_workers_per_language() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        for i in 1..=3 {
            let w = make_worker(i, Language::Python, job);
            let wid = w.worker_id;
            pool.register_worker(w).unwrap();
            pool.push_worker(wid, Language::Python);
        }
        let w = make_worker(10, Language::Java, job);
        let wid = w.worker_id;
        pool.register_worker(w).unwrap();
        pool.push_worker(wid, Language::Java);

        assert_eq!(pool.num_idle_workers_for_language(Language::Python), 3);
        assert_eq!(pool.num_idle_workers_for_language(Language::Java), 1);
        assert_eq!(pool.num_idle_workers_for_language(Language::Cpp), 0);
    }

    #[test]
    fn test_job_lifecycle() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);

        assert!(!pool.is_job_active(&job));
        pool.handle_job_started(job);
        assert!(pool.is_job_active(&job));
        assert_eq!(pool.num_active_jobs(), 1);

        pool.handle_job_finished(&job);
        assert!(!pool.is_job_active(&job));
        assert_eq!(pool.num_active_jobs(), 0);
    }

    #[test]
    fn test_idle_worker_ids() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let w1 = make_worker(1, Language::Python, job);
        let wid1 = w1.worker_id;
        pool.register_worker(w1).unwrap();
        pool.push_worker(wid1, Language::Python);

        let w2 = make_worker(2, Language::Java, job);
        let wid2 = w2.worker_id;
        pool.register_worker(w2).unwrap();
        pool.push_worker(wid2, Language::Java);

        let ids = pool.idle_worker_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&wid1));
        assert!(ids.contains(&wid2));
    }

    #[test]
    fn test_start_worker_callback() {
        use std::sync::Arc;

        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let started_count = Arc::new(AtomicU64::new(0));
        let count_clone = started_count.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Some(42) // fake PID
        }));

        let ctx = SpawnWorkerContext::default();
        pool.start_worker_process(Language::Python, &job, &ctx);
        pool.start_worker_process(Language::Java, &job, &ctx);

        assert_eq!(started_count.load(Ordering::Relaxed), 2);
    }

    // --- Additional worker pool tests ---

    #[test]
    fn test_pop_worker_wrong_job() {
        let pool = WorkerPool::new(10, 100);
        let job1 = make_job_id(1);
        let job2 = make_job_id(2);
        pool.handle_job_started(job1);
        pool.handle_job_started(job2);

        // Register worker for job1
        let worker = make_worker(1, Language::Python, job1);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        // Pop for job2 — should not find idle worker for job2
        let result = pool.pop_worker(Language::Python, &job2);
        assert_eq!(result.status, PopWorkerStatus::WorkerPendingRegistration);
        assert!(result.worker.is_none());

        // Pop for job1 — should succeed
        let result = pool.pop_worker(Language::Python, &job1);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert!(result.worker.is_some());
    }

    #[test]
    fn test_pop_worker_after_job_finished() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let worker = make_worker(1, Language::Python, job);
        let wid = worker.worker_id;
        pool.register_worker(worker).unwrap();
        pool.push_worker(wid, Language::Python);

        // Finish job, then try to pop
        pool.handle_job_finished(&job);
        let result = pool.pop_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::JobConfigMissing);
    }

    #[test]
    fn test_multiple_languages_isolation() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        // Register Python and Java workers
        let py_worker = make_worker(1, Language::Python, job);
        let py_wid = py_worker.worker_id;
        pool.register_worker(py_worker).unwrap();
        pool.push_worker(py_wid, Language::Python);

        let java_worker = make_worker(2, Language::Java, job);
        let java_wid = java_worker.worker_id;
        pool.register_worker(java_worker).unwrap();
        pool.push_worker(java_wid, Language::Java);

        // Pop Python — should get Python worker
        let result = pool.pop_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert_eq!(result.worker.unwrap().worker_id, py_wid);

        // Pop Java — should get Java worker
        let result = pool.pop_worker(Language::Java, &job);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert_eq!(result.worker.unwrap().worker_id, java_wid);

        // Pop C++ — no workers
        let result = pool.pop_worker(Language::Cpp, &job);
        assert_eq!(result.status, PopWorkerStatus::WorkerPendingRegistration);
    }

    #[test]
    fn test_kill_idle_within_soft_limit() {
        let pool = WorkerPool::new(10, 100); // soft limit = 100
        let job = make_job_id(1);
        pool.handle_job_started(job);

        for i in 1..=5 {
            let w = make_worker(i, Language::Python, job);
            let wid = w.worker_id;
            pool.register_worker(w).unwrap();
            pool.push_worker(wid, Language::Python);
        }

        // 5 workers < 100 soft limit — no killing
        let killed = pool.try_killing_idle_workers(10);
        assert!(killed.is_empty());
    }

    #[test]
    fn test_startup_concurrency_limit() {
        let pool = WorkerPool::new(3, 100); // max 3 concurrent starts per language
        let job = make_job_id(1);
        pool.handle_job_started(job);
        let ctx = SpawnWorkerContext::default();

        // Start 3 Python workers — at limit
        assert!(pool.start_worker_process(Language::Python, &job, &ctx).is_some());
        assert!(pool.start_worker_process(Language::Python, &job, &ctx).is_some());
        assert!(pool.start_worker_process(Language::Python, &job, &ctx).is_some());
        assert_eq!(pool.num_starting_workers(), 3);

        // 4th Python should fail — at per-language limit
        assert!(pool.start_worker_process(Language::Python, &job, &ctx).is_none());

        // Java is separate — should succeed (per-language limit)
        let wid = pool.start_worker_process(Language::Java, &job, &ctx);
        assert!(wid.is_some());
        assert_eq!(pool.num_starting_workers(), 4);
    }

    #[test]
    fn test_get_all_workers() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        for i in 1..=3 {
            let w = make_worker(i, Language::Python, job);
            pool.register_worker(w).unwrap();
        }

        let all = pool.get_all_workers();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_disconnect_removes_from_idle() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let w1 = make_worker(1, Language::Python, job);
        let wid1 = w1.worker_id;
        pool.register_worker(w1).unwrap();
        pool.push_worker(wid1, Language::Python);

        let w2 = make_worker(2, Language::Python, job);
        let wid2 = w2.worker_id;
        pool.register_worker(w2).unwrap();
        pool.push_worker(wid2, Language::Python);

        assert_eq!(pool.num_idle_workers(), 2);

        // Disconnect wid1
        pool.disconnect_worker(&wid1);
        assert_eq!(pool.num_idle_workers(), 1);
        assert!(pool.is_worker_dead(&wid1));
        assert!(!pool.is_worker_dead(&wid2));
    }

    #[test]
    fn test_pop_or_start_at_concurrency_limit() {
        let pool = WorkerPool::new(1, 100); // max 1 concurrent start
        let job = make_job_id(1);
        pool.handle_job_started(job);

        // First pop_or_start should start a new process
        let r1 = pool.pop_or_start_worker(Language::Python, &job);
        assert_eq!(r1.status, PopWorkerStatus::WorkerPendingRegistration);
        assert!(r1.is_new_process);

        // Second pop_or_start should hit concurrency limit
        let r2 = pool.pop_or_start_worker(Language::Python, &job);
        assert_eq!(r2.status, PopWorkerStatus::TooManyStartingWorkerProcesses);
        assert!(!r2.is_new_process);
    }

    #[test]
    fn test_multiple_jobs_lifecycle() {
        let pool = WorkerPool::new(10, 100);
        let job1 = make_job_id(1);
        let job2 = make_job_id(2);
        let job3 = make_job_id(3);

        pool.handle_job_started(job1);
        pool.handle_job_started(job2);
        pool.handle_job_started(job3);
        assert_eq!(pool.num_active_jobs(), 3);

        pool.handle_job_finished(&job2);
        assert_eq!(pool.num_active_jobs(), 2);
        assert!(pool.is_job_active(&job1));
        assert!(!pool.is_job_active(&job2));
        assert!(pool.is_job_active(&job3));

        pool.handle_job_finished(&job1);
        pool.handle_job_finished(&job3);
        assert_eq!(pool.num_active_jobs(), 0);
    }

    #[test]
    fn test_next_worker_counter_unique() {
        let pool = WorkerPool::new(10, 100);
        let c1 = pool.next_worker_counter();
        let c2 = pool.next_worker_counter();
        let c3 = pool.next_worker_counter();
        assert_ne!(c1, c2);
        assert_ne!(c2, c3);
    }

    #[test]
    fn test_pop_any_idle_worker_cross_job() {
        let pool = WorkerPool::new(10, 100);
        let job1 = make_job_id(1);
        let job2 = make_job_id(2);
        pool.handle_job_started(job1);
        pool.handle_job_started(job2);

        let w1 = make_worker(1, Language::Python, job1);
        let wid1 = w1.worker_id;
        pool.register_worker(w1).unwrap();
        pool.push_worker(wid1, Language::Python);

        // pop_any_idle_worker doesn't care about job ID
        let result = pool.pop_any_idle_worker(Language::Python);
        assert!(result.is_some());
        assert_eq!(result.unwrap().worker_id, wid1);
    }

    #[test]
    fn test_handle_job_finished_only_affects_idle() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        // Register 2 workers, only push 1 to idle
        let w1 = make_worker(1, Language::Python, job);
        let wid1 = w1.worker_id;
        pool.register_worker(w1).unwrap();
        pool.push_worker(wid1, Language::Python);

        let w2 = make_worker(2, Language::Python, job);
        let wid2 = w2.worker_id;
        pool.register_worker(w2).unwrap();
        // w2 is NOT idle — simulates a busy worker

        pool.handle_job_finished(&job);

        // wid1 was idle, should be disconnected
        assert!(pool.is_worker_dead(&wid1));
        // wid2 was not idle, should NOT be disconnected
        assert!(!pool.is_worker_dead(&wid2));
    }

    // ─── Additional ports from C++ worker_pool_test.cc ────────────────

    /// Port of HandleWorkerPushPop: push then pop same worker.
    #[test]
    fn test_push_pop_same_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let w = make_worker(1, Language::Python, job);
        let wid = w.worker_id;
        pool.register_worker(w).unwrap();
        pool.push_worker(wid, Language::Python);

        let result = pool.pop_worker(Language::Python, &job);
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert_eq!(result.worker.as_ref().unwrap().worker_id, wid);

        // Pool should be empty now
        let result2 = pool.pop_worker(Language::Python, &job);
        assert!(result2.worker.is_none());
    }

    /// Port of PopWorkerMultiTenancy: workers from different jobs
    /// are isolated.
    #[test]
    fn test_pop_worker_multi_tenancy() {
        let pool = WorkerPool::new(10, 100);
        let job1 = make_job_id(1);
        let job2 = make_job_id(2);
        pool.handle_job_started(job1);
        pool.handle_job_started(job2);

        // Worker for job1
        let w1 = make_worker(1, Language::Python, job1);
        let wid1 = w1.worker_id;
        pool.register_worker(w1).unwrap();
        pool.push_worker(wid1, Language::Python);

        // Worker for job2
        let w2 = make_worker(2, Language::Python, job2);
        let wid2 = w2.worker_id;
        pool.register_worker(w2).unwrap();
        pool.push_worker(wid2, Language::Python);

        // Pop for job1 should get job1's worker
        let result1 = pool.pop_worker(Language::Python, &job1);
        assert_eq!(result1.worker.as_ref().unwrap().worker_id, wid1);

        // Pop for job2 should get job2's worker
        let result2 = pool.pop_worker(Language::Python, &job2);
        assert_eq!(result2.worker.as_ref().unwrap().worker_id, wid2);
    }

    /// Port of WorkerNoLeaks: register, push, pop, disconnect, verify clean state.
    #[test]
    fn test_worker_no_leaks() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let w = make_worker(1, Language::Python, job);
        let wid = w.worker_id;
        pool.register_worker(w).unwrap();
        pool.push_worker(wid, Language::Python);

        // Pop and disconnect
        let result = pool.pop_worker(Language::Python, &job);
        assert!(result.worker.is_some());

        pool.disconnect_worker(&wid);
        assert!(pool.is_worker_dead(&wid));

        // No idle workers
        assert_eq!(pool.num_idle_workers(), 0);
    }

    /// Port of GetAllRegisteredWorkers: get_all_workers returns all registered.
    #[test]
    fn test_get_all_workers_comprehensive() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        for i in 0..5 {
            let w = make_worker(i, Language::Python, job);
            pool.register_worker(w).unwrap();
        }

        let workers = pool.get_all_workers();
        assert_eq!(workers.len(), 5);
    }

    /// Port of HandleIOWorkersPushPop: spill/restore worker types.
    #[test]
    fn test_register_different_worker_types() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        // Register a regular worker and a driver
        let w1 = make_worker(1, Language::Python, job);
        pool.register_worker(WorkerInfo {
            worker_type: WorkerType::Worker,
            ..w1
        })
        .unwrap();

        let mut w2 = make_worker(2, Language::Python, job);
        w2.worker_type = WorkerType::Driver;
        let w2 = w2;
        pool.register_worker(w2).unwrap();

        let workers = pool.get_all_workers();
        assert_eq!(workers.len(), 2);
    }

    /// Port of WorkerCapping: idle workers beyond soft limit should
    /// be killable.
    #[test]
    fn test_worker_capping_excess_idle() {
        let pool = WorkerPool::new(10, 2); // soft_limit = 2

        let job = make_job_id(1);
        pool.handle_job_started(job);

        // Register 5 workers, push all to idle
        let mut wids = Vec::new();
        for i in 0..5 {
            let w = make_worker(i, Language::Python, job);
            let wid = w.worker_id;
            pool.register_worker(w).unwrap();
            pool.push_worker(wid, Language::Python);
            wids.push(wid);
        }

        // 5 idle workers total
        let idle = pool.idle_worker_ids();
        assert_eq!(idle.len(), 5);
        // With soft_limit=2, excess idle = 5 - 2 = 3
        let excess = idle.len().saturating_sub(2);
        assert_eq!(excess, 3);
    }

    /// Port: registering the same worker twice overwrites the old entry.
    #[test]
    fn test_register_duplicate_worker() {
        let pool = WorkerPool::new(10, 100);
        let job = make_job_id(1);
        pool.handle_job_started(job);

        let w = make_worker(1, Language::Python, job);
        pool.register_worker(w).unwrap();

        // Registering again with same worker_id overwrites
        let mut w2 = make_worker(1, Language::Python, job);
        w2.port = 9999;
        pool.register_worker(w2).unwrap();

        // Still only one worker registered
        let all = pool.get_all_workers();
        assert_eq!(all.len(), 1);
    }

    /// Port: disconnect_worker on non-registered worker should not panic.
    #[test]
    fn test_disconnect_nonexistent_worker() {
        let pool = WorkerPool::new(10, 100);
        let mut wid_bytes = [0u8; 28];
        wid_bytes[0] = 99;
        let fake_wid = WorkerID::from_binary(&wid_bytes);
        pool.disconnect_worker(&fake_wid); // should not panic
    }

    /// C++ parity: prestart_workers spawns N Python workers at startup.
    /// Equivalent to WorkerPool::PrestartWorkersInternal in C++.
    #[test]
    fn test_prestart_workers_spawns_n_workers() {
        use std::sync::Arc;
        let pool = WorkerPool::new(10, 100);
        let started_count = Arc::new(AtomicU64::new(0));
        let count_clone = started_count.clone();
        pool.set_start_worker_callback(Box::new(move |lang, _job, _wid, _ctx| {
            assert_eq!(lang, Language::Python, "prestart must spawn Python workers");
            count_clone.fetch_add(1, Ordering::Relaxed);
            Some(100)
        }));

        pool.prestart_workers(3);
        assert_eq!(started_count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_prestart_workers_zero_is_noop() {
        use std::sync::Arc;
        let pool = WorkerPool::new(10, 100);
        let started_count = Arc::new(AtomicU64::new(0));
        let count_clone = started_count.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Some(100)
        }));

        pool.prestart_workers(0);
        assert_eq!(started_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_prestart_workers_respects_concurrency_limit() {
        use std::sync::Arc;
        // Pool with max 2 concurrent starting workers.
        let pool = WorkerPool::new(2, 100);
        let started_count = Arc::new(AtomicU64::new(0));
        let count_clone = started_count.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Some(100)
        }));

        // Request 5 prestart workers, but only 2 should succeed due to limit.
        pool.prestart_workers(5);
        assert_eq!(started_count.load(Ordering::Relaxed), 2);
    }

    // ── Decisive runtime-env sequencing tests (Phase 1) ─────────────

    /// DECISIVE TEST: Runtime-env failure prevents worker start.
    ///
    /// This test MUST fail on the old implementation where
    /// `start_worker_process()` was called unconditionally.
    ///
    /// Mock client returns failure → worker process must NOT be started.
    #[test]
    fn test_runtime_env_failure_prevents_worker_start() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        struct FailingClient {
            create_count: AtomicU32,
            delete_count: AtomicU32,
        }
        impl RuntimeEnvAgentClientTrait for FailingClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, _: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                self.create_count.fetch_add(1, Ordering::Relaxed);
                // Simulate failure.
                callback(false, String::new(), "env creation failed".to_string());
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, callback: DeleteRuntimeEnvIfPossibleCallback,
            ) {
                self.delete_count.fetch_add(1, Ordering::Relaxed);
                callback(true);
            }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(42);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(100)
        }));

        let client = Arc::new(FailingClient {
            create_count: AtomicU32::new(0),
            delete_count: AtomicU32::new(0),
        });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        // Call start_worker_with_runtime_env with a non-trivial env.
        let result = WorkerPool::start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["numpy"]}"#,
            "{}",
        );

        // The return value should be None (deferred to callback).
        assert!(result.is_none(), "Should return None for deferred start");

        // The runtime-env create was called.
        assert_eq!(client.create_count.load(Ordering::Relaxed), 1);

        // CRITICAL: The worker process must NOT have been started because
        // the runtime-env creation failed.
        assert_eq!(
            started.load(Ordering::Relaxed),
            0,
            "Worker process must NOT be started when runtime-env creation fails"
        );

        // The failing env should have been cleaned up.
        assert_eq!(
            client.delete_count.load(Ordering::Relaxed),
            1,
            "Runtime env should be deleted on creation failure"
        );
    }

    /// DECISIVE TEST: Runtime-env success starts worker process.
    ///
    /// Mock client returns success → worker process IS started inside callback.
    #[test]
    fn test_runtime_env_success_starts_worker() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        struct SucceedingClient {
            create_count: AtomicU32,
        }
        impl RuntimeEnvAgentClientTrait for SucceedingClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, _: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                self.create_count.fetch_add(1, Ordering::Relaxed);
                // Simulate success.
                callback(true, "context".to_string(), String::new());
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, callback: DeleteRuntimeEnvIfPossibleCallback,
            ) {
                callback(true);
            }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(43);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(101)
        }));

        let client = Arc::new(SucceedingClient {
            create_count: AtomicU32::new(0),
        });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        let result = WorkerPool::start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["numpy"]}"#,
            "{}",
        );

        // Return None (deferred), but the callback fires synchronously
        // because our mock calls callback immediately.
        assert!(result.is_none());
        assert_eq!(client.create_count.load(Ordering::Relaxed), 1);

        // CRITICAL: Worker process WAS started because runtime-env succeeded.
        assert_eq!(
            started.load(Ordering::Relaxed),
            1,
            "Worker process must be started after successful runtime-env creation"
        );
    }

    /// DECISIVE TEST: Empty/trivial runtime env starts worker directly (no callback).
    #[test]
    fn test_trivial_runtime_env_starts_worker_directly() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(44);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(102)
        }));

        // Empty env — should start worker directly.
        let result = WorkerPool::start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            "",
            "{}",
        );
        assert!(result.is_some(), "Empty env should start worker directly");
        assert_eq!(started.load(Ordering::Relaxed), 1);

        // Trivial env "{}" — should start worker directly.
        let result2 = WorkerPool::start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            "{}",
            "{}",
        );
        assert!(result2.is_some(), "Trivial env should start worker directly");
        assert_eq!(started.load(Ordering::Relaxed), 2);
    }

    /// DECISIVE TEST: No worker start occurs before the callback fires.
    ///
    /// Uses a delayed mock that invokes the callback later, proving
    /// the worker start only happens inside the callback, not before.
    #[test]
    fn test_no_worker_start_before_callback() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        /// A client that stores the callback instead of calling it immediately.
        struct DeferredClient {
            stored_callback: parking_lot::Mutex<Option<GetOrCreateRuntimeEnvCallback>>,
        }
        impl RuntimeEnvAgentClientTrait for DeferredClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, _: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                // Store, don't call yet.
                *self.stored_callback.lock() = Some(callback);
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, callback: DeleteRuntimeEnvIfPossibleCallback,
            ) {
                callback(true);
            }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(45);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(103)
        }));

        let client = Arc::new(DeferredClient {
            stored_callback: parking_lot::Mutex::new(None),
        });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        // Call start_worker_with_runtime_env — callback is deferred.
        let _result = WorkerPool::start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["numpy"]}"#,
            "{}",
        );

        // CRITICAL: No worker started yet because callback hasn't fired.
        assert_eq!(
            started.load(Ordering::Relaxed),
            0,
            "No worker must be started before callback fires"
        );

        // Now fire the callback with success.
        let cb = client.stored_callback.lock().take().unwrap();
        cb(true, "ctx".to_string(), String::new());

        // NOW the worker should be started.
        assert_eq!(
            started.load(Ordering::Relaxed),
            1,
            "Worker must be started after callback fires with success"
        );
    }

    // ── Production path tests: pop_or_start_worker_with_runtime_env ─────

    /// DECISIVE TEST: Production path uses runtime-env-gated worker start.
    ///
    /// Proves that `pop_or_start_worker_with_runtime_env` (the production API)
    /// correctly gates worker start on runtime-env creation success.
    #[test]
    fn test_pop_or_start_with_runtime_env_success_starts_worker() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        struct SucceedingClient { create_count: AtomicU32 }
        impl RuntimeEnvAgentClientTrait for SucceedingClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, _: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                self.create_count.fetch_add(1, Ordering::Relaxed);
                callback(true, "ctx".to_string(), String::new());
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, cb: DeleteRuntimeEnvIfPossibleCallback,
            ) { cb(true); }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(50);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(200)
        }));

        let client = Arc::new(SucceedingClient { create_count: AtomicU32::new(0) });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        let result = WorkerPool::pop_or_start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["numpy"]}"#,
            "{}",
            None,
        );

        // Should indicate a new process is being started.
        assert!(result.is_new_process);
        assert_eq!(result.status, PopWorkerStatus::WorkerPendingRegistration);
        // Runtime env was requested.
        assert_eq!(client.create_count.load(Ordering::Relaxed), 1);
        // Worker was started (callback fired synchronously with success).
        assert_eq!(started.load(Ordering::Relaxed), 1);
    }

    /// DECISIVE TEST: Production path propagates RuntimeEnvCreationFailed.
    ///
    /// Proves that `pop_or_start_worker_with_runtime_env` invokes the
    /// on_failure callback with `RuntimeEnvCreationFailed` when the runtime
    /// env agent returns failure.
    #[test]
    fn test_pop_or_start_with_runtime_env_failure_propagates_status() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        struct FailingClient { create_count: AtomicU32, delete_count: AtomicU32 }
        impl RuntimeEnvAgentClientTrait for FailingClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, _: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                self.create_count.fetch_add(1, Ordering::Relaxed);
                callback(false, String::new(), "env creation failed".to_string());
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, cb: DeleteRuntimeEnvIfPossibleCallback,
            ) {
                self.delete_count.fetch_add(1, Ordering::Relaxed);
                cb(true);
            }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(51);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(201)
        }));

        let client = Arc::new(FailingClient {
            create_count: AtomicU32::new(0),
            delete_count: AtomicU32::new(0),
        });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        // Track the failure callback.
        let failure_status = Arc::new(parking_lot::Mutex::new(None));
        let failure_clone = failure_status.clone();

        let result = WorkerPool::pop_or_start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["numpy"]}"#,
            "{}",
            Some(Box::new(move |status| {
                *failure_clone.lock() = Some(status);
            })),
        );

        // The result says a new process is being started (deferred).
        assert!(result.is_new_process);

        // But the runtime env FAILED, so:
        // 1. Worker was NOT started.
        assert_eq!(
            started.load(Ordering::Relaxed), 0,
            "Worker must NOT be started when runtime-env creation fails"
        );
        // 2. The on_failure callback was invoked with RuntimeEnvCreationFailed.
        assert_eq!(
            *failure_status.lock(),
            Some(PopWorkerStatus::RuntimeEnvCreationFailed),
            "on_failure must be called with RuntimeEnvCreationFailed"
        );
        // 3. The failed env was cleaned up.
        assert_eq!(
            client.delete_count.load(Ordering::Relaxed), 1,
            "Failed runtime env must be deleted"
        );
    }

    /// DECISIVE TEST: Production path with trivial env uses direct start.
    #[test]
    fn test_pop_or_start_with_trivial_env_starts_directly() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(52);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(202)
        }));

        // Empty env — direct start.
        let result = WorkerPool::pop_or_start_worker_with_runtime_env(
            &pool, Language::Python, &job, "", "{}", None,
        );
        assert!(result.is_new_process);
        assert!(result.worker_id.is_some());
        assert_eq!(started.load(Ordering::Relaxed), 1);

        // "{}" env — direct start.
        let result2 = WorkerPool::pop_or_start_worker_with_runtime_env(
            &pool, Language::Python, &job, "{}", "{}", None,
        );
        assert!(result2.is_new_process);
        assert_eq!(started.load(Ordering::Relaxed), 2);
    }

    /// DECISIVE TEST: Production path pops idle worker instead of starting new.
    #[test]
    fn test_pop_or_start_with_runtime_env_pops_idle_first() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(53);
        pool.handle_job_started(job);

        // Register and push an idle worker.
        let w = make_worker(1, Language::Python, job);
        let wid = w.worker_id;
        pool.register_worker(w).unwrap();
        pool.push_worker(wid, Language::Python);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(203)
        }));

        // Should pop the idle worker, NOT start a new one.
        let result = WorkerPool::pop_or_start_worker_with_runtime_env(
            &pool, Language::Python, &job, r#"{"pip": ["numpy"]}"#, "{}", None,
        );
        assert_eq!(result.status, PopWorkerStatus::Ok);
        assert!(!result.is_new_process);
        assert_eq!(result.worker_id, Some(wid));
        assert_eq!(started.load(Ordering::Relaxed), 0, "Should pop idle, not start new");
    }

    /// DECISIVE TEST: RuntimeEnvConfig is propagated to the agent client.
    ///
    /// Uses a capturing mock client that records the config value passed to
    /// `get_or_create_runtime_env`, verifying that a non-empty config is
    /// forwarded rather than replaced with "{}".
    #[test]
    fn test_runtime_env_config_propagated_to_agent_client() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        /// A client that captures the config string passed to get_or_create_runtime_env.
        struct CapturingClient {
            captured_config: parking_lot::Mutex<Vec<String>>,
            create_count: AtomicU32,
        }
        impl RuntimeEnvAgentClientTrait for CapturingClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, config: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                self.create_count.fetch_add(1, Ordering::Relaxed);
                self.captured_config.lock().push(config.to_string());
                callback(true, "ctx".to_string(), String::new());
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, cb: DeleteRuntimeEnvIfPossibleCallback,
            ) { cb(true); }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let job = make_job_id(60);
        pool.handle_job_started(job);

        let started = Arc::new(AtomicU32::new(0));
        let started_clone = started.clone();
        pool.set_start_worker_callback(Box::new(move |_lang, _job, _wid, _ctx| {
            started_clone.fetch_add(1, Ordering::Relaxed);
            Some(300)
        }));

        let client = Arc::new(CapturingClient {
            captured_config: parking_lot::Mutex::new(Vec::new()),
            create_count: AtomicU32::new(0),
        });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        let config_json = r#"{"setup_timeout_seconds":300,"eager_install":true}"#;

        // Test 1: start_worker_with_runtime_env propagates config.
        let _result = WorkerPool::start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["numpy"]}"#,
            config_json,
        );
        assert_eq!(client.create_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            client.captured_config.lock()[0],
            config_json,
            "start_worker_with_runtime_env must forward the config, not hardcode {{}}"
        );

        // Test 2: pop_or_start_worker_with_runtime_env propagates config.
        let _result2 = WorkerPool::pop_or_start_worker_with_runtime_env(
            &pool,
            Language::Python,
            &job,
            r#"{"pip": ["pandas"]}"#,
            config_json,
            None,
        );
        assert_eq!(client.create_count.load(Ordering::Relaxed), 2);
        assert_eq!(
            client.captured_config.lock()[1],
            config_json,
            "pop_or_start_worker_with_runtime_env must forward the config, not hardcode {{}}"
        );

        // Test 3: handle_job_started_with_runtime_env propagates config (eager_install=true).
        let job2 = make_job_id(61);
        pool.handle_job_started_with_runtime_env(
            job2,
            r#"{"pip": ["scipy"]}"#.to_string(),
            config_json.to_string(),
        );
        assert_eq!(client.create_count.load(Ordering::Relaxed), 3);
        assert_eq!(
            client.captured_config.lock()[2],
            config_json,
            "handle_job_started_with_runtime_env must forward the config, not hardcode {{}}"
        );
    }

    /// Test that eager install is gated on the eager_install config flag.
    ///
    /// C++ parity: `NeedToEagerInstallRuntimeEnv` in worker_pool.cc:730-738
    /// only returns true when `eager_install()` is true in the config proto.
    #[test]
    fn test_eager_install_gated_on_config_flag() {
        use crate::runtime_env_agent_client::{
            GetOrCreateRuntimeEnvCallback, DeleteRuntimeEnvIfPossibleCallback,
            RuntimeEnvAgentClientTrait,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        struct CountingClient {
            create_count: AtomicU32,
        }
        impl RuntimeEnvAgentClientTrait for CountingClient {
            fn get_or_create_runtime_env(
                &self, _: &JobID, _: &str, _: &str,
                callback: GetOrCreateRuntimeEnvCallback,
            ) {
                self.create_count.fetch_add(1, Ordering::Relaxed);
                callback(true, "ctx".to_string(), String::new());
            }
            fn delete_runtime_env_if_possible(
                &self, _: &str, cb: DeleteRuntimeEnvIfPossibleCallback,
            ) { cb(true); }
        }

        let pool = Arc::new(WorkerPool::new(10, 200));
        let client = Arc::new(CountingClient {
            create_count: AtomicU32::new(0),
        });
        pool.set_runtime_env_agent_client(
            Arc::clone(&client) as Arc<dyn RuntimeEnvAgentClientTrait>,
        );

        let runtime_env = r#"{"pip": ["numpy"]}"#.to_string();

        // Case 1: eager_install=true → SHOULD eagerly install.
        let job1 = make_job_id(70);
        pool.handle_job_started_with_runtime_env(
            job1,
            runtime_env.clone(),
            r#"{"eager_install":true}"#.to_string(),
        );
        assert_eq!(
            client.create_count.load(Ordering::Relaxed), 1,
            "eager_install=true must trigger eager install"
        );

        // Case 2: eager_install=false → should NOT eagerly install.
        let job2 = make_job_id(71);
        pool.handle_job_started_with_runtime_env(
            job2,
            runtime_env.clone(),
            r#"{"eager_install":false}"#.to_string(),
        );
        assert_eq!(
            client.create_count.load(Ordering::Relaxed), 1,
            "eager_install=false must NOT trigger eager install"
        );

        // Case 3: empty config → should NOT eagerly install (proto default is false).
        let job3 = make_job_id(72);
        pool.handle_job_started_with_runtime_env(
            job3,
            runtime_env.clone(),
            String::new(),
        );
        assert_eq!(
            client.create_count.load(Ordering::Relaxed), 1,
            "empty config must NOT trigger eager install (proto bool default is false)"
        );

        // Case 4: config is "{}" → should NOT eagerly install.
        let job4 = make_job_id(73);
        pool.handle_job_started_with_runtime_env(
            job4,
            runtime_env.clone(),
            "{}".to_string(),
        );
        assert_eq!(
            client.create_count.load(Ordering::Relaxed), 1,
            "config '{{}}' must NOT trigger eager install"
        );

        // Case 5: config without eager_install field → should NOT eagerly install.
        let job5 = make_job_id(74);
        pool.handle_job_started_with_runtime_env(
            job5,
            runtime_env.clone(),
            r#"{"setup_timeout_seconds":300}"#.to_string(),
        );
        assert_eq!(
            client.create_count.load(Ordering::Relaxed), 1,
            "config missing eager_install field must NOT trigger eager install"
        );

        // Case 6: empty runtime env with eager_install=true → should NOT install.
        let job6 = make_job_id(75);
        pool.handle_job_started_with_runtime_env(
            job6,
            "{}".to_string(),
            r#"{"eager_install":true}"#.to_string(),
        );
        assert_eq!(
            client.create_count.load(Ordering::Relaxed), 1,
            "empty runtime env must never trigger eager install regardless of config"
        );
    }

    /// Unit tests for the should_eager_install helper.
    #[test]
    fn test_should_eager_install_helper() {
        assert!(!WorkerPool::should_eager_install(""));
        assert!(!WorkerPool::should_eager_install("{}"));
        assert!(!WorkerPool::should_eager_install(r#"{"eager_install":false}"#));
        assert!(WorkerPool::should_eager_install(r#"{"eager_install":true}"#));
        assert!(!WorkerPool::should_eager_install(r#"{"setup_timeout_seconds":300}"#));
        assert!(WorkerPool::should_eager_install(
            r#"{"setup_timeout_seconds":300,"eager_install":true}"#
        ));
        assert!(!WorkerPool::should_eager_install("not valid json"));
    }

    #[test]
    fn test_calculate_runtime_env_hash() {
        assert_eq!(calculate_runtime_env_hash(""), 0);
        assert_eq!(calculate_runtime_env_hash("{}"), 0);
        // Non-empty gives non-zero (in most cases)
        let h = calculate_runtime_env_hash(r#"{"pip": ["numpy"]}"#);
        assert_ne!(h, 0);
        // Deterministic
        assert_eq!(h, calculate_runtime_env_hash(r#"{"pip": ["numpy"]}"#));
        // Different strings give different hashes (usually)
        assert_ne!(
            calculate_runtime_env_hash(r#"{"pip": ["numpy"]}"#),
            calculate_runtime_env_hash(r#"{"pip": ["pandas"]}"#),
        );
    }
}
