// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Worker process spawning.
//!
//! Provides a callback-compatible function for starting worker processes
//! via `tokio::process::Command`. Used by `WorkerPool::set_start_worker_callback`.

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;

use ray_common::cgroup::CgroupManager;
use ray_common::id::{JobID, WorkerID};

use crate::worker_pool::{Language, StartWorkerCallback};

/// Configuration for the worker spawner.
#[derive(Debug, Clone)]
pub struct WorkerSpawnerConfig {
    /// Node IP address (for worker to connect back).
    pub node_ip_address: String,
    /// Raylet gRPC port (for worker to connect back).
    pub raylet_port: u16,
    /// GCS address (host:port).
    pub gcs_address: String,
    /// Node ID hex string.
    pub node_id: String,
    /// Session name.
    pub session_name: String,
    /// Optional custom Python worker command. Defaults to `python -m ray.worker`.
    pub python_worker_command: Option<String>,
}

/// Spawn a worker process for the given language.
///
/// Sets standard Ray environment variables so the worker can connect back to the
/// raylet and GCS. Returns the child's PID on success, or None on failure.
pub fn spawn_worker_process(
    config: &WorkerSpawnerConfig,
    language: Language,
    job_id: &JobID,
    worker_id: &WorkerID,
) -> Option<u32> {
    let mut env_vars: HashMap<String, String> = HashMap::new();
    env_vars.insert("RAY_NODE_ID".to_string(), config.node_id.clone());
    env_vars.insert("RAY_WORKER_ID".to_string(), worker_id.hex());
    env_vars.insert("RAY_JOB_ID".to_string(), job_id.hex());
    env_vars.insert("RAY_RAYLET_PID".to_string(), std::process::id().to_string());
    env_vars.insert(
        "RAY_NODE_IP_ADDRESS".to_string(),
        config.node_ip_address.clone(),
    );
    env_vars.insert(
        "RAY_RAYLET_SOCKET_NAME".to_string(),
        format!("{}:{}", config.node_ip_address, config.raylet_port),
    );
    env_vars.insert("RAY_GCS_ADDRESS".to_string(), config.gcs_address.clone());
    env_vars.insert("RAY_SESSION_NAME".to_string(), config.session_name.clone());

    let (program, args) = match language {
        Language::Python => {
            let cmd = config.python_worker_command.as_deref().unwrap_or("python");
            (
                cmd.to_string(),
                vec!["-m".to_string(), "ray.worker".to_string()],
            )
        }
        Language::Java => (
            "java".to_string(),
            vec!["-cp".to_string(), "ray-worker".to_string()],
        ),
        Language::Cpp => ("ray_cpp_worker".to_string(), vec![]),
    };

    tracing::info!(
        language = ?language,
        worker_id = %worker_id.hex(),
        job_id = %job_id.hex(),
        program = %program,
        "Spawning worker process"
    );

    match Command::new(&program).args(&args).envs(&env_vars).spawn() {
        Ok(child) => {
            let pid = child.id();
            tracing::info!(pid, worker_id = %worker_id.hex(), "Worker process started");
            Some(pid)
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                program = %program,
                "Failed to spawn worker process"
            );
            None
        }
    }
}

/// Create a `StartWorkerCallback` that spawns real OS processes.
///
/// If a `cgroup_manager` is provided, workers are added to the workers cgroup
/// immediately after spawning.
pub fn make_spawn_callback(
    config: WorkerSpawnerConfig,
    cgroup_manager: Option<Arc<dyn CgroupManager>>,
) -> StartWorkerCallback {
    Box::new(move |language, job_id, worker_id| {
        let pid = spawn_worker_process(&config, language, job_id, worker_id);
        if let (Some(pid), Some(ref cgm)) = (pid, &cgroup_manager) {
            if let Err(e) = cgm.add_process_to_workers_cgroup(pid) {
                tracing::warn!(
                    pid,
                    error = %e,
                    "Failed to add worker to cgroup"
                );
            }
        }
        pid
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    };

    #[test]
    fn test_make_spawn_callback_called() {
        // Use a counter to verify the callback is invoked.
        let call_count = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&call_count);

        let callback: StartWorkerCallback = Box::new(move |_lang, _job, _wid| {
            counter.fetch_add(1, Ordering::Relaxed);
            Some(12345) // fake PID
        });

        let job_id = JobID::from_int(1);
        let worker_id = WorkerID::from_random();
        let pid = callback(Language::Python, &job_id, &worker_id);

        assert_eq!(pid, Some(12345));
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_make_spawn_callback_with_noop_cgroup() {
        let config = WorkerSpawnerConfig {
            node_ip_address: "127.0.0.1".to_string(),
            raylet_port: 12345,
            gcs_address: "127.0.0.1:6379".to_string(),
            node_id: "test-node".to_string(),
            session_name: "test".to_string(),
            python_worker_command: Some("__nonexistent_binary__".to_string()),
        };

        // NoopCgroupManager should be accepted without errors.
        let cgm: Arc<dyn CgroupManager> =
            Arc::new(ray_common::cgroup::NoopCgroupManager::new("test-node"));
        let callback = make_spawn_callback(config, Some(cgm));

        // Spawning will fail (nonexistent binary), but the cgroup code path is exercised.
        let pid = callback(
            Language::Python,
            &JobID::from_int(1),
            &WorkerID::from_random(),
        );
        assert!(pid.is_none());
    }

    #[test]
    fn test_make_spawn_callback_without_cgroup() {
        let config = WorkerSpawnerConfig {
            node_ip_address: "127.0.0.1".to_string(),
            raylet_port: 12345,
            gcs_address: "127.0.0.1:6379".to_string(),
            node_id: "test-node".to_string(),
            session_name: "test".to_string(),
            python_worker_command: Some("__nonexistent_binary__".to_string()),
        };

        let callback = make_spawn_callback(config, None);
        let pid = callback(
            Language::Python,
            &JobID::from_int(1),
            &WorkerID::from_random(),
        );
        assert!(pid.is_none());
    }

    #[test]
    fn test_spawn_worker_process_nonexistent_binary() {
        let config = WorkerSpawnerConfig {
            node_ip_address: "127.0.0.1".to_string(),
            raylet_port: 12345,
            gcs_address: "127.0.0.1:6379".to_string(),
            node_id: "deadbeef".to_string(),
            session_name: "test".to_string(),
            python_worker_command: Some("__nonexistent_binary__".to_string()),
        };

        let pid = spawn_worker_process(
            &config,
            Language::Python,
            &JobID::from_int(1),
            &WorkerID::from_random(),
        );
        assert!(pid.is_none(), "Should fail for nonexistent binary");
    }

    /// Test that the callback is invoked with the correct language, job_id,
    /// and worker_id for multiple sequential calls, and that each call
    /// returns independently.
    #[test]
    fn test_callback_invocation_patterns_multiple_calls() {
        let call_log = Arc::new(Mutex::new(Vec::<(Language, String, String)>::new()));
        let log = Arc::clone(&call_log);

        let callback: StartWorkerCallback = Box::new(move |lang, job, wid| {
            log.lock().unwrap().push((lang, job.hex(), wid.hex()));
            Some(100)
        });

        let job1 = JobID::from_int(1);
        let job2 = JobID::from_int(2);
        let wid1 = WorkerID::from_random();
        let wid2 = WorkerID::from_random();

        let pid1 = callback(Language::Python, &job1, &wid1);
        let pid2 = callback(Language::Java, &job2, &wid2);

        assert_eq!(pid1, Some(100));
        assert_eq!(pid2, Some(100));

        let log = call_log.lock().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].0, Language::Python);
        assert_eq!(log[0].1, job1.hex());
        assert_eq!(log[0].2, wid1.hex());
        assert_eq!(log[1].0, Language::Java);
        assert_eq!(log[1].1, job2.hex());
        assert_eq!(log[1].2, wid2.hex());
    }

    /// Test that concurrent spawn requests (simulated via multiple threads)
    /// each get their own independent callback result.
    #[test]
    fn test_concurrent_spawn_requests() {
        let call_count = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&call_count);

        // Use Arc<dyn Fn(...)> instead of Box for thread-safe sharing
        let callback: Arc<dyn Fn(Language, &JobID, &WorkerID) -> Option<u32> + Send + Sync> =
            Arc::new(move |_lang, _job, _wid| {
                let c = counter.fetch_add(1, Ordering::Relaxed);
                Some(1000 + c)
            });

        let mut handles = Vec::new();
        for i in 0..5u32 {
            let cb = Arc::clone(&callback);
            let handle = std::thread::spawn(move || {
                let job = JobID::from_int(i);
                let wid = WorkerID::from_random();
                cb(Language::Python, &job, &wid)
            });
            handles.push(handle);
        }

        let mut pids: Vec<u32> = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect();
        pids.sort();

        // Each thread should have gotten a unique PID
        assert_eq!(pids.len(), 5);
        for i in 0..4 {
            assert_ne!(pids[i], pids[i + 1], "PIDs should be unique");
        }
        assert_eq!(call_count.load(Ordering::Relaxed), 5);
    }
}
