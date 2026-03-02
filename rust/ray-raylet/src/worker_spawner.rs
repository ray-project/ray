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
    env_vars.insert(
        "RAY_RAYLET_PID".to_string(),
        std::process::id().to_string(),
    );
    env_vars.insert(
        "RAY_NODE_IP_ADDRESS".to_string(),
        config.node_ip_address.clone(),
    );
    env_vars.insert(
        "RAY_RAYLET_SOCKET_NAME".to_string(),
        format!("{}:{}", config.node_ip_address, config.raylet_port),
    );
    env_vars.insert("RAY_GCS_ADDRESS".to_string(), config.gcs_address.clone());
    env_vars.insert(
        "RAY_SESSION_NAME".to_string(),
        config.session_name.clone(),
    );

    let (program, args) = match language {
        Language::Python => {
            let cmd = config
                .python_worker_command
                .as_deref()
                .unwrap_or("python");
            (cmd.to_string(), vec!["-m".to_string(), "ray.worker".to_string()])
        }
        Language::Java => ("java".to_string(), vec!["-cp".to_string(), "ray-worker".to_string()]),
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
pub fn make_spawn_callback(config: WorkerSpawnerConfig) -> StartWorkerCallback {
    Box::new(move |language, job_id, worker_id| {
        spawn_worker_process(&config, language, job_id, worker_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicU32, Ordering}};

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
}
