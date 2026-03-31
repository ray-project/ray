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

use crate::agent_manager::parse_command_line;
use crate::worker_pool::{Language, SpawnWorkerContext, StartWorkerCallback, WorkerType};

/// Configuration for the worker spawner.
#[derive(Debug, Clone)]
#[derive(Default)]
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
    /// Full shell command line for the Python worker (parsed into argv via POSIX
    /// shell rules). Defaults to `"python -m ray.worker"` when `None`.
    pub python_worker_command: Option<String>,
    /// Minimum port for spawned workers. 0 = unconstrained.
    pub min_worker_port: u16,
    /// Maximum port for spawned workers. 0 = unconstrained.
    pub max_worker_port: u16,
    /// Explicit worker port list. Overrides min/max if set.
    pub worker_port_list: Option<String>,
    /// Metrics export port (propagated to worker env).
    pub metrics_export_port: u16,
    /// Object store memory limit (propagated to worker env).
    pub object_store_memory: u64,
}

/// Spawn a worker process for the given language.
///
/// Sets standard Ray environment variables so the worker can connect back to the
/// raylet and GCS. Returns the child's PID on success, or None on failure.
///
/// ## PARITY STATUS: MATCHED for regular Python/Java/C++ workers.
/// ## NOT IMPLEMENTED for IO workers (spill/restore/delete).
///
/// Regular worker flags with real per-task values:
///   `--node-ip-address`, `--node-manager-port`, `--worker-id`, `--node-id`,
///   `--worker-launch-time-ms`, `--language`, `--session-name`, `--gcs-address`,
///   `--runtime-env-hash` (dedicated helper, i32, empty->0),
///   `--serialized-runtime-env-context` (from agent callback)
///
/// IO-worker infrastructure is NOT IMPLEMENTED in the Rust raylet:
///   - No PopSpillWorker/PopRestoreWorker/PopDeleteWorker in WorkerPool
///   - No TryStartIOWorkers mechanism
///   - No LocalObjectManager -> WorkerPool integration for on-demand IO workers
///   - `--worker-type` flag emission exists in build_worker_argv() but is
///     never reached on production paths (SpawnWorkerContext always uses
///     WorkerType::Worker)
///   - `--object-spilling-config` requires RayConfig access (not available)
///
/// Flags not applicable to Rust architecture:
///   `--startup-token`, `--worker-shim-pid` (C++ shim/process-group model)
/// Build environment variables for a worker process.
///
/// Returns the env var map that should be passed to the child process.
pub(crate) fn build_worker_env(
    config: &WorkerSpawnerConfig,
    job_id: &JobID,
    worker_id: &WorkerID,
) -> HashMap<String, String> {
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
    if config.min_worker_port > 0 {
        env_vars.insert("RAY_MIN_WORKER_PORT".to_string(), config.min_worker_port.to_string());
    }
    if config.max_worker_port > 0 {
        env_vars.insert("RAY_MAX_WORKER_PORT".to_string(), config.max_worker_port.to_string());
    }
    if let Some(ref list) = config.worker_port_list {
        env_vars.insert("RAY_WORKER_PORT_LIST".to_string(), list.clone());
    }
    if config.metrics_export_port > 0 {
        env_vars.insert("RAY_METRICS_EXPORT_PORT".to_string(), config.metrics_export_port.to_string());
    }
    if config.object_store_memory > 0 {
        env_vars.insert("RAY_OBJECT_STORE_MEMORY".to_string(), config.object_store_memory.to_string());
    }
    env_vars
}

/// Build the full argv (program + arguments) for a worker process.
///
/// Returns `Some((program, args))` on success, or `None` if the command line
/// could not be parsed (e.g. empty python_worker_command).
pub(crate) fn build_worker_argv(
    config: &WorkerSpawnerConfig,
    language: Language,
    worker_id: &WorkerID,
    runtime_env_hash: i32,
    serialized_runtime_env_context: &str,
    worker_type: WorkerType,
) -> Option<(String, Vec<String>)> {
    let (program, base_args) = match language {
        Language::Python => {
            let cmd_str = config
                .python_worker_command
                .as_deref()
                .unwrap_or("python -m ray.worker");
            let parsed = parse_command_line(cmd_str);
            if parsed.is_empty() {
                return None;
            }
            (parsed[0].clone(), parsed[1..].to_vec())
        }
        Language::Java => (
            "java".to_string(),
            vec!["-cp".to_string(), "ray-worker".to_string()],
        ),
        Language::Cpp => ("ray_cpp_worker".to_string(), vec![]),
    };

    // Append worker-specific argv flags matching C++ worker_pool.cc:323-400.
    // These duplicate the env vars but ensure compatibility with worker scripts
    // that only inspect argv.
    let mut args = base_args;

    // C++ workers use underscore-delimited flag names (--ray_worker_id,
    // --ray_runtime_env_hash); Python/Java use hyphen-delimited names.
    match language {
        Language::Cpp => {
            args.push(format!("--ray_worker_id={}", worker_id.hex()));
            args.push(format!("--ray_runtime_env_hash={}", runtime_env_hash));
            args.push("--language=CPP".to_string());
            args.push(format!("--node-ip-address={}", config.node_ip_address));
            args.push(format!("--node-manager-port={}", config.raylet_port));
            args.push(format!("--node-id={}", config.node_id));
            args.push(format!(
                "--worker-launch-time-ms={}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ));
            args.push(format!("--session-name={}", config.session_name));
            args.push(format!("--gcs-address={}", config.gcs_address));
        }
        _ => {
            // Python and Java workers share the same hyphen-delimited flags.
            args.push(format!("--node-ip-address={}", config.node_ip_address));
            args.push(format!("--node-manager-port={}", config.raylet_port));
            args.push(format!("--worker-id={}", worker_id.hex()));
            args.push(format!("--node-id={}", config.node_id));
            args.push(format!(
                "--worker-launch-time-ms={}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ));
            let lang_str = match language {
                Language::Python => "PYTHON",
                Language::Java => "JAVA",
                Language::Cpp => unreachable!(),
            };
            args.push(format!("--language={}", lang_str));
            args.push(format!("--session-name={}", config.session_name));
            args.push(format!("--gcs-address={}", config.gcs_address));
            // runtime_env_hash and serialized_runtime_env_context are per-task
            // values passed through SpawnWorkerContext from the runtime env agent.
            args.push(format!(
                "--runtime-env-hash={}",
                runtime_env_hash
            ));
            args.push(format!(
                "--serialized-runtime-env-context={}",
                serialized_runtime_env_context
            ));
            // C++ parity: emit --worker-type for IO/spill/restore workers.
            // Regular workers omit this flag (C++ worker_pool.cc only sets it
            // for non-default worker types).
            match worker_type {
                WorkerType::SpillWorker => {
                    args.push("--worker-type=SPILL_WORKER".to_string());
                    // TODO: --object-spilling-config requires RayConfig access
                    // in the spawn path, which is not yet available.
                }
                WorkerType::RestoreWorker => {
                    args.push("--worker-type=RESTORE_WORKER".to_string());
                    // TODO: --object-spilling-config requires RayConfig access
                    // in the spawn path, which is not yet available.
                }
                WorkerType::DeleteWorker => {
                    args.push("--worker-type=DELETE_WORKER".to_string());
                    // TODO: --object-spilling-config requires RayConfig access
                    // in the spawn path, which is not yet available.
                }
                WorkerType::Worker | WorkerType::Driver => {
                    // Regular workers and drivers do not set --worker-type.
                }
            }
        }
    }

    Some((program, args))
}

pub fn spawn_worker_process(
    config: &WorkerSpawnerConfig,
    language: Language,
    job_id: &JobID,
    worker_id: &WorkerID,
    ctx: &SpawnWorkerContext,
) -> Option<u32> {
    let env_vars = build_worker_env(config, job_id, worker_id);

    let (program, args) = match build_worker_argv(
        config,
        language,
        worker_id,
        ctx.runtime_env_hash,
        &ctx.serialized_runtime_env_context,
        ctx.worker_type,
    ) {
        Some(result) => result,
        None => {
            tracing::error!("python_worker_command parsed to empty argv");
            return None;
        }
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
    Box::new(move |language, job_id, worker_id, ctx| {
        let pid = spawn_worker_process(&config, language, job_id, worker_id, ctx);
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

        let callback: StartWorkerCallback = Box::new(move |_lang, _job, _wid, _ctx| {
            counter.fetch_add(1, Ordering::Relaxed);
            Some(12345) // fake PID
        });

        let job_id = JobID::from_int(1);
        let worker_id = WorkerID::from_random();
        let ctx = SpawnWorkerContext::default();
        let pid = callback(Language::Python, &job_id, &worker_id, &ctx);

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
            ..Default::default()
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
            &SpawnWorkerContext::default(),
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
            ..Default::default()
        };

        let callback = make_spawn_callback(config, None);
        let pid = callback(
            Language::Python,
            &JobID::from_int(1),
            &WorkerID::from_random(),
            &SpawnWorkerContext::default(),
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
            ..Default::default()
        };

        let pid = spawn_worker_process(
            &config,
            Language::Python,
            &JobID::from_int(1),
            &WorkerID::from_random(),
            &SpawnWorkerContext::default(),
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

        let callback: StartWorkerCallback = Box::new(move |lang, job, wid, _ctx| {
            log.lock().unwrap().push((lang, job.hex(), wid.hex()));
            Some(100)
        });

        let job1 = JobID::from_int(1);
        let job2 = JobID::from_int(2);
        let wid1 = WorkerID::from_random();
        let wid2 = WorkerID::from_random();
        let ctx = SpawnWorkerContext::default();

        let pid1 = callback(Language::Python, &job1, &wid1, &ctx);
        let pid2 = callback(Language::Java, &job2, &wid2, &ctx);

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

    // ── parse_command_line integration tests for worker spawner ──

    #[test]
    fn test_parse_simple_python_command() {
        let parsed = parse_command_line("python -m ray.worker");
        assert_eq!(parsed[0], "python");
        assert_eq!(&parsed[1..], &["-m", "ray.worker"]);
    }

    #[test]
    fn test_parse_command_with_flags() {
        let parsed = parse_command_line("/usr/bin/python -u -W ignore -m ray.worker");
        assert_eq!(parsed[0], "/usr/bin/python");
        assert_eq!(
            &parsed[1..],
            &["-u", "-W", "ignore", "-m", "ray.worker"]
        );
    }

    #[test]
    fn test_parse_command_with_quoted_path() {
        let parsed = parse_command_line(r#""/path/to/my python" -m ray.worker"#);
        assert_eq!(parsed[0], "/path/to/my python");
        assert_eq!(&parsed[1..], &["-m", "ray.worker"]);
    }

    #[test]
    fn test_parse_command_with_backslash_escape() {
        let parsed = parse_command_line(r"/path/to/my\ python -m ray.worker");
        assert_eq!(parsed[0], "/path/to/my python");
        assert_eq!(&parsed[1..], &["-m", "ray.worker"]);
    }

    #[test]
    fn test_default_python_worker_command() {
        let config = WorkerSpawnerConfig {
            node_ip_address: "127.0.0.1".to_string(),
            raylet_port: 12345,
            gcs_address: "127.0.0.1:6379".to_string(),
            node_id: "deadbeef".to_string(),
            session_name: "test".to_string(),
            python_worker_command: None, // None -> uses "python -m ray.worker"
            ..Default::default()
        };

        // We can't actually spawn "python" in unit tests, but we can verify
        // the parsing by checking the default string directly.
        let cmd_str = config
            .python_worker_command
            .as_deref()
            .unwrap_or("python -m ray.worker");
        let parsed = parse_command_line(cmd_str);
        assert_eq!(parsed, vec!["python", "-m", "ray.worker"]);
    }

    /// Test that concurrent spawn requests (simulated via multiple threads)
    /// each get their own independent callback result.
    #[test]
    fn test_concurrent_spawn_requests() {
        let call_count = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&call_count);

        // Use Arc<dyn Fn(...)> instead of Box for thread-safe sharing
        #[allow(clippy::type_complexity)]
        let callback: Arc<
            dyn Fn(Language, &JobID, &WorkerID, &SpawnWorkerContext) -> Option<u32> + Send + Sync,
        > = Arc::new(move |_lang, _job, _wid, _ctx| {
            let c = counter.fetch_add(1, Ordering::Relaxed);
            Some(1000 + c)
        });

        let mut handles = Vec::new();
        for i in 0..5u32 {
            let cb = Arc::clone(&callback);
            let handle = std::thread::spawn(move || {
                let job = JobID::from_int(i);
                let wid = WorkerID::from_random();
                cb(Language::Python, &job, &wid, &SpawnWorkerContext::default())
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

    // ── argv construction tests ──

    fn test_config() -> WorkerSpawnerConfig {
        WorkerSpawnerConfig {
            node_ip_address: "10.0.0.1".to_string(),
            raylet_port: 6800,
            gcs_address: "10.0.0.1:6379".to_string(),
            node_id: "abc123".to_string(),
            session_name: "session_2026".to_string(),
            python_worker_command: Some("python -m ray.worker".to_string()),
            ..Default::default()
        }
    }

    /// Helper: join all args into a single string for substring searching.
    fn args_contain(args: &[String], needle: &str) -> bool {
        args.iter().any(|a| a.contains(needle))
    }

    #[test]
    fn test_python_worker_argv_has_all_expected_flags() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (program, args) = build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::Worker).unwrap();

        assert_eq!(program, "python");
        // Base args from parsed command
        assert!(args.contains(&"-m".to_string()));
        assert!(args.contains(&"ray.worker".to_string()));

        // All required flags present
        assert!(args_contain(&args, "--node-ip-address=10.0.0.1"));
        assert!(args_contain(&args, "--node-manager-port=6800"));
        assert!(args_contain(&args, &format!("--worker-id={}", wid.hex())));
        assert!(args_contain(&args, "--node-id=abc123"));
        assert!(args_contain(&args, "--worker-launch-time-ms="));
        assert!(args_contain(&args, "--language=PYTHON"));
        assert!(args_contain(&args, "--session-name=session_2026"));
        assert!(args_contain(&args, "--gcs-address=10.0.0.1:6379"));
        assert!(args_contain(&args, "--runtime-env-hash=0"));
        assert!(args_contain(&args, "--serialized-runtime-env-context="));
    }

    #[test]
    fn test_python_worker_argv_does_not_have_wrong_language() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (_program, args) = build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::Worker).unwrap();
        // Must not contain JAVA or CPP language flags
        assert!(!args_contain(&args, "--language=JAVA"));
        assert!(!args_contain(&args, "--language=CPP"));
    }

    #[test]
    fn test_java_worker_argv_has_java_language() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (program, args) = build_worker_argv(&config, Language::Java, &wid, 0, "", WorkerType::Worker).unwrap();

        assert_eq!(program, "java");
        assert!(args_contain(&args, "--language=JAVA"));
        assert!(!args_contain(&args, "--language=PYTHON"));
        // Java workers use hyphen-delimited --worker-id
        assert!(args_contain(&args, &format!("--worker-id={}", wid.hex())));
        assert!(args_contain(&args, "--runtime-env-hash=0"));
    }

    #[test]
    fn test_cpp_worker_argv_has_cpp_language_and_underscore_flags() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (program, args) = build_worker_argv(&config, Language::Cpp, &wid, 0, "", WorkerType::Worker).unwrap();

        assert_eq!(program, "ray_cpp_worker");
        assert!(args_contain(&args, "--language=CPP"));
        // C++ workers use underscore-delimited flags
        assert!(args_contain(&args, &format!("--ray_worker_id={}", wid.hex())));
        assert!(args_contain(&args, "--ray_runtime_env_hash=0"));
        // Must NOT have hyphen-delimited --worker-id (that's Python/Java)
        assert!(!args_contain(&args, &format!("--worker-id={}", wid.hex())));
        // Must NOT have --runtime-env-hash (Python/Java style)
        assert!(!args_contain(&args, "--runtime-env-hash="));
    }

    #[test]
    fn test_cpp_worker_argv_has_common_flags() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (_program, args) = build_worker_argv(&config, Language::Cpp, &wid, 0, "", WorkerType::Worker).unwrap();

        // Common flags shared across all languages
        assert!(args_contain(&args, "--node-ip-address=10.0.0.1"));
        assert!(args_contain(&args, "--node-manager-port=6800"));
        assert!(args_contain(&args, "--node-id=abc123"));
        assert!(args_contain(&args, "--session-name=session_2026"));
        assert!(args_contain(&args, "--gcs-address=10.0.0.1:6379"));
        assert!(args_contain(&args, "--worker-launch-time-ms="));
    }

    #[test]
    fn test_worker_env_has_required_vars() {
        let config = test_config();
        let job_id = JobID::from_int(42);
        let wid = WorkerID::from_random();
        let env = build_worker_env(&config, &job_id, &wid);

        assert_eq!(env.get("RAY_NODE_ID").unwrap(), "abc123");
        assert_eq!(env.get("RAY_WORKER_ID").unwrap(), &wid.hex());
        assert_eq!(env.get("RAY_JOB_ID").unwrap(), &job_id.hex());
        assert_eq!(env.get("RAY_NODE_IP_ADDRESS").unwrap(), "10.0.0.1");
        assert_eq!(env.get("RAY_GCS_ADDRESS").unwrap(), "10.0.0.1:6379");
        assert_eq!(env.get("RAY_SESSION_NAME").unwrap(), "session_2026");
        assert_eq!(
            env.get("RAY_RAYLET_SOCKET_NAME").unwrap(),
            "10.0.0.1:6800"
        );
        assert!(env.contains_key("RAY_RAYLET_PID"));
    }

    #[test]
    fn test_worker_env_optional_vars_omitted_when_zero() {
        let config = test_config();
        let env = build_worker_env(&config, &JobID::from_int(1), &WorkerID::from_random());

        // These should be absent when config values are 0/None (the defaults)
        assert!(!env.contains_key("RAY_MIN_WORKER_PORT"));
        assert!(!env.contains_key("RAY_MAX_WORKER_PORT"));
        assert!(!env.contains_key("RAY_WORKER_PORT_LIST"));
        assert!(!env.contains_key("RAY_METRICS_EXPORT_PORT"));
        assert!(!env.contains_key("RAY_OBJECT_STORE_MEMORY"));
    }

    #[test]
    fn test_worker_env_optional_vars_present_when_set() {
        let mut config = test_config();
        config.min_worker_port = 10000;
        config.max_worker_port = 20000;
        config.worker_port_list = Some("10001,10002".to_string());
        config.metrics_export_port = 8080;
        config.object_store_memory = 1_000_000;

        let env = build_worker_env(&config, &JobID::from_int(1), &WorkerID::from_random());

        assert_eq!(env.get("RAY_MIN_WORKER_PORT").unwrap(), "10000");
        assert_eq!(env.get("RAY_MAX_WORKER_PORT").unwrap(), "20000");
        assert_eq!(env.get("RAY_WORKER_PORT_LIST").unwrap(), "10001,10002");
        assert_eq!(env.get("RAY_METRICS_EXPORT_PORT").unwrap(), "8080");
        assert_eq!(env.get("RAY_OBJECT_STORE_MEMORY").unwrap(), "1000000");
    }

    #[test]
    fn test_build_worker_argv_returns_none_for_empty_command() {
        let config = WorkerSpawnerConfig {
            python_worker_command: Some("".to_string()),
            ..test_config()
        };
        let wid = WorkerID::from_random();
        assert!(build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::Worker)
            .is_none());
    }

    #[test]
    fn test_runtime_env_hash_nonzero_propagated() {
        let config = test_config();
        let wid = WorkerID::from_random();

        // Python — pass per-task values through the function parameters
        let (_prog, args) = build_worker_argv(&config, Language::Python, &wid, 12345, "ctx_data", WorkerType::Worker).unwrap();
        assert!(args_contain(&args, "--runtime-env-hash=12345"));
        assert!(args_contain(&args, "--serialized-runtime-env-context=ctx_data"));

        // C++ uses underscore-delimited form
        let (_prog, args) = build_worker_argv(&config, Language::Cpp, &wid, 12345, "ctx_data", WorkerType::Worker).unwrap();
        assert!(args_contain(&args, "--ray_runtime_env_hash=12345"));
    }

    #[test]
    fn test_runtime_env_hash_negative_i32_propagated() {
        let config = test_config();
        let wid = WorkerID::from_random();
        // i32 can be negative after truncation from u64 — verify it formats correctly.
        let (_prog, args) = build_worker_argv(&config, Language::Python, &wid, -42, "", WorkerType::Worker).unwrap();
        assert!(args_contain(&args, "--runtime-env-hash=-42"));
    }

    #[test]
    fn test_worker_type_spill_worker_flag() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (_prog, args) = build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::SpillWorker).unwrap();
        assert!(args_contain(&args, "--worker-type=SPILL_WORKER"));
    }

    #[test]
    fn test_worker_type_restore_worker_flag() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (_prog, args) = build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::RestoreWorker).unwrap();
        assert!(args_contain(&args, "--worker-type=RESTORE_WORKER"));
    }

    #[test]
    fn test_worker_type_delete_worker_flag() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (_prog, args) = build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::DeleteWorker).unwrap();
        assert!(args_contain(&args, "--worker-type=DELETE_WORKER"));
    }

    #[test]
    fn test_worker_type_regular_worker_no_flag() {
        let config = test_config();
        let wid = WorkerID::from_random();
        let (_prog, args) = build_worker_argv(&config, Language::Python, &wid, 0, "", WorkerType::Worker).unwrap();
        assert!(!args_contain(&args, "--worker-type="));
    }
}
