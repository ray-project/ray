// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Agent manager — manages agent subprocesses (dashboard agent, runtime env agent).
//!
//! Replaces `src/ray/raylet/agent_manager.h/cc`.
//!
//! In C++, the raylet:
//! 1. Validates that agent command lines are non-empty (FATAL if empty)
//! 2. Launches dashboard agent and runtime env agent as child processes
//! 3. Monitors the processes and respawns on failure (fate_shares=true)
//! 4. Reports agent PIDs via HandleGetAgentPIDs RPC
//! 5. Cleans up on shutdown

use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

/// Placeholder constant for node_manager_port in agent command lines.
/// C++ equivalent: `kNodeManagerPortPlaceholder` in `node_manager.cc`.
const NODE_MANAGER_PORT_PLACEHOLDER: &str = "RAY_NODE_MANAGER_PORT_PLACEHOLDER";

/// Options for creating an agent manager.
///
/// C++ equivalent: `AgentManager::Options`.
#[derive(Debug, Clone)]
pub struct AgentManagerOptions {
    /// The node ID (for logging).
    pub node_id: String,
    /// Agent name (e.g., "dashboard_agent", "runtime_env_agent").
    pub agent_name: String,
    /// Command line to launch the agent.
    pub command_line: Vec<String>,
    /// Whether the raylet and agent share fate (raylet dies if agent dies).
    pub fate_shares: bool,
    /// Whether to respawn the agent on exit.
    pub respawn_on_exit: bool,
}

/// Maximum number of respawn attempts before giving up.
const MAX_RESPAWN_ATTEMPTS: u32 = 10;

/// Delay between respawn attempts (exponential backoff base).
const RESPAWN_BASE_DELAY_MS: u64 = 1000;

/// Manages a single agent subprocess with monitoring and respawn.
///
/// C++ equivalent: `AgentManager` in `agent_manager.h`.
///
/// Lifecycle:
/// 1. `start(port)` — launches the subprocess
/// 2. `start_monitoring()` — spawns a tokio task that watches the child
///    and respawns on exit if `respawn_on_exit` is true
/// 3. If `fate_shares` is true and the agent cannot be restarted,
///    the raylet is signaled to shut down gracefully
/// 4. `stop()` — kills the subprocess and cancels monitoring
pub struct AgentManager {
    options: AgentManagerOptions,
    /// PID of the agent process (0 if not started).
    pid: AtomicU32,
    /// The child process handle.
    child: Mutex<Option<Child>>,
    /// The raylet port (saved from start() for respawn).
    raylet_port: std::sync::atomic::AtomicU16,
    /// Whether monitoring has been stopped (prevents respawn after stop).
    stopped: std::sync::atomic::AtomicBool,
    /// Resolved command line (after placeholder replacement).
    resolved_command: Mutex<Vec<String>>,
}

impl AgentManager {
    /// Create a new agent manager with the given options.
    ///
    /// Does NOT start the process — call `start()` to launch.
    pub fn new(options: AgentManagerOptions) -> Self {
        tracing::info!(
            agent = %options.agent_name,
            command = ?options.command_line,
            "Creating AgentManager"
        );
        Self {
            options,
            pid: AtomicU32::new(0),
            child: Mutex::new(None),
            raylet_port: std::sync::atomic::AtomicU16::new(0),
            stopped: std::sync::atomic::AtomicBool::new(false),
            resolved_command: Mutex::new(Vec::new()),
        }
    }

    /// Start the agent subprocess.
    ///
    /// Replaces the `NODE_MANAGER_PORT_PLACEHOLDER` in the command line
    /// with the actual raylet port, then spawns the process.
    ///
    /// C++ equivalent: `AgentManager::StartAgent()`.
    pub fn start(&self, raylet_port: u16) -> Result<u32, String> {
        self.raylet_port
            .store(raylet_port, std::sync::atomic::Ordering::Release);
        self.stopped
            .store(false, std::sync::atomic::Ordering::Release);

        let mut args = self.options.command_line.clone();
        if args.is_empty() {
            return Err(format!(
                "{} command must be non-empty",
                self.options.agent_name
            ));
        }

        // Replace port placeholder in all arguments.
        // C++ equivalent: node_manager.cc line 3307-3313.
        for arg in &mut args {
            if let Some(pos) = arg.find(NODE_MANAGER_PORT_PLACEHOLDER) {
                arg.replace_range(
                    pos..pos + NODE_MANAGER_PORT_PLACEHOLDER.len(),
                    &raylet_port.to_string(),
                );
            }
        }

        // Save resolved command for respawn.
        *self.resolved_command.lock() = args.clone();

        self.spawn_process(&args)
    }

    /// Internal: spawn the process from a resolved command line.
    fn spawn_process(&self, args: &[String]) -> Result<u32, String> {
        if args.is_empty() {
            return Err("Empty command line".to_string());
        }
        let program = &args[0];
        let cmd_args = &args[1..];

        tracing::info!(
            agent = %self.options.agent_name,
            program = %program,
            args = ?cmd_args,
            "Launching agent subprocess"
        );

        match Command::new(program).args(cmd_args).spawn() {
            Ok(child) => {
                let pid = child.id();
                self.pid.store(pid, Ordering::Release);
                *self.child.lock() = Some(child);
                tracing::info!(
                    agent = %self.options.agent_name,
                    pid,
                    "Agent subprocess started"
                );
                Ok(pid)
            }
            Err(e) => {
                let msg = format!(
                    "Failed to start {} agent: {}",
                    self.options.agent_name, e
                );
                tracing::error!("{}", msg);
                Err(msg)
            }
        }
    }

    /// Respawn the agent subprocess.
    ///
    /// C++ equivalent: `AgentManager` with `respawn_on_exit=true`.
    /// Returns Ok(new_pid) on success, Err on failure.
    fn respawn(&self) -> Result<u32, String> {
        let args = self.resolved_command.lock().clone();
        if args.is_empty() {
            return Err("No resolved command available for respawn".to_string());
        }
        self.spawn_process(&args)
    }

    /// Start monitoring the agent subprocess in a background task.
    ///
    /// C++ equivalent: The AgentManager's internal monitoring loop that
    /// checks whether the child is still alive and respawns if needed.
    ///
    /// The monitoring task:
    /// 1. Polls the child process every second
    /// 2. If the child exits and `respawn_on_exit` is true, respawns
    /// 3. If respawn fails after MAX_RESPAWN_ATTEMPTS and `fate_shares`
    ///    is true, signals the raylet to shut down
    ///
    /// Returns a JoinHandle for the monitoring task.
    pub fn start_monitoring(
        self: &std::sync::Arc<Self>,
    ) -> tokio::task::JoinHandle<()> {
        let mgr = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let mut respawn_count: u32 = 0;

            loop {
                // Check every second.
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                // If stopped, exit the monitoring loop.
                if mgr.stopped.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                // Check if child is still alive.
                if mgr.is_alive() {
                    respawn_count = 0; // Reset on successful run.
                    continue;
                }

                // Child has exited.
                let old_pid = mgr.pid.load(Ordering::Acquire);
                if old_pid == 0 {
                    // Never started or already stopped.
                    continue;
                }

                tracing::warn!(
                    agent = %mgr.options.agent_name,
                    pid = old_pid,
                    "Agent subprocess exited"
                );

                // Should we respawn?
                if !mgr.options.respawn_on_exit {
                    if mgr.options.fate_shares {
                        tracing::error!(
                            agent = %mgr.options.agent_name,
                            "Fate-sharing agent died without respawn — requesting raylet shutdown"
                        );
                        // In production, this would trigger shutdown_raylet_gracefully.
                        // For now, we send SIGTERM to ourselves.
                        #[cfg(unix)]
                        {
                            unsafe {
                                nix::libc::kill(nix::libc::getpid(), nix::libc::SIGTERM);
                            }
                        }
                    }
                    break;
                }

                // Attempt respawn with exponential backoff.
                respawn_count += 1;
                if respawn_count > MAX_RESPAWN_ATTEMPTS {
                    tracing::error!(
                        agent = %mgr.options.agent_name,
                        attempts = MAX_RESPAWN_ATTEMPTS,
                        "Agent subprocess respawn limit reached"
                    );
                    if mgr.options.fate_shares {
                        tracing::error!(
                            agent = %mgr.options.agent_name,
                            "Fate-sharing agent cannot be restarted — requesting raylet shutdown"
                        );
                        #[cfg(unix)]
                        {
                            unsafe {
                                nix::libc::kill(nix::libc::getpid(), nix::libc::SIGTERM);
                            }
                        }
                    }
                    break;
                }

                let delay = RESPAWN_BASE_DELAY_MS * (1 << respawn_count.min(5));
                tracing::info!(
                    agent = %mgr.options.agent_name,
                    attempt = respawn_count,
                    delay_ms = delay,
                    "Respawning agent subprocess"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;

                match mgr.respawn() {
                    Ok(new_pid) => {
                        tracing::info!(
                            agent = %mgr.options.agent_name,
                            old_pid,
                            new_pid,
                            "Agent subprocess respawned"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            agent = %mgr.options.agent_name,
                            error = %e,
                            attempt = respawn_count,
                            "Failed to respawn agent subprocess"
                        );
                    }
                }
            }
        })
    }

    /// Get the PID of the agent subprocess.
    ///
    /// C++ equivalent: `AgentManager::GetPid()`.
    pub fn get_pid(&self) -> u32 {
        self.pid.load(Ordering::Acquire)
    }

    /// Check if the agent process is still alive.
    pub fn is_alive(&self) -> bool {
        let mut guard = self.child.lock();
        if let Some(ref mut child) = *guard {
            match child.try_wait() {
                Ok(None) => true,  // Still running
                Ok(Some(_)) => false,  // Exited
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Stop the agent subprocess and cancel monitoring.
    ///
    /// Sends SIGTERM on Unix, kills on Windows.
    pub fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);

        let mut guard = self.child.lock();
        if let Some(ref mut child) = *guard {
            tracing::info!(
                agent = %self.options.agent_name,
                pid = self.pid.load(Ordering::Acquire),
                "Stopping agent subprocess"
            );
            let _ = child.kill();
            let _ = child.wait();
        }
        *guard = None;
        self.pid.store(0, Ordering::Release);
    }

    /// The agent name.
    pub fn agent_name(&self) -> &str {
        &self.options.agent_name
    }

    /// The agent options.
    pub fn options(&self) -> &AgentManagerOptions {
        &self.options
    }
}

impl Drop for AgentManager {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Parse a shell command string into a vector of arguments.
///
/// C++ equivalent: `ParseCommandLine()` in `node_manager.cc`.
/// Follows POSIX shell parsing semantics:
/// - Outside quotes: backslash escapes the next character
/// - Single quotes: everything is literal (no escape processing)
/// - Double quotes: backslash escapes `"` and `\` only; other `\X` kept as-is
pub fn parse_command_line(command: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_single_quote {
            // Inside single quotes: everything is literal until closing quote.
            if ch == '\'' {
                in_single_quote = false;
            } else {
                current.push(ch);
            }
        } else if in_double_quote {
            // Inside double quotes: backslash escapes " and \ only.
            if ch == '"' {
                in_double_quote = false;
            } else if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    if next == '"' || next == '\\' {
                        current.push(chars.next().unwrap());
                    } else {
                        // Backslash before other chars is kept literally.
                        current.push('\\');
                    }
                } else {
                    current.push('\\');
                }
            } else {
                current.push(ch);
            }
        } else {
            // Outside any quotes.
            match ch {
                '\'' => {
                    in_single_quote = true;
                }
                '"' => {
                    in_double_quote = true;
                }
                '\\' => {
                    // Backslash escapes the next character.
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                }
                ' ' | '\t' => {
                    if !current.is_empty() {
                        args.push(std::mem::take(&mut current));
                    }
                }
                _ => {
                    current.push(ch);
                }
            }
        }
    }
    if !current.is_empty() {
        args.push(current);
    }
    args
}

/// Create a dashboard agent manager.
///
/// C++ equivalent: `NodeManager::CreateDashboardAgentManager()`.
/// Returns None if the command is empty.
pub fn create_dashboard_agent_manager(
    node_id: &str,
    dashboard_agent_command: &str,
    enable_metrics_collection: bool,
) -> Option<AgentManager> {
    let mut command_line = parse_command_line(dashboard_agent_command);
    if command_line.is_empty() {
        tracing::warn!("Dashboard agent command is empty, skipping agent launch");
        return None;
    }

    // C++ equivalent: node_manager.cc line 3315-3318.
    if !enable_metrics_collection {
        command_line.push("--disable-metrics-collection".to_string());
    }

    Some(AgentManager::new(AgentManagerOptions {
        node_id: node_id.to_string(),
        agent_name: "dashboard_agent".to_string(),
        command_line,
        fate_shares: true,
        respawn_on_exit: true,
    }))
}

/// Create a runtime env agent manager.
///
/// C++ equivalent: `NodeManager::CreateRuntimeEnvAgentManager()`.
/// Returns None if the command is empty.
pub fn create_runtime_env_agent_manager(
    node_id: &str,
    runtime_env_agent_command: &str,
) -> Option<AgentManager> {
    let command_line = parse_command_line(runtime_env_agent_command);
    if command_line.is_empty() {
        tracing::warn!("Runtime env agent command is empty, skipping agent launch");
        return None;
    }

    Some(AgentManager::new(AgentManagerOptions {
        node_id: node_id.to_string(),
        agent_name: "runtime_env_agent".to_string(),
        command_line,
        fate_shares: true,
        respawn_on_exit: true,
    }))
}

/// Wait for a port file to appear in the session directory.
///
/// C++ equivalent: `WaitForPersistedPort()` in `node_manager.cc`.
/// C++ contract: `GetPortFileName(node_id, port_name)` produces
/// `{session_dir}/{port_name}_{node_id_hex}` (no `ports/` subdirectory).
pub async fn wait_for_persisted_port(
    session_dir: &str,
    node_id: &str,
    port_name: &str,
    timeout: std::time::Duration,
) -> Option<u16> {
    let filename = format!("{}_{}", port_name, node_id);
    let port_file = std::path::Path::new(session_dir).join(&filename);

    let deadline = tokio::time::Instant::now() + timeout;
    let poll_interval = std::time::Duration::from_millis(50);

    tracing::debug!(
        port_file = %port_file.display(),
        "Waiting for persisted port"
    );

    while tokio::time::Instant::now() < deadline {
        if let Ok(contents) = tokio::fs::read_to_string(&port_file).await {
            if let Ok(port) = contents.trim().parse::<u16>() {
                tracing::info!(
                    port_name,
                    port,
                    "Read persisted port"
                );
                return Some(port);
            }
        }
        tokio::time::sleep(poll_interval).await;
    }

    tracing::warn!(
        port_name,
        port_file = %port_file.display(),
        "Timed out waiting for persisted port"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_line_simple() {
        let args = parse_command_line("python -m ray.dashboard.agent");
        assert_eq!(args, vec!["python", "-m", "ray.dashboard.agent"]);
    }

    #[test]
    fn test_parse_command_line_with_quotes() {
        let args = parse_command_line(r#"python -c "import ray" --flag"#);
        assert_eq!(args, vec!["python", "-c", "import ray", "--flag"]);
    }

    #[test]
    fn test_parse_command_line_empty() {
        let args = parse_command_line("");
        assert!(args.is_empty());
    }

    #[test]
    fn test_parse_command_line_backslash_escape() {
        let args = parse_command_line(r"/path/to/my\ python -m ray.worker");
        assert_eq!(args, vec!["/path/to/my python", "-m", "ray.worker"]);
    }

    #[test]
    fn test_parse_command_line_backslash_in_double_quotes() {
        let args = parse_command_line(r#""/path/to/my python" -m ray.worker"#);
        assert_eq!(args, vec!["/path/to/my python", "-m", "ray.worker"]);
    }

    #[test]
    fn test_parse_command_line_single_quotes_no_escape() {
        // Single quotes: backslash is literal, not an escape
        let args = parse_command_line(r"'hello\world' foo");
        assert_eq!(args, vec![r"hello\world", "foo"]);
    }

    #[test]
    fn test_parse_command_line_backslash_escaping_space() {
        // Backslash escaping: `hello\ world` -> ["hello world"]
        let args = parse_command_line(r"hello\ world");
        assert_eq!(args, vec!["hello world"]);
    }

    #[test]
    fn test_parse_command_line_mixed_quotes() {
        // Mixed quotes: `"hello 'world'"` -> ["hello 'world'"]
        let args = parse_command_line(r#""hello 'world'""#);
        assert_eq!(args, vec!["hello 'world'"]);
    }

    #[test]
    fn test_parse_command_line_backslash_in_double_quotes_escapes() {
        // Backslash in double quotes: `"hello \"world\""` -> [`hello "world"`]
        let args = parse_command_line(r#""hello \"world\"""#);
        assert_eq!(args, vec![r#"hello "world""#]);
    }

    #[test]
    fn test_parse_command_line_with_placeholder() {
        let args = parse_command_line(
            "python -m ray.dashboard.agent --port RAY_NODE_MANAGER_PORT_PLACEHOLDER",
        );
        assert_eq!(args.len(), 5);
        assert_eq!(args[4], "RAY_NODE_MANAGER_PORT_PLACEHOLDER");
    }

    #[test]
    fn test_create_dashboard_agent_manager_empty() {
        let mgr = create_dashboard_agent_manager("node1", "", true);
        assert!(mgr.is_none());
    }

    #[test]
    fn test_create_dashboard_agent_manager_with_command() {
        let mgr = create_dashboard_agent_manager(
            "node1",
            "python -m ray.dashboard.agent",
            true,
        );
        assert!(mgr.is_some());
        assert_eq!(mgr.unwrap().agent_name(), "dashboard_agent");
    }

    #[test]
    fn test_create_dashboard_agent_manager_metrics_disabled() {
        let mgr = create_dashboard_agent_manager(
            "node1",
            "python -m ray.dashboard.agent",
            false,
        );
        let mgr = mgr.unwrap();
        // The command line should include --disable-metrics-collection
        assert_eq!(mgr.options.command_line.len(), 4);
        assert_eq!(
            mgr.options.command_line[3],
            "--disable-metrics-collection"
        );
    }

    #[test]
    fn test_create_runtime_env_agent_manager_empty() {
        let mgr = create_runtime_env_agent_manager("node1", "");
        assert!(mgr.is_none());
    }

    #[test]
    fn test_create_runtime_env_agent_manager_with_command() {
        let mgr = create_runtime_env_agent_manager(
            "node1",
            "python -m ray._private.runtime_env.agent",
        );
        assert!(mgr.is_some());
        assert_eq!(mgr.unwrap().agent_name(), "runtime_env_agent");
    }

    #[test]
    fn test_agent_manager_start_nonexistent_binary() {
        let mgr = AgentManager::new(AgentManagerOptions {
            node_id: "test".to_string(),
            agent_name: "test_agent".to_string(),
            command_line: vec!["__nonexistent_binary__".to_string()],
            fate_shares: false,
            respawn_on_exit: false,
        });
        let result = mgr.start(12345);
        assert!(result.is_err());
    }

    #[test]
    fn test_agent_manager_pid_initially_zero() {
        let mgr = AgentManager::new(AgentManagerOptions {
            node_id: "test".to_string(),
            agent_name: "test_agent".to_string(),
            command_line: vec!["echo".to_string(), "hello".to_string()],
            fate_shares: false,
            respawn_on_exit: false,
        });
        assert_eq!(mgr.get_pid(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_persisted_port() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = "abc123def456";
        // C++ contract: file is {port_name}_{node_id_hex} in session_dir root.
        std::fs::write(
            dir.path().join(format!("metrics_agent_port_{}", node_id)),
            "8080",
        )
        .unwrap();

        let port = wait_for_persisted_port(
            dir.path().to_str().unwrap(),
            node_id,
            "metrics_agent_port",
            std::time::Duration::from_secs(1),
        )
        .await;
        assert_eq!(port, Some(8080));
    }

    #[tokio::test]
    async fn test_wait_for_persisted_port_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let port = wait_for_persisted_port(
            dir.path().to_str().unwrap(),
            "abc123def456",
            "nonexistent_port",
            std::time::Duration::from_millis(100),
        )
        .await;
        assert!(port.is_none());
    }

    /// Verify C++ port-file naming contract is respected.
    ///
    /// C++ contract: `GetPortFileName(node_id, port_name)` produces
    /// `"{port_name}_{node_id_hex}"`.  The file lives at
    /// `{session_dir}/{port_name}_{node_id_hex}` (no `ports/` subdirectory).
    #[tokio::test]
    async fn test_cpp_port_file_naming_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = "deadbeef01234567";

        // C++ port names all end with `_port`.
        let port_names = [
            ("metrics_agent_port", 9090u16),
            ("metrics_export_port", 9091),
            ("dashboard_agent_listen_port", 9092),
            ("runtime_env_agent_port", 9093),
        ];

        for (name, port) in &port_names {
            // C++ contract: GetPortFileName(node_id, port_name) = "{port_name}_{node_id_hex}"
            // File lives at: {session_dir}/{port_name}_{node_id_hex}
            let filename = format!("{}_{}", name, node_id);
            std::fs::write(dir.path().join(&filename), port.to_string()).unwrap();
        }

        for (name, expected_port) in &port_names {
            let port = wait_for_persisted_port(
                dir.path().to_str().unwrap(),
                node_id,
                name,
                std::time::Duration::from_secs(1),
            )
            .await;
            assert_eq!(
                port,
                Some(*expected_port),
                "Port file {} should resolve to {}",
                name,
                expected_port,
            );
        }
    }
}
