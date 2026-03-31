// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Runtime env agent client -- raylet-side client for the runtime env agent.
//!
//! Replaces `src/ray/raylet/runtime_env_agent_client.h/cc`.
//!
//! In C++, the raylet creates a `RuntimeEnvAgentClient` that:
//! 1. Connects to the runtime env agent HTTP server at `runtime_env_agent_port`
//! 2. Is installed into the `WorkerPool` via `SetRuntimeEnvAgentClient`
//! 3. Provides `GetOrCreateRuntimeEnv` and `DeleteRuntimeEnvIfPossible` RPCs
//!
//! The runtime env agent is a Python HTTP server that manages runtime
//! environments (pip packages, conda envs, working dirs, etc.) for workers.
//!
//! Wire format: serialized protobuf with `application/octet-stream` content type,
//! matching the C++ implementation exactly.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message;
use ray_proto::ray::rpc as proto;

/// Callback for runtime env creation result.
///
/// C++ equivalent: `GetOrCreateRuntimeEnvCallback`.
/// Arguments: (success, serialized_runtime_env_context, error_message)
pub type GetOrCreateRuntimeEnvCallback =
    Box<dyn FnOnce(bool, String, String) + Send + 'static>;

/// Callback for runtime env deletion result.
///
/// C++ equivalent: `DeleteRuntimeEnvIfPossibleCallback`.
pub type DeleteRuntimeEnvIfPossibleCallback =
    Box<dyn FnOnce(bool) + Send + 'static>;

/// Trait for the runtime env agent client.
///
/// C++ equivalent: `RuntimeEnvAgentClient` abstract class.
pub trait RuntimeEnvAgentClientTrait: Send + Sync {
    /// Request the agent to create or get a runtime environment.
    ///
    /// C++ equivalent: `GetOrCreateRuntimeEnv`.
    fn get_or_create_runtime_env(
        &self,
        job_id: &ray_common::id::JobID,
        serialized_runtime_env: &str,
        serialized_runtime_env_config: &str,
        callback: GetOrCreateRuntimeEnvCallback,
    );

    /// Request the agent to delete a runtime environment if possible.
    ///
    /// C++ equivalent: `DeleteRuntimeEnvIfPossible`.
    fn delete_runtime_env_if_possible(
        &self,
        serialized_runtime_env: &str,
        callback: DeleteRuntimeEnvIfPossibleCallback,
    );
}

/// HTTP-based runtime env agent client.
///
/// C++ equivalent: `RuntimeEnvAgentClientImpl` (uses HTTP, not gRPC).
/// The runtime env agent runs an HTTP server with endpoints:
///   POST /get_or_create_runtime_env
///   POST /delete_runtime_env_if_possible
///
/// Sends serialized protobuf with `application/octet-stream` content type,
/// matching the C++ wire format.
pub struct RuntimeEnvAgentClient {
    address: String,
    port: u16,
    /// Retry interval for transient errors.
    retry_interval: Duration,
    /// Timeout for agent registration.
    register_timeout: Duration,
    /// Optional auth token for `Authorization: Bearer <token>` header.
    auth_token: Option<String>,
    /// Fatal shutdown callback invoked when the agent deadline is exceeded.
    /// C++ equivalent: calls `raylet_client->ShutdownRaylet()`.
    shutdown_raylet: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Configuration for the runtime env agent client.
pub struct RuntimeEnvAgentClientConfig {
    /// Agent register timeout in ms (C++ default: 30000).
    pub agent_register_timeout_ms: u32,
    /// Retry interval in ms (C++ default: 1000).
    pub agent_manager_retry_interval_ms: u32,
    /// Optional auth token. When set, adds `Authorization: Bearer {token}` header.
    pub auth_token: Option<String>,
    /// Fatal shutdown callback invoked when agent deadline is exceeded.
    /// C++ equivalent: the raylet exits immediately on timeout.
    pub shutdown_raylet: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Default for RuntimeEnvAgentClientConfig {
    fn default() -> Self {
        Self {
            agent_register_timeout_ms: 30_000,
            agent_manager_retry_interval_ms: 1_000,
            auth_token: None,
            shutdown_raylet: None,
        }
    }
}

impl RuntimeEnvAgentClient {
    /// Create a new runtime env agent client.
    ///
    /// C++ equivalent: `RuntimeEnvAgentClient::Create()`.
    pub fn new(address: &str, port: u16, config: RuntimeEnvAgentClientConfig) -> Self {
        tracing::info!(address, port, "Creating RuntimeEnvAgentClient");
        Self {
            address: address.to_string(),
            port,
            retry_interval: Duration::from_millis(
                config.agent_manager_retry_interval_ms as u64,
            ),
            register_timeout: Duration::from_millis(
                config.agent_register_timeout_ms as u64,
            ),
            auth_token: config.auth_token,
            shutdown_raylet: config.shutdown_raylet,
        }
    }

    /// The port this client is configured for.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Send an HTTP POST request with binary body and return the response body bytes.
    /// Uses raw TCP to avoid external HTTP dependencies.
    /// Content-Type is `application/octet-stream` to match the C++ contract.
    fn http_post_bytes(&self, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
        let addr = format!("{}:{}", self.address, self.port);
        let mut stream = std::net::TcpStream::connect_timeout(
            &addr.parse().map_err(|e| format!("Bad address: {}", e))?,
            self.register_timeout,
        )
        .map_err(|e| format!("Connection failed: {}", e))?;

        stream
            .set_read_timeout(Some(self.register_timeout))
            .ok();
        stream
            .set_write_timeout(Some(self.register_timeout))
            .ok();

        // Build HTTP headers.
        let mut headers = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n",
            path, addr, body.len()
        );

        // Add auth header if configured (C++ parity: AuthenticationTokenLoader).
        if let Some(ref token) = self.auth_token {
            headers.push_str(&format!("Authorization: Bearer {}\r\n", token));
        }

        headers.push_str("\r\n");

        // Write headers then body.
        stream
            .write_all(headers.as_bytes())
            .map_err(|e| format!("Write headers failed: {}", e))?;
        stream
            .write_all(body)
            .map_err(|e| format!("Write body failed: {}", e))?;

        // Read the full response.
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .map_err(|e| format!("Read failed: {}", e))?;

        // Parse HTTP response: find the body after \r\n\r\n.
        let header_end = find_header_end(&response)
            .ok_or_else(|| "Invalid HTTP response".to_string())?;

        let status_line = {
            let header_bytes = &response[..header_end];
            let header_str = String::from_utf8_lossy(header_bytes);
            header_str.lines().next().unwrap_or("").to_string()
        };

        let body_start = header_end + 4; // skip \r\n\r\n
        if status_line.contains("200") {
            Ok(response[body_start..].to_vec())
        } else if status_line.contains("404") {
            Err("Agent returned 404 (not ready)".to_string())
        } else {
            Err(format!("Agent returned: {}", status_line))
        }
    }
}

/// Find the position of \r\n\r\n in raw bytes.
fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4)
        .position(|w| w == b"\r\n\r\n")
}

/// Parse `serialized_runtime_env_config` JSON string into a `RuntimeEnvConfig` proto.
/// Falls back to default if the string is empty or unparseable.
fn parse_runtime_env_config(serialized: &str) -> Option<proto::RuntimeEnvConfig> {
    if serialized.is_empty() || serialized == "{}" {
        return None;
    }
    serde_json::from_str::<proto::RuntimeEnvConfig>(serialized).ok()
}

/// Returns `true` if the error string indicates a network-level / transient error
/// (connection refused, timeout, 404-not-ready). Application errors (HTTP response
/// received with a non-retryable status) return `false`.
fn is_network_error(err: &str) -> bool {
    err.contains("Connection failed")
        || err.contains("Connection refused")
        || err.contains("timed out")
        || err.contains("Read failed")
        || err.contains("Write")
        || err.contains("404")
}

impl RuntimeEnvAgentClientTrait for RuntimeEnvAgentClient {
    fn get_or_create_runtime_env(
        &self,
        job_id: &ray_common::id::JobID,
        serialized_runtime_env: &str,
        serialized_runtime_env_config: &str,
        callback: GetOrCreateRuntimeEnvCallback,
    ) {
        // Build protobuf request matching C++ wire format.
        let request = proto::GetOrCreateRuntimeEnvRequest {
            serialized_runtime_env: serialized_runtime_env.to_string(),
            runtime_env_config: parse_runtime_env_config(serialized_runtime_env_config),
            // C++ does: request.set_job_id(job_id.Hex()) -- sets bytes field to hex string.
            job_id: job_id.hex().into_bytes(),
            source_process: "raylet".to_string(),
        };
        let payload = request.encode_to_vec();

        let retry_interval = self.retry_interval;
        let register_timeout = self.register_timeout;

        // Clone fields for the spawned task.
        let address = self.address.clone();
        let port = self.port;
        let auth_token = self.auth_token.clone();
        let shutdown_raylet = self.shutdown_raylet.clone();

        // Use spawn_blocking because http_post_bytes does blocking TCP I/O.
        tokio::task::spawn_blocking(move || {
            let client = RuntimeEnvAgentClient {
                address,
                port,
                retry_interval,
                register_timeout,
                auth_token,
                shutdown_raylet: None, // not needed on the inner clone
            };

            let deadline = Instant::now() + register_timeout;

            loop {
                match client.http_post_bytes("/get_or_create_runtime_env", &payload) {
                    Ok(resp_bytes) => {
                        match proto::GetOrCreateRuntimeEnvReply::decode(
                            resp_bytes.as_slice(),
                        ) {
                            Ok(reply) => {
                                let success = reply.status
                                    == proto::AgentRpcStatus::Ok as i32;
                                callback(
                                    success,
                                    reply.serialized_runtime_env_context,
                                    reply.error_message,
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "Failed to decode GetOrCreateRuntimeEnvReply"
                                );
                                callback(
                                    false,
                                    String::new(),
                                    format!("Protobuf decode error: {}", e),
                                );
                            }
                        }
                        return;
                    }
                    Err(e) if is_network_error(&e) => {
                        // Network / transient error -- retry until deadline.
                        if Instant::now() > deadline {
                            tracing::error!(
                                timeout_ms = register_timeout.as_millis() as u64,
                                address = %client.address,
                                port = client.port,
                                "The raylet exited immediately because the runtime env \
                                 agent timed out. This can happen because the runtime env \
                                 agent was never started, or is listening to the wrong port."
                            );
                            if let Some(ref shutdown) = shutdown_raylet {
                                shutdown();
                            }
                            callback(
                                false,
                                String::new(),
                                "Runtime env agent timed out".to_string(),
                            );
                            return;
                        }
                        std::thread::sleep(retry_interval);
                    }
                    Err(e) => {
                        // Application-level error (non-retryable) -- fail immediately.
                        tracing::error!(
                            error = %e,
                            "Non-retryable error from runtime env agent"
                        );
                        callback(false, String::new(), e);
                        return;
                    }
                }
            }
        });
    }

    fn delete_runtime_env_if_possible(
        &self,
        serialized_runtime_env: &str,
        callback: DeleteRuntimeEnvIfPossibleCallback,
    ) {
        // Build protobuf request matching C++ wire format.
        let request = proto::DeleteRuntimeEnvIfPossibleRequest {
            serialized_runtime_env: serialized_runtime_env.to_string(),
            source_process: "raylet".to_string(),
        };
        let payload = request.encode_to_vec();

        let address = self.address.clone();
        let port = self.port;
        let retry_interval = self.retry_interval;
        let register_timeout = self.register_timeout;
        let auth_token = self.auth_token.clone();
        let shutdown_raylet = self.shutdown_raylet.clone();

        // Use spawn_blocking because http_post_bytes does blocking TCP I/O.
        tokio::task::spawn_blocking(move || {
            let client = RuntimeEnvAgentClient {
                address,
                port,
                retry_interval,
                register_timeout,
                auth_token,
                shutdown_raylet: None,
            };

            let deadline = Instant::now() + register_timeout;

            loop {
                match client.http_post_bytes("/delete_runtime_env_if_possible", &payload)
                {
                    Ok(resp_bytes) => {
                        match proto::DeleteRuntimeEnvIfPossibleReply::decode(
                            resp_bytes.as_slice(),
                        ) {
                            Ok(reply) => {
                                let success = reply.status
                                    == proto::AgentRpcStatus::Ok as i32;
                                callback(success);
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "Failed to decode DeleteRuntimeEnvIfPossibleReply"
                                );
                                callback(false);
                            }
                        }
                        return;
                    }
                    Err(e) if is_network_error(&e) => {
                        if Instant::now() > deadline {
                            tracing::error!(
                                timeout_ms = register_timeout.as_millis() as u64,
                                address = %client.address,
                                port = client.port,
                                "The raylet exited immediately because the runtime env \
                                 agent timed out. This can happen because the runtime env \
                                 agent was never started, or is listening to the wrong port."
                            );
                            if let Some(ref shutdown) = shutdown_raylet {
                                shutdown();
                            }
                            callback(false);
                            return;
                        }
                        std::thread::sleep(retry_interval);
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "Non-retryable error from runtime env agent (delete)"
                        );
                        callback(false);
                        return;
                    }
                }
            }
        });
    }
}

/// No-op implementation for testing or when agent is not available.
pub struct NoopRuntimeEnvAgentClient;

impl RuntimeEnvAgentClientTrait for NoopRuntimeEnvAgentClient {
    fn get_or_create_runtime_env(
        &self,
        _job_id: &ray_common::id::JobID,
        _serialized_runtime_env: &str,
        _serialized_runtime_env_config: &str,
        callback: GetOrCreateRuntimeEnvCallback,
    ) {
        callback(true, String::new(), String::new());
    }

    fn delete_runtime_env_if_possible(
        &self,
        _serialized_runtime_env: &str,
        callback: DeleteRuntimeEnvIfPossibleCallback,
    ) {
        callback(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_client_get_or_create() {
        let client = NoopRuntimeEnvAgentClient;
        let (tx, rx) = std::sync::mpsc::channel();
        client.get_or_create_runtime_env(
            &ray_common::id::JobID::from_int(1),
            "{}",
            "{}",
            Box::new(move |success, context, error| {
                tx.send((success, context, error)).unwrap();
            }),
        );
        let (success, _context, _error) = rx.recv().unwrap();
        assert!(success);
    }

    #[test]
    fn test_noop_client_delete() {
        let client = NoopRuntimeEnvAgentClient;
        let (tx, rx) = std::sync::mpsc::channel();
        client.delete_runtime_env_if_possible(
            "{}",
            Box::new(move |success| {
                tx.send(success).unwrap();
            }),
        );
        assert!(rx.recv().unwrap());
    }

    #[test]
    fn test_client_creation() {
        let client = RuntimeEnvAgentClient::new(
            "127.0.0.1",
            8080,
            RuntimeEnvAgentClientConfig::default(),
        );
        assert_eq!(client.port(), 8080);
        assert!(client.auth_token.is_none());
    }

    #[test]
    fn test_client_creation_with_auth_token() {
        let client = RuntimeEnvAgentClient::new(
            "127.0.0.1",
            8080,
            RuntimeEnvAgentClientConfig {
                auth_token: Some("test-token-123".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(client.port(), 8080);
        assert_eq!(client.auth_token.as_deref(), Some("test-token-123"));
    }

    #[test]
    fn test_get_or_create_request_serialization() {
        // Verify the protobuf request is built correctly.
        let job_id = ray_common::id::JobID::from_int(42);
        let request = proto::GetOrCreateRuntimeEnvRequest {
            serialized_runtime_env: r#"{"pip": ["numpy"]}"#.to_string(),
            runtime_env_config: None,
            job_id: job_id.hex().into_bytes(),
            source_process: "raylet".to_string(),
        };
        let payload = request.encode_to_vec();
        assert!(!payload.is_empty());

        // Round-trip: decode should match.
        let decoded =
            proto::GetOrCreateRuntimeEnvRequest::decode(payload.as_slice()).unwrap();
        assert_eq!(decoded.serialized_runtime_env, r#"{"pip": ["numpy"]}"#);
        assert_eq!(decoded.source_process, "raylet");
        assert_eq!(
            String::from_utf8(decoded.job_id).unwrap(),
            job_id.hex()
        );
    }

    #[test]
    fn test_delete_request_serialization() {
        let request = proto::DeleteRuntimeEnvIfPossibleRequest {
            serialized_runtime_env: r#"{"pip": ["numpy"]}"#.to_string(),
            source_process: "raylet".to_string(),
        };
        let payload = request.encode_to_vec();
        assert!(!payload.is_empty());

        let decoded =
            proto::DeleteRuntimeEnvIfPossibleRequest::decode(payload.as_slice()).unwrap();
        assert_eq!(decoded.serialized_runtime_env, r#"{"pip": ["numpy"]}"#);
        assert_eq!(decoded.source_process, "raylet");
    }

    #[test]
    fn test_reply_deserialization() {
        // Simulate a successful reply from the agent.
        let reply = proto::GetOrCreateRuntimeEnvReply {
            status: proto::AgentRpcStatus::Ok as i32,
            error_message: String::new(),
            serialized_runtime_env_context: r#"{"env_vars": {}}"#.to_string(),
        };
        let encoded = reply.encode_to_vec();
        let decoded =
            proto::GetOrCreateRuntimeEnvReply::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.status, proto::AgentRpcStatus::Ok as i32);
        assert_eq!(
            decoded.serialized_runtime_env_context,
            r#"{"env_vars": {}}"#
        );

        // Simulate a failed reply.
        let fail_reply = proto::GetOrCreateRuntimeEnvReply {
            status: proto::AgentRpcStatus::Failed as i32,
            error_message: "pip install failed".to_string(),
            serialized_runtime_env_context: String::new(),
        };
        let encoded = fail_reply.encode_to_vec();
        let decoded =
            proto::GetOrCreateRuntimeEnvReply::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.status, proto::AgentRpcStatus::Failed as i32);
        assert_eq!(decoded.error_message, "pip install failed");
    }

    #[test]
    fn test_delete_reply_deserialization() {
        let reply = proto::DeleteRuntimeEnvIfPossibleReply {
            status: proto::AgentRpcStatus::Ok as i32,
            error_message: String::new(),
        };
        let encoded = reply.encode_to_vec();
        let decoded =
            proto::DeleteRuntimeEnvIfPossibleReply::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.status, proto::AgentRpcStatus::Ok as i32);
    }

    #[test]
    fn test_parse_runtime_env_config_empty() {
        assert!(parse_runtime_env_config("").is_none());
        assert!(parse_runtime_env_config("{}").is_none());
    }

    #[test]
    fn test_parse_runtime_env_config_valid() {
        let config = parse_runtime_env_config(
            r#"{"setup_timeout_seconds": 600, "eager_install": true, "log_files": []}"#,
        );
        assert!(config.is_some());
        let c = config.unwrap();
        assert_eq!(c.setup_timeout_seconds, 600);
        assert!(c.eager_install);
    }

    #[test]
    fn test_find_header_end() {
        let data = b"HTTP/1.1 200 OK\r\n\r\nbody";
        let pos = find_header_end(data);
        assert!(pos.is_some());
        // \r\n\r\n starts at byte 15 (0-indexed: "HTTP/1.1 200 OK" = 15 chars).
        assert_eq!(pos.unwrap(), 15);
        assert_eq!(find_header_end(b"no separator here"), None);
    }

    #[test]
    fn test_auth_token_passthrough() {
        let token = "my-secret-token-xyz".to_string();
        let config = RuntimeEnvAgentClientConfig {
            auth_token: Some(token.clone()),
            ..Default::default()
        };
        assert_eq!(config.auth_token.as_deref(), Some("my-secret-token-xyz"));

        let client = RuntimeEnvAgentClient::new("127.0.0.1", 9999, config);
        assert_eq!(client.auth_token.as_deref(), Some("my-secret-token-xyz"));
        assert_eq!(client.port(), 9999);
    }

    #[test]
    fn test_shutdown_callback_passthrough() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();
        let config = RuntimeEnvAgentClientConfig {
            shutdown_raylet: Some(Arc::new(move || {
                called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            })),
            ..Default::default()
        };
        assert!(config.shutdown_raylet.is_some());

        let client = RuntimeEnvAgentClient::new("127.0.0.1", 9999, config);
        assert!(client.shutdown_raylet.is_some());

        // Invoke the callback directly.
        (client.shutdown_raylet.as_ref().unwrap())();
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_is_network_error_classification() {
        assert!(is_network_error("Connection failed: refused"));
        assert!(is_network_error("Connection refused"));
        assert!(is_network_error("operation timed out"));
        assert!(is_network_error("Agent returned 404 (not ready)"));
        assert!(is_network_error("Read failed: broken pipe"));
        assert!(!is_network_error("Agent returned: HTTP/1.1 500 Internal Server Error"));
    }

    #[tokio::test]
    async fn test_deadline_exceeded_calls_failure_callback() {
        // Bind a TCP listener then drop it immediately so the port is known-unused
        // but will refuse connections quickly (RST), unlike port 1 which may
        // cause slow OS-level SYN retries on macOS.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let (tx, rx) = std::sync::mpsc::channel();
        let shutdown_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_flag = shutdown_called.clone();

        let client = RuntimeEnvAgentClient::new(
            "127.0.0.1",
            port,
            RuntimeEnvAgentClientConfig {
                agent_register_timeout_ms: 200,
                agent_manager_retry_interval_ms: 20,
                auth_token: None,
                shutdown_raylet: Some(Arc::new(move || {
                    shutdown_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                })),
            },
        );

        client.get_or_create_runtime_env(
            &ray_common::id::JobID::from_int(1),
            "{}",
            "{}",
            Box::new(move |success, _context, error| {
                tx.send((success, error)).unwrap();
            }),
        );

        let (success, error) = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert!(!success, "Expected failure callback");
        assert!(
            error.contains("timed out"),
            "Expected timeout error, got: {}",
            error
        );
        assert!(
            shutdown_called.load(std::sync::atomic::Ordering::SeqCst),
            "Expected shutdown callback to be invoked"
        );
    }

    /// Verify that the shutdown callback can signal a oneshot channel,
    /// matching the C++ graceful-then-force shutdown pattern used in
    /// `NodeManager::run()`.
    #[tokio::test]
    async fn test_shutdown_callback_signals_oneshot_channel() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_tx = Arc::new(std::sync::Mutex::new(Some(shutdown_tx)));
        let shutdown_tx_clone = Arc::clone(&shutdown_tx);

        // Build the same kind of callback that node_manager.rs installs.
        let shutdown_cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(tx) = shutdown_tx_clone.lock().unwrap().take() {
                let _ = tx.send(());
            }
            // In production this also spawns a 10s forced-exit thread;
            // we skip that in the test.
        });

        // Invoke the callback (simulates deadline exceeded).
        shutdown_cb();

        // The receiver should complete immediately.
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            shutdown_rx,
        )
        .await;
        assert!(
            result.is_ok(),
            "shutdown_rx should have received signal"
        );
        assert!(
            result.unwrap().is_ok(),
            "oneshot channel should carry Ok(())"
        );
    }

    /// Verify that invoking the shutdown callback a second time is safe
    /// (the oneshot sender is already consumed).
    #[tokio::test]
    async fn test_shutdown_callback_idempotent() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_tx = Arc::new(std::sync::Mutex::new(Some(shutdown_tx)));
        let shutdown_tx_clone = Arc::clone(&shutdown_tx);

        let shutdown_cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(tx) = shutdown_tx_clone.lock().unwrap().take() {
                let _ = tx.send(());
            }
        });

        // First call sends the signal.
        shutdown_cb();
        // Second call should not panic.
        shutdown_cb();

        let result = shutdown_rx.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_deadline_exceeded() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let (tx, rx) = std::sync::mpsc::channel();
        let shutdown_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_flag = shutdown_called.clone();

        let client = RuntimeEnvAgentClient::new(
            "127.0.0.1",
            port,
            RuntimeEnvAgentClientConfig {
                agent_register_timeout_ms: 200,
                agent_manager_retry_interval_ms: 20,
                auth_token: None,
                shutdown_raylet: Some(Arc::new(move || {
                    shutdown_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                })),
            },
        );

        client.delete_runtime_env_if_possible(
            "{}",
            Box::new(move |success| {
                tx.send(success).unwrap();
            }),
        );

        let success = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert!(!success, "Expected failure callback for delete");
        assert!(
            shutdown_called.load(std::sync::atomic::Ordering::SeqCst),
            "Expected shutdown callback to be invoked for delete"
        );
    }
}
