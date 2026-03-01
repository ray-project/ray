// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC client framework wrapping tonic channels.
//!
//! Replaces `src/ray/rpc/grpc_client.h` and `retryable_grpc_client.h/cc`.

use std::future::Future;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::{Code, Status};

/// Connection state of the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectionState {
    Connected = 0,
    Disconnected = 1,
    Reconnecting = 2,
}

impl ConnectionState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Connected,
            1 => Self::Disconnected,
            2 => Self::Reconnecting,
            _ => Self::Disconnected,
        }
    }
}

/// Configuration for retry behavior on gRPC calls.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial retry delay.
    pub initial_delay: Duration,
    /// Maximum retry delay.
    pub max_delay: Duration,
    /// Backoff multiplier.
    pub multiplier: f64,
    /// How long to wait before declaring the server unavailable.
    pub server_unavailable_timeout: Duration,
    /// Maximum bytes of pending (in-flight) requests before rejecting new ones.
    pub max_pending_bytes: usize,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            server_unavailable_timeout: Duration::from_secs(60),
            max_pending_bytes: 100 * 1024 * 1024, // 100MB
        }
    }
}

/// A gRPC client wrapper with retry support.
///
/// In C++, this is the `RetryableGrpcClient` template class.
/// In Rust, we wrap a tonic `Channel` with retry logic. Use
/// [`call_with_retry`](Self::call_with_retry) to execute an RPC with automatic
/// retries on transient failures.
#[derive(Clone)]
pub struct RetryableGrpcClient {
    channel: Channel,
    retry_config: RetryConfig,
    connection_state: Arc<AtomicU8>,
    pending_bytes: Arc<AtomicUsize>,
}

impl RetryableGrpcClient {
    pub fn new(channel: Channel, retry_config: RetryConfig) -> Self {
        Self {
            channel,
            retry_config,
            connection_state: Arc::new(AtomicU8::new(ConnectionState::Connected as u8)),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the underlying channel for making RPC calls.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Get a cloned channel suitable for creating tonic service stubs.
    pub fn channel_cloned(&self) -> Channel {
        self.channel.clone()
    }

    /// Get the retry configuration.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Current connection state.
    pub fn connection_state(&self) -> ConnectionState {
        ConnectionState::from_u8(self.connection_state.load(Ordering::Relaxed))
    }

    /// Current pending request bytes.
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    /// Whether the client believes the server is reachable.
    pub fn is_connected(&self) -> bool {
        self.connection_state() == ConnectionState::Connected
    }

    /// Connect to a gRPC endpoint and create a channel.
    pub async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
        Channel::from_shared(addr.to_string())
            .expect("invalid URI")
            .connect()
            .await
    }

    /// Create a lazily-connecting channel (does not connect until first RPC).
    pub fn connect_lazy(addr: &str) -> Channel {
        Channel::from_shared(addr.to_string())
            .expect("invalid URI")
            .connect_lazy()
    }

    /// Execute an RPC call with automatic retry on transient failures.
    ///
    /// `request_size` is used for pending-bytes throttling. Pass 0 if throttling
    /// is not needed. `timeout` overrides the default server_unavailable_timeout.
    /// `rpc_fn` is a closure that performs the actual RPC call — it will be called
    /// multiple times on transient failure.
    pub async fn call_with_retry<F, Fut, T>(
        &self,
        request_size: usize,
        timeout: Option<Duration>,
        rpc_fn: F,
    ) -> Result<T, Status>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        // Check pending bytes limit
        let prev = self.pending_bytes.fetch_add(request_size, Ordering::Relaxed);
        if request_size > 0 && prev + request_size > self.retry_config.max_pending_bytes {
            self.pending_bytes
                .fetch_sub(request_size, Ordering::Relaxed);
            return Err(Status::resource_exhausted(
                "max pending request bytes exceeded",
            ));
        }

        let result = self.retry_loop(timeout, &rpc_fn).await;

        self.pending_bytes
            .fetch_sub(request_size, Ordering::Relaxed);
        result
    }

    async fn retry_loop<F, Fut, T>(
        &self,
        timeout: Option<Duration>,
        rpc_fn: &F,
    ) -> Result<T, Status>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let timeout_duration = timeout.unwrap_or(self.retry_config.server_unavailable_timeout);
        let deadline = tokio::time::Instant::now() + timeout_duration;
        let mut delay = self.retry_config.initial_delay;
        let mut attempts = 0u32;

        loop {
            let result = rpc_fn().await;

            match &result {
                Ok(_) => {
                    self.connection_state
                        .store(ConnectionState::Connected as u8, Ordering::Relaxed);
                    return result;
                }
                Err(status) => {
                    if !is_transient(status.code()) {
                        return result;
                    }

                    attempts += 1;
                    if attempts > self.retry_config.max_retries {
                        self.connection_state
                            .store(ConnectionState::Disconnected as u8, Ordering::Relaxed);
                        return result;
                    }

                    if tokio::time::Instant::now() + delay > deadline {
                        self.connection_state
                            .store(ConnectionState::Disconnected as u8, Ordering::Relaxed);
                        return Err(Status::deadline_exceeded(format!(
                            "server unavailable after {timeout_duration:?}"
                        )));
                    }

                    self.connection_state
                        .store(ConnectionState::Reconnecting as u8, Ordering::Relaxed);

                    tracing::debug!(
                        attempts,
                        code = ?status.code(),
                        delay_ms = delay.as_millis() as u64,
                        "retrying RPC"
                    );

                    tokio::time::sleep(delay).await;

                    // Exponential backoff capped at max_delay
                    delay = std::cmp::min(
                        Duration::from_secs_f64(
                            delay.as_secs_f64() * self.retry_config.multiplier,
                        ),
                        self.retry_config.max_delay,
                    );
                }
            }
        }
    }
}

/// Whether a gRPC status code represents a transient error worth retrying.
pub fn is_transient(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    fn make_client(config: RetryConfig) -> RetryableGrpcClient {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        RetryableGrpcClient::new(channel, config)
    }

    fn fast_retry_config() -> RetryConfig {
        RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            multiplier: 2.0,
            server_unavailable_timeout: Duration::from_secs(5),
            max_pending_bytes: 1024,
        }
    }

    #[tokio::test]
    async fn test_successful_call_passes_through() {
        let client = make_client(RetryConfig::default());
        let result: Result<i32, Status> =
            client.call_with_retry(0, None, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(client.connection_state(), ConnectionState::Connected);
    }

    #[tokio::test]
    async fn test_non_transient_error_returns_immediately() {
        let client = make_client(fast_retry_config());
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let result: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::Relaxed);
                    Err(Status::invalid_argument("bad request"))
                }
            })
            .await;
        assert_eq!(result.unwrap_err().code(), Code::InvalidArgument);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_transient_error_triggers_retry_then_succeeds() {
        let client = make_client(fast_retry_config());
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let result: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    let n = cc.fetch_add(1, Ordering::Relaxed);
                    if n < 2 {
                        Err(Status::unavailable("server unavailable"))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
        assert_eq!(client.connection_state(), ConnectionState::Connected);
    }

    #[tokio::test]
    async fn test_max_retries_exceeded() {
        let client = make_client(RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            multiplier: 2.0,
            server_unavailable_timeout: Duration::from_secs(5),
            max_pending_bytes: 1024,
        });
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let result: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::Relaxed);
                    Err(Status::unavailable("always down"))
                }
            })
            .await;
        assert_eq!(result.unwrap_err().code(), Code::Unavailable);
        // 1 initial + 2 retries = 3 calls total
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
        assert_eq!(client.connection_state(), ConnectionState::Disconnected);
    }

    #[tokio::test]
    async fn test_deadline_exceeded_error_is_transient() {
        let client = make_client(fast_retry_config());
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let result: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    let n = cc.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        Err(Status::deadline_exceeded("timeout"))
                    } else {
                        Ok(99)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_resource_exhausted_is_transient() {
        let client = make_client(fast_retry_config());
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let result: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    let n = cc.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        Err(Status::resource_exhausted("busy"))
                    } else {
                        Ok(77)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 77);
    }

    #[tokio::test]
    async fn test_aborted_is_transient() {
        let client = make_client(fast_retry_config());
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let result: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    let n = cc.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        Err(Status::aborted("aborted"))
                    } else {
                        Ok(55)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 55);
    }

    #[tokio::test]
    async fn test_non_transient_codes() {
        assert!(!is_transient(Code::InvalidArgument));
        assert!(!is_transient(Code::NotFound));
        assert!(!is_transient(Code::AlreadyExists));
        assert!(!is_transient(Code::PermissionDenied));
        assert!(!is_transient(Code::Unauthenticated));
        assert!(!is_transient(Code::Unimplemented));
        assert!(!is_transient(Code::Internal));
        assert!(!is_transient(Code::Ok));
    }

    #[tokio::test]
    async fn test_transient_codes() {
        assert!(is_transient(Code::Unavailable));
        assert!(is_transient(Code::DeadlineExceeded));
        assert!(is_transient(Code::ResourceExhausted));
        assert!(is_transient(Code::Aborted));
    }

    #[tokio::test]
    async fn test_max_pending_bytes_exceeded() {
        let client = make_client(RetryConfig {
            max_pending_bytes: 100,
            ..fast_retry_config()
        });
        // First call: 80 bytes — should succeed
        let result: Result<i32, Status> =
            client.call_with_retry(80, None, || async { Ok(1) }).await;
        assert!(result.is_ok());
        assert_eq!(client.pending_bytes(), 0); // Released after call

        // Simulate in-flight: manually add pending
        client.pending_bytes.store(80, Ordering::Relaxed);

        // Second call: 30 bytes — exceeds 100 limit
        let result: Result<i32, Status> =
            client.call_with_retry(30, None, || async { Ok(2) }).await;
        assert_eq!(result.unwrap_err().code(), Code::ResourceExhausted);

        // Reset
        client.pending_bytes.store(0, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_pending_bytes_released_on_error() {
        let client = make_client(RetryConfig {
            max_retries: 0,
            ..fast_retry_config()
        });
        let result: Result<i32, Status> = client
            .call_with_retry(50, None, || async {
                Err(Status::internal("fail"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(client.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn test_timeout_override() {
        let client = make_client(RetryConfig {
            max_retries: 100, // Many retries
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(10),
            server_unavailable_timeout: Duration::from_secs(60),
            ..fast_retry_config()
        });
        let start = tokio::time::Instant::now();
        let result: Result<i32, Status> = client
            .call_with_retry(
                0,
                Some(Duration::from_millis(50)), // Short timeout override
                || async { Err(Status::unavailable("down")) },
            )
            .await;
        let elapsed = start.elapsed();
        assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_connection_state_transitions() {
        let client = make_client(fast_retry_config());
        assert_eq!(client.connection_state(), ConnectionState::Connected);

        // Transient error followed by success
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let _: Result<i32, Status> = client
            .call_with_retry(0, None, || {
                let cc = cc.clone();
                async move {
                    let n = cc.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        Err(Status::unavailable("down"))
                    } else {
                        Ok(1)
                    }
                }
            })
            .await;
        assert_eq!(client.connection_state(), ConnectionState::Connected);

        // All retries fail → disconnected
        let _: Result<i32, Status> = client
            .call_with_retry(0, None, || async {
                Err(Status::unavailable("always down"))
            })
            .await;
        assert_eq!(client.connection_state(), ConnectionState::Disconnected);
    }

    #[tokio::test]
    async fn test_is_connected() {
        let client = make_client(fast_retry_config());
        assert!(client.is_connected());

        let _: Result<i32, Status> = client
            .call_with_retry(0, None, || async {
                Err(Status::unavailable("down"))
            })
            .await;
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_zero_size_request_skips_throttle() {
        let client = make_client(RetryConfig {
            max_pending_bytes: 0, // Zero limit
            ..fast_retry_config()
        });
        // request_size=0 should bypass the check
        let result: Result<i32, Status> =
            client.call_with_retry(0, None, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_concurrent_calls() {
        let client = make_client(RetryConfig {
            max_pending_bytes: 10000,
            ..fast_retry_config()
        });
        let mut handles = Vec::new();
        for i in 0..10 {
            let c = client.clone();
            handles.push(tokio::spawn(async move {
                c.call_with_retry(100, None, || async move { Ok(i) })
                    .await
            }));
        }
        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap().unwrap());
        }
        results.sort();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let client = make_client(fast_retry_config());
        let clone = client.clone();

        // Both share the same connection state
        let _: Result<i32, Status> = client
            .call_with_retry(0, None, || async {
                Err(Status::unavailable("down"))
            })
            .await;
        assert_eq!(clone.connection_state(), ConnectionState::Disconnected);
    }

    #[tokio::test]
    async fn test_connect_lazy() {
        let channel = RetryableGrpcClient::connect_lazy("http://127.0.0.1:9999");
        let client = RetryableGrpcClient::new(channel, RetryConfig::default());
        assert!(client.is_connected());
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(5));
        assert_eq!(config.multiplier, 2.0);
        assert_eq!(config.server_unavailable_timeout, Duration::from_secs(60));
        assert_eq!(config.max_pending_bytes, 100 * 1024 * 1024);
    }

    #[test]
    fn test_connection_state_from_u8() {
        assert_eq!(ConnectionState::from_u8(0), ConnectionState::Connected);
        assert_eq!(ConnectionState::from_u8(1), ConnectionState::Disconnected);
        assert_eq!(ConnectionState::from_u8(2), ConnectionState::Reconnecting);
        assert_eq!(ConnectionState::from_u8(255), ConnectionState::Disconnected);
    }
}
