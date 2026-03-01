// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Async-aware wait utilities for tests.
//!
//! Rust equivalents of C++ `WaitForCondition`, `WaitReady`, `WaitForExpectedCount`.

use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

/// Poll a synchronous condition every 10ms until it returns true or timeout.
///
/// Returns `true` if the condition was met, `false` on timeout.
pub async fn wait_for_condition<F>(condition: F, timeout_ms: u64) -> bool
where
    F: Fn() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if condition() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Poll an async condition every 10ms until it returns true or timeout.
///
/// Returns `true` if the condition was met, `false` on timeout.
pub async fn wait_for_condition_async<F, Fut>(condition: F, timeout_ms: u64) -> bool
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if condition().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait for a future to produce `true` within the given timeout.
///
/// Returns `true` if the future completed with `true`, `false` on timeout.
pub async fn wait_ready<F>(future: F, timeout: Duration) -> bool
where
    F: Future<Output = bool>,
{
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_default()
}

/// Wait for an atomic counter to reach the expected value.
///
/// Returns `true` if the counter reached the expected value, `false` on timeout.
pub async fn wait_for_expected_count(
    counter: &AtomicI32,
    expected: i32,
    timeout_ms: u64,
) -> bool {
    wait_for_condition(|| counter.load(Ordering::SeqCst) == expected, timeout_ms).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_wait_for_condition_immediate_true() {
        let result = wait_for_condition(|| true, 1000).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_for_condition_timeout_false() {
        let result = wait_for_condition(|| false, 50).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_wait_for_condition_eventual_true() {
        let counter = Arc::new(AtomicI32::new(0));
        let counter_clone = Arc::clone(&counter);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            counter_clone.store(1, Ordering::SeqCst);
        });

        let result = wait_for_condition(|| counter.load(Ordering::SeqCst) == 1, 1000).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_for_condition_async_immediate() {
        let result = wait_for_condition_async(|| async { true }, 1000).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_for_condition_async_timeout() {
        let result = wait_for_condition_async(|| async { false }, 50).await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_wait_ready_success() {
        let result = wait_ready(async { true }, Duration::from_secs(1)).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_ready_timeout() {
        let result = wait_ready(
            async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                true
            },
            Duration::from_millis(50),
        )
        .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_wait_for_expected_count_immediate() {
        let counter = AtomicI32::new(42);
        let result = wait_for_expected_count(&counter, 42, 1000).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_for_expected_count_eventual() {
        let counter = Arc::new(AtomicI32::new(0));
        let counter_clone = Arc::clone(&counter);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            counter_clone.store(5, Ordering::SeqCst);
        });

        let result = wait_for_expected_count(&counter, 5, 1000).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_for_expected_count_timeout() {
        let counter = AtomicI32::new(0);
        let result = wait_for_expected_count(&counter, 99, 50).await;
        assert!(!result);
    }
}
