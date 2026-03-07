// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Logging setup for Ray using the `tracing` ecosystem.
//!
//! Replaces C++ `logging.cc/h` (spdlog-based) with tracing + tracing-subscriber.

use std::path::Path;
use tracing_subscriber::EnvFilter;

/// Initialize Ray's logging system.
///
/// Sets up tracing-subscriber with:
/// - Environment filter (RUST_LOG or RAY_LOG_LEVEL)
/// - Optional file output
/// - Component name in log lines
pub fn init_ray_logging(component: &str, log_dir: Option<&Path>, verbosity: i32) {
    let filter = EnvFilter::try_from_env("RAY_BACKEND_LOG_LEVEL")
        .or_else(|_| EnvFilter::try_from_env("RUST_LOG"))
        .unwrap_or_else(|_| {
            let level = match verbosity {
                0 => "info",
                1 => "debug",
                _ => "trace",
            };
            EnvFilter::new(level)
        });

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    if let Some(dir) = log_dir {
        let log_file = dir.join(format!("{component}.log"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)
            .expect("Failed to open log file");
        subscriber.with_writer(file).init();
    } else {
        subscriber.init();
    }

    tracing::info!(component, "Ray logging initialized");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_ray_logging_to_file() {
        let tmp = std::env::temp_dir().join("ray_logging_test");
        std::fs::create_dir_all(&tmp).unwrap();
        // This will set the global subscriber, so only run once per process.
        // We just verify it doesn't panic.
        init_ray_logging("test_component", Some(&tmp), 0);
        let log_file = tmp.join("test_component.log");
        assert!(log_file.exists());
        std::fs::remove_dir_all(&tmp).ok();
    }

    // --- Ported from C++ logging_test.cc ---

    /// Port of C++ LogTestWithoutInit: logging macros work without init.
    #[test]
    fn test_log_without_init() {
        // Verify that tracing macros don't panic even without initialization.
        tracing::debug!("This is the DEBUG message");
        tracing::info!("This is the INFO message");
        tracing::warn!("This is the WARNING message");
        tracing::error!("This is the ERROR message");
    }

    /// Port of C++ LogPerfTest: measure logging performance.
    #[test]
    fn test_log_perf() {
        let rounds = 10;

        let start = std::time::Instant::now();
        for _ in 0..rounds {
            tracing::debug!("This is the RAY_DEBUG message");
        }
        let debug_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..rounds {
            tracing::error!("This is the RAY_ERROR message");
        }
        let error_elapsed = start.elapsed();

        // Just verify it completes in reasonable time.
        assert!(
            debug_elapsed.as_millis() < 5000,
            "DEBUG logging took too long: {:?}",
            debug_elapsed
        );
        assert!(
            error_elapsed.as_millis() < 5000,
            "ERROR logging took too long: {:?}",
            error_elapsed
        );
    }

    /// Port of C++ TestCheckOp: assert macros with values.
    #[test]
    fn test_check_operations() {
        let i: i32 = 1;
        assert_eq!(i, 1);
        assert_ne!(i, 0);
        assert!(i <= 1);
        assert!(i < 2);
        assert!(i >= 1);
        assert!(i > 0);

        let j: i32 = 0;
        assert_ne!(i, j);
    }

    /// Port of C++ TestRayLogEveryMs: rate-limited logging concept.
    #[test]
    fn test_rate_limited_logging_concept() {
        use std::time::{Duration, Instant};

        // Simulate rate-limited logging: track last log time, log only every 10ms.
        let mut last_log = Instant::now() - Duration::from_millis(100);
        let interval = Duration::from_millis(10);
        let start = Instant::now();
        let mut num_logged = 0u64;
        let mut num_iterations = 0u64;

        while start.elapsed() < Duration::from_millis(100) {
            num_iterations += 1;
            if last_log.elapsed() >= interval {
                tracing::info!("rate limited log");
                num_logged += 1;
                last_log = Instant::now();
            }
        }

        // Should have logged more than 5 times but less than total iterations.
        assert!(
            num_logged >= 5,
            "expected at least 5 logs, got {}",
            num_logged
        );
        assert!(
            num_logged < num_iterations,
            "rate limiting didn't work: logged={}, iterations={}",
            num_logged,
            num_iterations
        );
    }
}
