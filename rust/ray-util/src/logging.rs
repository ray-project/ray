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
