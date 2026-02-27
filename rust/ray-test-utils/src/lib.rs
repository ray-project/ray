// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Shared test helpers for Ray Rust crates.

use ray_common::id::*;

/// Initialize tracing for tests.
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("debug")
        .try_init();
}

/// Create a random JobID for testing.
pub fn random_job_id() -> JobID {
    JobID::from_int(rand::random::<u16>() as u32 + 1)
}

/// Create a random ActorID for testing.
pub fn random_actor_id() -> ActorID {
    let job_id = random_job_id();
    let task_id = TaskID::from_random();
    ActorID::of(&job_id, &task_id, rand::random::<usize>())
}

/// Create a temporary directory for test data.
pub fn test_temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}
