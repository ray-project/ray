// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Core worker logic for Ray.
//!
//! Replaces `src/ray/core_worker/` (39 files).
//! Handles task submission, object get/put, actor management,
//! reference counting, and the CoreWorkerService gRPC server.

pub mod error;
pub mod options;
pub mod context;
pub mod memory_store;
pub mod reference_counter;
pub mod actor_handle;
pub mod actor_manager;
pub mod task_spec_builder;
pub mod normal_task_submitter;
pub mod actor_task_submitter;
pub mod dependency_resolver;
pub mod core_worker;
pub mod grpc_service;

// Re-export primary types.
pub use core_worker::CoreWorker;
pub use error::{CoreWorkerError, CoreWorkerResult};
pub use memory_store::RayObject;
pub use options::{CoreWorkerOptions, Language, WorkerType};
