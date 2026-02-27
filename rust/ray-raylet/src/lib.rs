// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Raylet (per-node scheduler/resource manager) for Ray.
//!
//! Replaces `src/ray/raylet/` — the per-node process that manages
//! workers, resources, and scheduling.

#![allow(clippy::result_large_err)]

pub mod cluster_resource_manager;
pub mod cluster_resource_scheduler;
pub mod grpc_service;
pub mod lease_manager;
pub mod local_resource_manager;
pub mod node_manager;
pub mod placement_group_resource_manager;
pub mod scheduling_policy;
pub mod scheduling_resources;
pub mod wait_manager;
pub mod worker_pool;
