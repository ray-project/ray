// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

// tonic::Status is inherently large; we can't Box it without changing the API contract.
#![allow(clippy::result_large_err)]
// &[u8] → &[u8; N] via try_into() is not useless, but clippy can't see past unwrap_or.
#![allow(clippy::useless_conversion)]

//! Global Control Service (GCS) server for Ray.
//!
//! Replaces `src/ray/gcs/gcs_server/` — the central cluster controller.
//! Implements all GCS gRPC services: JobInfo, ActorInfo, NodeInfo,
//! WorkerInfo, PlacementGroupInfo, TaskInfo, InternalKV, RuntimeEnv,
//! InternalPubSub, AutoscalerState, NodeResourceInfo.

pub mod actor_manager;
pub mod autoscaler_state_manager;
pub mod grpc_services;
pub mod health_check_manager;
pub mod job_manager;
pub mod kv_manager;
pub mod node_manager;
pub mod placement_group_manager;
pub mod pubsub_handler;
pub mod resource_manager;
pub mod server;
pub mod store_client;
pub mod table_storage;
pub mod task_manager;
pub mod worker_manager;
