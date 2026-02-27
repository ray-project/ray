// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Global Control Service (GCS) server for Ray.
//!
//! Replaces `src/ray/gcs/gcs_server/` — the central cluster controller.
//! Implements all GCS gRPC services: JobInfo, ActorInfo, NodeInfo,
//! WorkerInfo, PlacementGroupInfo, TaskInfo, InternalKV, RuntimeEnv,
//! InternalPubSub, AutoscalerState, NodeResourceInfo.

pub mod server;

// Phase 7: Full GCS implementation including:
// - GcsJobManager
// - GcsNodeManager
// - GcsResourceManager
// - GcsActorManager + GcsActorScheduler
// - GcsPlacementGroupManager + GcsPlacementGroupScheduler
// - GcsWorkerManager
// - GcsTaskManager
// - GcsKvManager
// - GcsHealthCheckManager
// - GcsAutoscalerStateManager
// - Store client (Redis + in-memory)
