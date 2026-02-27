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

pub mod node_manager;

// Phase 8: Full raylet implementation including:
// - NodeManager (central class)
// - WorkerPool
// - LocalObjectManager
// - WaitManager
// - All scheduling policies (hybrid, random, spread, etc.)
// - ClusterResourceScheduler
// - NodeManagerService gRPC implementation
