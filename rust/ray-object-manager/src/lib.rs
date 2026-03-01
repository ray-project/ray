// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object store and plasma for Ray.
//!
//! Replaces `src/ray/object_manager/` (22 files) and `plasma/` (25 files).
//!
//! Three-layer model:
//! 1. **ObjectManager** — coordinates inter-node object transfers via gRPC
//! 2. **PullManager/PushManager** — fetch strategy + rate-limited sends
//! 3. **PlasmaStore** — local shared-memory object store (mmap-based)

pub mod common;
pub mod object_buffer_pool;
pub mod object_directory;
pub mod object_manager;
pub mod plasma;
pub mod pull_manager;
pub mod push_manager;
pub mod readers;
