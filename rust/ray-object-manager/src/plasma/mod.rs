// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Plasma shared-memory object store.
//!
//! Replaces `src/ray/object_manager/plasma/` (25 files).

pub mod allocator;
pub mod eviction;
pub mod fling;
pub mod object_store;
pub mod store;
