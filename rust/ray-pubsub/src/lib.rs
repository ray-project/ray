// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Pub/sub messaging system for Ray.
//!
//! Replaces `src/ray/pubsub/` (14 files).
//! Implements the GCS-backed publish/subscribe pattern using long-polling.

pub mod publisher;
pub mod subscriber;

pub use publisher::Publisher;
pub use subscriber::Subscriber;
