// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC server/client framework for Ray.
//!
//! Replaces `src/ray/rpc/` — provides tonic-based server/client wrappers,
//! retry middleware, and authentication interceptors.

pub mod auth;
pub mod client;
pub mod server;
