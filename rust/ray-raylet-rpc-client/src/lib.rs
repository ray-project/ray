// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Raylet RPC client library.
//!
//! Replaces raylet client from `src/ray/raylet_client/`.
//! Provides a typed client for calling NodeManagerService RPCs with
//! automatic retry on transient failures.

pub mod client;
pub mod traits;

pub use client::RayletRpcClient;
pub use traits::RayletClient;
