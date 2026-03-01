// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS RPC client library.
//!
//! Replaces `src/ray/gcs_rpc_client/`.
//! Provides a typed client for calling all GCS gRPC services with
//! automatic retry on transient failures.

pub mod client;
pub mod subscriber;
pub mod traits;

pub use client::GcsRpcClient;
pub use subscriber::GcsSubscriberClient;
pub use traits::GcsClient;
