// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#![allow(clippy::large_enum_variant)]
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::doc_overindented_list_items)]

//! Generated protobuf and gRPC types for Ray.
//!
//! This crate contains all protobuf message types and tonic gRPC service
//! traits/client stubs generated from Ray's `.proto` files. The generated
//! code is wire-compatible with the C++ protobuf implementation.

/// All Ray protobuf types organized by package.
pub mod ray {
    /// Main RPC types (package `ray.rpc`).
    pub mod rpc {
        tonic::include_proto!("ray.rpc");

        /// Event aggregator types (package `ray.rpc.events`).
        pub mod events {
            tonic::include_proto!("ray.rpc.events");
        }

        /// Autoscaler types (package `ray.rpc.autoscaler`).
        pub mod autoscaler {
            tonic::include_proto!("ray.rpc.autoscaler");

            /// Instance manager types (package `ray.rpc.autoscaler.im`).
            pub mod im {
                tonic::include_proto!("ray.rpc.autoscaler.im");
            }
        }

        /// Syncer types (package `ray.rpc.syncer`).
        pub mod syncer {
            tonic::include_proto!("ray.rpc.syncer");
        }
    }

    /// Serialization types (package `ray.serialization`).
    pub mod serialization {
        tonic::include_proto!("ray.serialization");
    }

    /// Serve types (package `ray.serve`).
    pub mod serve {
        tonic::include_proto!("ray.serve");
    }

    /// Usage types (package `ray.usage`).
    pub mod usage {
        tonic::include_proto!("ray.usage");
    }
}

// Re-export the main namespace for convenience
pub use ray::rpc;
