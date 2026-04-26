//! Generated protobuf and gRPC types for Ray GCS.
//!
//! This crate contains all the Rust types generated from Ray's `.proto` files.
//! It provides both the protobuf message types (via prost) and the gRPC service
//! client/server stubs (via tonic).

/// Core Ray RPC types -- contains all generated types from `ray.rpc` package,
/// plus the `autoscaler` and `events` sub-packages nested within.
pub mod ray {
    pub mod rpc {
        tonic::include_proto!("ray.rpc");

        /// Autoscaler service types from `ray.rpc.autoscaler` package.
        pub mod autoscaler {
            tonic::include_proto!("ray.rpc.autoscaler");
        }

        /// Event export types from `ray.rpc.events` package.
        pub mod events {
            tonic::include_proto!("ray.rpc.events");
        }

        /// Ray syncer service + messages from `ray.rpc.syncer` package
        /// (`src/ray/protobuf/ray_syncer.proto`). Mirrors the C++
        /// `ray::rpc::syncer::RaySyncer` gRPC surface registered in
        /// `GcsServer::InitRaySyncer` (`gcs/gcs_server.cc:607-621`).
        pub mod syncer {
            tonic::include_proto!("ray.rpc.syncer");
        }
    }
}
