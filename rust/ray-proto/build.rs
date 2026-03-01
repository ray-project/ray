// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The Ray repo root is two levels up from rust/ray-proto/
    let ray_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // rust/
        .unwrap()
        .parent() // ray/
        .unwrap()
        .to_path_buf();

    let proto_root = ray_root.join("src/ray/protobuf");

    // All proto files to compile
    let proto_files: Vec<PathBuf> = vec![
        // Core types
        "common.proto",
        "gcs.proto",
        "pubsub.proto",
        "serialization.proto",
        "dependency.proto",
        "event.proto",
        "profile_events.proto",
        "usage.proto",
        "logging.proto",
        // Services
        "gcs_service.proto",
        "node_manager.proto",
        "core_worker.proto",
        "object_manager.proto",
        "ray_client.proto",
        "ray_syncer.proto",
        // "reporter.proto" — excluded: depends on opencensus proto not in tree
        "autoscaler.proto",
        "runtime_env_agent.proto",
        "runtime_env_common.proto",
        "serve.proto",
        "instance_manager.proto",
        "test_service.proto",
        "events_event_aggregator_service.proto",
        "events_task_profile_events.proto",
        // Export types
        "export_event.proto",
        "export_actor_data.proto",
        "export_node_data.proto",
        "export_task_event.proto",
        "export_driver_job_event.proto",
        "export_submission_job_event.proto",
        "export_runtime_env.proto",
        "export_dataset_metadata.proto",
        "export_dataset_operator_event.proto",
        "export_dataset_operator_schema.proto",
        "export_train_state.proto",
        // Public event types
        "public/events_base_event.proto",
        "public/events_actor_definition_event.proto",
        "public/events_actor_lifecycle_event.proto",
        "public/events_actor_task_definition_event.proto",
        "public/events_driver_job_definition_event.proto",
        "public/events_driver_job_lifecycle_event.proto",
        "public/events_node_definition_event.proto",
        "public/events_node_lifecycle_event.proto",
        "public/events_task_definition_event.proto",
        "public/events_task_lifecycle_event.proto",
        "public/runtime_environment.proto",
    ]
    .into_iter()
    .map(|p| proto_root.join(p))
    .collect();

    // The proto import root is the ray repo root, since imports are like:
    // import "src/ray/protobuf/common.proto"
    let include_dirs = vec![ray_root.clone()];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(true)
        // Serde derives on all generated message/enum types for JSON conformance testing.
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Redirect well-known types to our serde-compatible wrappers (see src/wkt.rs).
        .extern_path(".google.protobuf.Timestamp", "crate::wkt::Timestamp")
        .extern_path(".google.protobuf.Struct", "crate::wkt::Struct")
        .extern_path(".google.protobuf.Value", "crate::wkt::Value")
        .extern_path(".google.protobuf.ListValue", "crate::wkt::ListValue")
        .extern_path(".google.protobuf.NullValue", "crate::wkt::NullValue")
        .compile_protos(&proto_files, &include_dirs)?;

    // Re-run if any proto file changes
    for proto in &proto_files {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    Ok(())
}
