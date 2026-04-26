use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The proto files live relative to the Ray repository root.
    // From this crate: ../../../../../../ gets us to ray/
    let ray_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .join("..");

    let proto_root = ray_root.clone();

    // All proto files needed by GCS, in dependency order.
    let proto_files = [
        // Public/shared protos
        "src/ray/protobuf/public/runtime_environment.proto",
        "src/ray/protobuf/public/events_base_event.proto",
        // Core protos
        "src/ray/protobuf/common.proto",
        "src/ray/protobuf/profile_events.proto",
        "src/ray/protobuf/logging.proto",
        "src/ray/protobuf/gcs.proto",
        "src/ray/protobuf/pubsub.proto",
        "src/ray/protobuf/events_event_aggregator_service.proto",
        // Raylet and worker protos (needed for actor scheduling RPCs)
        "src/ray/protobuf/node_manager.proto",
        "src/ray/protobuf/core_worker.proto",
        // Service protos (gRPC services)
        "src/ray/protobuf/gcs_service.proto",
        "src/ray/protobuf/autoscaler.proto",
        "src/ray/protobuf/ray_syncer.proto",
    ];

    let proto_paths: Vec<PathBuf> = proto_files
        .iter()
        .map(|p| proto_root.join(p))
        .collect();

    // Include path is the Ray repo root (so imports like "src/ray/protobuf/..." resolve).
    let include_dirs = [proto_root.as_path()];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&proto_paths, &include_dirs)?;

    // Re-run if any proto files change.
    for proto in &proto_paths {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    Ok(())
}
