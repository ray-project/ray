# === @begin: raylet_cpp_lib (original: raylet_lib) ===
raylet_cpp_lib_dependencies = [
    # Package-managed

    # Eugo-managed


    # Eugo-managed, in-tree
#    grpc_health_proto_cpp_dep,
#    grpc_health_proto_cpp_grpc_dep,
]

raylet_cpp_lib = static_library(
    'raylet_cpp_lib',
    [

    ],
    install: false,
    dependencies: raylet_cpp_lib_dependencies
)

raylet_cpp_lib_dep = declare_dependency(
    link_with: [raylet_cpp_lib],
    dependencies: raylet_cpp_lib_dependencies
)
# === @end: raylet_cpp_lib (original: raylet_lib) ===


#
#ray_cc_library(
#    name = "raylet_lib",
#    srcs = glob(
#    [
#    "src/ray/raylet/**/*.cc",
#    ],
#    exclude = [
#    "src/ray/raylet/**/*_test.cc",
#    "src/ray/raylet/scheduling/**/*.cc",
#    "src/ray/raylet/main.cc",
#    ],
#),
#hdrs = glob(
#[
#"src/ray/raylet/**/*.h",
#],
#exclude = [
#"src/ray/raylet/scheduling/**/*.h",
#"src/ray/raylet/main.cc",
#],
#),
#linkopts = select({
#"@platforms//os:windows": [
#],
#"//conditions:default": [
#"-lpthread",
#],
#}),
#deps = [
#":gcs",
#":gcs_client_lib",
#":node_manager_fbs",
#":node_manager_rpc",
#":object_manager",
#":plasma_client",
#":pubsub_lib",
#":ray_common",
#":scheduler",
#":stats_lib",
#":worker_rpc",
#"//src/ray/protobuf:agent_manager_cc_proto",
#"//src/ray/protobuf:common_cc_proto",
#"//src/ray/protobuf:runtime_env_agent_cc_proto",
#"//src/ray/util",
#"@boost//:asio",
#"@boost//:beast",
#"@boost//:system",
#"@com_github_jupp0r_prometheus_cpp//pull",
#"@com_google_absl//absl/base:core_headers",
#"@com_google_absl//absl/container:flat_hash_set",
#"@com_google_absl//absl/memory",
#"@com_google_absl//absl/strings",
#"@com_google_googletest//:gtest",
#"@io_opencensus_cpp//opencensus/exporters/stats/prometheus:prometheus_exporter",
#"@io_opencensus_cpp//opencensus/stats",
#"@io_opencensus_cpp//opencensus/tags",
#],
#)
#

# === @begin: raylet_executable (original: raylet) ===
raylet_executable = executable(
    'raylet',
    ['main.cc'],
    install: false,
    dependencies: [
        raylet_cpp_lib_dep,
        util_cpp_lib_dep,
        gflags
    ]
)
# === @end: raylet_executable (original: raylet) ===
