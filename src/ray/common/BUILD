load("//bazel:ray.bzl", "ray_cc_library", "ray_cc_test")

ray_cc_library(
    name = "compat",
    hdrs = ["compat.h"],
)

ray_cc_library(
    name = "constants",
    hdrs = ["constants.h"],
)

ray_cc_library(
    name = "test_util",
    srcs = ["test_util.cc"],
    hdrs = ["test_util.h"],
    deps = [
        ":id",
        ":network",
        ":ray_object",
        "//src/ray/protobuf:common_cc_proto",
        "//src/ray/util",
        "//src/ray/util:cmd_line_utils",
        "@boost//:optional",
        "@com_google_googletest//:gtest",
    ],
)

ray_cc_library(
    name = "ray_object",
    srcs = ["ray_object.cc"],
    hdrs = [
        "buffer.h",
        "ray_object.h",
    ],
    deps = [
        ":id",
        ":status",
        "//:aligned_alloc",
        "//src/ray/protobuf:gcs_cc_proto",
        "//src/ray/util",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
    ],
)

ray_cc_library(
    name = "grpc_util",
    hdrs = ["grpc_util.h"],
    deps = [
        ":ray_config",
        ":status",
        "//src/ray/util",
        "//src/ray/util:logging",
        "//src/ray/util:type_traits",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

ray_cc_library(
    name = "memory_monitor",
    srcs = [
        "memory_monitor.cc",
    ],
    hdrs = [
        "memory_monitor.h",
    ],
    deps = [
        ":asio",
        ":ray_config",
        "//src/ray/util",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_prod",
        "@nlohmann_json",
    ],
)

ray_cc_library(
    name = "file_system_monitor",
    srcs = ["file_system_monitor.cc"],
    hdrs = ["file_system_monitor.h"],
    deps = [
        ":asio",
        "//src/ray/util",
        "//src/ray/util:event",
        "@com_google_googletest//:gtest_prod",
    ],
)

ray_cc_library(
    name = "runtime_env",
    srcs = [
        "runtime_env_common.cc",
        "runtime_env_manager.cc",
    ],
    hdrs = [
        "runtime_env_common.h",
        "runtime_env_manager.h",
    ],
    deps = [
        ":id",
        "//src/ray/protobuf:common_cc_proto",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

ray_cc_library(
    name = "network",
    srcs = [
        "client_connection.cc",
        "network_util.cc",
    ],
    hdrs = [
        "client_connection.h",
        "network_util.h",
    ],
    deps = [
        ":asio",
        ":id",
        ":status",
        "//:node_manager_fbs",
    ],
)

ray_cc_library(
    name = "id",
    srcs = [
        "common_protocol.cc",
        "id.cc",
    ],
    hdrs = [
        "common_protocol.h",
        "id.h",
        "id_def.h",
    ],
    deps = [
        ":constants",
        ":status",
        "//src/ray/protobuf:common_cc_proto",
        "//src/ray/protobuf:gcs_cc_proto",
        "//src/ray/util",
        "//src/ray/util:random",
        "@com_github_google_flatbuffers//:flatbuffers",
        "@msgpack",
    ],
)

ray_cc_library(
    name = "task_common",
    srcs = [
        "bundle_location_index.cc",
        "bundle_spec.cc",
        "function_descriptor.cc",
        "placement_group.cc",
        "scheduling/cluster_resource_data.cc",
        "scheduling/fixed_point.cc",
        "scheduling/resource_instance_set.cc",
        "scheduling/resource_set.cc",
        "scheduling/scheduling_ids.cc",
        "task/task.cc",
        "task/task_spec.cc",
    ],
    hdrs = [
        "bundle_location_index.h",
        "bundle_spec.h",
        "function_descriptor.h",
        "placement_group.h",
        "scheduling/cluster_resource_data.h",
        "scheduling/fixed_point.h",
        "scheduling/resource_instance_set.h",
        "scheduling/resource_set.h",
        "scheduling/scheduling_ids.h",
        "task/task.h",
        "task/task_common.h",
        "task/task_spec.h",
        "task/task_util.h",
    ],
    deps = [
        ":event_stats",
        ":grpc_util",
        ":id",
        ":ray_config",
        ":ray_object",
        ":runtime_env",
        "//:node_manager_fbs",
        "//src/ray/util",
        "//src/ray/util:container_util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

ray_cc_library(
    name = "asio",
    srcs = [
        "asio/asio_chaos.cc",
        "asio/instrumented_io_context.cc",
        "asio/io_service_pool.cc",
        "asio/periodical_runner.cc",
    ],
    hdrs = [
        "asio/asio_chaos.h",
        "asio/asio_util.h",
        "asio/instrumented_io_context.h",
        "asio/io_service_pool.h",
        "asio/periodical_runner.h",
        "asio/postable.h",
    ],
    deps = [
        ":event_stats",
        ":ray_config",
        "//src/ray/util",
        "//src/ray/util:array",
        "//src/ray/util:function_traits",
        "@boost//:asio",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
    ],
)

ray_cc_library(
    name = "event_stats",
    srcs = [
        "event_stats.cc",
    ],
    hdrs = [
        "event_stats.h",
    ],
    deps = [
        ":ray_config",
        "//:stats_metric",
        "//src/ray/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
    ],
)

ray_cc_library(
    name = "ray_config",
    srcs = ["ray_config.cc"],
    hdrs = [
        "ray_config.h",
        "ray_config_def.h",
        "ray_internal_flag_def.h",
    ],
    deps = [
        "//src/ray/util",
        "@com_google_absl//absl/algorithm",
        "@com_google_absl//absl/strings",
        "@nlohmann_json",
    ],
)

ray_cc_library(
    name = "ray_syncer",
    srcs = [
        "ray_syncer/node_state.cc",
        "ray_syncer/ray_syncer.cc",
        "ray_syncer/ray_syncer_client.cc",
        "ray_syncer/ray_syncer_server.cc",
    ],
    hdrs = [
        "ray_syncer/common.h",
        "ray_syncer/node_state.h",
        "ray_syncer/ray_syncer.h",
        "ray_syncer/ray_syncer_bidi_reactor.h",
        "ray_syncer/ray_syncer_bidi_reactor_base.h",
        "ray_syncer/ray_syncer_client.h",
        "ray_syncer/ray_syncer_server.h",
    ],
    deps = [
        ":asio",
        ":id",
        "//:ray_syncer_cc_grpc",
        "//src/ray/util",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

ray_cc_library(
    name = "status",
    srcs = ["status.cc"],
    hdrs = ["status.h"],
    deps = [
        ":source_location",
        "//src/ray/util:logging",
        "//src/ray/util:macros",
        "//src/ray/util:visibility",
        "@boost//:system",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
    ],
)

ray_cc_library(
    name = "macros",
    hdrs = ["macros.h"],
)

ray_cc_library(
    name = "status_or",
    hdrs = ["status_or.h"],
    deps = [
        ":macros",
        ":status",
        "@com_google_absl//absl/base:core_headers",
    ],
)

ray_cc_library(
    name = "source_location",
    srcs = ["source_location.cc"],
    hdrs = ["source_location.h"],
)

ray_cc_test(
    name = "source_location_test",
    size = "small",
    srcs = ["source_location_test.cc"],
    tags = ["team:core"],
    deps = [
        ":source_location",
        "@com_google_googletest//:gtest_main",
    ],
)
