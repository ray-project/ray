load("//bazel:ray.bzl", "cc_library")

cc_library(
    name = "constants",
    hdrs = ["constants.h"],
)

cc_library(
    name = "ray_object",
    hdrs = [
        "buffer.h",
        "ray_object.h"],
    srcs = ["ray_object.cc"],
    deps = [
        "//src/ray/util:util",
        "//src/ray/protobuf:gcs_cc_proto",
        ":status",
        ":id",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "//:aligned_alloc",
    ]
)

cc_library(
    name = "grpc_util",
    hdrs = ["grpc_util.h"],
    deps = [
        "@com_github_grpc_grpc//:grpc++",
        ":status",
        ":ray_config",
        "@com_google_absl//absl/container:flat_hash_map",
    ]
)


cc_library(
    name = "memory_monitor",
    srcs = [
        "file_system_monitor.cc",
        "memory_monitor.cc",
    ],
    hdrs = [
        "file_system_monitor.h",
        "memory_monitor.h",
    ],
    deps = [
        ":asio",
        "//src/ray/util:util",
        ":ray_config",
        "@com_google_absl//absl/strings",
        "@nlohmann_json",
        "@com_google_googletest//:gtest_prod",
    ]
)

cc_library(
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
    ]
)

cc_library(
    name = "network",
    srcs = [
        "network_util.cc",
        "client_connection.cc"],
    hdrs = [
        "client_connection.h",
        "network_util.h",
    ],
    deps = [
        ":asio",
        ":id",
        ":status",
        "//:node_manager_fbs",
    ]
)

cc_library(
    name = "id",
    srcs = [
        "id.cc",
    ],
    hdrs = [
        "id.h",
        "common_protocol.h",
        "id_def.h",
    ],
    deps = [
        "//src/ray/util:util",
        ":constants",
        "//src/ray/protobuf:common_cc_proto",
        "@com_github_google_flatbuffers//:flatbuffers",
        ":status",
        "@msgpack",
    ]
)

cc_library(
    name = "task_common",
    srcs =[
        "task/task.cc",
        "task/task_spec.cc",
        "bundle_location_index.cc",
        "bundle_spec.cc",
        "scheduling/cluster_resource_data.cc",
        "scheduling/fixed_point.cc",
        "scheduling/scheduling_ids.cc",
        "scheduling/scheduling_resources.cc",
    ],
    hdrs = [
        "scheduling/cluster_resource_data.h",
        "scheduling/fixed_point.h",
        "scheduling/scheduling_ids.h",
        "scheduling/scheduling_resources.h",
        "function_descriptor.h",
        "bundle_spec.h",
        "placement_group.h",
        "bundle_location_index.h",
        "task/task.h",
        "task/task_common.h",
        "task/task_spec.h",
        "task/task_util.h",
    ],
    deps = [
        "//src/ray/util:util",
        ":id",
        ":grpc_util",
        "//:node_manager_fbs",
        ":ray_object",
        ":runtime_env",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/strings",
        ":ray_config",
        ":event_stats",
    ]
)

cc_library(
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
    ],
    deps = [
        "@boost//:asio",
        ":ray_config",
        ":event_stats",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/container:flat_hash_map",
        "//src/ray/util:util",
    ]
)

cc_library(
    name = "event_stats",
    srcs = [
        "event_stats.cc"
    ],
    hdrs = [
        "event_stats.h"
    ],
    deps = [
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/container:flat_hash_map",
        ":ray_config",
        "//src/ray/util:util",
        "//:stats_metric",
    ]
)

cc_library(
    name = "ray_config",
    srcs = ["ray_config.cc"],
    hdrs = ["ray_config_def.h", "ray_config.h", "ray_internal_flag_def.h"],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/algorithm",
        "//src/ray/util:util",
        "@nlohmann_json",
    ]
)

cc_library(
    name = "ray_syncer",
    srcs = [
        "ray_syncer/ray_syncer.cc",
    ],
    hdrs = [
        "ray_syncer/ray_syncer.h",
        "ray_syncer/ray_syncer-inl.h",
    ],
    deps = [
        "//src/ray/util:util",
        "//:ray_syncer_cc_grpc",
        ":asio",
        ":id",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
    ]
)

cc_library(
    name = "status",
    srcs = [
        "status.cc"
    ],
    hdrs = [
        "status.h"
    ],
    deps = [
        "//src/ray/util:util",
    ]
)

# cc_library(
#     name = "stats",
#     srcs = [
#         ""
#     ],
#     hdrs = [
#     ]
# )
    
