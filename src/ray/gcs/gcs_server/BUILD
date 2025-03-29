load("//bazel:ray.bzl", "ray_cc_library")
load("@rules_cc//cc:defs.bzl", "cc_library", "cc_binary")

package(default_visibility = ["//visibility:public"])

# Debug tools
ray_cc_library(
    name = "debug_tools",
    srcs = ["debug_tools.cc"],
    hdrs = ["debug_tools.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
    ],
)

# Raft-related libraries
ray_cc_library(
    name = "raft_types",
    srcs = ["raft_types.cc"],
    hdrs = ["raft_types.h"],
    deps = [
        "//src/ray/common:status",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_tables_cc_proto",
    ],
)

ray_cc_library(
    name = "raft_network",
    srcs = ["raft_network.cc"],
    hdrs = ["raft_network.h"],
    deps = [
        ":raft_types",
        "//src/ray/common:asio",
        "//src/ray/common:status",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

ray_cc_library(
    name = "raft_storage",
    srcs = ["raft_storage.cc"],
    hdrs = ["raft_storage.h"],
    deps = [
        ":raft_types",
        "//src/ray/common:status",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_tables_cc_proto",
    ],
)

ray_cc_library(
    name = "raft_state_machine",
    srcs = ["raft_state_machine.cc"],
    hdrs = ["raft_state_machine.h"],
    deps = [
        ":raft_network",
        ":raft_storage",
        ":raft_types",
        "//src/ray/common:asio",
        "//src/ray/common:status",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

ray_cc_library(
    name = "raft_impl",
    srcs = ["raft_impl.cc"],
    hdrs = ["raft_impl.h"],
    deps = [
        ":raft_network",
        ":raft_state_machine",
        ":raft_storage",
        ":raft_types",
        "//src/ray/common:asio",
        "//src/ray/common:status",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

ray_cc_library(
    name = "raft_gcs_coordinator",
    srcs = ["raft_gcs_coordinator.cc"],
    hdrs = ["raft_gcs_coordinator.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        ":raft_types",  # Move raft_types before raft_impl
        ":raft_impl",
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

# Forward declarations
ray_cc_library(
    name = "gcs_server_fwd",
    hdrs = ["gcs_server_fwd.h"],
    deps = [
        "//src/ray/common:status",
        "//src/ray/common:id",
        "//src/ray/common:asio",
        "//src/ray/rpc:grpc_server",
        "//src/ray/protobuf:common_proto",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
    ],
)

# Main GCS server library
ray_cc_library(
    name = "gcs_server",
    srcs = [
        "gcs_server.cc",
        "gcs_server.h",
    ],
    hdrs = ["gcs_server.h"],
    deps = [
        ":gcs_server_fwd",
        "//src/ray/common:asio",
        "//src/ray/common:ray_syncer",
        "//src/ray/common:runtime_env_manager",
        "//src/ray/common:status",
        "//src/ray/common:id",
        "//src/ray/gcs:gcs_pub_sub",
        "//src/ray/gcs:redis_client",
        "//src/ray/gcs/gcs_server:gcs_function_manager",
        "//src/ray/gcs/gcs_server:gcs_health_check_manager",
        "//src/ray/gcs/gcs_server:gcs_init_data",
        "//src/ray/gcs/gcs_server:gcs_kv_manager",
        "//src/ray/gcs/gcs_server:gcs_redis_failure_detector",
        "//src/ray/gcs/gcs_server:gcs_resource_manager",
        "//src/ray/gcs/gcs_server:gcs_server_io_context_policy",
        "//src/ray/gcs/gcs_server:gcs_table_storage",
        "//src/ray/gcs/gcs_server:gcs_task_manager",
        "//src/ray/gcs/gcs_server:pubsub_handler",
        "//src/ray/gcs/gcs_server:runtime_env_handler",
        "//src/ray/gcs/gcs_server:usage_stats_client",
        "//src/ray/raylet:scheduling",
        "//src/ray/rpc:client_call",
        "//src/ray/rpc:grpc_server",
        "//src/ray/rpc:node_manager_client_pool",
        "//src/ray/rpc:raft_server",
        "//src/ray/rpc:raft_client",
        "//src/ray/util:throttler",
        "//src/ray/util:logging",
        "//ray/protobuf:gcs_service_cc_proto",
        "//ray/protobuf:head_node_service_cc_proto",
        "@com_github_tencent_rapidjson//:rapidjson",
        "//src/ray/common:ray_config",
    ],
)

# Raft service handler (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "raft_service_handler",
    srcs = ["raft_service_handler.cc"],
    hdrs = ["raft_service_handler.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        ":raft_types",  # Move raft_types before raft_coordinator
        ":raft_coordinator",
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

# Raft coordinator (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "raft_coordinator",
    srcs = ["raft_coordinator.cc"],
    hdrs = ["raft_coordinator.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        ":raft_types",  # Move raft_types before raft_impl
        ":raft_impl",
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

# Monitoring (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "monitoring",
    srcs = ["monitoring.cc"],
    hdrs = ["monitoring.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        ":raft_types",  # Move raft_types before other dependencies
        ":service_discovery",
        ":load_balancer",
        ":state_sync",
        ":config_manager",
        ":metrics_manager",
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

# Service discovery (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "service_discovery",
    srcs = ["service_discovery.cc"],
    hdrs = ["service_discovery.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
    ],
)

# Load balancer (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "load_balancer",
    srcs = ["load_balancer.cc"],
    hdrs = ["load_balancer.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
    ],
)

# State sync (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "state_sync",
    srcs = ["state_sync.cc"],
    hdrs = ["state_sync.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        ":raft_types",  # Move raft_types before raft_coordinator
        ":raft_coordinator",
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
        "//src/ray/rpc:raft_client",
    ],
)

# Config manager (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "config_manager",
    srcs = ["config_manager.cc"],
    hdrs = ["config_manager.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
    ],
)

# Metrics manager (moved after gcs_server to avoid circular dependency)
ray_cc_library(
    name = "metrics_manager",
    srcs = ["metrics_manager.cc"],
    hdrs = ["metrics_manager.h"],
    deps = [
        ":gcs_server_fwd",  # Use forward declarations instead of full gcs_server
        "//src/ray/common:status",
        "//src/ray/common:asio",
        "//src/ray/protobuf:raft_cc_proto",
        "//src/ray/protobuf:gcs_service_cc_proto",
        "//src/ray/protobuf:head_node_service_cc_proto",
    ],
) 