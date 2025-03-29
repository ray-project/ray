load("//bazel:ray.bzl", "ray_cc_test")

package(default_visibility = ["//visibility:public"])

# Common dependencies for both tests
COMMON_DEPS = [
    # Core dependencies
    "//src/ray/common:status",
    "//src/ray/common:asio",
    "//src/ray/common:id",
    "//src/ray/common:test_util",
    "//src/ray/common:ray_config",
    "//src/ray/common:ray_syncer",
    "//src/ray/common:runtime_env_manager",
    "//src/ray/util:logging",
    "//src/ray/util:throttler",
    
    # Proto dependencies
    "//src/ray/protobuf:common_proto",
    "//src/ray/protobuf:raft_cc_proto",
    "//src/ray/protobuf:gcs_tables_cc_proto",
    "//src/ray/protobuf:gcs_service_cc_proto",
    "//src/ray/protobuf:head_node_service_cc_proto",
    "//src/ray/protobuf:gcs_cc_proto",
    
    # RPC dependencies
    "//src/ray/rpc:grpc_server",
    "//src/ray/rpc:raft_server",
    "//src/ray/rpc:raft_client",
    "//src/ray/rpc:client_call",
    "//src/ray/rpc:node_manager_client_pool",
    
    # GCS dependencies
    "//src/ray/gcs:gcs_pub_sub",
    "//src/ray/gcs:redis_client",
    
    # GCS Server dependencies - Raft components first
    "//src/ray/gcs/gcs_server:raft_types",
    "//src/ray/gcs/gcs_server:raft_network",
    "//src/ray/gcs/gcs_server:raft_storage",
    "//src/ray/gcs/gcs_server:raft_state_machine",
    "//src/ray/gcs/gcs_server:raft_impl",
    "//src/ray/gcs/gcs_server:raft_coordinator",
    "//src/ray/gcs/gcs_server:raft_gcs_coordinator",
    "//src/ray/gcs/gcs_server:raft_service_handler",
    
    # GCS Server dependencies - Core components
    "//src/ray/gcs/gcs_server:gcs_server_fwd",
    "//src/ray/gcs/gcs_server:gcs_server",
    "//src/ray/gcs/gcs_server:debug_tools",
    "//src/ray/gcs/gcs_server:service_discovery",
    "//src/ray/gcs/gcs_server:load_balancer",
    "//src/ray/gcs/gcs_server:state_sync",
    "//src/ray/gcs/gcs_server:config_manager",
    "//src/ray/gcs/gcs_server:metrics_manager",
    "//src/ray/gcs/gcs_server:monitoring",
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
    
    # Raylet dependencies
    "//src/ray/raylet:scheduling",
    
    # External dependencies
    "@com_github_tencent_rapidjson//:rapidjson",
    "@com_google_googletest//:gtest",
    "@com_google_googletest//:gtest_main",
    "@com_google_googletest//:gmock",
]

# Common copts for both tests
COMMON_COPTS = [
    "-I$(GENDIR)/src/ray/protobuf",
    "-I$(GENDIR)/external/com_google_googletest/googletest/include",
    "-I$(GENDIR)/external/com_google_googletest/googlemock/include",
    "-I$(GENDIR)/src/ray",
]

ray_cc_test(
    name = "gcs_server_test",
    srcs = ["gcs_server_test.cc"],
    deps = COMMON_DEPS,
    linkstatic = 1,
    copts = COMMON_COPTS,
)

ray_cc_test(
    name = "gcs_server_integration_test",
    srcs = ["gcs_server_integration_test.cc"],
    deps = COMMON_DEPS + [
        "//src/ray/rpc:head_node_client",  # Additional dependency for integration test
    ],
    linkstatic = 1,
    copts = COMMON_COPTS,
) 