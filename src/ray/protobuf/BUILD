package(default_visibility = ["//visibility:public"])

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_proto_library", "cc_test")
load("@rules_proto_grpc//python:defs.bzl", "python_grpc_compile")

proto_library(
    name = "monitor_proto",
    srcs = ["monitor.proto"],
)

cc_proto_library(
    name = "monitor_cc_proto",
    deps = [":monitor_proto"],
)

python_grpc_compile(
    name = "monitor_py_proto",
    deps = [":monitor_proto"],
)

proto_library(
    name = "common_proto",
    srcs = ["common.proto"],
    deps = [":runtime_env_common_proto"],
    visibility = ["//java:__subpackages__"],
)

cc_proto_library(
    name = "common_cc_proto",
    deps = [":common_proto"],
)

python_grpc_compile(
    name = "common_py_proto",
    deps = [":common_proto"],
)

proto_library(
    name = "gcs_proto",
    srcs = ["gcs.proto"],
    visibility = ["//java:__subpackages__"],
    deps = [
        ":common_proto",
        ":runtime_env_common_proto",
    ],
)

proto_library(
    name = "runtime_env_common_proto",
    srcs = ["runtime_env_common.proto"],
    visibility = ["//java:__subpackages__"],
)

proto_library(
    name = "ray_syncer_proto",
    srcs = ["ray_syncer.proto"],
)

cc_proto_library(
    name = "ray_syncer_cc_proto",
    deps = [":ray_syncer_proto"],
)

cc_proto_library(
    name = "runtime_env_common_cc_proto",
    deps = [":runtime_env_common_proto"],
)

python_grpc_compile(
    name = "runtime_env_common_py_proto",
    deps = [":runtime_env_common_proto"],
)

cc_proto_library(
    name = "gcs_cc_proto",
    deps = [":gcs_proto"],
)

python_grpc_compile(
    name = "gcs_py_proto",
    deps = [":gcs_proto"],
)

# Function and class dependencies.
proto_library(
    name = "dependency_proto",
    srcs = ["dependency.proto"],
)

python_grpc_compile(
    name = "dependency_py_proto",
    deps = [":dependency_proto"],
)

# Text logging.
proto_library(
    name = "logging_proto",
    srcs = ["logging.proto"],
)

cc_proto_library(
    name = "logging_cc_proto",
    deps = [":logging_proto"],
)

python_grpc_compile(
    name = "logging_py_proto",
    deps = [":logging_proto"],
)

proto_library(
    name = "node_manager_proto",
    srcs = ["node_manager.proto"],
    deps = [
      ":common_proto",
      ":gcs_proto",
    ],
)

cc_proto_library(
    name = "node_manager_cc_proto",
    deps = [":node_manager_proto"],
)

python_grpc_compile(
    name = "node_manager_py_proto",
    deps = [":node_manager_proto"],
)

proto_library(
    name = "reporter_proto",
    srcs = ["reporter.proto"],
    deps = [
        ":common_proto",
        "@io_opencensus_proto//opencensus/proto/metrics/v1:metrics_proto"
    ],
)

cc_proto_library(
    name = "reporter_cc_proto",
    deps = [":reporter_proto"],
)

python_grpc_compile(
    name = "reporter_py_proto",
    deps = [":reporter_proto"],
)

proto_library(
    name = "gcs_service_proto",
    srcs = ["gcs_service.proto"],
    deps = [
        ":common_proto",
        ":gcs_proto",
        ":pubsub_proto",
    ],
)

cc_proto_library(
    name = "gcs_service_cc_proto",
    deps = [":gcs_service_proto"],
)

python_grpc_compile(
    name = "gcs_service_py_proto",
    deps = [":gcs_service_proto"],
)

proto_library(
    name = "test_service_proto",
    srcs = ["test_service.proto"],
    deps = [
        ":common_proto",
        ":gcs_proto",
    ],
)

cc_proto_library(
    name = "test_service_cc_proto",
    deps = [":test_service_proto"],
)

python_grpc_compile(
    name = "test_service_py_proto",
    deps = [":test_service_proto"],
)

proto_library(
    name = "object_manager_proto",
    srcs = ["object_manager.proto"],
    deps = [":common_proto"],
)

cc_proto_library(
    name = "object_manager_cc_proto",
    deps = [":object_manager_proto"],
)

proto_library(
    name = "core_worker_proto",
    srcs = ["core_worker.proto"],
    deps = [
        ":common_proto",
        ":gcs_service_proto",
        ":pubsub_proto",
    ],
)

python_grpc_compile(
    name = "core_worker_py_proto",
    deps = [":core_worker_proto"],
)

cc_proto_library(
    name = "worker_cc_proto",
    deps = ["core_worker_proto"],
)

proto_library(
    name = "serialization_proto",
    srcs = ["serialization.proto"],
)

cc_proto_library(
    name = "serialization_cc_proto",
    deps = ["serialization_proto"],
)

proto_library(
    name = "event_proto",
    srcs = ["event.proto"],
)

cc_proto_library(
    name = "event_cc_proto",
    deps = [":event_proto"],
)

python_grpc_compile(
    name = "event_py_proto",
    deps = [":event_proto"],
)

# Job agent.
proto_library(
    name = "job_agent_proto",
    srcs = ["job_agent.proto"],
    deps = [":agent_manager_proto"],
)

python_grpc_compile(
    name = "job_agent_py_proto",
    deps = [":job_agent_proto"],
)

# Agent manager gRPC lib.
proto_library(
    name = "agent_manager_proto",
    srcs = ["agent_manager.proto"],
    deps = [],
)

python_grpc_compile(
    name = "agent_manager_py_proto",
    deps = [":agent_manager_proto"],
)

cc_proto_library(
    name = "agent_manager_cc_proto",
    deps = [":agent_manager_proto"],
)

# Ray Client gRPC lib
proto_library(
    name = "ray_client_proto",
    srcs = ["ray_client.proto"],
    deps = [":common_proto"],
)

python_grpc_compile(
    name = "ray_client_py_proto",
    deps = [":ray_client_proto"],
)

# Pubsub
proto_library(
    name = "pubsub_proto",
    srcs = ["pubsub.proto"],
    deps = [
        ":common_proto",
        ":dependency_proto",
        ":gcs_proto",
        ":logging_proto",
        ":reporter_proto",
    ],
)

cc_proto_library(
    name = "pubsub_cc_proto",
    deps = [":pubsub_proto"],
)

# runtime env agent gRPC lib.
proto_library(
    name = "runtime_env_agent_proto",
    srcs = ["runtime_env_agent.proto"],
    deps = [
        ":common_proto",
        ":agent_manager_proto",
        ":runtime_env_common_proto",
    ],
)

python_grpc_compile(
    name = "runtime_env_agent_py_proto",
    deps = [":runtime_env_agent_proto"],
)

cc_proto_library(
    name = "runtime_env_agent_cc_proto",
    deps = [":runtime_env_agent_proto"],
)

proto_library(
    name = "serve_proto",
    srcs = ["serve.proto"],
    visibility = ["//java:__subpackages__"],
)

python_grpc_compile(
    name = "serve_py_proto",
    deps = [":serve_proto"],
)

proto_library(
    name = "usage_proto",
    srcs = ["usage.proto"],
)

python_grpc_compile(
    name = "usage_py_proto",
    deps = [":usage_proto"],
)

cc_proto_library(
    name = "usage_cc_proto",
    deps = [":usage_proto"],
)
