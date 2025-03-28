load("//bazel:ray.bzl", "ray_cc_library")

package(default_visibility = ["//visibility:public"])

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
        "@boost//:asio",
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
    ],
)

ray_cc_library(
    name = "raft_gcs_coordinator",
    srcs = ["raft_gcs_coordinator.cc"],
    hdrs = ["raft_gcs_coordinator.h"],
    deps = [
        ":raft_state_machine",
        ":raft_types",
        "//src/ray/common:asio",
        "//src/ray/common:status",
        "//src/ray/protobuf:raft_cc_proto",
    ],
) 