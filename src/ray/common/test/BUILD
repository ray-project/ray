load("//bazel:ray.bzl", "ray_cc_binary", "ray_cc_library", "ray_cc_test")

ray_cc_test(
    name = "resource_request_test",
    size = "small",
    srcs = [
        "resource_request_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:task_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "resource_set_test",
    size = "small",
    srcs = [
        "resource_set_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:task_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "resource_instance_set_test",
    size = "small",
    srcs = [
        "resource_instance_set_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:task_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "ray_syncer_test",
    srcs = ["ray_syncer_test.cc"],
    tags = [
        "no_tsan",
        "no_ubsan",
        "no_windows",
        "team:core",
    ],
    deps = [
        "//:grpc_common_lib",
        "//:ray_mock_syncer",
        "//src/ray/common:ray_syncer",
        "@com_google_googletest//:gtest",
    ],
)

ray_cc_test(
    name = "asio_defer_test",
    size = "small",
    srcs = ["asio_defer_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:asio",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "event_stats_test",
    size = "small",
    srcs = ["event_stats_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:event_stats",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "ray_config_test",
    size = "small",
    srcs = ["ray_config_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:grpc_util",
        "//src/ray/common:ray_config",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "postable_test",
    size = "small",
    srcs = ["postable_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:asio",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "id_test",
    size = "small",
    srcs = ["id_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:id",
        "//src/ray/common:task_common",
        "//src/ray/protobuf:common_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_binary(
    name = "syncer_service_e2e_test",
    srcs = ["syncer_service_e2e_test.cc"],
    deps = [
        "//src/ray/common:ray_syncer",
    ],
)

ray_cc_test(
    name = "task_spec_test",
    srcs = ["task_spec_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:task_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "bundle_location_index_test",
    srcs = [
        "bundle_location_index_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:task_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "client_connection_test",
    size = "small",
    srcs = ["client_connection_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:asio",
        "//src/ray/common:id",
        "//src/ray/common:network",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_library(
    name = "testing",
    testonly = True,
    hdrs = ["testing.h"],
    deps = ["//src/ray/util:macros"],
)

ray_cc_test(
    name = "status_test",
    size = "small",
    srcs = [
        "status_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//:grpc_common_lib",
        "//src/ray/common:grpc_util",
        "//src/ray/common:status",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "status_or_test",
    size = "small",
    srcs = ["status_or_test.cc"],
    tags = ["team:core"],
    deps = [
        ":testing",
        "//src/ray/common:status_or",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "memory_monitor_test",
    size = "small",
    srcs = [
        "memory_monitor_test.cc",
    ],
    tags = [
        "no_windows",
        "team:core",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//src/ray/common:id",
        "//src/ray/common:memory_monitor",
        "@boost//:filesystem",
        "@boost//:thread",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "scheduling_ids_test",
    size = "small",
    srcs = [
        "scheduling_ids_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:task_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "grpc_util_test",
    size = "small",
    srcs = [
        "grpc_util_test.cc",
    ],
    tags = ["team:core"],
    deps = [
        "//src/ray/common:grpc_util",
        "//src/ray/protobuf:common_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)
