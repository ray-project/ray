load("//bazel:ray.bzl", "COPTS", "ray_cc_test", "ray_cc_library")

ray_cc_test(
    name = "array_test",
    srcs = ["array_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)


ray_cc_test(
    name = "function_traits_test",
    srcs = ["function_traits_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "thread_checker_test",
    srcs = ["thread_checker_test.cc"],
    size = "small",
    tags = ["team:core"],
    deps = [
        "//src/ray/util:thread_checker",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "container_util_test",
    size = "small",
    srcs = ["container_util_test.cc"],
    tags = ["team:core"],
    linkstatic = True,
    deps = [
        "//src/ray/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "counter_test",
    size = "small",
    srcs = ["counter_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "event_test",
    size = "small",
    srcs = ["event_test.cc"],
    tags = [
        "no_tsan",
        "no_windows",
        "team:core",
    ],
    deps = [
        "//src/ray/util",
        "@boost//:range",
        "@com_google_googletest//:gtest_main",
        "//src/ray/protobuf:gcs_cc_proto",
    ],
)

ray_cc_test(
    name = "exponential_backoff_test",
    size = "small",
    srcs = ["exponential_backoff_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "filesystem_test",
    size = "small",
    srcs = ["filesystem_test.cc"],
    tags = ["team:core"],
    deps = [
        "//:ray_common",
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "logging_test",
    size = "small",
    srcs = ["logging_test.cc"],
    args = [
        "--gtest_filter=PrintLogTest*",
        # Disable so we can test terminate handler.
        "--gtest_catch_exceptions=0",
    ],
    tags = [
        "no_ubsan",
        "team:core",
    ],
    deps = [
        "//src/ray/util",
        "@boost//:asio",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
        "//src/ray/common:status",
    ],
)

ray_cc_test(
    name = "sample_test",
    size = "small",
    srcs = ["sample_test.cc"],
    tags = ["team:core"],
    deps = [
        "//:ray_common",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "sequencer_test",
    size = "small",
    srcs = ["sequencer_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "signal_test",
    size = "small",
    srcs = ["signal_test.cc"],
    tags = ["team:core"],
    deps = [
        "//:raylet_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "throttler_test",
    size = "small",
    srcs = ["throttler_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "util_test",
    size = "small",
    srcs = ["util_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@boost//:asio",
        "@boost//:process",
        "@com_google_googletest//:gtest_main",
    ],
)


ray_cc_test(
    name = "proto_schema_backward_compatibility_test",
    size = "small",
    srcs = ["proto_schema_backward_compatibility_test.cc"],
    tags = ["team:core"],
    deps = [
        "@boost//:range",
        "@com_google_googletest//:gtest_main",
        "//src/ray/protobuf:gcs_cc_proto",
    ],
)

ray_cc_test(
    name = "size_literals_test",
    srcs = ["size_literals_test.cc"],
    size = "small",
    tags = ["team:core"],
    deps = [
        "//src/ray/util:size_literals",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "shared_lru_test",
    srcs = ["shared_lru_test.cc"],
    deps = [
        "//src/ray/util:shared_lru",
        "@com_google_googletest//:gtest_main",
    ],
    size = "small",
    tags = ["team:core"],
)

ray_cc_library(
    name = "unix_test_utils",
    hdrs = ["unix_test_utils.h"],
    srcs = ["unix_test_utils.cc"],
    deps = [
        "//src/ray/util",
        "@boost//:scope_exit",
    ],
    testonly = True,
)

ray_cc_test(
    name = "pipe_logger_test",
    srcs = ["pipe_logger_test.cc"],
    deps = [
        ":unix_test_utils",
        "//src/ray/util",
        "//src/ray/util:pipe_logger",
        "@com_google_googletest//:gtest_main",
    ],
    size = "small",
    tags = ["team:core"],
)

ray_cc_test(
    name = "stream_redirection_utils_test",
    srcs = ["stream_redirection_utils_test.cc"],
    deps = [
        "//src/ray/util:stream_redirection_utils",
        ":unix_test_utils",
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
    size = "small",
    tags = ["team:core"],
)

ray_cc_test(
    name = "cmd_line_utils_test",
    srcs = ["cmd_line_utils_test.cc"],
    deps = [
        "//src/ray/util:cmd_line_utils",
        "@com_google_googletest//:gtest_main",
    ],
)
