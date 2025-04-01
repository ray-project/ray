load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "array_test",
    srcs = ["array_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:array",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "function_traits_test",
    srcs = ["function_traits_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:function_traits",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "thread_checker_test",
    size = "small",
    srcs = ["thread_checker_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:thread_checker",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "container_util_test",
    size = "small",
    srcs = ["container_util_test.cc"],
    linkstatic = True,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:container_util",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "counter_test",
    size = "small",
    srcs = ["counter_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:counter_map",
        "@googletest//:gtest_main",
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
        "//src/ray/common:ray_config",
        "//src/ray/protobuf:gcs_cc_proto",
        "//src/ray/util",
        "//src/ray/util:event",
        "@boost.range",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "exponential_backoff_test",
    size = "small",
    srcs = ["exponential_backoff_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:exponential_backoff",
        "@googletest//:gtest_main",
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
        "@googletest//:gtest_main",
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
        "//src/ray/common:status",
        "//src/ray/util",
        "//src/ray/util:env",
        "@abseil-cpp//absl/strings:str_format",
        "@boost.asio",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "sample_test",
    size = "small",
    srcs = ["sample_test.cc"],
    tags = ["team:core"],
    deps = [
        "//:ray_common",
        "//src/ray/util:sample",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "sequencer_test",
    size = "small",
    srcs = ["sequencer_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:sequencer",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "signal_test",
    size = "small",
    srcs = ["signal_test.cc"],
    tags = ["team:core"],
    deps = [
        "//:raylet_lib",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "throttler_test",
    size = "small",
    srcs = ["throttler_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "//src/ray/util:throttler",
        "@abseil-cpp//absl/time",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "util_test",
    size = "small",
    srcs = ["util_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@boost.asio",
        "@boost.process",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "proto_schema_backward_compatibility_test",
    size = "small",
    srcs = ["proto_schema_backward_compatibility_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/protobuf:gcs_cc_proto",
        "@boost.range",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "size_literals_test",
    size = "small",
    srcs = ["size_literals_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:size_literals",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "shared_lru_test",
    size = "small",
    srcs = ["shared_lru_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:shared_lru",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "scoped_env_setter_test",
    size = "small",
    srcs = ["scoped_env_setter_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:scoped_env_setter",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "pipe_logger_test",
    size = "small",
    srcs = ["pipe_logger_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common/test:testing",
        "//src/ray/util",
        "//src/ray/util:pipe_logger",
        "//src/ray/util:scoped_env_setter",
        "//src/ray/util:temporary_directory",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "stream_redirection_exit_test",
    size = "small",
    srcs = ["stream_redirection_exit_test.cc"],
    tags = [
        "team:core",
        # TSAN fails to understand synchroization logic, from the stacktrace, it shows we flush
        # ostream concurrently at pipe dumper thread and main thread, which we have ordered
        # properly. Disable the complete test suite here since it always contains exactly one test
        # case.
        "no_tsan",
    ],
    deps = [
        "//src/ray/common/test:testing",
        "//src/ray/util",
        "//src/ray/util:stream_redirection",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "cmd_line_utils_test",
    size = "small",
    srcs = ["cmd_line_utils_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:cmd_line_utils",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "spdlog_fd_sink_test",
    size = "small",
    srcs = ["spdlog_fd_sink_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:compat",
        "//src/ray/util:spdlog_fd_sink",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "spdlog_newliner_sink_test",
    size = "small",
    srcs = ["spdlog_newliner_sink_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common/test:testing",
        "//src/ray/util:filesystem",
        "//src/ray/util:spdlog_fd_sink",
        "//src/ray/util:spdlog_newliner_sink",
        "//src/ray/util:temporary_directory",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "temporary_directory_test",
    size = "small",
    srcs = ["temporary_directory_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:temporary_directory",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "compat_test",
    size = "small",
    srcs = ["compat_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:compat",
        "//src/ray/util:filesystem",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "process_cleanup_test",
    srcs = ["process_cleanup_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common/test:testing",
        "//src/ray/util",
        "//src/ray/util:filesystem",
        "//src/ray/util:process_cleaner",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "scoped_dup2_wrapper_test",
    size = "small",
    srcs = ["scoped_dup2_wrapper_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/common/test:testing",
        "//src/ray/util:compat",
        "//src/ray/util:filesystem",
        "//src/ray/util:scoped_dup2_wrapper",
        "//src/ray/util:temporary_directory",
        "@boost.iostreams",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "invoke_once_token_test",
    size = "small",
    srcs = ["invoke_once_token_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:invoke_once_token",
        "@googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "concurrent_flat_map_test",
    size = "small",
    srcs = ["concurrent_flat_map_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/util:concurrent_flat_map",
        "@googletest//:gtest_main",
    ],
)
