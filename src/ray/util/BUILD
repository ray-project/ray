load("//bazel:ray.bzl", "ray_cc_library")

ray_cc_library(
    name = "visibility",
    hdrs = ["visibility.h"],
)

ray_cc_library(
    name = "macros",
    hdrs = ["macros.h"],
)

ray_cc_library(
    name = "event_label",
    hdrs = ["event_label.h"],
)

ray_cc_library(
    name = "array",
    hdrs = ["array.h"],
)

ray_cc_library(
    name = "thread_utils",
    hdrs = ["thread_utils.h"],
    deps = [
        ":thread_checker",
    ],
)

ray_cc_library(
    name = "env",
    srcs = ["env.cc"],
    hdrs = ["env.h"],
    deps = [
        ":logging",
        "@com_google_absl//absl/strings",
    ],
)

ray_cc_library(
    name = "exponential_backoff",
    srcs = ["exponential_backoff.cc"],
    hdrs = ["exponential_backoff.h"],
    deps = [
        ":logging",
    ],
)

ray_cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    deps = [
        ":event_label",
        ":macros",
        ":string_utils",
        ":thread_utils",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_prod",
        "@nlohmann_json",
    ],
)

ray_cc_library(
    name = "filesystem",
    srcs = ["filesystem.cc"],
    hdrs = ["filesystem.h"],
    deps = [
        "//src/ray/common:status_or",
    ],
)

ray_cc_library(
    name = "container_util",
    hdrs = ["container_util.h"],
    deps = [
        ":logging",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
    ],
)

ray_cc_library(
    name = "process",
    srcs = [
        "process.cc",
        "subreaper.cc",
    ],
    hdrs = [
        "process.h",
        "subreaper.h",
    ],
    deps = [
        ":cmd_line_utils",
        ":compat",
        ":filesystem",
        ":logging",
        ":macros",
        "@boost//:asio",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/synchronization",
    ],
)

ray_cc_library(
    name = "function_traits",
    hdrs = ["function_traits.h"],
    deps = [
        "@boost//:functional",
    ],
)

ray_cc_library(
    name = "counter_map",
    hdrs = ["counter_map.h"],
    deps = [
        ":logging",
        ":mutex_protected",
    ],
)

ray_cc_library(
    name = "event",
    srcs = ["event.cc"],
    hdrs = ["event.h"],
    deps = [
        ":logging",
        ":random",
        ":string_utils",
        ":timestamp_utils",
        "//src/ray/protobuf:event_cc_proto",
        "//src/ray/protobuf:export_event_cc_proto",
        "@boost//:asio",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_googletest//:gtest_prod",
        "@com_google_protobuf//:protobuf",
        "@nlohmann_json",
    ],
)

ray_cc_library(
    name = "scoped_env_setter",
    srcs = ["scoped_env_setter.cc"],
    hdrs = ["scoped_env_setter.h"],
    deps = [
        ":env",
    ],
)

ray_cc_library(
    name = "timestamp_utils",
    hdrs = ["timestamp_utils.h"],
)

ray_cc_library(
    name = "random",
    hdrs = ["random.h"],
    deps = [
        "@com_google_absl//absl/random",
    ],
)

ray_cc_library(
    name = "string_utils",
    srcs = ["string_utils.cc"],
    hdrs = ["string_utils.h"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

ray_cc_library(
    name = "memory",
    srcs = ["memory.cc"],
    hdrs = ["memory.h"],
)

ray_cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
)

ray_cc_library(
    name = "throttler",
    hdrs = ["throttler.h"],
    deps = [
        "@com_google_absl//absl/time",
    ],
)

ray_cc_library(
    name = "sequencer",
    hdrs = ["sequencer.h"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
    ],
)

ray_cc_library(
    name = "sample",
    hdrs = ["sample.h"],
)

ray_cc_library(
    name = "cmd_line_utils",
    srcs = ["cmd_line_utils.cc"],
    hdrs = ["cmd_line_utils.h"],
    deps = [
        ":logging",
        ":string_utils",
    ],
)

# TODO(hjiang): Split URL related functions into a separate util target.
ray_cc_library(
    name = "util",
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    deps = [
        ":filesystem",
        ":logging",
        ":macros",
        ":process",
        "//:sha256",
        "@boost//:asio",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

ray_cc_library(
    name = "size_literals",
    hdrs = ["size_literals.h"],
)

ray_cc_library(
    name = "thread_checker",
    srcs = ["thread_checker.cc"],
    hdrs = ["thread_checker.h"],
)

ray_cc_library(
    name = "map_utils",
    hdrs = ["map_utils.h"],
)

ray_cc_library(
    name = "shared_lru",
    hdrs = ["shared_lru.h"],
    deps = [
        ":map_utils",
        ":util",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

ray_cc_library(
    name = "compat",
    srcs = ["compat.cc"],
    hdrs = ["compat.h"],
    deps = [
        ":logging",
        "//src/ray/common:status",
    ],
)

ray_cc_library(
    name = "stream_redirection_options",
    hdrs = ["stream_redirection_options.h"],
)

ray_cc_library(
    name = "pipe_logger",
    srcs = ["pipe_logger.cc"],
    hdrs = ["pipe_logger.h"],
    deps = [
        ":compat",
        ":spdlog_fd_sink",
        ":spdlog_newliner_sink",
        ":stream_redirection_options",
        ":thread_utils",
        ":util",
        "//src/ray/common:ray_config",
        "@boost//:iostreams",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/strings",
    ],
)

ray_cc_library(
    name = "stream_redirection_utils",
    srcs = ["stream_redirection_utils.cc"],
    hdrs = ["stream_redirection_utils.h"],
    deps = [
        ":pipe_logger",
        ":stream_redirection_options",
        ":util",
    ],
)

ray_cc_library(
    name = "spdlog_fd_sink",
    hdrs = ["spdlog_fd_sink.h"],
    deps = [
        ":compat",
        ":util",
        "@com_github_spdlog//:spdlog",
    ],
)

ray_cc_library(
    name = "spdlog_newliner_sink",
    hdrs = ["spdlog_newliner_sink.h"],
    deps = [
        ":compat",
        ":util",
        "@com_github_spdlog//:spdlog",
    ],
)

ray_cc_library(
    name = "temporary_directory",
    srcs = ["temporary_directory.cc"],
    hdrs = ["temporary_directory.h"],
    deps = [
        ":util",
        "@com_google_absl//absl/strings:str_format",
    ],
)

ray_cc_library(
    name = "mutex_protected",
    hdrs = ["mutex_protected.h"],
    deps = [
        "@com_google_absl//absl/synchronization",
    ],
)

ray_cc_library(
    name = "scoped_dup2_wrapper",
    srcs = select({
        "@platforms//os:windows": ["scoped_dup2_wrapper_windows.cc"],
        "//conditions:default": ["scoped_dup2_wrapper_posix.cc"],
    }),
    hdrs = ["scoped_dup2_wrapper.h"],
    deps = [
        ":compat",
        ":logging",
    ],
)

ray_cc_library(
    name = "process_cleaner",
    srcs = ["process_cleaner.cc"],
    hdrs = ["process_cleaner.h"],
    deps = [":invoke_once_token"] +
           select({
               "@platforms//os:windows": [],
               "//conditions:default": ["//src/ray/util:logging"],
           }),
)

ray_cc_library(
    name = "invoke_once_token",
    hdrs = ["invoke_once_token.h"],
    deps = [
        ":logging",
    ],
)
