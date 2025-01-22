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
    name = "exponential_backoff",
    hdrs = ["exponential_backoff.h"],
    srcs = ["exponential_backoff.cc"],
    deps = [
        ":logging",
    ],
)

# TODO(hjiang): filesystem and logging has interdependency, we should split them into three targets: filesystem, logging, ray_check_macros.
ray_cc_library(
    name = "logging",
    hdrs = [
        "filesystem.h",
        "logging.h",
    ],
    srcs = [
        "filesystem.cc",
        "logging.cc",
    ],
    deps = [
        ":event_label",
        ":macros",
        ":thread_utils",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
        "@nlohmann_json",
    ],
)

ray_cc_library(
    name = "container_util",
    hdrs = ["container_util.h"],
    deps = [
        ":logging",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

ray_cc_library(
    name = "process",
    hdrs = [
        "process.h",
        "subreaper.h",
    ],
    srcs = [
        "process.cc",
        "subreaper.cc",
    ],
    deps = [
        ":cmd_line_utils",
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
    ],
)

ray_cc_library(
    name = "event",
    hdrs = ["event.h"],
    srcs = ["event.cc"],
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
    hdrs = ["string_utils.h"],
    srcs = ["string_utils.cc"],
)

ray_cc_library(
    name = "memory",
    hdrs = ["memory.h"],
    srcs = ["memory.cc"],
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
    hdrs = ["cmd_line_utils.h"],
    srcs = ["cmd_line_utils.cc"],
    deps = [
        ":logging",
        ":string_utils",
    ],
)

# A giant 'util' target is split since PR https://github.com/ray-project/ray/pull/49938, here we keep the 'util' target for API compatibility.
#
# TODO(hjiang): We include a bunch of misc util function/class inside of the class, should split into multiple files and build targets.
ray_cc_library(
    name = "util",
    hdrs = ["util.h"],
    srcs = ["util.cc"],
    deps = [
        ":array",
        ":cmd_line_utils",
        ":container_util",
        ":counter_map",
        ":event",
        ":event_label",
        ":exponential_backoff",
        ":function_traits",
        ":logging",
        ":macros",
        ":memory",
        ":process",
        ":random",
        ":sample",
        ":sequencer",
        ":string_utils",
        ":timestamp_utils",
        ":throttler",
        ":thread_utils",
        ":type_traits",
        ":visibility",
        "//:sha256",
    ],
)

ray_cc_library(
    name = "size_literals",
    hdrs = ["size_literals.h"],
)

ray_cc_library(
    name = "thread_checker",
    hdrs = ["thread_checker.h"],
    srcs = ["thread_checker.cc"],

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
    hdrs = ["compat.h"],
)

ray_cc_library(
    name = "stream_redirection_options",
    hdrs = ["stream_redirection_options.h"],
)

ray_cc_library(
    name = "pipe_logger",
    hdrs = ["pipe_logger.h"],
    srcs = ["pipe_logger.cc"],
    deps = [
        ":compat",
        ":stream_redirection_options",
        ":thread_utils",
        ":util",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/strings",
    ],
)

ray_cc_library(
    name = "stream_redirection_utils",
    hdrs = ["stream_redirection_utils.h"],
    srcs = ["stream_redirection_utils.cc"],
    deps = [
        ":pipe_logger",
        ":stream_redirection_options",
        ":util",
    ],
)
