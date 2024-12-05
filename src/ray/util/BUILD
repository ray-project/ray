load("//bazel:ray.bzl", "ray_cc_library")

ray_cc_library(
    name = "util",
    srcs = glob(
        [
            "*.cc",
        ],
        exclude = [
            "*_test.cc",
        ],
    ),
    hdrs = glob([
        "*.h",
    ]),
    linkopts = select({
        "@platforms//os:windows": [],
        "//conditions:default": ["-lpthread"],
    }),
    deps = [
        ":thread_checker",
        "//:aligned_alloc",
        "//:sha256",
        "//src/ray/protobuf:event_cc_proto",
        "//src/ray/protobuf:export_event_cc_proto",
        "@boost//:asio",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@nlohmann_json",
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
