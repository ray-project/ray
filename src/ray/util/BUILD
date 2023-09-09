load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_proto_library", "cc_test")
load("//bazel:ray.bzl", "COPTS")

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
  name = "counter_map",
  hdrs = ["counter_map.h"],
  strip_include_prefix = "//src",
  deps = [
    ":logging",
    "@com_google_absl//absl/container:flat_hash_map",
    "@com_google_absl//absl/container:flat_hash_set",
    "@com_google_absl//absl/synchronization",
  ],
)

cc_library(
  name = "macros",
  hdrs = ["macros.h"],
  strip_include_prefix = "//src",
  deps = [],
)

cc_library(
  name = "logging",
  hdrs = [
   "event_label.h",
   "filesystem.h",
   "logging.h",
  ],
  srcs = [
    "filesystem.cc",
    "logging.cc",
  ],
  strip_include_prefix = "//src",
  deps = [
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_googletest//:gtest_main",
  ],
)

cc_library(
  name = "process",
  hdrs = ["process.h"],
  srcs = ["process.cc"],
  copts = COPTS,
  linkopts = select({
      "@bazel_tools//src/conditions:windows": [
      ],
      "//conditions:default": [
          "-lpthread",
      ],
  }),
  strip_include_prefix = "//src",
  deps = [
    ":logging",
    ":macros",
  ],
)

cc_library(
  name = "container_util",
  hdrs = ["container_util.h"],
  strip_include_prefix = "//src",
  deps = [
    ":logging",
    "@com_google_absl//absl/container:flat_hash_map",
    "@com_google_absl//absl/container:flat_hash_set",
  ],
)

cc_library(
  name = "util",
  hdrs = ["util.h"],
  srcs = ["util.cc"],
  strip_include_prefix = "//src",
  deps = [
    ":macros",
    ":logging",
    ":process",
    "@boost//:asio",
    "@com_google_absl//absl/container:flat_hash_map",
    "@com_google_absl//absl/random",
  ],
)

cc_library(
  name = "event",
  hdrs = ["event.h"],
  srcs = ["event.cc"],
  strip_include_prefix = "//src",
  deps = [
    ":logging",
    ":util",
    "//src/ray/protobuf:event_cc_proto",
    "@boost//:asio",
    "@com_github_spdlog//:spdlog",
    "@com_google_absl//absl/container:flat_hash_map",
    "@com_google_absl//absl/time",
    "@nlohmann_json",
  ],
)

cc_library(
  name = "exponential_backoff",
  hdrs = ["exponential_backoff.h"],
  srcs = ["exponential_backoff.cc"],
  strip_include_prefix = "//src",
  deps = [
    ":logging",
    "@com_google_googletest//:gtest_main",
  ],
)

cc_library(
  name = "memory",
  hdrs = ["memory.h"],
  srcs = ["memory.cc"],
  strip_include_prefix = "//src",
  deps = [],
)

cc_library(
  name = "ordered_set",
  hdrs = ["ordered_set.h"],
  strip_include_prefix = "//src",
  deps = [
    "@com_google_absl//absl/container:flat_hash_map",
  ],
)

cc_library(
  name = "sample",
  hdrs = ["sample.h"],
  strip_include_prefix = "//src",
  deps = [
    "@com_google_absl//absl/time",
  ],
)

cc_library(
  name = "sequencer",
  hdrs = ["sequencer.h"],
  strip_include_prefix = "//src",
  deps = [
    "@com_google_absl//absl/synchronization",
    "@com_google_absl//absl/container:flat_hash_map",
  ],
)

cc_library(
  name = "throttler",
  hdrs = ["throttler.h"],
  strip_include_prefix = "//src",
  deps = [
    "@com_google_absl//absl/time",
  ],
)

cc_library(
  name = "visibility",
  hdrs = ["visibility.h"],
  strip_include_prefix = "//src",
  deps = [],
)

cc_library(
    name = "bad",
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
    copts = COPTS,
    linkopts = select({
        "@bazel_tools//src/conditions:windows": [
        ],
        "//conditions:default": [
            "-lpthread",
        ],
    }),
    strip_include_prefix = "//src",
    visibility = ["//visibility:public"],
    deps = [
        ":logging",
        ":macros",
        ":process",
        "//:aligned_alloc",
        "//:sha256",
        "@boost//:asio",
        "@com_github_spdlog//:spdlog",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/random",
        "@com_google_googletest//:gtest_main",
        "@nlohmann_json",
    ],
)

