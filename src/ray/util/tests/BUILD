load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:ray.bzl", "COPTS")

cc_test(
    name = "container_util_test",
    size = "small",
    srcs = ["container_util_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "counter_test",
    size = "small",
    srcs = ["counter_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "event_test",
    size = "small",
    srcs = ["event_test.cc"],
    copts = COPTS,
    tags = [
        "no_tsan",
        "no_windows",
        "team:core",
    ],
    deps = [
        "//src/ray/util",
        "@boost//:range",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "exponential_backoff_test",
    size = "small",
    srcs = ["exponential_backoff_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "filesystem_test",
    size = "small",
    srcs = ["filesystem_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//:ray_common",
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "logging_test",
    size = "small",
    srcs = ["logging_test.cc"],
    args = [
        "--gtest_filter=PrintLogTest*",
        # Disable so we can test terminate handler.
        "--gtest_catch_exceptions=0",
    ],
    copts = COPTS,
    tags = [
        "no_ubsan",
        "team:core",
    ],
    deps = [
        "//src/ray/util",
        "@boost//:asio",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "sample_test",
    size = "small",
    srcs = ["sample_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//:ray_common",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "sequencer_test",
    size = "small",
    srcs = ["sequencer_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "signal_test",
    size = "small",
    srcs = ["signal_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//:raylet_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "throttler_test",
    size = "small",
    srcs = ["throttler_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "util_test",
    size = "small",
    srcs = ["util_test.cc"],
    copts = COPTS,
    tags = ["team:core"],
    deps = [
        "//src/ray/util",
        "@boost//:asio",
        "@boost//:process",
        "@com_google_googletest//:gtest_main",
    ],
)
