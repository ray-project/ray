load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "open_telemetry_metric_recorder_test",
    size = "small",
    srcs = ["open_telemetry_metric_recorder_test.cc"],
    tags = ["team:core"],
    deps = [
        "//src/ray/telemetry:open_telemetry_metric_recorder",
        "@com_google_googletest//:gtest_main",
    ],
)
