load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "metric_with_open_telemetry_test",
    size = "small",
    srcs = ["metric_with_open_telemetry_test.cc"],
    env = {
        "RAY_experimental_enable_open_telemetry_on_core": "1",
    },
    tags = ["team:core"],
    deps = [
        "//src/ray/stats:stats_metric",
        "@com_google_googletest//:gtest_main",
    ],
)
