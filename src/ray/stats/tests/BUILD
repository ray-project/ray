load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "opentelemetry_metrics_test",
    size = "small",
    srcs = ["opentelemetry_metrics_test.cc"],
    tags = ["team:core"],
    deps = [
        "//:stats_opentelemetry",
        "@com_google_googletest//:gtest_main",
    ],
)
