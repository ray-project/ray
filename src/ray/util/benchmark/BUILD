load("//bazel:ray.bzl", "ray_cc_binary")

ray_cc_binary(
    name = "cord_benchmark",
    srcs = ["cord_benchmark.cc"],
    deps = [
        "@benchmark",
        "@com_google_absl//absl/strings:cord",
    ],
)
