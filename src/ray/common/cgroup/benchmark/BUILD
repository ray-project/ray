load("//bazel:ray.bzl", "ray_cc_binary")

ray_cc_binary(
    name = "app_cgroup_benchmark",
    srcs = ["app_cgroup_benchmark.cc"],
    deps = [
        "//src/ray/common/cgroup:cgroup_utils",
        "//src/ray/common/cgroup:constants",
        "//src/ray/util:logging",
        "@benchmark",
    ],
)
