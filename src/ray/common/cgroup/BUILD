load("//bazel:ray.bzl", "ray_cc_library")

ray_cc_library(
    name = "cgroup_setup",
    srcs = ["cgroup_setup.cc"],
    hdrs = ["cgroup_setup.h"],
    deps = [
        "//src/ray/util",
        "@com_google_absl//absl/strings:str_format",
    ],
)
