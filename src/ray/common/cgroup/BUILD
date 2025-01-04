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

ray_cc_library(
    name = "cgroup_context",
    hdrs = ["cgroup_context.h"],
)

ray_cc_library(
    name = "cgroup_utils",
    srcs = ["cgroup_utils.cc"],
    hdrs = ["cgroup_utils.h"],
    deps = [
        ":cgroup_context",
        "//src/ray/util",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings",
    ],
)
