load("//bazel:ray.bzl", "ray_cc_library")

ray_cc_library(
    name = "cgroup_setup",
    srcs = ["cgroup_setup.cc"],
    hdrs = ["cgroup_setup.h"],
    deps = [
        "//src/ray/util",
        "@abseil-cpp//absl/strings:str_format",
    ],
)

ray_cc_library(
    name = "cgroup_context",
    hdrs = ["cgroup_context.h"],
    deps = [
        "//src/ray/util:compat",
    ],
)

ray_cc_library(
    name = "cgroup_utils",
    srcs = ["cgroup_utils.cc"],
    hdrs = ["cgroup_utils.h"],
    deps = [
        ":cgroup_context",
        "//src/ray/util",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
    ],
)

ray_cc_library(
    name = "scoped_cgroup_handle",
    hdrs = ["scoped_cgroup_handle.h"],
)

ray_cc_library(
    name = "base_cgroup_setup",
    hdrs = ["base_cgroup_setup.h"],
    deps = [
        ":cgroup_context",
        ":scoped_cgroup_handle",
    ],
)

ray_cc_library(
    name = "fake_cgroup_setup",
    testonly = True,
    srcs = ["fake_cgroup_setup.cc"],
    hdrs = ["fake_cgroup_setup.h"],
    deps = [
        ":base_cgroup_setup",
        "//src/ray/common:status",
        "//src/ray/util:logging",
        "//src/ray/util:process",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@abseil-cpp//absl/synchronization",
    ],
)
