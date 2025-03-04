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
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
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
        "//src/ray/util:logging",
        "//src/ray/util:process",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/synchronization",
    ],
)
