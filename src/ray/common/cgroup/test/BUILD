load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "cgroup_v2_utils_test",
    size = "small",
    srcs = ["cgroup_v2_utils_test.cc"],
    tags = [
        "manual",  # Expect cgroupv2 mount, not intended for unit test and CI.
        "team:core",
    ],
    deps = [
        "//src/ray/common/cgroup:cgroup_setup",
        "@com_google_googletest//:gtest_main",
    ],
)
