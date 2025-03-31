load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "cgroup_v2_utils_privileged_test",
    size = "small",
    srcs = ["cgroup_v2_utils_privileged_test.cc"],
    tags = [
        "cgroup",
        "exclusive",
        "team:core",
    ],
    deps = [
        "//src/ray/common/cgroup:cgroup_setup",
        "//src/ray/common/test:testing",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "cgroup_v2_utils_unprivileged_test",
    size = "small",
    srcs = ["cgroup_v2_utils_unprivileged_test.cc"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [
        "//src/ray/common/cgroup:cgroup_setup",
        "//src/ray/common/test:testing",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "fake_cgroup_setup_test",
    srcs = ["fake_cgroup_setup_test.cc"],
    tags = [
        "team:core",
    ],
    deps = [
        "//src/ray/common/cgroup:fake_cgroup_setup",
        "@com_google_googletest//:gtest_main",
    ],
)
