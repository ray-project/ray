load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "cgroup_v2_utils_privileged_test",
    size = "small",
    srcs = ["cgroup_v2_utils_privileged_test.cc"],
    tags = [
        # TODO(hjiang, ibrahim, lonnie): Setup CI for cgroup testing environment.
        "manual",
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
        "//src/ray/common/test:testing",
        "@com_google_googletest//:gtest_main",
    ],
)

ray_cc_test(
    name = "cgroup_v2_setup_test",
    srcs = ["cgroup_v2_setup_test.cc"],
    tags = [
        # TODO(hjiang, ibrahim, lonnie): Setup CI for cgroup testing environment.
        "manual",
        "team:core",
    ],
    deps = [
        "//src/ray/common/cgroup:cgroup_setup",
        "//src/ray/common/cgroup:cgroup_utils",
        "//src/ray/common/test:testing",
        "//src/ray/util:container_util",
        "//src/ray/util:filesystem",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)
