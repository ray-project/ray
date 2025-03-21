load("//bazel:ray.bzl", "ray_cc_test")

ray_cc_test(
    name = "stream_redirection_handle_test",
    srcs = ["stream_redirection_handle_test.cc"],
    tags = [
        "team:core",
        # TSAN fails to understand synchronization logic, from the stacktrace, it shows we flush
        # ostream concurrently at pipe dumper thread and main thread, which we have ordered
        # properly. Disable the complete test suite here since it always contains exactly one test
        # case.
        "no_tsan",
    ],
    deps = [
        "//src/ray/common/test:testing",
        "//src/ray/util",
        "//src/ray/util/internal:stream_redirection_handle",
        "@com_google_googletest//:gtest_main",
    ],
)
