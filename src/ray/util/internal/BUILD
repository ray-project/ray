load("//bazel:ray.bzl", "ray_cc_library")

package(default_visibility = ["//src/ray/util:__subpackages__"])

ray_cc_library(
    name = "stream_redirection_utils",
    srcs = ["stream_redirection_utils.cc"],
    hdrs = ["stream_redirection_utils.h"],
    deps = [
        "//src/ray/util:compat",
        "//src/ray/util:pipe_logger",
        "//src/ray/util:scoped_dup2_wrapper",
        "//src/ray/util:stream_redirection_options",
    ],
)
