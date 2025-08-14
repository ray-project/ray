load("//bazel:ray.bzl", "ray_cc_library")

package(default_visibility = ["//src/ray/util:__subpackages__"])

ray_cc_library(
    name = "stream_redirection_handle",
    srcs = ["stream_redirection_handle.cc"],
    hdrs = ["stream_redirection_handle.h"],
    deps = [
        "//src/ray/util:compat",
        "//src/ray/util:pipe_logger",
        "//src/ray/util:scoped_dup2_wrapper",
        "//src/ray/util:stream_redirection_options",
    ],
)
