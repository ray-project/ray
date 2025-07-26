load("//bazel:python.bzl", "py_test_run_all_subdirectory")

filegroup(
    name = "serve_examples",
    srcs = glob(["*.py"]),
    visibility = ["//doc:__subpackages__"],
)

# GPU Tests
py_test_run_all_subdirectory(
    size = "large",
    include = ["*.py"],
    exclude = [],
    extra_srcs = [],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:llm",
    ],
    deps = ["//:ray_lib"],
)
