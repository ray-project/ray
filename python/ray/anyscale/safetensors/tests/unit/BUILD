load("//bazel:python.bzl", "py_test_run_all_subdirectory")

py_test_run_all_subdirectory(
    size = "small",
    include = glob(["test_*.py"]),
    exclude = [],
    extra_srcs = [],
    tags = ["team:serve"],
    deps = ["//:ray_lib"],
)
