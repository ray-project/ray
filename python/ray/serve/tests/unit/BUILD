load("//bazel:python.bzl", "py_test_run_all_subdirectory")

py_library(
    name = "conftest",
    srcs = ["conftest.py"],
)

py_test_run_all_subdirectory(
    size = "small",
    include = glob(["test_*.py"]),
    exclude = [],
    extra_srcs = [],
    tags = ["team:serve"],
    deps = [
        ":conftest",
        "//python/ray/serve:serve_lib",
        "//python/ray/serve/tests:common",
    ],
)
