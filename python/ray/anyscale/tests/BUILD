load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    size = "small",
    files = glob(["test_*.py"]),
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)
