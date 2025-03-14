load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    size = "small",
    files = glob(["tests/test_*.py"]),
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = ["//:ray_lib"],
)
