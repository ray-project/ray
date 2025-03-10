load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"]
)

py_test_module_list(
    files = glob(["tests/test_*.py"], exclude=["tests/test_elastic_e2e.py"]),
    size = "small",
    tags = ["team:ml", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_elastic_e2e",
    size = "medium",
    srcs = ["tests/test_elastic_e2e.py"],
    tags = ["team:ml", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)
