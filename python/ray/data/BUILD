# --------------------------------------------------------------------
# Tests from the python/ray/data/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

py_test(
    name = "test_preprocessors",
    size = "small",
    srcs = ["tests/test_preprocessors.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = glob(
    include=["tests/test_*.py"],
    exclude=["tests/test_preprocessors.py"]
  ),
  size = "large",
  tags = ["team:core", "exclusive"],
  deps = ["//:ray_lib", ":conftest"],
)
