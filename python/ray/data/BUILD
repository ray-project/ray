# --------------------------------------------------------------------
# Tests from the python/ray/data/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

SRCS = [] + select({
    "@bazel_tools//src/conditions:windows": glob([
        "**/conftest.py",
    ]),
    "//conditions:default": [],
})

py_test_module_list(
  files = glob(["tests/test_*.py"]),
  size = "large",
  extra_srcs = SRCS,
  tags = ["team:core", "exclusive"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "tests/test_sort.py",
  ],
  size = "small",
  extra_srcs = SRCS,
  name_suffix = "_push_based",
  env = {"RAY_DATASET_PUSH_BASED_SHUFFLE": "1"},
  tags = ["team:core", "exclusive"],
  deps = ["//:ray_lib"],
)
