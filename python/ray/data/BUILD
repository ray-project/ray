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

py_test(
    name = "test_preprocessors",
    size = "small",
    srcs = ["tests/test_preprocessors.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = ["//:ray_lib"],
)

py_test_module_list(
  files = glob(
    include=["tests/test_*.py"],
    exclude=["tests/test_preprocessors.py"]
  ),
  size = "large",
  extra_srcs = SRCS,
  tags = ["team:core", "exclusive"],
  deps = ["//:ray_lib"],
)
