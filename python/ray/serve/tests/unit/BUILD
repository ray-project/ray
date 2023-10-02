load("//bazel:python.bzl", "py_test_run_all_subdirectory")

py_library(
  name = "conftest",
  srcs = ["conftest.py"],
)

py_test_run_all_subdirectory(
  include = glob(["test_*.py"]),
  exclude = [],
  extra_srcs = [],
  size = "small",
  tags = ["team:serve"],
  deps = ["//python/ray/serve:serve_lib", "//python/ray/serve/tests:common", ":conftest"],
)
