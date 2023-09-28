load("//bazel:python.bzl", "py_test_run_all_subdirectory")

py_test_run_all_subdirectory(
  include = ["*.py"],
  exclude = ["conftext.py"],
  extra_srcs = ["conftest.py"],
  size = "small",
  tags = ["exclusive", "manual", "team:serve"],
  deps = ["//:serve_lib"],
)
