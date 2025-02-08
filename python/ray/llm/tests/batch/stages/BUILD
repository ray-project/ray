load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
  files = glob(["test_*.py"]),
  size = "small",
  tags = ["exclusive", "team:llm"],
  deps = ["//:ray_lib"],
)
