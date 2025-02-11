load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
  files = glob(["cpu/**/test_*.py"]),
  size = "small",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = glob(["gpu/**/test_*.py"]),
  size = "large",
  tags = ["exclusive", "gpu", "team:llm"],
  deps = ["//:ray_lib"],
)
