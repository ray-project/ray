load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
  files = glob(["**/test_*.py"]),
  size = "medium",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib"],
)
