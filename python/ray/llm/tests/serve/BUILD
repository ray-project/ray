load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
  files = glob(["configs/**/test_*.py"]),
  size = "small",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib"],
)
