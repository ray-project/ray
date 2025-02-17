load("//bazel:python.bzl", "py_test_module_list")

py_library(
  name = "conftest",
  srcs = glob(["**/conftest.py"]),
  visibility = [
    "//python/ray/llm/tests:__subpackages__",
  ],
)

# Batch test
py_test_module_list(
  files = glob(["batch/cpu/**/test_*.py"]),
  size = "small",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = glob(["batch/gpu/**/test_*.py"]),
  size = "large",
  tags = ["exclusive", "gpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
)

# Serve test
py_test_module_list(
  files = glob(["serve/**/test_*.py"]),
  data = glob(["serve/**/*.yaml"]),
  size = "small",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
)
