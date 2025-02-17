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
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = glob(["batch/gpu/**/test_*.py"]),
  size = "large",
  tags = ["exclusive", "gpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
  env = {
    "VLLM_FLASH_ATTN_VERSION": "2",
  }
)
