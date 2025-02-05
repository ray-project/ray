load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
  files = glob(["test_*.py"], exclude=["test_vllm_engine_stage.py"]),
  size = "small",
  tags = ["exclusive", "team:llm"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = glob(["test_vllm_engine_stage.py"]),
  size = "large",
  tags = ["exclusive", "gpu", "team:llm"],
  deps = ["//:ray_lib"],
)
