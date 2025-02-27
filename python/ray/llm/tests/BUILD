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

# Serve test
# Small tests
py_test_module_list(
  files = [
    "serve/configs/test_json_mode_utils.py",
    "serve/configs/test_models.py",
    "serve/configs/test_prompt_formats.py",
    "serve/deployments/llm/multiplex/test_lora_model_loader.py",
    "serve/deployments/llm/multiplex/test_multiplex_deployment.py",
    "serve/deployments/llm/multiplex/test_multiplex_utils.py",
    "serve/deployments/llm/test_image_retriever.py",
    "serve/deployments/test_streaming_error_handler.py",
    "serve/observability/usage_telemetry/test_usage.py",
    "serve/deployments/llm/vllm/test_vllm_engine.py",
    "serve/utils/test_cloud_utils.py",
  ],
  data = glob(["serve/**/*.yaml"]),
  size = "small",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
)

# Medium tests
py_test_module_list(
  files = [
    "serve/builders/test_application_builders.py",
    "serve/deployments/llm/multiplex/test_lora_deployment_base_client.py",
  ],
  data = glob(["serve/**/*.yaml"]),
  size = "medium",
  tags = ["exclusive", "cpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
)

# Large GPU tests
py_test_module_list(
  files = [
    "serve/integration/test_openai_compatibility.py",
  ],
  data = glob(["serve/**/*.yaml"]),
  size = "large",
  tags = ["exclusive", "gpu", "team:llm"],
  deps = ["//:ray_lib", ":conftest"],
)
