load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = glob(["**/conftest.py"]),
    visibility = [
        "//python/ray/llm/tests:__subpackages__",
    ],
)

# Common tests
py_test_module_list(
    size = "small",
    files = glob(["common/**/test_cloud_utils.py"]),
    tags = [
        "cpu",
        "exclusive",
        "team:llm",
    ],
    deps = ["//:ray_lib"],
)

# Batch test
py_test_module_list(
    size = "small",
    files = glob([
        "batch/cpu/**/test_*.py",
        "batch/observability/usage_telemetry/test_*.py",
    ]),
    tags = [
        "cpu",
        "exclusive",
        "team:llm",
    ],
    deps = ["//:ray_lib"],
)

py_test_module_list(
    size = "large",
    env = {
        "VLLM_FLASH_ATTN_VERSION": "2",
    },
    files = glob(["batch/gpu/**/test_*.py"]),
    tags = [
        "exclusive",
        "gpu",
        "team:llm",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

# Serve test
# Small tests
py_test_module_list(
    size = "small",
    data = glob(["serve/**/*.yaml"]),
    files = [
        "serve/config_generator/test_input_converter.py",
        "serve/config_generator/test_text_completion.py",
        "serve/configs/test_json_mode_utils.py",
        "serve/configs/test_models.py",
        "serve/configs/test_prompt_formats.py",
        "serve/deployments/llm/multiplex/test_lora_model_loader.py",
        "serve/deployments/llm/multiplex/test_multiplex_deployment.py",
        "serve/deployments/llm/multiplex/test_multiplex_utils.py",
        "serve/deployments/llm/test_image_retriever.py",
        "serve/deployments/llm/vllm/test_vllm_engine.py",
        "serve/deployments/test_streaming_error_handler.py",
        "serve/observability/usage_telemetry/test_usage.py",
    ],
    tags = [
        "cpu",
        "exclusive",
        "team:llm",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

# Medium tests
py_test_module_list(
    size = "medium",
    data = glob(["serve/**/*.yaml"]),
    files = [
        "serve/builders/test_application_builders.py",
        "serve/deployments/llm/multiplex/test_lora_deployment_base_client.py",
    ],
    tags = [
        "cpu",
        "exclusive",
        "team:llm",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

# Large GPU tests
py_test_module_list(
    size = "large",
    data = glob(["serve/**/*.yaml"]),
    files = [
        "serve/integration/test_openai_compatibility.py",
        "serve/integration/test_openai_compatibility_no_accelerator_type.py",
    ],
    tags = [
        "exclusive",
        "gpu",
        "team:llm",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
