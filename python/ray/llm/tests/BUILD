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
    files = glob(["common/**/test_*.py"]),
    tags = [
        "cpu",
        "exclusive",
        "team:llm",
    ],
    deps = ["//:ray_lib"],
)

# Batch test
py_test_module_list(
    size = "medium",
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
    files = glob(
        ["batch/gpu/**/test_*.py"],
    ),
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
# CPU tests
py_test_module_list(
    size = "large",
    data = glob(["serve/**/*.yaml"]),
    files = glob(["serve/cpu/**/test_*.py"]),
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
    files = glob(["serve/gpu/**/test_*.py"]),
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
