load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    size = "small",
    files = [
        "test_logging.py",
        "test_prestart.py",
        "test_usage_stats.py",
    ],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)

py_test_module_list(
    size = "medium",
    files = [
        "test_runtime_env_image_uri.py",
    ],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)
