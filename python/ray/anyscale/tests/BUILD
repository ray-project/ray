load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
  files = [
    "test_runtime_env_image_uri.py",
    "test_logging.py",
  ],
  size = "small",
  tags = ["exclusive", "team:core"],
  deps = ["//:ray_lib"],
)
