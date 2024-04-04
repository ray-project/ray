load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    files = glob(["tests/test_*.py"]),
    size = "medium",
    tags = ["team:serve", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

# Test compact scheduling
py_test_module_list(
  name_suffix="_with_compact_scheduling",
  env={"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "1"},
  files = [
    "tests/test_deployment_scheduler.py",
    "tests/test_draining.py",
    "tests/test_az_routing.py",
    "tests/test_centralized_scheduler.py",
  ],
  size = "medium",
  tags = ["team:serve", "exclusive"],
  deps = ["//:ray_lib", ":conftest"],
)
