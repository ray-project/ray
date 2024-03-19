load("//bazel:python.bzl", "py_test_run_all_subdirectory")
load("//bazel:python.bzl", "py_test_module_list")

py_library(
  name = "conftest",
  srcs = ["conftest.py"],
)

py_test_run_all_subdirectory(
  include = glob(["test_*.py"]),
  exclude = [],
  extra_srcs = [],
  size = "small",
  tags = ["team:serve"],
  deps = ["//python/ray/serve:serve_lib", "//python/ray/serve/tests:common", ":conftest"],
)

py_test_module_list(
  name_suffix="_with_stop_fully_then_start_behavior",
  env={"RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS": "0"},
  files = [
    "test_deployment_state.py",
  ],
  size = "medium",
  tags = ["team:serve", "no_windows"],
  deps = ["//python/ray/serve:serve_lib", "//python/ray/serve/tests:common", ":conftest"],
)

py_test_module_list(
  name_suffix="_with_compact_scheduling",
  env={"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "1"},
  files = [
    "test_deployment_scheduler.py",
    "test_deployment_state.py",
  ],
  size = "medium",
  tags = ["no_windows", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", "//python/ray/serve/tests:common", ":conftest"],
)

py_test_module_list(
  name_suffix="_with_metr_disab",
  env={"RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE": "0"},
  files = [
    "test_deployment_state.py",
    "test_autoscaling_policy.py",
    "test_router.py",
  ],
  size = "medium",
  tags = ["team:serve", "no_windows"],
  deps = ["//python/ray/serve:serve_lib", "//python/ray/serve/tests:common", ":conftest"],
)
