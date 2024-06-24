load("//bazel:python.bzl", "doctest")
load("//bazel:python.bzl", "py_test_module_list")

doctest(
    files = glob(["**/*.py"], exclude=["**/experimental/**/*.py"]),
    tags = ["team:core"],
    deps = [":dag_lib"]
)


# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dag_lib",
    srcs = glob(["**/*.py"], exclude=["tests/**/*.py"]),
    visibility = [
        "//python/ray/dag:__pkg__",
        "//python/ray/dag:__subpackages__",
        "//release:__pkg__"
    ],
)

dag_tests_srcs = glob(["tests/**/*.py"])

py_test(
    name = "test_function_dag",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)

py_test(
    name = "test_class_dag",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)

py_test(
    name = "test_input_node",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)

py_test(
    name = "test_output_node",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)

py_test(
    name = "test_plot",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)

py_test(
    name = "test_py_obj_scanner",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)

py_test_module_list(
  files = [
    "tests/experimental/test_torch_tensor_dag.py",
    "tests/experimental/test_detect_deadlock_dag.py",
  ],
  size = "medium",
  tags = ["exclusive", "accelerated_dag", "no_windows", "team:core"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "tests/experimental/test_accelerated_dag.py",
  ],
  size = "medium",
  tags = ["exclusive", "accelerated_dag", "no_windows", "team:core", "manual"],
  deps = ["//:ray_lib"],
)

py_test(
  name = "test_torch_tensor_dag_gpu",
  srcs = [
    "tests/experimental/test_torch_tensor_dag.py",
  ],
  main = "tests/experimental/test_torch_tensor_dag.py",
  size = "medium",
  tags = ["exclusive", "accelerated_dag", "no_windows", "team:core", "multi_gpu"],
  env = {"RAY_PYTEST_USE_GPU": "1"},
  deps = ["//:ray_lib"],
)
