load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "doctest", "py_test_module_list")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = ["**/experimental/**/*.py"],
    ),
    tags = ["team:core"],
    deps = [":dag_lib"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dag_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/**/*.py"],
    ),
    visibility = [
        "//python/ray/dag:__pkg__",
        "//python/ray/dag:__subpackages__",
        "//release:__pkg__",
    ],
)

dag_tests_srcs = glob(["tests/**/*.py"])

py_test(
    name = "test_function_dag",
    size = "small",
    srcs = dag_tests_srcs,
    tags = [
        "exclusive",
        "ray_dag_tests",
        "team:core",
    ],
    deps = [":dag_lib"],
)

py_test(
    name = "test_class_dag",
    size = "small",
    srcs = dag_tests_srcs,
    tags = [
        "exclusive",
        "ray_dag_tests",
        "team:core",
    ],
    deps = [":dag_lib"],
)

py_test(
    name = "test_input_node",
    size = "small",
    srcs = dag_tests_srcs,
    tags = [
        "exclusive",
        "ray_dag_tests",
        "team:core",
    ],
    deps = [":dag_lib"],
)

py_test(
    name = "test_output_node",
    size = "small",
    srcs = dag_tests_srcs,
    tags = [
        "exclusive",
        "ray_dag_tests",
        "team:core",
    ],
    deps = [":dag_lib"],
)

py_test(
    name = "test_plot",
    size = "small",
    srcs = dag_tests_srcs,
    tags = [
        "exclusive",
        "ray_dag_tests",
        "team:core",
    ],
    deps = [":dag_lib"],
)

py_test(
    name = "test_py_obj_scanner",
    size = "small",
    srcs = dag_tests_srcs,
    tags = [
        "exclusive",
        "ray_dag_tests",
        "team:core",
    ],
    deps = [":dag_lib"],
)

py_test_module_list(
    size = "medium",
    files = [
        "tests/experimental/test_collective_dag.py",
        "tests/experimental/test_dag_visualization.py",
        "tests/experimental/test_execution_schedule.py",
        "tests/experimental/test_mocked_nccl_dag.py",
        "tests/experimental/test_multi_node_dag.py",
        "tests/experimental/test_torch_tensor_dag.py",
    ],
    tags = [
        "accelerated_dag",
        "exclusive",
        "no_windows",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)

py_test_module_list(
    size = "enormous",
    files = [
        "tests/experimental/test_accelerated_dag.py",
    ],
    tags = [
        "accelerated_dag",
        "exclusive",
        "no_windows",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_torch_tensor_dag_gpu",
    size = "enormous",
    srcs = [
        "tests/experimental/test_torch_tensor_dag.py",
    ],
    env = {"RAY_PYTEST_USE_GPU": "1"},
    main = "tests/experimental/test_torch_tensor_dag.py",
    tags = [
        "accelerated_dag",
        "exclusive",
        "multi_gpu",
        "no_windows",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_torch_tensor_transport_gpu",
    size = "enormous",
    srcs = [
        "tests/experimental/test_torch_tensor_transport.py",
    ],
    env = {"RAY_PYTEST_USE_GPU": "1"},
    main = "tests/experimental/test_torch_tensor_transport.py",
    tags = [
        "accelerated_dag",
        "exclusive",
        "multi_gpu",
        "no_windows",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)
