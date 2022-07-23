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
    name = "test_plot",
    size = "small",
    srcs = dag_tests_srcs,
    tags = ["exclusive", "team:core", "ray_dag_tests"],
    deps = [":dag_lib"],
)
