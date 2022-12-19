load("//bazel:python.bzl", "py_test_run_all_subdirectory")

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dashboard_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/*"],
    ),
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

py_test(
    name= "test_serve_agent",
    size = "medium",
    srcs = ["modules/serve/tests/test_serve_agent.py"],
    #exclude = [
    #    "client/node_modules/**",
    #    "modules/test/**", 
    #    "modules/node/tests/test_node.py", 
    #    "tests/test_dashboard.py", 
    #    "tests/test_state_head.py"
    #],
    #extra_srcs = [],
    data = [
        "modules/job/tests/backwards_compatibility_scripts/test_backwards_compatibility.sh",
        "modules/job/tests/pip_install_test-0.5-py3-none-any.whl",
        "modules/snapshot/snapshot_schema.json",
        "modules/tests/test_config_files/basic_runtime_env.yaml",
    ] + glob([
        "modules/job/tests/subprocess_driver_scripts/*.py",
    ]),
    deps = [":conftest"],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "test_node",
    size = "medium",
    srcs = ["modules/node/tests/test_node.py"],
    deps = [":conftest"],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "test_dashboard",
    size = "medium",
    srcs = ["tests/test_dashboard.py"],
    deps = [":conftest"],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "test_state_head",
    size = "small",
    srcs = ["tests/test_state_head.py"],
    tags = ["team:core"],
)
