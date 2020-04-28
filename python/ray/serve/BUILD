# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "serve_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)

# This test aggregates all serve tests and run them in a single session
# similar to `pytest .`
# Serve tests need to run in a single session because starting and stopping
# serve cluster take a large chunk of time. All serve tests use a shared
# cluster.
py_test(
    name = "test_serve",
    size = "medium",
    srcs = glob(["tests/*.py"],
                exclude=["tests/test_nonblocking.py",
                         "tests/test_master_crashes.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

# Runs test_api and test_failure with injected failures in the master actor.
py_test(
    name = "test_master_crashes",
    size = "medium",
    srcs = glob(["tests/test_master_crashes.py",
                 "tests/test_api.py",
                 "tests/test_failure.py"],
                exclude=["tests/test_nonblocking.py",
                         "tests/test_serve.py"]),
)

py_test(
    name = "echo_full",
    size = "small",
    srcs = glob(["examples/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "test_nonblocking",
    size = "small",
    srcs = glob(["tests/test_nonblocking.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

# Make sure the example showing in doc is tested
py_test(
    name = "quickstart_class",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "quickstart_function",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)
