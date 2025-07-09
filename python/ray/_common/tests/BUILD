load("//bazel:python.bzl", "py_test_module_list")

# Small tests.
py_test_module_list(
    size = "small",
    files = [
        "test_signal_semaphore_utils.py",
        "test_signature.py",
        "test_utils.py",
        "test_wait_for_condition.py",
    ],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [
        "//:ray_lib",
    ],
)
