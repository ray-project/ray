# --------------------------------------------------------------------
# Tests from the python/ray.workflow/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "doctest", "py_test_module_list")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = [
            "tests/**/*",
            "examples/**/*",
        ],
    ),
    tags = ["team:none"],
)

SRCS = glob(["**/conftest.py"])

LARGE_TESTS = [
    "tests/test_error_handling.py",
    "tests/test_recovery.py",
    "tests/test_basic_workflows_2.py",
    "tests/test_metadata.py",
]

LARGE_ALL_CORE_TESTS = [
    "tests/test_http_events_2.py",
    "tests/test_events.py",
]

py_test_module_list(
    size = "medium",
    extra_srcs = SRCS,
    files = glob(
        [
            "tests/test_*.py",
            "examples/**/*.py",
        ],
        exclude = LARGE_TESTS + LARGE_ALL_CORE_TESTS,
    ),
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)

py_test_module_list(
    size = "large",
    extra_srcs = SRCS,
    files = LARGE_TESTS,
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//:ray_lib"],
)

py_test_module_list(
    size = "large",
    extra_srcs = SRCS,
    files = LARGE_ALL_CORE_TESTS,
    tags = [
        "exclusive",
        "team:core",
        "use_all_core",
    ],
    deps = ["//:ray_lib"],
)
