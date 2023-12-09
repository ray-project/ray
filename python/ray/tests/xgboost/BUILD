# --------------------------------------------------------------------
# Tests from the python/ray/tests/xgboost directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "simple_example",
    size = "small",
    srcs = ["simple_example.py"],
    tags = [
        "compat",
        "exclusive",
        "team:ml",
    ],
    deps = ["//:ray_lib"],
)

py_test(
    name = "simple_tune",
    size = "small",
    srcs = ["simple_tune.py"],
    args = ["--smoke-test"],
    tags = [
        "compat",
        "exclusive",
        "team:ml",
    ],
    deps = ["//:ray_lib"],
)
