"""Wrappers that inject default Ray Data test environment variables.

Keeps the Parquet footer-reader actor pool tiny for unit/integration tests
that exercise small fixtures. Release/perf jobs that need the production
default should not use these wrappers (or should override ``env``).
"""

load("@rules_python//python:defs.bzl", _py_test = "py_test")
load("//bazel:python.bzl", _doctest = "doctest", _py_test_module_list = "py_test_module_list")

_DATA_TEST_ENV = {
    # Default 32-actor footer pool times out / warns under CI parallelism.
    "RAY_DATA_PARQUET_FOOTER_NUM_ACTORS": "1",
}

def _merge_env(env):
    merged = dict(_DATA_TEST_ENV)
    if env:
        merged.update(env)
    return merged

def py_test(**kwargs):
    kwargs["env"] = _merge_env(kwargs.pop("env", None))
    _py_test(**kwargs)

def py_test_module_list(**kwargs):
    kwargs["env"] = _merge_env(kwargs.pop("env", None))
    _py_test_module_list(**kwargs)

def doctest(**kwargs):
    kwargs["env"] = _merge_env(kwargs.pop("env", None))
    _doctest(**kwargs)
