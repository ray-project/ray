from inspect import getmembers, isfunction, ismodule
import sys

import pytest

import ray
from ray._private.test_utils import run_string_as_driver


# NOTE: Before adding a new API to Ray (and modifying this test), the new API
# must have Ray Client support.
def test_api_functions():
    # These are pulled from the "Python API" of the Ray Docs
    # util.queue.Queue & util.ActorPool are removed because they are under
    # util.*, not the top-level Ray namespace.
    PYTHON_API = [
        "init",
        "is_initialized",
        "remote",
        "get",
        "wait",
        "put",
        "kill",
        "cancel",
        "get_actor",
        "get_gpu_ids",
        "shutdown",
        "method",
        "nodes",
        "timeline",
        "cluster_resources",
        "available_resources",
        "java_function",
        "java_actor_class",
        "client",
    ]

    OTHER_ALLOWED_FUNCTIONS = [
        # In the RuntimeContext API
        "get_runtime_context",
    ]

    IMPL_FUNCTIONS = ["__getattr__"]

    functions = getmembers(ray, isfunction)
    function_names = [f[0] for f in functions]
    assert set(function_names) == set(
        PYTHON_API + OTHER_ALLOWED_FUNCTIONS + IMPL_FUNCTIONS
    )


def test_non_ray_modules():
    modules = getmembers(ray, ismodule)
    for name, mod in modules:
        assert "ray" in str(mod), f"Module {mod} should not be reachable via ray.{name}"


def test_dynamic_subpackage_import():
    # Test that subpackages are dynamically imported and properly cached.
    script = """
import sys
import ray

# ray.autoscaler
assert "ray.autoscaler" not in sys.modules
ray.autoscaler
# Check that the package is cached.
assert "ray.autoscaler" in sys.modules
"""
    run_string_as_driver(script)


def test_dynamic_subpackage_missing():
    # Test nonexistent subpackage dynamic attribute access and imports raise expected
    # errors.

    script = """
import ray

# Test that nonexistent subpackage attribute access raises an AttributeError.
try:
    ray.foo  # noqa:F401
except AttributeError:
    pass

# Test that nonexistent subpackage import raises an ImportError.
try:
    from ray.foo import bar  # noqa:F401
except ImportError:
    pass
"""

    run_string_as_driver(script)


def test_dynamic_subpackage_fallback_only():
    # Test that the __getattr__ dynamic
    script = """
import ray
import sys

assert "ray._raylet" in sys.modules
assert ray.__getattribute__("_raylet") is ray._raylet
try:
    ray.__getattr__("_raylet")
except AttributeError:
    pass
"""
    run_string_as_driver(script)


def test_for_strings():
    strings = getmembers(ray, lambda obj: isinstance(obj, str))
    for string, _ in strings:
        assert string.startswith("__"), f"Invalid string: {string} found in "
        "top level Ray. Please delete at the end of __init__.py."


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
