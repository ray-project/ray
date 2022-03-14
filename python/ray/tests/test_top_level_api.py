from inspect import getmembers, isfunction, ismodule

import ray


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
        "client",
    ]

    OTHER_ALLOWED_FUNCTIONS = [
        # In the RuntimeContext API
        "get_runtime_context",
    ]

    functions = getmembers(ray, isfunction)
    function_names = [f[0] for f in functions]
    assert set(function_names) == set(PYTHON_API + OTHER_ALLOWED_FUNCTIONS)


def test_non_ray_modules():
    modules = getmembers(ray, ismodule)
    for name, mod in modules:
        assert "ray" in str(mod), f"Module {mod} should not be reachable via ray.{name}"


def test_for_strings():
    strings = getmembers(ray, lambda obj: isinstance(obj, str))
    for string, _ in strings:
        assert string.startswith("__"), f"Invalid string: {string} found in "
        "top level Ray. Please delete at the end of __init__.py."


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
