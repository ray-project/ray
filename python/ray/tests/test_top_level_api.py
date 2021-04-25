from inspect import getmembers, isfunction

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
        "get_gpu_ids",
        "shutdown",
        "method",
        "nodes",
        "timeline",
        "cluster_resources",
        "available_resources",
        "java_function",
        "java_actor_class",
    ]

    OTHER_ALLOWED_FUNCTIONS = [
        # TODO(ilr) either move this to PYTHON_API or remove.
        "get_actor",
        # In the RuntimeContext API
        "get_runtime_context",
    ]

    functions = getmembers(ray, isfunction)
    function_names = [f[0] for f in functions]
    assert set(function_names) == set(PYTHON_API + OTHER_ALLOWED_FUNCTIONS)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
