import sys
import os
import pytest

import ray
from ray._private.function_manager import FunctionActorManager


@ray.remote
def wrapped_func():
    return "ok"


def normal_func():
    return "ok"


def test_load_remote_function(shutdown_only):
    ray.init(num_cpus=1)

    function_manager = FunctionActorManager(None)

    python_descriptor = ray._raylet.PythonFunctionDescriptor(
        "test_function_manager", "wrapped_func", "", "function_hash"
    )
    function_manager._load_function_from_local(python_descriptor)

    assert (
        wrapped_func._function
        == function_manager._function_execution_info[
            python_descriptor.function_id
        ].function
    )


def test_load_local_function(shutdown_only):
    ray.init(num_cpus=1)

    function_manager = FunctionActorManager(None)

    python_descriptor = ray._raylet.PythonFunctionDescriptor(
        "test_function_manager", "normal_func", "", "function_hash"
    )
    function_manager._load_function_from_local(python_descriptor)

    assert (
        normal_func
        == function_manager._function_execution_info[
            python_descriptor.function_id
        ].function
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
