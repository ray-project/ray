import os
import pytest
import sys

import ray
from ray._private.test_utils import wait_for_condition


def test_list_named_actors_restarting_actor(ray_start_regular):
    @ray.remote(max_restarts=-1)
    class A:
        def __init__(self):
            os._exit(0)

    a = A.options(name="hi").remote()
    for _ in range(10000):
        assert ray.util.list_named_actors() == ["hi"]

    del a
    wait_for_condition(lambda: not ray.util.list_named_actors())


if __name__ == "__main__":
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
