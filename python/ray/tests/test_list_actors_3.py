import pytest
import sys

import ray
from ray._private.test_utils import run_string_as_driver


def test_list_named_actors_ray_kill(ray_start_regular):
    """Verify that names are returned even while actors are restarting."""

    @ray.remote(max_restarts=-1)
    class A:
        def __init__(self):
            pass

    a = A.options(name="hi").remote()
    assert ray.util.list_named_actors() == ["hi"]
    ray.kill(a, no_restart=False)
    assert ray.util.list_named_actors() == ["hi"]
    ray.kill(a, no_restart=True)
    assert not ray.util.list_named_actors()


def test_list_named_actors_detached(ray_start_regular):
    """Verify that names are returned for detached actors until killed."""
    address = ray_start_regular["address"]

    driver_script = """
import ray
ray.init(address="{}", namespace="default_test_namespace")

@ray.remote
class A:
    pass

A.options(name="hi", lifetime="detached").remote()
a = A.options(name="sad").remote()

assert len(ray.util.list_named_actors()) == 2
assert "hi" in ray.util.list_named_actors()
assert "sad" in ray.util.list_named_actors()
""".format(
        address
    )

    run_string_as_driver(driver_script)

    assert ray.util.list_named_actors() == ["hi"]


if __name__ == "__main__":
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
