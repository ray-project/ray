import pytest
import sys

import ray
from ray._private.test_utils import run_string_as_driver


def test_list_named_actors_namespace(ray_start_regular):
    """Verify that actor names are filtered on namespace by default."""
    address = ray_start_regular["address"]

    driver_script_1 = """
import ray
ray.init(address="{}", namespace="test")

@ray.remote
class A:
    pass

A.options(name="hi", lifetime="detached").remote()

assert len(ray.util.list_named_actors()) == 1
assert ray.util.list_named_actors() == ["hi"]
assert ray.util.list_named_actors(all_namespaces=True) == \
    [dict(name="hi", namespace="test")]
""".format(
        address
    )

    run_string_as_driver(driver_script_1)

    assert not ray.util.list_named_actors()
    assert ray.util.list_named_actors(all_namespaces=True) == [
        {"name": "hi", "namespace": "test"}
    ]

    driver_script_2 = """
import ray
ray.init(address="{}", namespace="test")

assert ray.util.list_named_actors() == ["hi"]
assert ray.util.list_named_actors(all_namespaces=True) == \
    [dict(name="hi", namespace="test")]
ray.kill(ray.get_actor("hi"), no_restart=True)
assert not ray.util.list_named_actors()
""".format(
        address
    )

    run_string_as_driver(driver_script_2)
    assert not ray.util.list_named_actors()
    assert not ray.util.list_named_actors(all_namespaces=True)


if __name__ == "__main__":
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
