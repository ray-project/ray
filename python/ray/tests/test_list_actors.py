import os
import pytest
import sys

import ray
from ray._private.test_utils import run_string_as_driver, wait_for_condition


def test_list_named_actors_basic(ray_start_regular):
    @ray.remote
    class A:
        pass

    a = A.remote()
    assert not ray.util.list_named_actors()

    a = A.options(name="hi").remote()
    assert len(ray.util.list_named_actors()) == 1
    assert "hi" in ray.util.list_named_actors()

    b = A.options(name="hi2").remote()
    assert len(ray.util.list_named_actors()) == 2
    assert "hi" in ray.util.list_named_actors()
    assert "hi2" in ray.util.list_named_actors()

    def one_actor():
        actors = ray.util.list_named_actors()
        return actors == ["hi2"]

    del a
    wait_for_condition(one_actor)

    del b
    wait_for_condition(lambda: not ray.util.list_named_actors())


@pytest.mark.parametrize("ray_start_regular", [{"local_mode": True}], indirect=True)
def test_list_named_actors_basic_local_mode(ray_start_regular):
    @ray.remote
    class A:
        pass

    a = A.remote()
    assert not ray.util.list_named_actors()

    a = A.options(name="hi").remote()  # noqa: F841
    assert len(ray.util.list_named_actors()) == 1
    assert "hi" in ray.util.list_named_actors()

    b = A.options(name="hi2").remote()  # noqa: F841
    assert len(ray.util.list_named_actors()) == 2
    assert "hi" in ray.util.list_named_actors()
    assert "hi2" in ray.util.list_named_actors()


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
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


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
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
