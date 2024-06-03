import asyncio
import pytest
import sys
import time

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


@pytest.mark.asyncio
async def test_list_named_actors_with_normal_task(shutdown_only):
    # The following parameters are all designed to increase the
    # probability of reproducing the situation where
    # `list_named_actors` gets hang.
    # https://github.com/ray-project/ray/issues/45581 for more details.
    TEST_RANGE = 10
    NORMAL_TASK_PER_ITEM = 100
    LIST_NAMED_ACTORS_PER_ITEM = 10
    for _ in range(TEST_RANGE):
        time.sleep(1)

        @ray.remote
        def test():
            return True

        res = []
        for i in range(NORMAL_TASK_PER_ITEM):
            res.append(test.remote())

        async def run():
            for i in range(LIST_NAMED_ACTORS_PER_ITEM):
                await asyncio.sleep(0)
                ray.util.list_named_actors(True)

        res.append(run())
        await asyncio.gather(*res)


if __name__ == "__main__":
    import os

    # Test suite is timing out. Disable on windows for now.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
