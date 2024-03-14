import asyncio
import os
import sys

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.util.state import list_tasks


def test_remote_cancel(ray_start_regular):
    @ray.remote
    class Actor:
        async def sleep(self):
            await asyncio.sleep(1000)

    @ray.remote
    def f(refs):
        ref = refs[0]
        ray.cancel(ref)

    a = Actor.remote()
    sleep_ref = a.sleep.remote()
    wait_for_condition(lambda: list_tasks(filters=[("name", "=", "Actor.sleep")]))
    ref = f.remote([sleep_ref])  # noqa

    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(sleep_ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
