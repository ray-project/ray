import os
import signal
import sys
from typing import List

import pytest

import ray
from ray._private.test_utils import SignalActor, wait_for_condition


@pytest.mark.parametrize("actor_type", ["asyncio", "threaded"])
def test_ray_get_during_graceful_shutdown(ray_start_regular_shared, actor_type: str):
    signal_actor = SignalActor.remote()

    assert actor_type in {"asyncio", "threaded"}
    if actor_type == "asyncio":

        @ray.remote
        class A:
            def getpid(self) -> int:
                return os.getpid()

            async def wait_then_get(self, nested_ref: List[ray.ObjectRef]) -> str:
                print("Waiting for signal...")
                await signal_actor.wait.remote()
                return ray.get(nested_ref[0])

    elif actor_type == "threaded":

        @ray.remote(max_concurrency=2)
        class A:
            def getpid(self) -> int:
                return os.getpid()

            def wait_then_get(self, nested_ref: List[ray.ObjectRef]):
                print("Waiting for signal...")
                ray.get(signal_actor.wait.remote())
                return ray.get(nested_ref[0])

    actor = A.remote()
    wait_ref = actor.wait_then_get.remote([ray.put("hi")])
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    os.kill(ray.get(actor.getpid.remote()), signal.SIGTERM)
    ray.get(signal_actor.send.remote())
    assert ray.get(wait_ref) == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
