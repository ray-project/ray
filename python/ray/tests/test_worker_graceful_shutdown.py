import os
import signal
import sys
from typing import List

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't handle SIGTERM gracefully."
)
@pytest.mark.parametrize("actor_type", ["asyncio", "threaded"])
def test_ray_get_during_graceful_shutdown(ray_start_regular_shared, actor_type: str):
    """Test that ray.get works as expected when draining tasks during shutdown.

    This currently only applies to concurrent actors, because single-threaded actors do
    not allow tasks to finish exiting after SIGTERM.
    """
    signal_actor = SignalActor.remote()

    assert actor_type in {"asyncio", "threaded"}
    if actor_type == "asyncio":

        @ray.remote
        class A:
            def exit(self):
                os.kill(os.getpid(), signal.SIGTERM)

            async def wait_then_get(self, nested_ref: List[ray.ObjectRef]) -> str:
                print("Waiting for signal...")
                await signal_actor.wait.remote()
                print("Got signal, calling ray.get")
                return await nested_ref[0]

    elif actor_type == "threaded":

        @ray.remote(max_concurrency=2)
        class A:
            def exit(self):
                os.kill(os.getpid(), signal.SIGTERM)

            def wait_then_get(self, nested_ref: List[ray.ObjectRef]):
                print("Waiting for signal...")
                ray.get(signal_actor.wait.remote())
                print("Got signal, calling ray.get")
                return ray.get(nested_ref[0])

    # Start the actor and wait for the method to begin executing and then block.
    actor = A.remote()
    wait_ref = actor.wait_then_get.remote([ray.put("hi")])
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # SIGTERM the process and then signal the method to unblock.
    ray.get(actor.exit.remote())
    ray.get(signal_actor.send.remote())

    # Check that the method succeeds as expected.
    assert ray.get(wait_ref) == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
