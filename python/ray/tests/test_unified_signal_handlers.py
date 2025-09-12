import pytest

import ray


def test_asyncio_actor_force_exit_is_immediate():
    ray.init()

    # Create an asyncio actor that can call the forced-exit binding.
    @ray.remote
    class A:
        async def ping(self):
            return "ok"

        async def force_quit(self):
            from ray._private.worker import global_worker

            # Use 'user' which maps to INTENDED_USER_EXIT in ForceExit and thus kForcedExit.
            global_worker.core_worker.force_exit_worker("user", b"force from test")
            # Should never reach here.
            return "unreachable"

    a = A.remote()
    # Ensure the actor is alive.
    assert ray.get(a.ping.remote()) == "ok"

    # Call force_quit; it should terminate the worker immediately and raise an error.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.force_quit.remote())

    ray.shutdown()
