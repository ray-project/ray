import asyncio
import time

import pytest

import ray
from ray import serve
from ray._private.test_utils import SignalActor


def test_serve_forceful_shutdown(serve_instance):
    @serve.deployment
    def sleeper():
        while True:
            time.sleep(1000)

    sleeper._config.experimental_graceful_shutdown_timeout_s = 0.1
    sleeper.deploy()

    handle = sleeper.get_handle()
    ref = handle.remote()
    sleeper.delete()

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(ref)


def test_serve_graceful_shutdown(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(name="wait", max_concurrent_queries=10)
    class Wait:
        async def __call__(self, signal_actor):
            await signal_actor.wait.remote()
            return ""

    Wait._config.experimental_graceful_shutdown_wait_loop_s = 0.5
    Wait._config.experimental_graceful_shutdown_timeout_s = 1000
    Wait.deploy()
    handle = Wait.get_handle()
    refs = [handle.remote(signal) for _ in range(10)]

    # Wait for all the queries to be enqueued
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(refs, timeout=1)

    @ray.remote(num_cpus=0)
    def do_blocking_delete():
        Wait.delete()

    # Now delete the backend. This should trigger the shutdown sequence.
    delete_ref = do_blocking_delete.remote()

    # The queries should be enqueued but not executed becuase they are blocked
    # by signal actor.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(refs, timeout=1)

    signal.send.remote()

    # All the queries should be drained and executed without error.
    ray.get(refs)
    # Blocking delete should complete.
    ray.get(delete_ref)


def test_parallel_start(serve_instance):
    # Test the ability to start multiple replicas in parallel.
    # In the past, when Serve scale up a backend, it does so one by one and
    # wait for each replica to initialize. This test avoid this by preventing
    # the first replica to finish initialization unless the second replica is
    # also started.
    @ray.remote
    class Barrier:
        def __init__(self, release_on):
            self.release_on = release_on
            self.current_waiters = 0
            self.event = asyncio.Event()

        async def wait(self):
            self.current_waiters += 1
            if self.current_waiters == self.release_on:
                self.event.set()
            else:
                await self.event.wait()

    barrier = Barrier.remote(release_on=2)

    @serve.deployment(num_replicas=2)
    class LongStartingServable:
        def __init__(self):
            ray.get(barrier.wait.remote(), timeout=10)

        def __call__(self):
            return "Ready"

    LongStartingServable.deploy()
    ray.get(LongStartingServable.get_handle().remote(), timeout=10)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
