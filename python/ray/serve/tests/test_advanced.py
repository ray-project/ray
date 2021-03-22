import asyncio
import time

import pytest

import ray
from ray import serve
from ray.test_utils import SignalActor
from ray.serve.config import BackendConfig


def test_serve_forceful_shutdown(serve_instance):
    def sleeper(_):
        while True:
            time.sleep(1000)

    serve.create_backend(
        "sleeper",
        sleeper,
        config=BackendConfig(experimental_graceful_shutdown_timeout_s=1))
    serve.create_endpoint("sleeper", backend="sleeper")
    handle = serve.get_handle("sleeper")
    ref = handle.remote()
    serve.delete_endpoint("sleeper")
    serve.delete_backend("sleeper")

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(ref)


def test_serve_graceful_shutdown(serve_instance):
    signal = SignalActor.remote()

    class WaitBackend:
        @serve.accept_batch
        async def __call__(self, requests):
            signal_actor = await requests[0].body()
            await signal_actor.wait.remote()
            return ["" for _ in range(len(requests))]

    serve.create_backend(
        "wait",
        WaitBackend,
        config=BackendConfig(
            # Make sure we can queue up queries in the replica side.
            max_concurrent_queries=10,
            max_batch_size=1,
            experimental_graceful_shutdown_wait_loop_s=0.5,
            experimental_graceful_shutdown_timeout_s=1000,
        ))
    serve.create_endpoint("wait", backend="wait")
    handle = serve.get_handle("wait")
    refs = [handle.remote(signal) for _ in range(10)]

    # Wait for all the queries to be enqueued
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(refs, timeout=1)

    @ray.remote(num_cpus=0)
    def do_blocking_delete():
        serve.delete_endpoint("wait")
        serve.delete_backend("wait")

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

    class LongStartingServable:
        def __init__(self):
            ray.get(barrier.wait.remote(), timeout=10)

        def __call__(self, _):
            return "Ready"

    config = BackendConfig(num_replicas=2)
    serve.create_backend("p:v0", LongStartingServable, config=config)
    serve.create_endpoint("test-parallel", backend="p:v0")
    handle = serve.get_handle("test-parallel")

    ray.get(handle.remote(), timeout=10)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
