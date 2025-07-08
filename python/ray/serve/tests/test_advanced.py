import asyncio
import sys
import time

import httpx
import pytest
from starlette.requests import Request

import ray
from ray import serve
from ray._common.test_utils import SignalActor
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.handle import DeploymentHandle
from ray.util.state import list_objects


def test_serve_forceful_shutdown(serve_instance):
    @serve.deployment(graceful_shutdown_timeout_s=0.1)
    def sleeper():
        while True:
            time.sleep(1000)

    handle = serve.run(sleeper.bind())
    response = handle.remote()
    serve.delete(SERVE_DEFAULT_APP_NAME)

    with pytest.raises(ray.exceptions.RayActorError):
        response.result()


def test_serve_graceful_shutdown(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(
        name="wait",
        max_ongoing_requests=10,
        graceful_shutdown_timeout_s=1000,
        graceful_shutdown_wait_loop_s=0.5,
    )
    class Wait:
        async def __call__(self, signal_actor):
            await signal_actor.wait.remote()

    handle = serve.run(Wait.bind())
    responses = [handle.remote(signal) for _ in range(10)]

    # Wait for all the queries to be enqueued
    with pytest.raises(TimeoutError):
        responses[0].result(timeout_s=1)

    @ray.remote(num_cpus=0)
    def do_blocking_delete():
        serve.delete(SERVE_DEFAULT_APP_NAME)

    # Now delete the deployment. This should trigger the shutdown sequence.
    delete_ref = do_blocking_delete.remote()

    # The queries should be enqueued but not executed becuase they are blocked
    # by signal actor.
    with pytest.raises(TimeoutError):
        responses[0].result(timeout_s=1)

    signal.send.remote()

    # All the queries should be drained and executed without error.
    [r.result() for r in responses]
    # Blocking delete should complete.
    ray.get(delete_ref)


def test_parallel_start(serve_instance):
    # Test the ability to start multiple replicas in parallel.
    # In the past, when Serve scale up a deployment, it does so one by one and
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

    handle = serve.run(LongStartingServable.bind())
    handle.remote().result(timeout_s=10)


def test_passing_object_ref_to_deployment_not_pinned_to_memory(serve_instance):
    """Passing object refs to deployments should not pin the refs in memory.

    We had issue that passing object ref to a deployment will result in memory leak
    due to _PyObjScanner/ cloudpickler pinning the object to memory. This test will
    ensure the object ref is released after the request is done.

    See: https://github.com/ray-project/ray/issues/43248
    """

    def _obj_ref_exists_in_state_api(obj_ref_hex: str) -> bool:
        return (
            len(
                list_objects(
                    filters=[("object_id", "=", obj_ref_hex)],
                    raise_on_missing_output=False,
                )
            )
            > 0
        )

    @serve.deployment
    class Dep1:
        def multiply_by_two(self, length: int):
            return length * 2

    @serve.deployment
    class Gateway:
        def __init__(self, dep1: DeploymentHandle):
            self.dep1: DeploymentHandle = dep1

        async def __call__(self, http_request: Request) -> str:
            length = int(http_request.query_params.get("length"))
            length_ref = ray.put(length)

            # Sanity check that the ObjectRef exists in the state API.
            assert _obj_ref_exists_in_state_api(length_ref.hex())
            return {
                "length": length,
                "result": await self.dep1.multiply_by_two.remote(length_ref),
                "length_ref_hex": length_ref.hex(),
            }

    serve.run(Gateway.bind(Dep1.bind()))

    length = 10
    response = httpx.get(f"http://localhost:8000?length={length}").json()
    assert response["length"] == length
    assert response["result"] == length * 2

    # Ensure the object ref is not in the memory anymore.
    assert not _obj_ref_exists_in_state_api(response["length_ref_hex"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
