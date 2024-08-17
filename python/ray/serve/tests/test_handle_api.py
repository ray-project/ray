import asyncio
import sys
from typing import Any, List

import pytest

import ray
from ray import serve
from ray._private.test_utils import SignalActor, async_wait_for_condition
from ray._private.utils import get_or_create_event_loop
from ray.serve._private.constants import RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentResponse,
    DeploymentResponseGenerator,
)


def test_basic(serve_instance):
    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle
            assert isinstance(self._handle, DeploymentHandle)

        async def __call__(self):
            response = self._handle.remote()
            assert isinstance(response, DeploymentResponse)
            val = await response

            # Check that the response can be awaited multiple times.
            for _ in range(10):
                assert (await response) == val

            return val

    handle: DeploymentHandle = serve.run(Deployment.bind(downstream.bind()))
    assert isinstance(handle, DeploymentHandle)
    r = handle.remote()
    assert r.result() == "hello"

    # Check that `.result()` can be called multiple times.
    for _ in range(10):
        assert r.result() == "hello"


def test_result_timeout(serve_instance):
    """Test `.result()` timeout parameter."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Deployment:
        async def __call__(self):
            await signal_actor.wait.remote()
            return "hi"

    handle = serve.run(Deployment.bind())
    ref = handle.remote()
    with pytest.raises(TimeoutError):
        ref.result(timeout_s=0.1)

    ray.get(signal_actor.send.remote())
    assert ref.result() == "hi"


def test_get_app_and_deployment_handle(serve_instance):
    """Test the `get_app_handle` and `get_deployment_handle` APIs."""

    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            pass

        async def check_get_deployment_handle(self):
            handle = serve.get_deployment_handle(deployment_name="downstream")
            assert isinstance(handle, DeploymentHandle)

            ref = handle.remote()
            assert isinstance(ref, DeploymentResponse)
            return await ref

    serve.run(Deployment.bind(downstream.bind()))
    handle = serve.get_app_handle("default")
    assert isinstance(handle, DeploymentHandle)
    assert handle.check_get_deployment_handle.remote().result() == "hello"

    handle = serve.get_deployment_handle("downstream", app_name="default")
    assert isinstance(handle, DeploymentHandle)
    assert handle.remote().result() == "hello"


def test_compose_deployments_in_app(serve_instance):
    """Test composing deployment handle refs within a deployment."""

    @serve.deployment
    class Downstream:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp: str):
            return f"{self._msg}|{inp}"

    @serve.deployment
    class Deployment:
        def __init__(self, handle1: DeploymentHandle, handle2: DeploymentHandle):
            self._handle1 = handle1
            self._handle2 = handle2

        async def __call__(self):
            result = await self._handle1.remote(self._handle2.remote("hi"))
            return f"driver|{result}"

    handle = serve.run(
        Deployment.bind(
            Downstream.options(name="downstream1").bind("downstream1"),
            Downstream.options(name="downstream2").bind("downstream2"),
        ),
    )
    assert handle.remote().result() == "driver|downstream1|downstream2|hi"


def test_compose_apps(serve_instance):
    """Test composing deployment handle refs outside of a deployment."""

    @serve.deployment
    class Deployment:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp: str):
            return f"{self._msg}|{inp}"

    handle1 = serve.run(Deployment.bind("app1"), name="app1", route_prefix="/app1")
    handle2 = serve.run(Deployment.bind("app2"), name="app2", route_prefix="/app2")

    assert handle1.remote(handle2.remote("hi")).result() == "app1|app2|hi"


def test_convert_to_object_ref(serve_instance):
    """Test converting deployment handle refs to Ray object refs."""

    @ray.remote
    def identity_task(inp: Any):
        return inp

    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle

        async def __call__(self):
            ref = self._handle.remote()
            return await identity_task.remote(await ref._to_object_ref())

    handle = serve.run(Deployment.bind(downstream.bind()))

    ref = handle.remote()
    assert ray.get(identity_task.remote(ref._to_object_ref_sync())) == "hello"


def test_generators(serve_instance):
    """Test generators inside and outside a deployment."""

    @serve.deployment
    def downstream():
        for i in range(10):
            yield i

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle.options(stream=True)

        async def __call__(self):
            gen = self._handle.remote()
            assert isinstance(gen, DeploymentResponseGenerator)
            async for i in gen:
                yield i

    handle = serve.run(Deployment.bind(downstream.bind()))

    gen = handle.options(stream=True).remote()
    assert isinstance(gen, DeploymentResponseGenerator)
    assert list(gen) == list(range(10))


def test_convert_to_object_ref_gen(serve_instance):
    """Test converting generators to obj ref gens inside and outside a deployment."""

    @serve.deployment
    def downstream():
        for i in range(10):
            yield i

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle.options(stream=True)

        async def __call__(self):
            gen = self._handle.remote()
            assert isinstance(gen, DeploymentResponseGenerator)

            obj_ref_gen = await gen._to_object_ref_gen()
            async for obj_ref in obj_ref_gen:
                yield await obj_ref

    handle = serve.run(Deployment.bind(downstream.bind()))

    gen = handle.options(stream=True).remote()
    assert isinstance(gen, DeploymentResponseGenerator)
    obj_ref_gen = gen._to_object_ref_gen_sync()
    assert ray.get(list(obj_ref_gen)) == list(range(10))


@pytest.mark.parametrize("stream", [False, True])
def test_sync_response_methods_fail_in_deployment(serve_instance, stream: bool):
    """Blocking `DeploymentResponse` (and generator) methods should fail in loop."""

    if stream:

        @serve.deployment
        def downstream():
            yield

    else:

        @serve.deployment
        def downstream():
            pass

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle.options(stream=stream)

        async def __call__(self):
            response = self._handle.remote()
            with pytest.raises(
                RuntimeError,
                match="should not be called from within an `asyncio` event loop",
            ):
                if stream:
                    for _ in response:
                        pass
                else:
                    response.result()

            return "OK"

    handle = serve.run(Deployment.bind(downstream.bind()))

    assert handle.remote().result() == "OK"


def test_handle_eager_execution(serve_instance):
    """Handle requests should be sent without fetching the result."""

    upstream_signal_actor = SignalActor.remote()
    downstream_signal_actor = SignalActor.remote()

    @serve.deployment
    async def downstream():
        await downstream_signal_actor.send.remote()

    @serve.deployment
    class Deployment:
        def __init__(self, handle: DeploymentHandle):
            self._handle = handle

        async def __call__(self):
            # Send a request without awaiting the response. It should still
            # executed (verified via signal actor).
            r = self._handle.remote()
            await upstream_signal_actor.send.remote()

            await downstream_signal_actor.wait.remote()

            return await r

    handle = serve.run(Deployment.bind(downstream.bind()))

    # Send a request without awaiting the response. It should still
    # executed (verified via signal actor).
    r = handle.remote()
    ray.get(upstream_signal_actor.wait.remote(), timeout=5)

    r.result() == "OK"


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS,
    reason="Strict enforcement must be enabled.",
)
@pytest.mark.asyncio
async def test_max_ongoing_requests_enforced(serve_instance):
    """Handles should respect max_ongoing_requests enforcement."""

    loop = get_or_create_event_loop()

    @ray.remote
    class Waiter:
        def __init__(self):
            self._waiters: List[asyncio.Event] = []

        async def wait(self):
            event = asyncio.Event()
            self._waiters.append(event)
            await event.wait()

        def unblock_one(self):
            self._waiters.pop().set()

        def get_num_waiters(self) -> int:
            return len(self._waiters)

    waiter = Waiter.remote()

    @serve.deployment(max_ongoing_requests=1)
    class Deployment:
        async def __call__(self):
            await waiter.wait.remote()

    handle = serve.run(Deployment.bind())

    async def _do_request():
        return await handle.remote()

    async def _assert_one_waiter():
        assert await waiter.get_num_waiters.remote() == 1
        return True

    # Send a batch of requests. Only one should be able to execute at a time
    # due to `max_ongoing_requests=1`.
    tasks = [loop.create_task(_do_request()) for _ in range(10)]
    for i in range(len(tasks)):
        # Check that only one starts executing.
        await async_wait_for_condition(_assert_one_waiter)
        _, pending = await asyncio.wait(tasks, timeout=0.1)
        assert len(tasks) == len(tasks)

        # Unblocking the one that is executing should cause it to finish.
        # Another request will then get scheduled.
        await waiter.unblock_one.remote()
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        assert len(done) == 1
        assert len(pending) == len(tasks) - 1
        tasks = pending


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
