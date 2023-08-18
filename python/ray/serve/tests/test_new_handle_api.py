import asyncio
import concurrent.futures
import sys
from typing import Any

import pytest

import ray
from ray._private.test_utils import SignalActor, wait_for_condition

from ray import serve
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentResponse,
    DeploymentResponseGenerator,
    RayServeHandle,
    RayServeSyncHandle,
)
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_NEW_ROUTING,
)


def test_basic(serve_instance):
    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: RayServeHandle):
            self._handle = handle
            assert isinstance(self._handle, RayServeHandle)
            self._new_handle = handle.options(use_new_handle_api=True)
            assert isinstance(self._new_handle, DeploymentHandle)

        async def __call__(self):
            ref = self._new_handle.remote()
            assert isinstance(ref, DeploymentResponse)
            return await ref

    handle: RayServeSyncHandle = serve.run(Deployment.bind(downstream.bind()))
    assert isinstance(handle, RayServeSyncHandle)
    new_handle = handle.options(use_new_handle_api=True)
    assert isinstance(new_handle, DeploymentHandle)
    assert new_handle.remote().result() == "hello"


def test_result_timeout(serve_instance):
    """Test `.result()` timeout parameter."""
    signal_actor = SignalActor.remote()

    @serve.deployment
    class Deployment:
        async def __call__(self):
            await signal_actor.wait.remote()
            return "hi"

    handle = serve.run(Deployment.bind()).options(use_new_handle_api=True)
    ref = handle.remote()
    with pytest.raises(TimeoutError):
        ref.result(timeout_s=0.1)

    ray.get(signal_actor.send.remote())
    assert ref.result() == "hi"


# TODO(edoakes): expand this test to cancel ongoing requests once actor cancellation
# is supported.
@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING,
    reason="`max_concurrent_queries` not properly enforced in old codepath."
)
def test_cancel(serve_instance):
    """Test `.cancel()` from inside and outside a deployment."""

    signal_actor = SignalActor.remote()

    # Set max_concurrent_queries=1 and block so subsequent calls won't be scheduled.
    @serve.deployment(max_concurrent_queries=1)
    def downstream():
        ray.get(signal_actor.wait.remote())
        return "hi"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: RayServeHandle):
            self._handle = handle.options(use_new_handle_api=True)

        async def check_cancel(self):
            ref = self._handle.remote()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ref, timeout=0.1)

            ref.cancel()
            with pytest.raises(asyncio.CancelledError):
                await ref

            return "Cancelled successfully."

    app_handle = serve.run(Deployment.bind(downstream.bind())).options(
        use_new_handle_api=True
    )

    # Send first blocking request so subsequent requests will be pending replica
    # assignment.
    deployment_handle = serve.get_deployment_handle("downstream", app_name="default")
    blocking_ref = deployment_handle.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)
    with pytest.raises(TimeoutError):
        blocking_ref.result(timeout_s=0.001)

    # Check cancellation behavior from outside deployment.
    cancelled_ref = deployment_handle.remote()
    cancelled_ref.cancel()
    with pytest.raises(concurrent.futures.CancelledError):
        cancelled_ref.result()

    # Check cancellation behavior from inside deployment.
    assert app_handle.check_cancel.remote().result() == "Cancelled successfully."

    # Unblock blocking request.
    ray.get(signal_actor.send.remote())
    assert blocking_ref.result() == "hi"


def test_get_app_and_deployment_handle(serve_instance):
    """Test the `get_app_handle` and `get_deployment_handle` APIs."""

    @serve.deployment
    def downstream():
        return "hello"

    @serve.deployment
    class Deployment:
        def __init__(self, handle: RayServeHandle):
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
        def __init__(self, handle1: RayServeHandle, handle2: RayServeHandle):
            self._handle1 = handle1.options(use_new_handle_api=True)
            self._handle2 = handle2.options(use_new_handle_api=True)

        async def __call__(self):
            result = await self._handle1.remote(self._handle2.remote("hi"))
            return f"driver|{result}"

    handle = serve.run(
        Deployment.bind(
            Downstream.options(name="downstream1").bind("downstream1"),
            Downstream.options(name="downstream2").bind("downstream2"),
        ),
    ).options(use_new_handle_api=True)
    assert handle.remote().result() == "driver|downstream1|downstream2|hi"


def test_compose_apps(serve_instance):
    """Test composing deployment handle refs outside of a deployment."""

    @serve.deployment
    class Deployment:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp: str):
            return f"{self._msg}|{inp}"

    handle1 = serve.run(
        Deployment.bind("app1"), name="app1", route_prefix="/app1"
    ).options(use_new_handle_api=True)
    handle2 = serve.run(
        Deployment.bind("app2"), name="app2", route_prefix="/app2"
    ).options(use_new_handle_api=True)

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
        def __init__(self, handle: RayServeHandle):
            self._handle = handle.options(use_new_handle_api=True)

        async def __call__(self):
            ref = self._handle.remote()
            return await identity_task.remote(await ref._to_object_ref())

    handle = serve.run(Deployment.bind(downstream.bind())).options(
        use_new_handle_api=True
    )

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
        def __init__(self, handle: RayServeHandle):
            self._handle = handle.options(use_new_handle_api=True, stream=True)

        async def __call__(self):
            gen = self._handle.remote()
            assert isinstance(gen, DeploymentResponseGenerator)
            async for i in gen:
                yield i

    handle = serve.run(Deployment.bind(downstream.bind())).options(
        use_new_handle_api=True
    )

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
        def __init__(self, handle: RayServeHandle):
            self._handle = handle.options(use_new_handle_api=True, stream=True)

        async def __call__(self):
            gen = self._handle.remote()
            assert isinstance(gen, DeploymentResponseGenerator)

            obj_ref_gen = await gen._to_object_ref_gen()
            async for obj_ref in obj_ref_gen:
                yield await obj_ref

    handle = serve.run(Deployment.bind(downstream.bind())).options(
        use_new_handle_api=True
    )

    gen = handle.options(stream=True).remote()
    assert isinstance(gen, DeploymentResponseGenerator)
    obj_ref_gen = gen._to_object_ref_gen_sync()
    assert ray.get(list(obj_ref_gen)) == list(range(10))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
