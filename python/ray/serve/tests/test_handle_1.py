import asyncio
import concurrent.futures
import sys
import threading
from typing import Any

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.constants import (
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't set handle source",
)
def test_replica_handle_source(serve_instance):
    @serve.deployment
    def f():
        return "hi"

    @serve.deployment
    class Router:
        def __init__(self, handle):
            self.handle = handle
            self.handle._init()

        def check(self):
            return self.handle.init_options._source == DeploymentHandleSource.REPLICA

    h = serve.run(Router.bind(f.bind()))
    assert h.check.remote().result()


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode work with tasks & actors",
)
def test_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    @ray.remote
    class DelegateActor:
        async def call_handle(self, handle):
            return await handle.remote()

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._handle = handle

        async def __call__(self):
            # Test pickling handle via `actor.method.remote()`.
            a = DelegateActor.remote()
            return await a.call_handle.remote(self._handle)

    app_handle = serve.run(Ingress.bind(f.bind()))
    assert app_handle.remote().result() == "hello"


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't support get_app_handle/get_deployment_handle",
)
def test_get_and_call_handle_in_thread(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    serve.run(f.bind())

    def get_and_call_app_handle():
        handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
        return handle.remote().result()

    def get_and_call_deployment_handle():
        handle = serve.get_deployment_handle("f", SERVE_DEFAULT_APP_NAME)
        return handle.remote().result()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        fut1 = executor.submit(get_and_call_app_handle)
        fut2 = executor.submit(get_and_call_deployment_handle)
        assert fut1.result() == "hello"
        assert fut2.result() == "hello"


def test_handle_option_chaining(serve_instance):
    # https://github.com/ray-project/ray/issues/12802
    # https://github.com/ray-project/ray/issues/12798

    @serve.deployment
    class MultiMethod:
        def method_a(self):
            return "method_a"

        def method_b(self):
            return "method_b"

        def __call__(self):
            return "__call__"

    handle1 = serve.run(MultiMethod.bind())
    counter = handle1.request_counter
    counter_info = counter.info
    assert handle1.remote().result() == "__call__"

    handle2 = handle1.options(method_name="method_a")

    assert handle2.remote().result() == "method_a"
    assert handle2.request_counter == counter
    assert handle2.request_counter.info == counter_info

    handle3 = handle1.options(method_name="method_b")

    assert handle3.remote().result() == "method_b"
    assert handle3.request_counter == counter
    assert handle2.request_counter.info == counter_info


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't support get_app_handle/get_deployment_handle",
)
def test_repeated_get_handle_cached(serve_instance):
    @serve.deployment
    def f(_):
        return ""

    serve.run(f.bind())

    handle_sets = {serve.get_app_handle("default") for _ in range(100)}
    assert len(handle_sets) == 1

    handle_sets = {serve.get_deployment_handle("f", "default") for _ in range(100)}
    assert len(handle_sets) == 1


def test_nonexistent_method_sync(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            pass

    handle = serve.run(A.bind())
    with pytest.raises(RayServeException) as excinfo:
        handle.does_not_exist.remote().result()

    exception_string = str(excinfo.value)
    assert "'does_not_exist'" in exception_string
    assert "Available methods: ['exists']" in exception_string


@pytest.mark.asyncio
async def test_nonexistent_method_async(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            pass

    handle = serve.run(A.bind())
    with pytest.raises(RayServeException) as excinfo:
        await handle.does_not_exist.remote()

    exception_string = str(excinfo.value)
    assert "'does_not_exist'" in exception_string
    assert "Available methods: ['exists']" in exception_string


def _get_asyncio_loop_running_in_thread() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    threading.Thread(
        daemon=True,
        target=loop.run_forever,
    ).start()
    return loop


@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't support get_app_handle/get_deployment_handle",
)
@pytest.mark.asyncio
async def test_call_handle_across_asyncio_loops(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            return True

    serve.run(A.bind())

    async def refresh_get():
        handle = serve.get_app_handle("default")
        assert await handle.exists.remote()

    for _ in range(10):
        loop = _get_asyncio_loop_running_in_thread()
        asyncio.run_coroutine_threadsafe(refresh_get(), loop).result()

    handle = serve.get_app_handle("default")

    async def cache_get():
        assert await handle.exists.remote()

    for _ in range(10):
        loop = _get_asyncio_loop_running_in_thread()
        asyncio.run_coroutine_threadsafe(cache_get(), loop).result()


def test_handle_typing(serve_instance):
    @serve.deployment
    class DeploymentClass:
        pass

    @serve.deployment
    def deployment_func():
        pass

    @serve.deployment
    class Ingress:
        def __init__(
            self, class_downstream: DeploymentHandle, func_downstream: DeploymentHandle
        ):
            # serve.run()'ing this deployment fails if these assertions fail.
            assert isinstance(class_downstream, DeploymentHandle)
            assert isinstance(func_downstream, DeploymentHandle)

    h = serve.run(Ingress.bind(DeploymentClass.bind(), deployment_func.bind()))
    assert isinstance(h, DeploymentHandle)


def test_call_function_with_argument(serve_instance):
    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    @serve.deployment
    class Ingress:
        def __init__(self, h: DeploymentHandle):
            self._h = h

        async def __call__(self, name: str):
            return await self._h.remote(name)

    h = serve.run(Ingress.bind(echo.bind()))
    assert h.remote("sned").result() == "Hi sned"


def test_handle_options_with_same_router(serve_instance):
    """Make sure that multiple handles share same router object."""

    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    handle = serve.run(echo.bind())
    handle2 = handle.options(multiplexed_model_id="model2")
    assert handle2._router is handle._router


def test_init(serve_instance):
    @serve.deployment
    def f():
        return "hi"

    h = serve.run(f.bind())
    h._init(_prefer_local_routing=True)
    for _ in range(10):
        assert h.remote().result() == "hi"


def test_init_twice_fails(serve_instance):
    @serve.deployment
    def f():
        return "hi"

    h = serve.run(f.bind())
    h._init()

    with pytest.raises(RuntimeError):
        h._init()


def test_init_after_options_fails(serve_instance):
    @serve.deployment
    def f():
        return "hi"

    h = serve.run(f.bind())

    with pytest.raises(RuntimeError):
        h.options(stream=True)._init(_prefer_local_routing=True)


def test_init_after_request_fails(serve_instance):
    @serve.deployment
    def f():
        return "hi"

    h = serve.run(f.bind())
    assert h.remote().result() == "hi"

    with pytest.raises(RuntimeError):
        h._init(_prefer_local_routing=True)


def test_response_used_in_multiple_calls(serve_instance):
    @serve.deployment(graceful_shutdown_timeout_s=0)
    class F:
        async def __call__(self, sleep_amt: int, x: Any):
            await asyncio.sleep(sleep_amt)
            return f"({x})"

    @serve.deployment(graceful_shutdown_timeout_s=0)
    class Ingress:
        async def __init__(self, h):
            self.h = h

        async def __call__(self):
            # r1 will take 5 seconds to finish. This makes sure when h.remote() is
            # started for r2 and r3 (and both rely on r1), r1 is still executing.
            r1 = self.h.remote(5, "r1")

            # Neither of these should get stuck.
            r2 = self.h.remote(0, r1)
            r3 = self.h.remote(0, r1)

            return await r2, await r3

    h = serve.run(Ingress.bind(F.bind()))
    assert h.remote().result(timeout_s=10) == ("((r1))", "((r1))")


@pytest.mark.asyncio
async def test_choose_replica_and_dispatch_single(serve_instance):
    """Test choose_replica + dispatch for simple single selection pattern."""

    @serve.deployment(num_replicas=2)
    class Backend:
        def process(self, msg: str):
            from ray.serve.api import get_replica_context

            replica_id = get_replica_context().replica_id.unique_id
            return {"actual_replica_id": replica_id, "response": msg}

    @serve.deployment
    class SimpleProxy:
        def __init__(self, backend: DeploymentHandle):
            self.backend = backend

        async def handle_request(self, request: str):
            # Context manager ensures slot is released if dispatch fails or is skipped
            async with self.backend.process.choose_replica(request) as selection:
                assert selection.replica_id is not None
                assert selection.node_ip is not None

                # Dispatch to the selected replica
                response = await self.backend.process.dispatch(selection, request)

                # Return both the selection and the response for verification
                return {"selected_replica_id": selection.replica_id, **response}

    h = serve.run(SimpleProxy.bind(Backend.bind()))
    result = await h.handle_request.remote("test_message")

    # Verify the result contains the message
    assert result["response"] == "test_message"

    # Verify that dispatch sent the request to the replica we selected
    assert result["actual_replica_id"] == result["selected_replica_id"], (
        f"dispatch sent request to wrong replica: "
        f"selected {result['selected_replica_id']}, but got response from {result['actual_replica_id']}"
    )


@pytest.mark.asyncio
async def test_choose_replica_and_dispatch_parallel(serve_instance):
    """Test parallel selection pattern (e.g., PD proxy) using AsyncExitStack."""
    from contextlib import AsyncExitStack

    @serve.deployment(num_replicas=2)
    class PrefillServer:
        def chat(self, msg: str):
            from ray.serve.api import get_replica_context

            replica_id = get_replica_context().replica_id.unique_id
            return {"actual_replica_id": replica_id, "response": msg}

    @serve.deployment(num_replicas=2)
    class DecodeServer:
        def chat(self, msg: str):
            from ray.serve.api import get_replica_context

            replica_id = get_replica_context().replica_id.unique_id
            return {"actual_replica_id": replica_id, "response": msg}

    @serve.deployment
    class PDProxy:
        def __init__(
            self,
            prefill_server: DeploymentHandle,
            decode_server: DeploymentHandle,
        ):
            self.prefill = prefill_server
            self.decode = decode_server

        async def handle_request(self, request: str):
            # Use AsyncExitStack to manage multiple context managers in parallel
            async with AsyncExitStack() as stack:
                #  Select and RESERVE replicas from BOTH deployments in parallel
                p_selection, d_selection = await asyncio.gather(
                    stack.enter_async_context(self.prefill.chat.choose_replica()),
                    stack.enter_async_context(self.decode.chat.choose_replica()),
                )

                p_msg = f"prefill:{request}"
                d_msg = f"decode:{request}"

                # Dispatch to both selected replicas
                p_result, d_result = await asyncio.gather(
                    self.prefill.chat.dispatch(p_selection, p_msg),
                    self.decode.chat.dispatch(d_selection, d_msg),
                )
                return {
                    "prefill": {
                        "selected_replica_id": p_selection.replica_id,
                        **p_result,
                    },
                    "decode": {
                        "selected_replica_id": d_selection.replica_id,
                        **d_result,
                    },
                }

    h = serve.run(PDProxy.bind(PrefillServer.bind(), DecodeServer.bind()))
    result = await h.handle_request.remote("test_parallel")

    assert result["prefill"]["response"] == "prefill:test_parallel"
    assert result["decode"]["response"] == "decode:test_parallel"

    # Verify that dispatch sent the request to the replica we selected
    assert (
        result["prefill"]["actual_replica_id"]
        == result["prefill"]["selected_replica_id"]
    ), (
        f"dispatch sent request to wrong replica for prefill: "
        f"selected {result['prefill']['selected_replica_id']}, but got response from {result['prefill']['actual_replica_id']}"
    )
    assert (
        result["decode"]["actual_replica_id"] == result["decode"]["selected_replica_id"]
    ), (
        f"dispatch sent request to wrong replica for prefill: "
        f"selected {result['decode']['selected_replica_id']}, but got response from {result['decode']['actual_replica_id']}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
