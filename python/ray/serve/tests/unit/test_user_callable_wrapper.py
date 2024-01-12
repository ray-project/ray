import asyncio
import sys
import threading
from typing import Callable, Optional

import pytest

from ray.exceptions import RayTaskError
from ray.serve._private.common import DeploymentID, RequestProtocol

# from ray.serve._private.http_util import MessageQueue
from ray.serve._private.replica import UserCallableWrapper
from ray.serve._private.router import RequestMetadata


class BasicClass:
    def __call__(self, suffix: Optional[str] = None, raise_exception: bool = False):
        if raise_exception:
            raise RuntimeError("uh-oh!")

        return "hi" + (suffix if suffix is not None else "")

    async def call_async(
        self, suffix: Optional[str] = None, raise_exception: bool = False
    ):
        if raise_exception:
            raise RuntimeError("uh-oh!")

        return "hi" + (suffix if suffix is not None else "")


def basic_sync_function(suffix: Optional[str] = None, raise_exception: bool = False):
    if raise_exception:
        raise RuntimeError("uh-oh!")

    return "hi" + (suffix if suffix is not None else "")


async def basic_async_function(
    suffix: Optional[str] = None, raise_exception: bool = False
):
    if raise_exception:
        raise RuntimeError("uh-oh!")

    return "hi" + (suffix if suffix is not None else "")


"""
Tests to write:
    - sync method calls
    - async method calls
    - sync generator calls
    - async generator calls
    - stuff related to grpc
    - stuff related to http
"""


def _make_user_callable_wrapper(
    callable: Optional[Callable] = None, *init_args, **init_kwargs
) -> UserCallableWrapper:
    return UserCallableWrapper(
        callable if callable is not None else BasicClass,
        init_args,
        init_kwargs,
        deployment_id=DeploymentID(app="test_app", name="test_name"),
    )


def _make_request_metadata(
    *,
    call_method: Optional[str] = None,
    is_http_request: bool = False,
    is_grpc_request: bool = False
) -> RequestMetadata:
    protocol = RequestProtocol.UNDEFINED
    if is_http_request:
        protocol = RequestProtocol.HTTP
    if is_grpc_request:
        protocol = RequestProtocol.GRPC

    return RequestMetadata(
        request_id="test_request",
        endpoint="test_endpoint",
        call_method=call_method if call_method is not None else "__call__",
        _request_protocol=protocol,
    )


@pytest.mark.asyncio
async def test_calling_initialize_twice():
    user_callable_wrapper = _make_user_callable_wrapper()

    await user_callable_wrapper.initialize_callable()
    assert isinstance(user_callable_wrapper.user_callable, BasicClass)
    with pytest.raises(RuntimeError):
        await user_callable_wrapper.initialize_callable()


@pytest.mark.asyncio
async def test_calling_methods_before_initialize():
    user_callable_wrapper = _make_user_callable_wrapper()

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_user_method(None, tuple(), dict())

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_user_health_check()

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_reconfigure(None)

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_destructor()


@pytest.mark.asyncio
async def test_basic_class_callable():
    user_callable_wrapper = _make_user_callable_wrapper()

    await user_callable_wrapper.initialize_callable()

    # Test calling default sync `__call__` method.
    request_metadata = _make_request_metadata()
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == "hi"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, ("-arg",), dict()
        )
    ) == "hi-arg"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"suffix": "-kwarg"}
        )
    ) == "hi-kwarg"
    with pytest.raises(RayTaskError, match="uh-oh"):
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"raise_exception": True}
        )

    # Test calling `call_async` method.
    request_metadata = _make_request_metadata(call_method="call_async")
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == "hi"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, ("-arg",), dict()
        )
    ) == "hi-arg"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"suffix": "-kwarg"}
        )
    ) == "hi-kwarg"
    with pytest.raises(RayTaskError, match="uh-oh"):
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"raise_exception": True}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("fn", [basic_sync_function, basic_async_function])
async def test_basic_function_callable(fn: Callable):
    user_callable_wrapper = _make_user_callable_wrapper(fn)
    await user_callable_wrapper.initialize_callable()
    request_metadata = _make_request_metadata()
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == "hi"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, ("-arg",), dict()
        )
    ) == "hi-arg"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"suffix": "-kwarg"}
        )
    ) == "hi-kwarg"
    with pytest.raises(RayTaskError, match="uh-oh"):
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"raise_exception": True}
        )


@pytest.mark.asyncio
async def test_user_code_runs_on_separate_loop():
    main_loop = asyncio.get_running_loop()

    class GetLoop:
        def __call__(self) -> asyncio.AbstractEventLoop:
            return asyncio.get_running_loop()

    user_callable_wrapper = _make_user_callable_wrapper(GetLoop)
    await user_callable_wrapper.initialize_callable()
    request_metadata = _make_request_metadata()
    user_code_loop = await user_callable_wrapper.call_user_method(
        request_metadata, tuple(), dict()
    )
    assert isinstance(user_code_loop, asyncio.AbstractEventLoop)
    assert user_code_loop != main_loop


@pytest.mark.asyncio
async def test_callable_with_async_init():
    class AsyncInitializer:
        async def __init__(self, msg: str):
            await asyncio.sleep(0.001)
            self._msg = msg

        def __call__(self) -> str:
            return self._msg

    msg = "hello world"
    user_callable_wrapper = _make_user_callable_wrapper(
        AsyncInitializer,
        msg,
    )
    await user_callable_wrapper.initialize_callable()
    request_metadata = _make_request_metadata()
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == msg


@pytest.mark.asyncio
@pytest.mark.parametrize("async_del", [False, True])
async def test_destructor_only_called_once(async_del: bool):
    num_destructor_calls = 0

    if async_del:

        class DestroyerOfNothing:
            async def __del__(self) -> str:
                nonlocal num_destructor_calls
                num_destructor_calls += 1

    else:

        class DestroyerOfNothing:
            def __del__(self) -> str:
                nonlocal num_destructor_calls
                num_destructor_calls += 1

    user_callable_wrapper = _make_user_callable_wrapper(
        DestroyerOfNothing,
    )
    await user_callable_wrapper.initialize_callable()

    # Call `call_destructor` many times in parallel; only the first one should actually
    # run the `__del__` method.
    await asyncio.gather(
        *[
            asyncio.ensure_future(user_callable_wrapper.call_destructor())
            for _ in range(100)
        ]
    )
    assert num_destructor_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("async_del", [False, True])
async def test_no_user_health_check_not_blocked(async_del: bool):
    """
    If there is no user-defined health check, it should not interact with the user code
    event loop at all and therefore still return if the event loop is blocked.
    """
    sync_event = threading.Event()

    class LoopBlocker:
        async def __call__(self) -> str:
            # Block the loop until the event is set.
            sync_event.wait()
            return "Sorry I got stuck!"

    user_callable_wrapper = _make_user_callable_wrapper(
        LoopBlocker,
    )
    await user_callable_wrapper.initialize_callable()
    request_metadata = _make_request_metadata()
    blocked_future = user_callable_wrapper.call_user_method(
        request_metadata, tuple(), dict()
    )
    _, pending = await asyncio.wait([blocked_future], timeout=0.01)
    assert len(pending) == 1

    for _ in range(100):
        # If this called something on the event loop, it'd be blocked.
        await user_callable_wrapper.call_user_health_check()

    sync_event.set()
    assert await blocked_future == "Sorry I got stuck!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
