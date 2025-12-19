import asyncio
import pickle
import sys
import threading
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, Generator, Optional, Tuple

import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from ray import serve
from ray.serve._private.common import (
    DeploymentID,
    RequestMetadata,
    RequestProtocol,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.http_util import ASGIReceiveProxy
from ray.serve._private.replica import UserCallableWrapper
from ray.serve.generated import serve_pb2


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

    def call_generator(
        self, n: int, raise_exception: bool = False
    ) -> Generator[int, None, None]:
        for i in range(n):
            yield i

            if raise_exception:
                raise RuntimeError("uh-oh!")

    async def call_async_generator(
        self, n: int, raise_exception: bool = False
    ) -> AsyncGenerator[int, None]:
        for i in range(n):
            yield i

            if raise_exception:
                raise RuntimeError("uh-oh!")


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


def basic_sync_generator(n: int, raise_exception: bool = False):
    for i in range(n):
        yield i

        if raise_exception:
            raise RuntimeError("uh-oh!")


async def basic_async_generator(n: int, raise_exception: bool = False):
    for i in range(n):
        yield i

        if raise_exception:
            raise RuntimeError("uh-oh!")


def _make_user_callable_wrapper(
    callable: Optional[Callable] = None,
    *,
    init_args: Optional[Tuple[Any]] = None,
    init_kwargs: Optional[Dict[str, Any]] = None,
    run_sync_methods_in_threadpool: bool = False,
    run_user_code_in_separate_thread: bool = True,
) -> UserCallableWrapper:
    return UserCallableWrapper(
        callable if callable is not None else BasicClass,
        init_args or tuple(),
        init_kwargs or dict(),
        deployment_id=DeploymentID(name="test_name"),
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
        local_testing_mode=False,
        deployment_config=DeploymentConfig(max_ongoing_requests=100),
        actor_id="test-actor-id",
    )


def _make_request_metadata(
    *,
    call_method: Optional[str] = None,
    is_http_request: bool = False,
    is_grpc_request: bool = False,
    is_streaming: bool = False,
) -> RequestMetadata:
    protocol = RequestProtocol.UNDEFINED
    if is_http_request:
        protocol = RequestProtocol.HTTP
    if is_grpc_request:
        protocol = RequestProtocol.GRPC

    return RequestMetadata(
        request_id="test_request",
        internal_request_id="test_internal_request",
        call_method=call_method if call_method is not None else "__call__",
        _request_protocol=protocol,
        is_streaming=is_streaming,
    )


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.asyncio
async def test_calling_initialize_twice(run_user_code_in_separate_thread: bool):
    user_callable_wrapper = _make_user_callable_wrapper(
        run_user_code_in_separate_thread=run_user_code_in_separate_thread
    )

    await user_callable_wrapper.initialize_callable()
    assert isinstance(user_callable_wrapper.user_callable, BasicClass)
    with pytest.raises(RuntimeError):
        await user_callable_wrapper.initialize_callable()


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.asyncio
async def test_calling_methods_before_initialize(
    run_user_code_in_separate_thread: bool,
):
    user_callable_wrapper = _make_user_callable_wrapper(
        run_user_code_in_separate_thread=run_user_code_in_separate_thread
    )

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_user_method(None, tuple(), dict())

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_user_health_check()

    with pytest.raises(RuntimeError):
        await user_callable_wrapper.call_reconfigure(None, rank=0)


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
@pytest.mark.asyncio
async def test_basic_class_callable(
    run_user_code_in_separate_thread: bool, run_sync_methods_in_threadpool: bool
):
    user_callable_wrapper = _make_user_callable_wrapper(
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )

    await user_callable_wrapper.initialize_callable()

    # Call non-generator method with is_streaming.
    request_metadata = _make_request_metadata(is_streaming=True)
    with pytest.raises(TypeError, match="did not return a generator."):
        async for _ in user_callable_wrapper.call_user_generator(
            request_metadata, tuple(), dict()
        ):
            pass

    # Test calling default sync `__call__` method.
    request_metadata = _make_request_metadata()
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == "hi"
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, ("-arg",), dict()
        )
        == "hi-arg"
    )
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"suffix": "-kwarg"}
        )
        == "hi-kwarg"
    )
    with pytest.raises(RuntimeError, match="uh-oh"):
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"raise_exception": True}
        )

    # Call non-generator async method with is_streaming.
    request_metadata = _make_request_metadata(
        call_method="call_async", is_streaming=True
    )
    with pytest.raises(TypeError, match="did not return a generator."):
        async for _ in user_callable_wrapper.call_user_generator(
            request_metadata, tuple(), dict()
        ):
            pass

    # Test calling `call_async` method.
    request_metadata = _make_request_metadata(call_method="call_async")
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
        == "hi"
    )
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, ("-arg",), dict()
        )
        == "hi-arg"
    )
    assert (
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"suffix": "-kwarg"}
        )
        == "hi-kwarg"
    )
    with pytest.raises(RuntimeError, match="uh-oh"):
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"raise_exception": True}
        )


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
@pytest.mark.asyncio
async def test_basic_class_callable_generators(
    run_sync_methods_in_threadpool: bool, run_user_code_in_separate_thread: bool
):
    user_callable_wrapper = _make_user_callable_wrapper(
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()

    result_list = []

    # Call sync generator without is_streaming.
    request_metadata = _make_request_metadata(
        call_method="call_generator", is_streaming=False
    )
    with pytest.raises(
        TypeError, match="Method 'call_generator' returned a generator."
    ):
        await user_callable_wrapper.call_user_method(
            request_metadata,
            (10,),
            dict(),
        )

    # Call sync generator.
    request_metadata = _make_request_metadata(
        call_method="call_generator", is_streaming=True
    )
    async for result in user_callable_wrapper.call_user_generator(
        request_metadata, (10,), dict()
    ):
        result_list.append(result)
    assert result_list == list(range(10))
    result_list.clear()

    # Call sync generator raising exception.
    with pytest.raises(RuntimeError, match="uh-oh"):
        async for result in user_callable_wrapper.call_user_generator(
            request_metadata,
            (10,),
            {"raise_exception": True},
        ):
            result_list.append(result)
    assert result_list == [0]
    result_list.clear()

    # Call async generator without is_streaming.
    request_metadata = _make_request_metadata(
        call_method="call_async_generator", is_streaming=False
    )
    with pytest.raises(
        TypeError, match="Method 'call_async_generator' returned a generator."
    ):
        await user_callable_wrapper.call_user_method(
            request_metadata,
            (10,),
            dict(),
        )

    # Call async generator.
    request_metadata = _make_request_metadata(
        call_method="call_async_generator", is_streaming=True
    )
    async for result in user_callable_wrapper.call_user_generator(
        request_metadata, (10,), dict()
    ):
        result_list.append(result)
    assert result_list == list(range(10))
    result_list.clear()

    # Call async generator raising exception.
    with pytest.raises(RuntimeError, match="uh-oh"):
        async for result in user_callable_wrapper.call_user_generator(
            request_metadata,
            (10,),
            {"raise_exception": True},
        ):
            result_list.append(result)
    assert result_list == [0]


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
@pytest.mark.parametrize("fn", [basic_sync_function, basic_async_function])
@pytest.mark.asyncio
async def test_basic_function_callable(
    fn: Callable,
    run_sync_methods_in_threadpool: bool,
    run_user_code_in_separate_thread: bool,
):
    user_callable_wrapper = _make_user_callable_wrapper(
        fn,
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()

    # Call non-generator function with is_streaming.
    request_metadata = _make_request_metadata(is_streaming=True)
    with pytest.raises(TypeError, match="did not return a generator."):
        async for _ in user_callable_wrapper.call_user_generator(
            request_metadata, tuple(), dict()
        ):
            pass

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
    with pytest.raises(RuntimeError, match="uh-oh"):
        await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), {"raise_exception": True}
        )


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
@pytest.mark.parametrize("fn", [basic_sync_generator, basic_async_generator])
@pytest.mark.asyncio
async def test_basic_function_callable_generators(
    fn: Callable,
    run_sync_methods_in_threadpool: bool,
    run_user_code_in_separate_thread: bool,
):
    user_callable_wrapper = _make_user_callable_wrapper(
        fn,
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()

    result_list = []

    # Call generator function without is_streaming.
    request_metadata = _make_request_metadata(is_streaming=False)
    with pytest.raises(
        TypeError, match=f"Method '{fn.__name__}' returned a generator."
    ):
        await user_callable_wrapper.call_user_method(
            request_metadata,
            (10,),
            dict(),
        )

    # Call generator function.
    request_metadata = _make_request_metadata(
        call_method="call_generator", is_streaming=True
    )
    async for result in user_callable_wrapper.call_user_generator(
        request_metadata, (10,), dict()
    ):
        result_list.append(result)
    assert result_list == list(range(10))
    result_list.clear()

    # Call generator function raising exception.
    with pytest.raises(RuntimeError, match="uh-oh"):
        async for result in user_callable_wrapper.call_user_generator(
            request_metadata,
            (10,),
            {"raise_exception": True},
        ):
            result_list.append(result)
    assert result_list == [0]


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.asyncio
async def test_callable_with_async_init(run_user_code_in_separate_thread: bool):
    class AsyncInitializer:
        async def __init__(self, msg: str):
            await asyncio.sleep(0.001)
            self._msg = msg

        def __call__(self) -> str:
            return self._msg

    msg = "hello world"
    user_callable_wrapper = _make_user_callable_wrapper(
        AsyncInitializer,
        init_args=(msg,),
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()
    request_metadata = _make_request_metadata()
    assert (
        await user_callable_wrapper.call_user_method(request_metadata, tuple(), dict())
    ) == msg


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("async_del", [False, True])
@pytest.mark.asyncio
async def test_destructor_only_called_once(
    async_del: bool, run_user_code_in_separate_thread: bool
):
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
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()

    # Call `call_destructor` many times in parallel; only the first one should actually
    # run the `__del__` method.
    await asyncio.gather(*[user_callable_wrapper.call_destructor() for _ in range(100)])
    assert num_destructor_calls == 1


class gRPCClass:
    def greet(self, msg: serve_pb2.UserDefinedMessage):
        return serve_pb2.UserDefinedResponse(greeting=f"Hello {msg.greeting}!")

    def stream(self, msg: serve_pb2.UserDefinedMessage):
        for i in range(10):
            yield serve_pb2.UserDefinedResponse(greeting=f"Hello {msg.greeting} {i}!")


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
@pytest.mark.asyncio
async def test_grpc_unary_request(
    run_sync_methods_in_threadpool: bool, run_user_code_in_separate_thread: bool
):
    user_callable_wrapper = _make_user_callable_wrapper(
        gRPCClass,
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()

    request_metadata = _make_request_metadata(call_method="greet", is_grpc_request=True)
    result = await user_callable_wrapper.call_user_method(
        request_metadata, (serve_pb2.UserDefinedResponse(greeting="world"),), dict()
    )
    assert isinstance(result, serve_pb2.UserDefinedResponse)
    assert result.greeting == "Hello world!"


@pytest.mark.asyncio
@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
async def test_grpc_streaming_request(
    run_sync_methods_in_threadpool: bool, run_user_code_in_separate_thread: bool
):
    user_callable_wrapper = _make_user_callable_wrapper(
        gRPCClass,
        run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
        run_user_code_in_separate_thread=run_user_code_in_separate_thread,
    )
    await user_callable_wrapper.initialize_callable()

    result_list = []

    request_metadata = _make_request_metadata(
        call_method="stream", is_grpc_request=True, is_streaming=True
    )
    async for result in user_callable_wrapper.call_user_generator(
        request_metadata,
        (serve_pb2.UserDefinedResponse(greeting="world"),),
        dict(),
    ):
        result_list.append(result)

    assert len(result_list) == 10
    for i, result in enumerate(result_list):
        assert isinstance(result, serve_pb2.UserDefinedResponse)
        assert result.greeting == f"Hello world {i}!"


class RawRequestHandler:
    async def __call__(self, request: Request) -> str:
        msg = await request.body()
        return PlainTextResponse(f"Hello {msg}!")


app = FastAPI()


@serve.ingress(app)
class FastAPIRequestHandler:
    @app.get("/")
    async def handle_root(self, request: Request) -> str:
        msg = await request.body()
        return PlainTextResponse(f"Hello {msg}!")


@pytest.mark.parametrize("run_user_code_in_separate_thread", [False, True])
@pytest.mark.parametrize("callable", [RawRequestHandler, FastAPIRequestHandler])
@pytest.mark.asyncio
async def test_http_handler(
    callable: Callable, monkeypatch, run_user_code_in_separate_thread: bool
):
    user_callable_wrapper = _make_user_callable_wrapper(
        callable, run_user_code_in_separate_thread=run_user_code_in_separate_thread
    )
    await user_callable_wrapper.initialize_callable()

    @dataclass
    class MockReplicaContext:
        servable_object: Callable

    monkeypatch.setattr(
        serve,
        "get_replica_context",
        lambda: MockReplicaContext(user_callable_wrapper.user_callable),
    )

    asgi_scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.1"},
        "http_version": "1.1",
        "server": ("127.0.0.1", 8000),
        "client": ("127.0.0.1", 51517),
        "scheme": "http",
        "method": "GET",
        "root_path": "",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": [
            (b"host", b"localhost:8000"),
            (b"user-agent", b"curl/8.1.2"),
            (b"accept", b"*/*"),
            (b"x-request-id", b"e45c04ad-bcd8-434f-8998-05689227e103"),
        ],
    }

    asgi_messages = [
        {"type": "http.request", "body": b'"world"', "more_body": False},
        {"type": "http.disconnect"},
    ]

    async def receive_asgi_messages(_: str):
        return pickle.dumps(asgi_messages)

    result_list = []

    request_metadata = _make_request_metadata(is_http_request=True, is_streaming=True)
    async for result in user_callable_wrapper.call_http_entrypoint(
        request_metadata,
        lambda *args: None,
        asgi_scope,
        ASGIReceiveProxy(asgi_scope, request_metadata, receive_asgi_messages),
    ):
        result_list.extend(result)

    assert result_list[0]["type"] == "http.response.start"
    assert result_list[0]["status"] == 200
    assert "headers" in result_list[0]
    assert result_list[1] == {
        "type": "http.response.body",
        "body": b"Hello b'\"world\"'!",
    }


class TestSeparateThread:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("run_sync_methods_in_threadpool", [False, True])
    async def test_user_code_runs_on_separate_loop(
        self, run_sync_methods_in_threadpool: bool
    ):
        main_loop = asyncio.get_running_loop()

        class GetLoop:
            def __init__(self):
                self._constructor_loop = asyncio.get_running_loop()

            async def check_health(self):
                check_health_loop = asyncio.get_running_loop()
                assert (
                    check_health_loop == self._constructor_loop
                ), "User constructor and health check should run on the same loop."
                return check_health_loop

            async def call_async(self) -> Optional[asyncio.AbstractEventLoop]:
                user_method_loop = asyncio.get_running_loop()
                assert (
                    user_method_loop == self._constructor_loop
                ), "User constructor and other methods should run on the same loop."

                return user_method_loop

            def call_sync(self):
                if run_sync_methods_in_threadpool:
                    with pytest.raises(RuntimeError, match="no running event loop"):
                        asyncio.get_running_loop()

                    user_method_loop = None
                else:
                    user_method_loop = asyncio.get_running_loop()
                    assert (
                        user_method_loop == self._constructor_loop
                    ), "User constructor and other methods should run on the same loop."

                return user_method_loop

        user_callable_wrapper = _make_user_callable_wrapper(
            GetLoop,
            run_sync_methods_in_threadpool=run_sync_methods_in_threadpool,
            run_user_code_in_separate_thread=True,
        )
        await user_callable_wrapper.initialize_callable()

        # Async methods should all run on the same loop.
        request_metadata = _make_request_metadata(call_method="call_async")
        user_code_loop = await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), dict()
        )
        assert isinstance(user_code_loop, asyncio.AbstractEventLoop)
        assert user_code_loop != main_loop

        # Sync methods should run on the same loop if run_sync_methods_in_threadpool is off,
        # else run in no asyncio loop.
        request_metadata = _make_request_metadata(call_method="call_sync")
        user_code_loop = await user_callable_wrapper.call_user_method(
            request_metadata, tuple(), dict()
        )
        if run_sync_methods_in_threadpool:
            assert user_code_loop is None
        else:
            assert isinstance(user_code_loop, asyncio.AbstractEventLoop)
            assert user_code_loop != main_loop

        # `check_health` method asserts that it runs on the correct loop.
        await user_callable_wrapper.call_user_health_check()

    @pytest.mark.asyncio
    async def test_no_user_health_check_not_blocked(self):
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
            run_user_code_in_separate_thread=True,
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
            # Instead, `user_callable_wrapper.call_user_health_check` returns None
            # when there's no user health check configured.
            assert user_callable_wrapper.call_user_health_check() is None

        sync_event.set()
        assert await blocked_future == "Sorry I got stuck!"


if __name__ == "__main__":
    # Tests are timing out on Windows for an unknown reason. Given this is just a unit
    # test, running on Linux and Mac should be sufficient.
    if sys.platform != "win32":
        sys.exit(pytest.main(["-v", "-s", __file__]))
