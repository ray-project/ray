import asyncio
import pickle
import sys
from typing import Generator, Tuple
from unittest.mock import MagicMock, patch

import pytest
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware

from ray._common.utils import get_or_create_event_loop
from ray.serve import HTTPOptions
from ray.serve._private.http_util import (
    ASGIReceiveProxy,
    MessageQueue,
    configure_http_middlewares,
    configure_http_options_with_defaults,
)


@pytest.mark.asyncio
async def test_message_queue_nowait():
    queue = MessageQueue()

    # Check that wait_for_message hangs until a message is sent.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.wait_for_message(), 0.001)

    assert len(list(queue.get_messages_nowait())) == 0

    await queue({"type": "http.response.start"})
    await queue.wait_for_message()
    assert len(list(queue.get_messages_nowait())) == 1

    # Check that messages are cleared after being consumed.
    assert len(list(queue.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.wait_for_message(), 0.001)

    # Check that consecutive messages are returned in order.
    await queue({"type": "http.response.start", "idx": 0})
    await queue({"type": "http.response.start", "idx": 1})
    await queue.wait_for_message()
    messages = list(queue.get_messages_nowait())
    assert len(messages) == 2
    assert messages[0]["idx"] == 0
    assert messages[1]["idx"] == 1

    assert len(list(queue.get_messages_nowait())) == 0
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.wait_for_message(), 0.001)

    # Check that a concurrent waiter is notified when a message is available.
    loop = asyncio.get_running_loop()
    waiting_task = loop.create_task(queue.wait_for_message())
    for _ in range(1000):
        assert not waiting_task.done()

    await queue({"type": "http.response.start"})
    await waiting_task
    assert len(list(queue.get_messages_nowait())) == 1

    # Check that once the queue is closed, new messages should be rejected and
    # ongoing and subsequent calls to wait for messages should return immediately.
    waiting_task = loop.create_task(queue.wait_for_message())
    queue.close()
    await waiting_task  # Ongoing call should return.

    for _ in range(100):
        with pytest.raises(RuntimeError):
            await queue({"hello": "world"})
        await queue.wait_for_message()
        assert queue.get_messages_nowait() == []


@pytest.mark.asyncio
async def test_message_queue_wait():
    queue = MessageQueue()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get_one_message(), 0.001)

    queue.put_nowait("A")
    assert await queue.get_one_message() == "A"

    # Check that messages are cleared after being consumed.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get_one_message(), 0.001)

    # Check that consecutive messages are returned in order.
    queue.put_nowait("B")
    queue.put_nowait("C")
    assert await queue.get_one_message() == "B"
    assert await queue.get_one_message() == "C"

    # Check that messages are cleared after being consumed.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get_one_message(), 0.001)

    # Check that a concurrent waiter is notified when a message is available.
    loop = asyncio.get_running_loop()
    fetch_task = loop.create_task(queue.get_one_message())
    for _ in range(1000):
        assert not fetch_task.done()
    queue.put_nowait("D")
    assert await fetch_task == "D"


@pytest.mark.asyncio
async def test_message_queue_wait_closed():
    queue = MessageQueue()

    queue.put_nowait("A")
    assert await queue.get_one_message() == "A"

    # Check that once the queue is closed, ongoing and subsequent calls
    # to get_one_message should raise an exception
    loop = asyncio.get_running_loop()
    fetch_task = loop.create_task(queue.get_one_message())
    queue.close()
    with pytest.raises(StopAsyncIteration):
        await fetch_task

    for _ in range(10):
        with pytest.raises(StopAsyncIteration):
            await queue.get_one_message()


@pytest.mark.asyncio
async def test_message_queue_wait_error():
    queue = MessageQueue()

    queue.put_nowait("A")
    assert await queue.get_one_message() == "A"

    # Check setting an error
    loop = asyncio.get_running_loop()
    fetch_task = loop.create_task(queue.get_one_message())
    queue.set_error(TypeError("uh oh! something went wrong."))
    with pytest.raises(TypeError, match="uh oh! something went wrong"):
        await fetch_task

    for _ in range(10):
        with pytest.raises(TypeError, match="uh oh! something went wrong"):
            await queue.get_one_message()


@pytest.fixture
@pytest.mark.asyncio
def setup_receive_proxy(
    request,
) -> Generator[Tuple[ASGIReceiveProxy, MessageQueue], None, None]:
    # Param can be 'http' (default) or 'websocket' (ASGI scope type).
    type = getattr(request, "param", "http")

    queue = MessageQueue()

    async def receive_asgi_messages(request_id: str) -> bytes:
        await queue.wait_for_message()
        messages = queue.get_messages_nowait()
        for message in messages:
            if isinstance(message, Exception):
                raise message

        return pickle.dumps(messages)

    loop = get_or_create_event_loop()
    asgi_receive_proxy = ASGIReceiveProxy({"type": type}, "", receive_asgi_messages)
    receiver_task = loop.create_task(asgi_receive_proxy.fetch_until_disconnect())
    try:
        yield asgi_receive_proxy, queue
    except Exception:
        receiver_task.cancel()


@pytest.mark.asyncio
class TestASGIReceiveProxy:
    async def test_basic(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, MessageQueue]
    ):
        asgi_receive_proxy, queue = setup_receive_proxy

        queue.put_nowait({"type": "foo"})
        queue.put_nowait({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        assert asgi_receive_proxy._queue.empty()

        # Once disconnect is received, it should be returned repeatedly.
        queue.put_nowait({"type": "http.disconnect"})
        for _ in range(100):
            assert await asgi_receive_proxy() == {"type": "http.disconnect"}

        # Subsequent messages should be ignored.
        queue.put_nowait({"type": "baz"})
        assert await asgi_receive_proxy() == {"type": "http.disconnect"}

    async def test_raises_exception(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, MessageQueue]
    ):
        asgi_receive_proxy, queue = setup_receive_proxy

        queue.put_nowait({"type": "foo"})
        queue.put_nowait({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        queue.put_nowait(RuntimeError("oopsies"))
        with pytest.raises(RuntimeError, match="oopsies"):
            await asgi_receive_proxy()

    @pytest.mark.parametrize(
        "setup_receive_proxy",
        ["http", "websocket"],
        indirect=True,
    )
    async def test_return_disconnect_on_key_error(
        self, setup_receive_proxy: Tuple[ASGIReceiveProxy, MessageQueue]
    ):
        """If the proxy is no longer handling a given request, it raises a KeyError.

        In these cases, the ASGI receive proxy should return a disconnect message.

        See https://github.com/ray-project/ray/pull/44647 for details.
        """
        asgi_receive_proxy, queue = setup_receive_proxy

        queue.put_nowait({"type": "foo"})
        queue.put_nowait({"type": "bar"})
        assert await asgi_receive_proxy() == {"type": "foo"}
        assert await asgi_receive_proxy() == {"type": "bar"}

        queue.put_nowait(KeyError("not found"))
        for _ in range(100):
            if asgi_receive_proxy._type == "http":
                assert await asgi_receive_proxy() == {"type": "http.disconnect"}
            else:
                assert await asgi_receive_proxy() == {
                    "type": "websocket.disconnect",
                    "code": 1005,
                }

    async def test_receive_asgi_messages_raises(self):
        async def receive_asgi_messages(request_id: str) -> bytes:
            raise RuntimeError("maybe actor crashed")

        loop = get_or_create_event_loop()
        asgi_receive_proxy = ASGIReceiveProxy(
            {"type": "http"}, "", receive_asgi_messages
        )
        receiver_task = loop.create_task(asgi_receive_proxy.fetch_until_disconnect())

        try:
            with pytest.raises(RuntimeError, match="maybe actor crashed"):
                await asgi_receive_proxy()
        finally:
            receiver_task.cancel()


class MockMiddleware:
    """Mock middleware class for testing."""

    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return isinstance(other, MockMiddleware) and self.name == other.name

    def __repr__(self):
        return f"MockMiddleware({self.name})"


@pytest.fixture
def base_http_options():
    """Provides basic HTTPOptions for testing."""
    return HTTPOptions(
        host="0.0.0.0",
        port=8000,
        request_timeout_s=30.0,
        keep_alive_timeout_s=5.0,
        middlewares=[],
    )


@pytest.fixture
def mock_env_constants():
    """Mock environment constants with default values."""
    with patch.multiple(
        "ray.serve._private.http_util",
        RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S=300,
        RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S=300,
        RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH=None,
    ):
        yield


class TestConfigureHttpOptionsWithDefaults:
    """Test suite for configure_http_options_with_defaults function."""

    def test_basic_configuration_with_mock_env(
        self, base_http_options, mock_env_constants
    ):
        """Test basic configuration with mocked environment constants."""
        result = configure_http_options_with_defaults(base_http_options)

        # Should apply default request timeout from mock (30)
        assert result.request_timeout_s == 30.0
        # Keep alive timeout should remain original since mock sets it to 300
        assert result.keep_alive_timeout_s == 300.0
        # Should initialize middlewares list
        assert result.middlewares == []
        # Original should not be modified
        assert base_http_options.request_timeout_s == 30.0

    def test_keep_alive_timeout_override_from_env(self, base_http_options):
        """Test keep alive timeout override from environment variable."""
        with patch(
            "ray.serve._private.http_util.RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S", 10
        ):
            result = configure_http_options_with_defaults(base_http_options)
            assert result.keep_alive_timeout_s == 10

    def test_request_timeout_preserved_when_already_set(self):
        """Test that existing request timeout is preserved when already set."""
        http_options = HTTPOptions(
            host="0.0.0.0",
            port=8000,
            request_timeout_s=120.0,
            keep_alive_timeout_s=5.0,
            middlewares=[],
        )

        with patch(
            "ray.serve._private.http_util.RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S", 300
        ):
            result = configure_http_options_with_defaults(http_options)
            assert result.request_timeout_s == 120.0

    @patch("ray.serve._private.http_util.call_function_from_import_path")
    @patch(
        "ray.serve._private.http_util.RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH",
        "my.module.callback",
    )
    def test_callback_middleware_injection(self, mock_call_function, base_http_options):
        """Test that the callback middleware is injected correctly."""

        # Arrange: Create a valid middleware by wrapping it with Starlette's Middleware class
        class CustomMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):
                response = await call_next(request)  # Simply pass the request through
                return response

        # Mock the app argument
        mock_app = MagicMock()

        wrapped_middleware = Middleware(CustomMiddleware, app=mock_app)
        mock_call_function.return_value = [
            wrapped_middleware
        ]  # Return list of wrapped middleware

        # Act
        result = configure_http_middlewares(base_http_options)

        # Assert
        mock_call_function.assert_called_once_with(
            "my.module.callback"
        )  # Verify callback execution
        assert len(result.middlewares) == 1  # Ensure one middleware was injected
        assert isinstance(result.middlewares[0], Middleware)

    def test_callback_middleware_disabled(self, base_http_options):
        """Test that callback middleware is not loaded when disabled."""
        with patch(
            "ray.serve._private.http_util.RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH",
            "",
        ):
            result = configure_http_options_with_defaults(base_http_options)

            # Assert that no callback middleware is added
            assert result.middlewares == []

    def test_deep_copy_behavior(self, base_http_options, mock_env_constants):
        """Test that an original HTTPOptions object is not modified."""
        original_timeout = base_http_options.request_timeout_s

        result = configure_http_options_with_defaults(base_http_options)

        # Original should remain unchanged
        assert base_http_options.request_timeout_s == original_timeout
        # Result should be a different object
        assert result is not base_http_options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
