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
from ray.serve._private.common import DeploymentID
from ray.serve._private.http_util import (
    ASGIReceiveProxy,
    MessageQueue,
    configure_http_middlewares,
    configure_http_options_with_defaults,
    convert_object_to_asgi_messages,
    get_http_response_status,
    retry_after_headers,
    send_http_response_on_exception,
)
from ray.serve._private.proxy_request_response import ResponseStatus
from ray.serve.exceptions import BackPressureError, DeploymentUnavailableError


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


class TestConfigureHttpOptionsWithDefaults:
    """Test suite for configure_http_options_with_defaults function."""

    def test_basic_configuration(self, base_http_options):
        """Test basic configuration preserves settings."""
        result = configure_http_options_with_defaults(base_http_options)

        # Request timeout should be preserved
        assert result.request_timeout_s == 30.0
        # Keep alive timeout should be preserved (no env override)
        assert result.keep_alive_timeout_s == 5.0
        # Should initialize middlewares list
        assert result.middlewares == []
        # Original should not be modified
        assert base_http_options.request_timeout_s == 30.0

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

    def test_deep_copy_behavior(self, base_http_options):
        """Test that an original HTTPOptions object is not modified."""
        original_timeout = base_http_options.request_timeout_s

        result = configure_http_options_with_defaults(base_http_options)

        # Original should remain unchanged
        assert base_http_options.request_timeout_s == original_timeout
        # Result should be a different object
        assert result is not base_http_options


class TestBackpressureHTTPResponse:
    def test_backpressure_error_defaults_to_503_without_headers(self):
        status = get_http_response_status(BackPressureError(1, 1), None, "req-1")
        assert status.code == 503
        assert status.is_error
        assert status.headers is None
        assert "backpressure" in status.message

    def test_backpressure_error_with_configured_status_and_retry_after(self):
        exc = BackPressureError(1, 1, status_code=429, retry_after_s=7)
        status = get_http_response_status(exc, None, "req-1")
        assert status.code == 429
        assert status.is_error
        assert status.headers == [(b"retry-after", b"7")]

    def test_retry_after_headers_rounds_up_to_delay_seconds(self):
        assert retry_after_headers(None) is None
        assert retry_after_headers(0) == [(b"retry-after", b"0")]
        assert retry_after_headers(0.2) == [(b"retry-after", b"1")]
        assert retry_after_headers(7.5) == [(b"retry-after", b"8")]
        assert retry_after_headers(10) == [(b"retry-after", b"10")]
        # Negative values can't come from config (validated >= 0), but the
        # helper clamps at 0 so an invalid header is never sent on the wire.
        assert retry_after_headers(-5) == [(b"retry-after", b"0")]

    def test_deployment_unavailable_error_stays_503_without_headers(self):
        exc = DeploymentUnavailableError(DeploymentID(name="d", app_name="app"))
        status = get_http_response_status(exc, None, "req-1")
        assert status.code == 503
        assert status.headers is None

    def test_send_http_response_on_exception_emits_429_with_headers(self):
        status = ResponseStatus(
            code=429,
            is_error=True,
            message="Request dropped due to backpressure",
            headers=[(b"retry-after", b"5")],
        )
        messages = send_http_response_on_exception(status, response_started=False)
        start = messages[0]
        assert start["type"] == "http.response.start"
        assert start["status"] == 429
        assert [b"retry-after", b"5"] in [list(h) for h in start["headers"]]

        # Nothing can be sent if the response already started.
        assert send_http_response_on_exception(status, response_started=True) == []

    def test_convert_object_to_asgi_messages_extra_headers(self):
        messages = convert_object_to_asgi_messages(
            "hi", status_code=429, extra_headers=[(b"retry-after", b"3")]
        )
        headers = [list(h) for h in messages[0]["headers"]]
        assert [b"retry-after", b"3"] in headers

    def test_backpressure_error_pickle_round_trip(self):
        # The carried fields must survive pickling (e.g., when the error
        # crosses process boundaries wrapped in a RayTaskError).
        exc = pickle.loads(
            pickle.dumps(BackPressureError(3, 2, status_code=429, retry_after_s=1.5))
        )
        assert exc.status_code == 429
        assert exc.retry_after_s == 1.5
        assert "backpressure" in exc.message

        # Old-style construction without the new arguments still works.
        exc = pickle.loads(pickle.dumps(BackPressureError(3, 2)))
        assert exc.status_code == 503
        assert exc.retry_after_s is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
