import asyncio
from dataclasses import dataclass
import json
from typing import Any, Callable, Dict, Tuple

import starlette.requests

from ray.serve.exceptions import RayServeException


@dataclass
class HTTPRequestWrapper:
    scope: Dict[Any, Any]
    body: bytes


def build_starlette_request(scope, serialized_body: bytes):
    """Build and return a Starlette Request from ASGI payload.

    This function is intended to be used immediately before task invocation
    happens.
    """

    # Simulates receiving HTTP body from TCP socket.  In reality, the body has
    # already been streamed in chunks and stored in serialized_body.
    received = False

    async def mock_receive():
        nonlocal received

        # If the request has already been received, starlette will keep polling
        # for HTTP disconnect. We will pause forever. The coroutine should be
        # cancelled by starlette after the response has been sent.
        if received:
            block_forever = asyncio.Event()
            await block_forever.wait()

        received = True
        return {
            "body": serialized_body,
            "type": "http.request",
            "more_body": False
        }

    return starlette.requests.Request(scope, mock_receive)


class Response:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> await Response({"k": "v"}).send(scope, receive, send)
    """

    def __init__(self, content=None, status_code=200):
        """Construct a HTTP Response based on input type.

        Args:
            content (optional): Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self.status_code = status_code
        self.raw_headers = []

        if content is None:
            self.body = b""
            self.set_content_type("text")
        elif isinstance(content, bytes):
            self.body = content
            self.set_content_type("text")
        elif isinstance(content, str):
            self.body = content.encode("utf-8")
            self.set_content_type("text-utf8")
        else:
            # Delayed import since utils depends on http_util
            from ray.serve.utils import ServeEncoder
            self.body = json.dumps(
                content, cls=ServeEncoder, indent=2).encode()
            self.set_content_type("json")

    def set_content_type(self, content_type):
        if content_type == "text":
            self.raw_headers.append([b"content-type", b"text/plain"])
        elif content_type == "text-utf8":
            self.raw_headers.append(
                [b"content-type", b"text/plain; charset=utf-8"])
        elif content_type == "json":
            self.raw_headers.append([b"content-type", b"application/json"])
        else:
            raise ValueError("Invalid content type {}".format(content_type))

    async def send(self, scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })
        await send({"type": "http.response.body", "body": self.body})


def make_startup_shutdown_hooks(app: Callable) -> Tuple[Callable, Callable]:
    """Given ASGI app, return two async callables (on_startup, on_shutdown)

    Detail spec at
    https://asgi.readthedocs.io/en/latest/specs/lifespan.html
    """
    scope = {"type": "lifespan"}

    class LifespanHandler:
        def __init__(self, lifespan_type):
            assert lifespan_type in {"startup", "shutdown"}
            self.lifespan_type = lifespan_type

        async def receive(self):
            return {"type": f"lifespan.{self.lifespan_type}"}

        async def send(self, msg):
            # We are not doing a strict equality check here because sometimes
            # starlette will output shutdown.complete on startup lifecycle
            # event!
            # https://github.com/encode/starlette/blob/5ee04ef9b1bc11dc14d299e6c855c9a3f7d5ff16/starlette/routing.py#L557 # noqa
            if msg["type"].endswith(".complete"):
                return
            elif msg["type"].endswith(".failed"):
                raise RayServeException(
                    f"Failed to run {self.lifespan_type} events for asgi app. "
                    f"Error: {msg.get('message', '')}")
            else:
                raise ValueError(f"Unknown ASGI type {msg}")

    async def startup():
        handler = LifespanHandler("startup")
        await app(scope, handler.receive, handler.send)

    async def shutdown():
        handler = LifespanHandler("shutdown")
        await app(scope, handler.receive, handler.send)

    return startup, shutdown


async def receive_http_body(scope, receive, send):
    body_buffer = []
    more_body = True
    while more_body:
        message = await receive()
        assert message["type"] == "http.request"

        more_body = message["more_body"]
        body_buffer.append(message["body"])

    return b"".join(body_buffer)
