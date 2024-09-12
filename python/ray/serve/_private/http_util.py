import asyncio
import inspect
import json
import logging
import pickle
import socket
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, List, Optional, Tuple, Type

import starlette
from fastapi.encoders import jsonable_encoder
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray._private.pydantic_compat import IS_PYDANTIC_2
from ray.serve._private.common import RequestMetadata
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import serve_encoders
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass(frozen=True)
class ASGIArgs:
    scope: Scope
    receive: Receive
    send: Send

    def to_args_tuple(self) -> Tuple[Scope, Receive, Send]:
        return (self.scope, self.receive, self.send)

    def to_starlette_request(self) -> starlette.requests.Request:
        return starlette.requests.Request(
            *self.to_args_tuple(),
        )


def make_buffered_asgi_receive(serialized_body: bytes) -> Receive:
    """Returns an ASGI receiver that returns the provided buffered body."""

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
        return {"body": serialized_body, "type": "http.request", "more_body": False}

    return mock_receive


def convert_object_to_asgi_messages(
    obj: Optional[Any] = None, status_code: int = 200
) -> List[Message]:
    """Serializes the provided object and converts it to ASGI messages.

    These ASGI messages can be sent via an ASGI `send` interface to comprise an HTTP
    response.
    """
    body = None
    content_type = None
    if obj is None:
        body = b""
        content_type = b"text/plain"
    elif isinstance(obj, bytes):
        body = obj
        content_type = b"text/plain"
    elif isinstance(obj, str):
        body = obj.encode("utf-8")
        content_type = b"text/plain; charset=utf-8"
    else:
        # `separators=(",", ":")` will remove all whitespaces between separators in the
        # json string and return a minimized json string. This helps to reduce the size
        # of the response similar to Starlette's JSONResponse.
        body = json.dumps(
            jsonable_encoder(obj, custom_encoder=serve_encoders),
            separators=(",", ":"),
        ).encode()
        content_type = b"application/json"

    return [
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": [[b"content-type", content_type]],
        },
        {"type": "http.response.body", "body": body},
    ]


class Response:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> from ray.serve.http_util import Response  # doctest: +SKIP
    >>> scope, receive = ... # doctest: +SKIP
    >>> await Response({"k": "v"}).send(scope, receive, send) # doctest: +SKIP
    """

    def __init__(self, content=None, status_code=200):
        """Construct a HTTP Response based on input type.

        Args:
            content: Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self._messages = convert_object_to_asgi_messages(
            obj=content,
            status_code=status_code,
        )

    async def send(self, scope, receive, send):
        for message in self._messages:
            await send(message)


async def receive_http_body(scope, receive, send):
    body_buffer = []
    more_body = True
    while more_body:
        message = await receive()
        assert message["type"] == "http.request"

        more_body = message["more_body"]
        body_buffer.append(message["body"])

    return b"".join(body_buffer)


class MessageQueue(Send):
    """Queue enables polling for received or sent messages.

    Implements the ASGI `Send` interface.

    This class:
        - Is *NOT* thread safe and should only be accessed from a single asyncio
          event loop.
        - Assumes a single consumer of the queue (concurrent calls to
          `get_messages_nowait` and `wait_for_message` is undefined behavior).
    """

    def __init__(self):
        self._message_queue = deque()
        self._new_message_event = asyncio.Event()
        self._closed = False

    def close(self):
        """Close the queue, rejecting new messages.

        Once the queue is closed, existing messages will be returned from
        `get_messages_nowait` and subsequent calls to `wait_for_message` will
        always return immediately.
        """
        self._closed = True
        self._new_message_event.set()

    def put_nowait(self, message: Message):
        self._message_queue.append(message)
        self._new_message_event.set()

    async def __call__(self, message: Message):
        """Send a message, putting it on the queue.

        `RuntimeError` is raised if the queue has been closed using `.close()`.
        """
        if self._closed:
            raise RuntimeError("New messages cannot be sent after the queue is closed.")

        self.put_nowait(message)

    def get_messages_nowait(self) -> List[Message]:
        """Returns all messages that are currently available (non-blocking).

        At least one message will be present if `wait_for_message` had previously
        returned and a subsequent call to `wait_for_message` blocks until at
        least one new message is available.
        """
        messages = []
        while len(self._message_queue) > 0:
            messages.append(self._message_queue.popleft())

        self._new_message_event.clear()
        return messages

    async def wait_for_message(self):
        """Wait until at least one new message is available.

        If a message is available, this method will return immediately on each call
        until `get_messages_nowait` is called.

        After the queue is closed using `.close()`, this will always return
        immediately.
        """
        if not self._closed:
            await self._new_message_event.wait()


class ASGIReceiveProxy:
    """Proxies ASGI receive from an actor.

    The `receive_asgi_messages` callback will be called repeatedly to fetch messages
    until a disconnect message is received.
    """

    def __init__(
        self,
        scope: Scope,
        request_metadata: RequestMetadata,
        receive_asgi_messages: Callable[[RequestMetadata], Awaitable[bytes]],
    ):
        self._type = scope["type"]  # Either 'http' or 'websocket'.
        self._queue = asyncio.Queue()
        self._request_metadata = request_metadata
        self._receive_asgi_messages = receive_asgi_messages
        self._disconnect_message = None

    def _get_default_disconnect_message(self) -> Message:
        """Return the appropriate disconnect message based on the connection type.

        HTTP ASGI spec:
            https://asgi.readthedocs.io/en/latest/specs/www.html#disconnect-receive-event

        WS ASGI spec:
            https://asgi.readthedocs.io/en/latest/specs/www.html#disconnect-receive-event-ws
        """
        if self._type == "websocket":
            return {
                "type": "websocket.disconnect",
                # 1005 is the default disconnect code according to the ASGI spec.
                "code": 1005,
            }
        else:
            return {"type": "http.disconnect"}

    async def fetch_until_disconnect(self):
        """Fetch messages repeatedly until a disconnect message is received.

        If a disconnect message is received, this function exits and returns it.

        If an exception occurs, it will be raised on the next __call__ and no more
        messages will be received.
        """
        while True:
            try:
                pickled_messages = await self._receive_asgi_messages(
                    self._request_metadata
                )
                for message in pickle.loads(pickled_messages):
                    self._queue.put_nowait(message)

                    if message["type"] in {"http.disconnect", "websocket.disconnect"}:
                        self._disconnect_message = message
                        return
            except KeyError:
                # KeyError can be raised if the request is no longer active in the proxy
                # (i.e., the user disconnects). This is expected behavior and we should
                # not log an error: https://github.com/ray-project/ray/issues/43290.
                message = self._get_default_disconnect_message()
                self._queue.put_nowait(message)
                self._disconnect_message = message
                return
            except Exception as e:
                # Raise unexpected exceptions in the next `__call__`.
                self._queue.put_nowait(e)
                return

    async def __call__(self) -> Message:
        """Return the next message once available.

        This will repeatedly return a disconnect message once it's been received.
        """
        if self._queue.empty() and self._disconnect_message is not None:
            return self._disconnect_message

        message = await self._queue.get()
        if isinstance(message, Exception):
            raise message

        return message


def make_fastapi_class_based_view(fastapi_app, cls: Type) -> None:
    """Transform the `cls`'s methods and class annotations to FastAPI routes.

    Modified from
    https://github.com/dmontagu/fastapi-utils/blob/master/fastapi_utils/cbv.py

    Usage:
    >>> from fastapi import FastAPI
    >>> app = FastAPI() # doctest: +SKIP
    >>> class A: # doctest: +SKIP
    ...     @app.route("/{i}") # doctest: +SKIP
    ...     def func(self, i: int) -> str: # doctest: +SKIP
    ...         return self.dep + i # doctest: +SKIP
    >>> # just running the app won't work, here.
    >>> make_fastapi_class_based_view(app, A) # doctest: +SKIP
    >>> # now app can be run properly
    """
    # Delayed import to prevent ciruclar imports in workers.
    from fastapi import APIRouter, Depends
    from fastapi.routing import APIRoute, APIWebSocketRoute

    def get_current_servable_instance():
        from ray import serve

        return serve.get_replica_context().servable_object

    # Find all the class method routes
    class_method_routes = [
        route
        for route in fastapi_app.routes
        if
        # User defined routes must all be APIRoute or APIWebSocketRoute.
        isinstance(route, (APIRoute, APIWebSocketRoute))
        # We want to find the route that's bound to the `cls`.
        # NOTE(simon): we can't use `route.endpoint in inspect.getmembers(cls)`
        # because the FastAPI supports different routes for the methods with
        # same name. See #17559.
        and (cls.__qualname__ in route.endpoint.__qualname__)
    ]

    # Modify these routes and mount it to a new APIRouter.
    # We need to to this (instead of modifying in place) because we want to use
    # the laster fastapi_app.include_router to re-run the dependency analysis
    # for each routes.
    new_router = APIRouter()
    for route in class_method_routes:
        fastapi_app.routes.remove(route)

        # This block just adds a default values to the self parameters so that
        # FastAPI knows to inject the object when calling the route.
        # Before: def method(self, i): ...
        # After: def method(self=Depends(...), *, i):...
        old_endpoint = route.endpoint
        old_signature = inspect.signature(old_endpoint)
        old_parameters = list(old_signature.parameters.values())
        if len(old_parameters) == 0:
            # TODO(simon): make it more flexible to support no arguments.
            raise RayServeException(
                "Methods in FastAPI class-based view must have ``self`` as "
                "their first argument."
            )
        old_self_parameter = old_parameters[0]
        new_self_parameter = old_self_parameter.replace(
            default=Depends(get_current_servable_instance)
        )
        new_parameters = [new_self_parameter] + [
            # Make the rest of the parameters keyword only because
            # the first argument is no longer positional.
            parameter.replace(kind=inspect.Parameter.KEYWORD_ONLY)
            for parameter in old_parameters[1:]
        ]
        new_signature = old_signature.replace(parameters=new_parameters)
        setattr(route.endpoint, "__signature__", new_signature)
        setattr(route.endpoint, "_serve_cls", cls)
        new_router.routes.append(route)
    fastapi_app.include_router(new_router)

    routes_to_remove = list()
    for route in fastapi_app.routes:
        if not isinstance(route, (APIRoute, APIWebSocketRoute)):
            continue

        # If there is a response model, FastAPI creates a copy of the fields.
        # But FastAPI creates the field incorrectly by missing the outer_type_.
        if (
            # TODO(edoakes): I don't think this check is complete because we need
            # to support v1 models in v2 (from pydantic.v1 import *).
            not IS_PYDANTIC_2
            and isinstance(route, APIRoute)
            and route.response_model
        ):
            route.secure_cloned_response_field.outer_type_ = (
                route.response_field.outer_type_
            )

        # Remove endpoints that belong to other class based views.
        serve_cls = getattr(route.endpoint, "_serve_cls", None)
        if serve_cls is not None and serve_cls != cls:
            routes_to_remove.append(route)
    fastapi_app.routes[:] = [r for r in fastapi_app.routes if r not in routes_to_remove]


def set_socket_reuse_port(sock: socket.socket) -> bool:
    """Mutate a socket object to allow multiple process listening on the same port.

    Returns:
        success: whether the setting was successful.
    """
    try:
        # These two socket options will allow multiple process to bind the the
        # same port. Kernel will evenly load balance among the port listeners.
        # Note: this will only work on Linux.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # In some Python binary distribution (e.g., conda py3.6), this flag
        # was not present at build time but available in runtime. But
        # Python relies on compiler flag to include this in binary.
        # Therefore, in the absence of socket.SO_REUSEPORT, we try
        # to use `15` which is value in linux kernel.
        # https://github.com/torvalds/linux/blob/master/tools/include/uapi/asm-generic/socket.h#L27
        else:
            sock.setsockopt(socket.SOL_SOCKET, 15, 1)
        return True
    except Exception as e:
        logger.debug(
            f"Setting SO_REUSEPORT failed because of {e}. SO_REUSEPORT is disabled."
        )
        return False


class ASGIAppReplicaWrapper:
    """Provides a common wrapper for replicas running an ASGI app."""

    def __init__(self, app: ASGIApp):
        self._asgi_app = app

        # Use uvicorn's lifespan handling code to properly deal with
        # startup and shutdown event.
        self._serve_asgi_lifespan = LifespanOn(Config(self._asgi_app, lifespan="on"))

        # Replace uvicorn logger with our own.
        self._serve_asgi_lifespan.logger = logger

    async def _run_asgi_lifespan_startup(self):
        # LifespanOn's logger logs in INFO level thus becomes spammy
        # Within this block we temporarily uplevel for cleaner logging
        from ray.serve._private.logging_utils import LoggingContext

        with LoggingContext(self._serve_asgi_lifespan.logger, level=logging.WARNING):
            await self._serve_asgi_lifespan.startup()
            if self._serve_asgi_lifespan.should_exit:
                raise RuntimeError(
                    "ASGI lifespan startup failed. Check replica logs for details."
                )

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> Optional[ASGIApp]:
        """Calls into the wrapped ASGI app."""
        await self._asgi_app(
            scope,
            receive,
            send,
        )

    # NOTE: __del__ must be async so that we can run ASGI shutdown
    # in the same event loop.
    async def __del__(self):
        # LifespanOn's logger logs in INFO level thus becomes spammy.
        # Within this block we temporarily uplevel for cleaner logging.
        from ray.serve._private.logging_utils import LoggingContext

        with LoggingContext(self._serve_asgi_lifespan.logger, level=logging.WARNING):
            await self._serve_asgi_lifespan.shutdown()


def validate_http_proxy_callback_return(
    middlewares: Any,
) -> [starlette.middleware.Middleware]:
    """Validate the return value of HTTP proxy callback.

    Middlewares should be a list of Starlette middlewares. If it is None, we
    will treat it as an empty list. If it is not a list, we will raise an
    error. If it is a list, we will check if all the items in the list are
    Starlette middlewares.
    """

    if middlewares is None:
        middlewares = []
    if not isinstance(middlewares, list):
        raise ValueError(
            "HTTP proxy callback must return a list of Starlette middlewares."
        )
    else:
        # All middlewares must be Starlette middlewares.
        # https://www.starlette.io/middleware/#using-pure-asgi-middleware
        for middleware in middlewares:
            if not issubclass(type(middleware), starlette.middleware.Middleware):
                raise ValueError(
                    "HTTP proxy callback must return a list of Starlette middlewares, "
                    f"instead got {type(middleware)} type item in the list."
                )
    return middlewares
