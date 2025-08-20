import asyncio
import inspect
import json
import logging
import pickle
import socket
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import starlette
import uvicorn
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from packaging import version
from starlette.datastructures import MutableHeaders
from starlette.middleware import Middleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray._common.pydantic_compat import IS_PYDANTIC_2
from ray.exceptions import RayActorError, RayTaskError
from ray.serve._private.common import RequestMetadata
from ray.serve._private.constants import (
    RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S,
    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH,
    RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S,
    SERVE_HTTP_REQUEST_ID_HEADER,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.proxy_request_response import ResponseStatus
from ray.serve._private.utils import (
    call_function_from_import_path,
    generate_request_id,
    serve_encoders,
)
from ray.serve.config import HTTPOptions
from ray.serve.exceptions import (
    BackPressureError,
    DeploymentUnavailableError,
    RayServeException,
)

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
        self._error = None

    def close(self):
        """Close the queue, rejecting new messages.

        Once the queue is closed, existing messages will be returned from
        `get_messages_nowait` and subsequent calls to `wait_for_message` will
        always return immediately.
        """
        self._closed = True
        self._new_message_event.set()

    def set_error(self, e: BaseException):
        self._error = e

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

    async def wait_for_message(self):
        """Wait until at least one new message is available.

        If a message is available, this method will return immediately on each call
        until `get_messages_nowait` is called.

        After the queue is closed using `.close()`, this will always return
        immediately.
        """
        if not self._closed:
            await self._new_message_event.wait()

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

    async def get_one_message(self) -> Message:
        """This blocks until a message is ready.

        This method should not be used together with get_messages_nowait.
        Please use either `get_one_message` or `get_messages_nowait`.

        Raises:
            StopAsyncIteration: if the queue is closed and there are no
                more messages.
            Exception (self._error): if there are no more messages in
                the queue and an error has been set.
        """

        if self._error:
            raise self._error

        await self._new_message_event.wait()

        if len(self._message_queue) > 0:
            msg = self._message_queue.popleft()

            if len(self._message_queue) == 0 and not self._closed:
                self._new_message_event.clear()

            return msg
        elif len(self._message_queue) == 0 and self._error:
            raise self._error
        elif len(self._message_queue) == 0 and self._closed:
            raise StopAsyncIteration

    async def fetch_messages_from_queue(
        self, call_fut: asyncio.Future
    ) -> AsyncGenerator[List[Any], None]:
        """Repeatedly consume messages from the queue and yield them.

        This is used to fetch queue messages in the system event loop in
        a thread-safe manner.

        Args:
            call_fut: The async Future pointing to the task from the user
                code event loop that is pushing messages onto the queue.

        Yields:
            List[Any]: Messages from the queue.
        """
        # Repeatedly consume messages from the queue.
        wait_for_msg_task = None
        try:
            while True:
                wait_for_msg_task = asyncio.create_task(self.wait_for_message())
                done, _ = await asyncio.wait(
                    [call_fut, wait_for_msg_task], return_when=asyncio.FIRST_COMPLETED
                )

                messages = self.get_messages_nowait()
                if messages:
                    yield messages

                # Exit once `call_fut` has finished. In this case, all
                # messages must have already been sent.
                if call_fut in done:
                    break

            e = call_fut.exception()
            if e is not None:
                raise e from None
        finally:
            if not call_fut.done():
                call_fut.cancel()

            if wait_for_msg_task is not None and not wait_for_msg_task.done():
                wait_for_msg_task.cancel()


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
        # Lazy init the queue to ensure it is created in the user code event loop.
        self._queue = None
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

    @property
    def queue(self) -> asyncio.Queue:
        if self._queue is None:
            self._queue = asyncio.Queue()

        return self._queue

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
                    self.queue.put_nowait(message)

                    if message["type"] in {"http.disconnect", "websocket.disconnect"}:
                        self._disconnect_message = message
                        return
            except KeyError:
                # KeyError can be raised if the request is no longer active in the proxy
                # (i.e., the user disconnects). This is expected behavior and we should
                # not log an error: https://github.com/ray-project/ray/issues/43290.
                message = self._get_default_disconnect_message()
                self.queue.put_nowait(message)
                self._disconnect_message = message
                return
            except Exception as e:
                # Raise unexpected exceptions in the next `__call__`.
                self.queue.put_nowait(e)
                return

    async def __call__(self) -> Message:
        """Return the next message once available.

        This will repeatedly return a disconnect message once it's been received.
        """
        if self.queue.empty() and self._disconnect_message is not None:
            return self._disconnect_message

        message = await self.queue.get()
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

    async def get_current_servable_instance():
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
        route.endpoint.__signature__ = new_signature
        route.endpoint._serve_cls = cls
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

    def __init__(self, app_or_func: Union[ASGIApp, Callable]):
        if inspect.isfunction(app_or_func):
            self._asgi_app = app_or_func()
        else:
            self._asgi_app = app_or_func

        # Use uvicorn's lifespan handling code to properly deal with
        # startup and shutdown event.
        # If log_config is not None, uvicorn will use the default logger.
        # and that interferes with our logging setup.
        self._serve_asgi_lifespan = LifespanOn(
            Config(
                self._asgi_app,
                lifespan="on",
                log_level=None,
                log_config=None,
                access_log=False,
            )
        )

        # Replace uvicorn logger with our own.
        self._serve_asgi_lifespan.logger = logger

    @property
    def app(self) -> ASGIApp:
        return self._asgi_app

    @property
    def docs_path(self) -> Optional[str]:
        if isinstance(self._asgi_app, FastAPI):
            return self._asgi_app.docs_url

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
) -> [Middleware]:
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
            if not issubclass(type(middleware), Middleware):
                raise ValueError(
                    "HTTP proxy callback must return a list of Starlette middlewares, "
                    f"instead got {type(middleware)} type item in the list."
                )
    return middlewares


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp):
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        headers = MutableHeaders(scope=scope)
        request_id = headers.get(SERVE_HTTP_REQUEST_ID_HEADER)

        if request_id is None:
            request_id = generate_request_id()
            headers.append(SERVE_HTTP_REQUEST_ID_HEADER, request_id)

        async def send_with_request_id(message: Message):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.append("X-Request-ID", request_id)
            if message["type"] == "websocket.accept":
                message["X-Request-ID"] = request_id
            await send(message)

        await self._app(scope, receive, send_with_request_id)


def _apply_middlewares(app: ASGIApp, middlewares: List[Callable]) -> ASGIApp:
    """Wrap the ASGI app with the provided middlewares.

    The built-in RequestIdMiddleware will always be applied first.
    """
    for middleware in [Middleware(RequestIdMiddleware)] + middlewares:
        if version.parse(starlette.__version__) < version.parse("0.35.0"):
            app = middleware.cls(app, **middleware.options)
        else:
            # In starlette >= 0.35.0, middleware.options does not exist:
            # https://github.com/encode/starlette/pull/2381.
            app = middleware.cls(
                app,
                *middleware.args,
                **middleware.kwargs,
            )

    return app


async def start_asgi_http_server(
    app: ASGIApp,
    http_options: HTTPOptions,
    *,
    event_loop: asyncio.AbstractEventLoop,
    enable_so_reuseport: bool = False,
) -> asyncio.Task:
    """Start an HTTP server to run the ASGI app.

    Returns a task that blocks until the server exits (e.g., due to error).
    """
    app = _apply_middlewares(app, http_options.middlewares)

    sock = socket.socket()
    if enable_so_reuseport:
        set_socket_reuse_port(sock)

    try:
        sock.bind((http_options.host, http_options.port))
    except OSError as e:
        raise RuntimeError(
            f"Failed to bind to address '{http_options.host}:{http_options.port}'."
        ) from e

    # Even though we set log_level=None, uvicorn adds MessageLoggerMiddleware
    # if log level for uvicorn.error is not set. And MessageLoggerMiddleware
    # has no use to us.
    logging.getLogger("uvicorn.error").level = logging.CRITICAL

    # NOTE: We have to use lower level uvicorn Config and Server
    # class because we want to run the server as a coroutine. The only
    # alternative is to call uvicorn.run which is blocking.
    server = uvicorn.Server(
        config=uvicorn.Config(
            lambda: app,
            factory=True,
            host=http_options.host,
            port=http_options.port,
            root_path=http_options.root_path,
            timeout_keep_alive=http_options.keep_alive_timeout_s,
            loop=event_loop,
            lifespan="off",
            access_log=False,
            log_level=None,
            log_config=None,
        )
    )

    # NOTE(edoakes): we need to override install_signal_handlers here
    # because the existing implementation fails if it isn't running in
    # the main thread and uvicorn doesn't expose a way to configure it.
    server.install_signal_handlers = lambda: None

    return event_loop.create_task(server.serve(sockets=[sock]))


def get_http_response_status(
    exc: BaseException, request_timeout_s: float, request_id: str
) -> ResponseStatus:
    if isinstance(exc, TimeoutError):
        return ResponseStatus(
            code=408,
            is_error=True,
            message=f"Request {request_id} timed out after {request_timeout_s}s.",
        )

    elif isinstance(exc, asyncio.CancelledError):
        message = f"Client for request {request_id} disconnected, cancelling request."
        logger.info(message)
        return ResponseStatus(
            code=499,
            is_error=True,
            message=message,
        )
    elif isinstance(exc, (BackPressureError, DeploymentUnavailableError)):
        if isinstance(exc, RayTaskError):
            logger.warning(f"Request failed: {exc}", extra={"log_to_stderr": False})
        return ResponseStatus(
            code=503,
            is_error=True,
            message=exc.message,
        )
    else:
        if isinstance(exc, (RayActorError, RayTaskError)):
            logger.warning(f"Request failed: {exc}", extra={"log_to_stderr": False})
        else:
            logger.exception("Request failed due to unexpected error.")
        return ResponseStatus(
            code=500,
            is_error=True,
            message=str(exc),
        )


def send_http_response_on_exception(
    status: ResponseStatus, response_started: bool
) -> List[Message]:
    if response_started or status.code not in (408, 503):
        return []
    return convert_object_to_asgi_messages(
        status.message,
        status_code=status.code,
    )


def configure_http_options_with_defaults(http_options: HTTPOptions) -> HTTPOptions:
    """Enhanced configuration with component-specific options."""

    http_options = deepcopy(http_options)

    # Apply environment defaults
    if (RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S or 0) > 0:
        http_options.keep_alive_timeout_s = RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S

    # TODO: Deprecate SERVE_REQUEST_PROCESSING_TIMEOUT_S env var
    if http_options.request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S:
        http_options.request_timeout_s = (
            http_options.request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
        )

    http_options.middlewares = http_options.middlewares or []

    return http_options


def configure_http_middlewares(http_options: HTTPOptions) -> HTTPOptions:
    http_options = deepcopy(http_options)

    # Add environment variable middleware
    if RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH:
        logger.info(
            f"Calling user-provided callback from import path "
            f"'{RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH}'."
        )

        # noinspection PyTypeChecker
        http_options.middlewares.extend(
            validate_http_proxy_callback_return(
                call_function_from_import_path(
                    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH
                )
            )
        )

    return http_options
