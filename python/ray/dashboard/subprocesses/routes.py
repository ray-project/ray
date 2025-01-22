import collections
import functools
import inspect
import multiprocessing
from typing import AsyncIterator, Awaitable, Callable

from ray.dashboard.optional_deps import aiohttp

from ray.dashboard.routes import BaseRouteTable
from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
from ray.dashboard.subprocesses.message import (
    ErrorMessage,
    RequestMessage,
    UnaryResponseMessage,
    StreamResponseDataMessage,
    StreamResponseEndMessage,
    StreamResponseStartMessage,
)
from ray.dashboard.subprocesses.module import SubprocessModule


class SubprocessRouteTable(BaseRouteTable):
    """
    A route table to bind http route to SubprocessModuleHandle. It provides decorators
    to wrap the handler function to be used in the dispatch_child_bound_messages
    function.

    This class is used in cls object: all the decorator methods are @classmethod, and
    the routes are binded to the cls object.

    Before decoration:

        @SubprocessRouteTable.get("/get_logs")
        async def get_logs_method(self, request_body: bytes) \
            -> aiohttp.web.Response

        @SubprocessRouteTable.get("/tail_logs", streaming=True)
        async def tail_logs_method(self, request_body: bytes) \
            -> AsyncIterator[bytes]

    After decoration:

        async def get_logs_method(self,
                                  message: RequestMessage,
                                  parent_bound_queue: multiprocessing.Queue) -> None

        async def tail_logs_method(self,
                                   message: RequestMessage,
                                   parent_bound_queue: multiprocessing.Queue) -> None

    Note we have 2 handlers:
    1. the child side handler, that is `handler` that contains real logic and
        is executed in the child process. It's added with __route_method__ and
        __route_path__ attributes.
    2. the parent side handler, that just sends the request to the
        SubprocessModuleHandle at cls._bind_map[method][path].instance.

    With modifications:
    - __route_method__ and __route_path__ are added to both side's handlers.
    - method and path are added to self._bind_map.

    Lifecycle of a request:
    1. Parent receives a aiohttp request.
    2. Router finds by [method][path] and calls parent_side_handler.
    3. `parent_side_handler` bookkeeps the request with a Future and sends a
            RequestMessage to the subprocess.
    4. `SubprocessModule.dispatch_child_bound_messages` receives the
            RequestMessage and calls the child side handler.
    (real work here)
    5. `child_side_handler` sends a ParentBoundMessage to parent.
    6. `dispatch_parent_bound_messages` receives the ParentBoundMessage and
        resolves the Future with the response.
    7. aiohttp receives the response and sends it back to the client.

    Exception handling:
    - If a non-streaming child side handler raises an exception, the parent side
      handler translates it to a 500 error.
    - If a streaming child side handler already sent a chunk of data, the parent
      side handler should already sent a 200 OK with that data to the client. It will
      send str(exception) to the client and close the stream.
    """

    _bind_map = collections.defaultdict(dict)
    _routes = aiohttp.web.RouteTableDef()

    @staticmethod
    def _decorated_streaming_handler(
        handler: Callable[[SubprocessModule, RequestMessage], AsyncIterator[bytes]]
    ) -> Callable[[RequestMessage, multiprocessing.Queue], Awaitable[None]]:
        """
        Requirements to and Behavior of the handler:
            It should NOT construct a StreamingResponse object. Instead yield bytes and
            the bytes will be streamed to the client.

            After the handler yields the first chunk of data, the server prepares the
            streaming response with default headers and starts streaming the data to the
            client. If an exception is raised BEFORE the first chunk of data is yielded,
            the server will catch it and respond an error of specified HttpException
            status code, or 500 if it's other exceptions. If an exception is raised
            AFTER the first chunk of data is yielded, the server will stream the error
            message to the client and close the connection.

            After the AsyncIterator is exhausted, the server will close the connection.
        """

        @functools.wraps(handler)
        async def _streaming_handler(
            self: SubprocessModule,
            message: RequestMessage,
            parent_bound_queue: multiprocessing.Queue,
        ) -> None:
            start_message_sent = False
            try:
                async_iter = handler(self, message.body)
                async for chunk in async_iter:
                    if not start_message_sent:
                        parent_bound_queue.put(
                            StreamResponseStartMessage(
                                request_id=message.request_id, body=chunk
                            )
                        )
                        start_message_sent = True
                    else:
                        parent_bound_queue.put(
                            StreamResponseDataMessage(
                                request_id=message.request_id, body=chunk
                            )
                        )
                parent_bound_queue.put(
                    StreamResponseEndMessage(request_id=message.request_id)
                )
            except aiohttp.web.HTTPException as e:
                if not start_message_sent:
                    parent_bound_queue.put(
                        UnaryResponseMessage(
                            request_id=message.request_id,
                            status=e.status,
                            body=e.text,
                        )
                    )
                else:
                    # HTTPException can't be pickled. Instead we just send its str.
                    parent_bound_queue.put(
                        ErrorMessage(request_id=message.request_id, error=str(e))
                    )
            except Exception as e:
                parent_bound_queue.put(
                    ErrorMessage(request_id=message.request_id, error=str(e))
                )

        return _streaming_handler

    @staticmethod
    def _decorated_non_streaming_handler(
        handler: Callable[[SubprocessModule, RequestMessage], aiohttp.web.Response]
    ) -> Callable[[RequestMessage, multiprocessing.Queue], Awaitable[None]]:
        @functools.wraps(handler)
        async def _non_streaming_handler(
            self: SubprocessModule,
            message: RequestMessage,
            parent_bound_queue: multiprocessing.Queue,
        ) -> None:
            try:
                response = await handler(self, message.body)
                reply_message = UnaryResponseMessage(
                    request_id=message.request_id,
                    status=response.status,
                    body=response.body,
                )
            except aiohttp.web.HTTPException as e:
                # aiohttp.web.HTTPException cannot be pickled. Instead we send a
                # UnaryResponseMessage with status and body.
                reply_message = UnaryResponseMessage(
                    request_id=message.request_id,
                    status=e.status,
                    body=e.text,
                )
            except Exception as e:
                reply_message = ErrorMessage(request_id=message.request_id, error=e)
            parent_bound_queue.put(reply_message)

        return _non_streaming_handler

    @classmethod
    def bind(cls, instance: SubprocessModuleHandle):
        # __route_method__ and __route_path__ are added to SubprocessModule's methods,
        # not the SubprocessModuleHandle's methods.
        def predicate(o):
            if inspect.isfunction(o):
                return hasattr(o, "__route_method__") and hasattr(o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(instance.module_cls, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__route_method__][h.__route_path__].instance = instance

    @classmethod
    def _register_route(cls, method, path, **kwargs):
        """
        Register a route to the module and return the decorated handler.
        """

        def _wrapper(handler):
            if path in cls._bind_map[method]:
                bind_info = cls._bind_map[method][path]
                raise Exception(
                    f"Duplicated route path: {path}, "
                    f"previous one registered at "
                    f"{bind_info.filename}:{bind_info.lineno}"
                )

            bind_info = cls._BindInfo(
                handler.__code__.co_filename, handler.__code__.co_firstlineno, None
            )

            if kwargs.get("streaming", False):
                handler = cls._decorated_streaming_handler(handler)
            else:
                handler = cls._decorated_non_streaming_handler(handler)

            cls._bind_map[method][path] = bind_info

            async def parent_side_handler(
                request: aiohttp.web.Request,
            ) -> aiohttp.web.Response:
                bind_info = cls._bind_map[method][path]
                subprocess_module_handle = bind_info.instance
                task = subprocess_module_handle.send_request(handler.__name__, request)
                return await task

            # Used in bind().
            handler.__route_method__ = method
            handler.__route_path__ = path
            # Used in bound_routes().
            parent_side_handler.__route_method__ = method
            parent_side_handler.__route_path__ = path

            cls._routes.route(method, path)(parent_side_handler)

            return handler

        return _wrapper
