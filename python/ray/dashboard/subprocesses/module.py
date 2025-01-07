import abc
import asyncio
import functools
import inspect
import multiprocessing
import threading
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, List, Tuple

# TODO: import from optional_deps ? or just declare it required?
import aiohttp.web

from ray._private.utils import get_or_create_event_loop
from ray.dashboard.routes import BaseRouteTable
from ray.dashboard.subprocesses.message import (
    ChildBoundMessage,
    ErrorMessage,
    HealthCheckMessage,
    HealthCheckResponseMessage,
    RequestMessage,
    ResponseMessage,
    StreamingResponseDataMessage,
    StreamingResponseEndMessage,
    StreamingResponseStartMessage,
)
from ray.dashboard.subprocesses.utils import assert_not_in_asyncio_loop


@dataclass
class SubprocessModuleConfig:
    """
    Configuration for a SubprocessModule.
    Pickleable.
    """

    session_name: str
    config: dict


class SubprocessModule(abc.ABC):
    """
    A Dashboard Head Module that runs in a subprocess. This is used with the decorators
    to define a (request -> response) endpoint, or a (request -> AsyncIterator[bytes])
    for a streaming endpoint.
    """

    def __init__(
        self,
        config: SubprocessModuleConfig,
        child_bound_queue: multiprocessing.Queue,
        parent_bound_queue: multiprocessing.Queue,
    ):
        """
        Initialize current module when DashboardHead loading modules.
        :param dashboard_head: The DashboardHead instance.
        """
        self._config = config
        self._child_bound_queue = child_bound_queue
        self._parent_bound_queue = parent_bound_queue
        # List of (method, path) tuples.
        self._routes: List[Tuple[str, str]] = []

    @abc.abstractmethod
    async def run(self):
        """
        Run the module in an asyncio loop. A head module can provide
        servicers to the server.

        Only after this method is returned, the module will start receiving messages
        from the parent queue.
        """
        pass

    def handle_child_bound_message(
        self,
        loop: asyncio.AbstractEventLoop,
        message: ChildBoundMessage,
    ):
        """Handles a message from the child bound queue."""
        if isinstance(message, RequestMessage):
            # Assume module has a method_name method that has signature:
            #
            # async def my_handler(self: SubprocessModule,
            #                      message: RequestMessage,
            #                      parent_bound_queue: multiprocessing.Queue) -> None
            #
            # which comes from the decorators from MethodRouteTable.
            method = getattr(self, message.method_name)
            # getattr() already binds self to method, so we don't need to pass it.
            asyncio.run_coroutine_threadsafe(
                method(message, self._parent_bound_queue), loop
            )
        elif isinstance(message, HealthCheckMessage):
            # dispatch to the loop and await once to detect if the module is healthy.
            # This goes through the loop to detect loop hangs, but does not hit the
            # module code.
            async def respond_health_check(queue: multiprocessing.Queue):
                await asyncio.sleep(0)
                try:
                    queue.put(HealthCheckResponseMessage(id=message.id), timeout=1)
                except Exception as e:
                    print(f"Exception responding to health check to {queue}: {e}")

            asyncio.run_coroutine_threadsafe(
                respond_health_check(self._parent_bound_queue), loop
            )
        else:
            raise ValueError(f"Unknown message type: {type(message)}")

    def dispatch_child_bound_messages(
        self,
        loop: asyncio.AbstractEventLoop,
    ):
        """
        Dispatch Messages to the module. This function should be run in a separate
        thread from the asyncio loop of the module.
        """
        assert_not_in_asyncio_loop()
        while True:
            message = self._child_bound_queue.get()
            try:
                self.handle_child_bound_message(loop, message)
            except Exception as e:
                print(f"Error handling child bound message: {e}")


class SubprocessRouteTable(BaseRouteTable):
    """
    A helper class to bind http route to SubprocessModuleHandle. It provides decorators
    to wrap the handler function to be used in the dispatch_child_bound_messages
    function.

    Before decoration:

        @my_route.get("/get_logs")
        async def get_logs_method(self, request_body: bytes) \
            -> aiohttp.web.Response

        @my_route.get("/tail_logs", streaming=True)
        async def tail_logs_method(self, request_body: bytes) \
            -> AsyncIterator[bytes]

    After decoration:

        async def get_logs_method(self,
                                  message: RequestMessage,
                                  parent_bound_queue: multiprocessing.Queue) -> None

        async def tail_logs_method(self,
                                   message: RequestMessage,
                                   parent_bound_queue: multiprocessing.Queue) -> None

    With modifications:
        - __route_method__ and __route_path__ are added to both side's handlers.
        - method and path are added to self._bind_map.

    """

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
            the server will catch it and respond a 500 error. If an exception is raised
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
            try:
                async_iter = handler(self, message.body)
                start_message_sent = False
                async for chunk in async_iter:
                    if not start_message_sent:
                        parent_bound_queue.put(
                            StreamingResponseStartMessage(id=message.id, body=chunk)
                        )
                        start_message_sent = True
                    else:
                        parent_bound_queue.put(
                            StreamingResponseDataMessage(id=message.id, body=chunk)
                        )
                parent_bound_queue.put(StreamingResponseEndMessage(id=message.id))
            except aiohttp.web.HTTPException as e:
                # aiohttp.web.HTTPException cannot be pickled. Instead we send a
                # ResponseMessage with status and body.
                parent_bound_queue.put(
                    ResponseMessage(
                        id=message.id,
                        status=e.status,
                        body=e.text,
                    )
                )
            except Exception as e:
                parent_bound_queue.put(ErrorMessage(id=message.id, error=e))

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
                reply_message = ResponseMessage(
                    id=message.id,
                    status=response.status,
                    body=response.body,
                )
            except aiohttp.web.HTTPException as e:
                # aiohttp.web.HTTPException cannot be pickled. Instead we send a
                # ResponseMessage with status and body.
                reply_message = ResponseMessage(
                    id=message.id,
                    status=e.status,
                    body=e.text,
                )
            except Exception as e:
                reply_message = ErrorMessage(id=message.id, error=e)
            parent_bound_queue.put(reply_message)

        return _non_streaming_handler

    @classmethod
    def bind(cls, instance):  # "SubprocessModuleHandle"
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

            # Note we have 2 handlers:
            # 1. the child side handler, that is `handler` that contains real logic and
            #    is executed in the child process. It's added with __route_method__ and
            #    __route_path__ attributes.
            # 2. the parent side handler, that just sends the request to the
            #    SubprocessModuleHandle at cls._bind_map[method][path].instance.
            #
            # Lifecycle of a request:
            # 1. Parent receives a aiohttp request.
            # 2. Router finds by [method][path] and calls parent_side_handler.
            # 3. parent_side_handler bookkeeps the request with a Future and sends a
            #       RequestMessage to the subprocess.
            # 4. SubprocessModule.dispatch_child_bound_messages receives the
            #       RequestMessage and calls the child side handler.
            # (real work here)
            # 5. child side handler sends a ParentBoundMessage to parent.
            # 6. dispatch_parent_bound_messages receives the ParentBoundMessage and
            #    resolves the Future with the response.
            # 7. aiohttp receives the response and sends it back to the client.
            cls._bind_map[method][path] = bind_info

            async def parent_side_handler(
                request: aiohttp.web.Request,
            ) -> aiohttp.web.Response:
                bind_info = cls._bind_map[method][path]
                subprocess_module_handle = bind_info.instance
                task = subprocess_module_handle.send_request(handler.__name__, request)
                return await task

            handler.__route_method__ = method
            handler.__route_path__ = path
            parent_side_handler.__route_method__ = method
            parent_side_handler.__route_path__ = path

            cls._routes.route(method, path)(parent_side_handler)

            return handler

        return _wrapper


def run_module(
    child_bound_queue: multiprocessing.Queue,
    parent_bound_queue: multiprocessing.Queue,
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
):
    """
    Entrypoint for a subprocess module.
    Creates a dedicated thread to listen from the the parent queue and dispatch messages
    to the module. Only listen to the parent queue AFTER the module is prepared by
    `module.run()`.
    """
    loop = get_or_create_event_loop()
    module = cls(config, child_bound_queue, parent_bound_queue)

    loop.run_until_complete(module.run())

    dispatch_child_bound_messages_thread = threading.Thread(
        target=module.dispatch_child_bound_messages,
        args=(loop,),
        daemon=True,
    )
    dispatch_child_bound_messages_thread.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()
    finally:
        loop.close()
