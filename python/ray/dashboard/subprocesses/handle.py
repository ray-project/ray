import asyncio
import concurrent.futures
import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import Awaitable, Dict, Optional

import aiohttp.web

from ray.dashboard.subprocesses.message import (
    ChildBoundMessage,
    ErrorMessage,
    ParentBoundMessage,
    RequestMessage,
    UnaryResponseMessage,
    StreamResponseDataMessage,
    StreamResponseEndMessage,
    StreamResponseStartMessage,
)
from ray.dashboard.subprocesses.module import (
    SubprocessModule,
    SubprocessModuleConfig,
    run_module,
)
from ray.dashboard.subprocesses.utils import assert_not_in_asyncio_loop, ThreadSafeDict

"""
This file contains code run in the parent process. It can start a subprocess and send
messages to it.
"""

logger = logging.getLogger(__name__)


class SubprocessModuleHandle:
    """
    A handle to a module created as a subprocess. Can send messages to the module and
    receive responses. On destruction, the subprocess is terminated.

    Lifecycle:
    1. In SubprocessModuleHandle creation, the subprocess is started, and 2 queues are
       created.
    2. user must call SubprocessModuleHandle.start() before it can handle parent bound
       messages.
    3. SubprocessRouteTable.bind(handle)
    4. app.add_routes(routes=SubprocessRouteTable.bound_routes())
    5. run the app.
    """

    @dataclass
    class ActiveRequest:
        request: aiohttp.web.Request
        response_fut: Awaitable[aiohttp.web.Response]
        # Only exists when the module decides this is a streaming response.
        # To keep the data sent in order, we use future to synchronize. This assumes
        # the Messages received from the Queue are in order.
        # StreamResponseStartMessage expects this to be None. It creates the future,
        # and in async, prepares a StreamResponse and resolves the future.
        # StreamResponseDataMessage expects a future. It *replaces* the future with a
        # new future by a coroutine that awaits the previous future, writes the data and
        # resolves the new future.
        # StreamResponseEndMessage expects a future. It resolves the future and sets
        # the stream_response to None.
        stream_response: Optional[
            concurrent.futures.Future[aiohttp.web.StreamResponse]
        ] = None

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        module_cls: type[SubprocessModule],
        config: SubprocessModuleConfig,
    ):
        self.loop = loop
        self.module_cls = module_cls
        self.config = config
        self.child_bound_queue = multiprocessing.Queue()
        self.parent_bound_queue = multiprocessing.Queue()

        self.dispatch_parent_bound_messages_thread = None

        self.active_requests = ThreadSafeDict[
            int, SubprocessModuleHandle.ActiveRequest
        ]()
        self.next_request_id = 0

        self.process = multiprocessing.Process(
            target=run_module,
            args=(
                self.child_bound_queue,
                self.parent_bound_queue,
                self.module_cls,
                self.config,
            ),
            daemon=True,
        )
        self.process.start()

    def start(self):
        self._start_dispatch_parent_bound_messages_thread()

    async def send_request(
        self, method_name: str, request: Optional[aiohttp.web.Request]
    ) -> Awaitable[aiohttp.web.Response]:
        """
        Sends a new request. Bookkeeps it in self.active_requests and sends the
        request to the module. Returns a Future that will be resolved with the response
        from the module.
        """
        request_id = self.next_request_id
        self.next_request_id += 1

        new_active_request = SubprocessModuleHandle.ActiveRequest(
            request=request, response_fut=self.loop.create_future()
        )
        self.active_requests.put_new(request_id, new_active_request)
        if request is None:
            body = b""
        else:
            body = await request.read()
        self._send_message(
            RequestMessage(request_id=request_id, method_name=method_name, body=body)
        )
        return await new_active_request.response_fut

    async def health_check(self):
        """
        Do internal health check. The module should respond immediately with a 200 OK.
        This can be used to measure module responsiveness in RTT, it also indicates
        subprocess event loop lag.

        Currently you get a 200 OK with body = b'ok!'. Later if we want we can add more
        observability payloads.
        """
        return await self.send_request("_internal_health_check", request=None)

    # PRIVATE METHODS
    def _start_dispatch_parent_bound_messages_thread(self):
        self.dispatch_parent_bound_messages_thread = threading.Thread(
            target=dispatch_parent_bound_messages,
            args=(self.loop, self.parent_bound_queue, self),
            daemon=True,
        )
        self.dispatch_parent_bound_messages_thread.start()

    def _send_message(self, message: ChildBoundMessage):
        self.child_bound_queue.put(message)

    @staticmethod
    async def handle_stream_response_start(
        request: aiohttp.web.Request, first_data: bytes
    ) -> aiohttp.web.StreamResponse:
        # TODO: error handling
        response = aiohttp.web.StreamResponse()
        response.content_type = "text/plain"
        await response.prepare(request)
        await response.write(first_data)
        return response

    @staticmethod
    async def handle_stream_response_data(
        prev_fut: Awaitable[aiohttp.web.StreamResponse], data: bytes
    ) -> aiohttp.web.StreamResponse:
        # TODO: error handling
        response = await asyncio.wrap_future(prev_fut)
        await response.write(data)
        return response

    @staticmethod
    async def handle_stream_response_end(
        prev_fut: Awaitable[aiohttp.web.StreamResponse],
        response_fut: Awaitable[aiohttp.web.Response],
    ) -> None:
        try:
            response = await asyncio.wrap_future(prev_fut)
            await response.write_eof()
            response_fut.set_result(response)
        except Exception as e:
            response_fut.set_exception(e)


def handle_parent_bound_message(
    loop: asyncio.AbstractEventLoop,
    message: ParentBoundMessage,
    handle: SubprocessModuleHandle,
):
    """Handles a message from the parent bound queue."""
    if isinstance(message, UnaryResponseMessage):
        active_request = handle.active_requests.pop_or_raise(message.request_id)
        # set_result is not thread safe.
        loop.call_soon_threadsafe(
            active_request.response_fut.set_result,
            aiohttp.web.Response(
                status=message.status,
                body=message.body,
            ),
        )
    elif isinstance(message, StreamResponseStartMessage):
        active_request = handle.active_requests.get_or_raise(message.request_id)
        assert active_request.stream_response is None
        # This assignment is thread safe, because a next read will come from another
        # handle_parent_bound_message call for a Stream.*Message, which will run on
        # the same thread and hence will happen-after this assignment.
        active_request.stream_response = asyncio.run_coroutine_threadsafe(
            SubprocessModuleHandle.handle_stream_response_start(
                active_request.request, message.body
            ),
            loop,
        )
    elif isinstance(message, StreamResponseDataMessage):
        active_request = handle.active_requests.get_or_raise(message.request_id)
        assert active_request.stream_response is not None
        active_request.stream_response = asyncio.run_coroutine_threadsafe(
            SubprocessModuleHandle.handle_stream_response_data(
                active_request.stream_response, message.body
            ),
            loop,
        )
    elif isinstance(message, StreamResponseEndMessage):
        active_request = handle.active_requests.pop_or_raise(message.request_id)
        assert active_request.stream_response is not None
        asyncio.run_coroutine_threadsafe(
            SubprocessModuleHandle.handle_stream_response_end(
                active_request.stream_response,
                active_request.response_fut,
            ),
            loop,
        )
    elif isinstance(message, ErrorMessage):
        # Propagate the error to aiohttp.
        active_request = handle.active_requests.pop_or_raise(message.request_id)
        loop.call_soon_threadsafe(
            active_request.response_fut.set_exception, message.error
        )
    else:
        raise ValueError(f"Unknown message type: {type(message)}")


def dispatch_parent_bound_messages(
    loop: asyncio.AbstractEventLoop,
    parent_bound_queue: multiprocessing.Queue,
    handle: SubprocessModuleHandle,
):
    """
    Dispatch Messages from the module. This function should be run in a separate thread
    from the asyncio loop of the parent process.
    """
    assert_not_in_asyncio_loop()
    while True:
        message = parent_bound_queue.get()
        try:
            handle_parent_bound_message(loop, message, handle)
        except Exception as e:
            logger.warning(
                f"Error handling parent bound message from module {handle.cls.__name__}"
                f": {e}. This may result in a http request never being responded to."
            )

    logger.info(
        f"Dispatching messages thread for module {handle.cls.__name__} is exiting"
    )
