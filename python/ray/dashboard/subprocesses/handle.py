import asyncio
import multiprocessing
import threading
from dataclasses import dataclass
from typing import Dict

import aiohttp.web

from ray.dashboard.subprocesses.message import (
    ChildBoundMessage,
    ErrorMessage,
    HealthCheckResponseMessage,
    ParentBoundMessage,
    RequestMessage,
    ResponseMessage,
    StreamingResponseDataMessage,
    StreamingResponseEndMessage,
    StreamingResponseStartMessage,
)
from ray.dashboard.subprocesses.module import (
    SubprocessModule,
    SubprocessModuleConfig,
    run_module,
)
from ray.dashboard.subprocesses.utils import assert_not_in_asyncio_loop

"""
This file contains code run in the parent process. It can start a subprocess and send
messages to it.
"""


class SubprocessModuleHandle:
    """
    A handle to a module created as a subprocess. Can send messages to the module and
    receive responses. On destruction, the subprocess is terminated.
    """

    @dataclass
    class ActiveRequest:
        request: aiohttp.web.Request
        response_fut: asyncio.Future[aiohttp.web.Response]

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

        self.active_requests: Dict[int, SubprocessModuleHandle.ActiveRequest] = {}
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

    def start_dispatch_parent_bound_messages_thread(self):
        self.dispatch_parent_bound_messages_thread = threading.Thread(
            target=dispatch_parent_bound_messages,
            args=(self.loop, self.parent_bound_queue, self),
            daemon=True,
        )
        self.dispatch_parent_bound_messages_thread.start()

    def send_message(self, message: ChildBoundMessage):
        self.child_bound_queue.put(message)

    async def send_request(
        self, method_name: str, request: aiohttp.web.Request
    ) -> asyncio.Future[aiohttp.web.Response]:
        """
        Sends a new request. Bookkeeps it in self.active_requests and sends the
        request to the module. Returns a Future that will be resolved with the response
        from the module.
        """
        request_id = self.next_request_id
        self.next_request_id += 1

        self.active_requests[request_id] = SubprocessModuleHandle.ActiveRequest(
            request=request, response_fut=self.loop.create_future()
        )
        body = await request.read()
        self.send_message(
            RequestMessage(id=request_id, method_name=method_name, body=body)
        )
        return await self.active_requests[request_id].response_fut


def handle_parent_bound_message(
    loop: asyncio.AbstractEventLoop,
    message: ParentBoundMessage,
    handle: SubprocessModuleHandle,
):
    """Handles a message from the parent bound queue."""
    if isinstance(message, ResponseMessage):
        response_fut = handle.active_requests[message.id].response_fut
        # set_result is not thread safe.
        loop.call_soon_threadsafe(
            response_fut.set_result,
            aiohttp.web.Response(
                status=message.status,
                body=message.body,
            ),
        )
        del handle.active_requests[message.id]
    elif isinstance(message, StreamingResponseStartMessage):
        # TODO(ryw): Implement streaming response. Problem is we need to keep the state
        # machine in order, so we need to "await" the start to resolve before we send
        # the data, and then await the data to resolve before we send a second data.
        raise NotImplementedError("Streaming response is not implemented yet")
    elif isinstance(message, StreamingResponseDataMessage):
        raise NotImplementedError("Streaming response is not implemented yet")
    elif isinstance(message, StreamingResponseEndMessage):
        raise NotImplementedError("Streaming response is not implemented yet")
    elif isinstance(message, ErrorMessage):
        # Propagate the error to aiohttp.
        response_fut = handle.active_requests[message.id].response_fut
        loop.call_soon_threadsafe(response_fut.set_exception, message.error)
        del handle.active_requests[message.id]
    elif isinstance(message, HealthCheckResponseMessage):
        # TODO(ryw): Implement health check timeouts, and cancel the timeout here.
        pass
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
            print(
                f"Error handling parent bound message from module {handle.cls.__name__}"
                f": {e}. This may result in a http request never being responded to."
            )

    print(f"Dispatching messages thread for module {handle.cls.__name__} is exiting")
