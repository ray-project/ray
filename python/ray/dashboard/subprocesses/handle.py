import asyncio
import concurrent.futures
import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import Awaitable, Optional

from ray.dashboard.optional_deps import aiohttp

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
from ray.dashboard.subprocesses.utils import (
    assert_not_in_asyncio_loop,
    ThreadSafeDict,
    module_logging_filename,
)

"""
This file contains code run in the parent process. It can start a subprocess and send
messages to it. Requires non-minimal Ray.
"""

logger = logging.getLogger(__name__)


class SubprocessModuleHandle:
    """
    A handle to a module created as a subprocess. Can send messages to the module and
    receive responses. On destruction, the subprocess is terminated.

    Lifecycle:
    1. In SubprocessModuleHandle creation, the subprocess is started, and 2 queues are
       created.
    2. User must call SubprocessModuleHandle.start_module() before it can handle parent
       bound messages.
    3. SubprocessRouteTable.bind(handle)
    4. app.add_routes(routes=SubprocessRouteTable.bound_routes())
    5. Run the app.

    Health check (_do_periodic_health_check):
    Every 1s, do a health check by _do_once_health_check. If the module is
    unhealthy:
      1. log the exception
      2. log the last N lines of the log file
      3. fail all active requests
      4. restart the module

    TODO(ryw): define policy for health check:
    - check period (Now: 1s)
    - define unhealthy. (Now: process exits. TODO: check_health() for event loop hang)
    - check number of failures in a row before we deem it unhealthy (Now: N/A)
    - "max number of restarts"? (Now: infinite)
    """

    @dataclass
    class ActiveRequest:
        request: aiohttp.web.Request
        # Future to a Response as the result of a aiohttp handler. It's can be a
        # Response for a unary request, or a StreamResponse for a streaming request.
        response_fut: Awaitable[aiohttp.web.StreamResponse]
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

        # Increment this when the module is restarted.
        self.incarnation = 0
        # Runtime states, set by start_module(), reset by destroy_module().
        self.next_request_id = None
        self.child_bound_queue = None
        self.parent_bound_queue = None
        self.active_requests = ThreadSafeDict[
            int, SubprocessModuleHandle.ActiveRequest
        ]()
        self.process = None
        self.dispatch_parent_bound_messages_thread = None
        self.health_check_task = None

    def str_for_state(self, incarnation: int, pid: Optional[int]):
        return f"SubprocessModuleHandle(module_cls={self.module_cls.__name__}, incarnation={incarnation}, pid={pid})"

    def __str__(self):
        return self.str_for_state(
            self.incarnation, self.process.pid if self.process else None
        )

    def start_module(self, start_dispatch_parent_bound_messages_thread: bool = True):
        """
        Params:
        - start_dispatch_parent_bound_messages_thread: used for testing.
        """
        self.next_request_id = 0
        self.child_bound_queue = multiprocessing.Queue()
        self.parent_bound_queue = multiprocessing.Queue()
        self.active_requests.pop_all()
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

        if start_dispatch_parent_bound_messages_thread:
            self.dispatch_parent_bound_messages_thread = threading.Thread(
                name=f"{self.module_cls.__name__}-dispatch_parent_bound_messages_thread",
                target=self.dispatch_parent_bound_messages,
                daemon=True,
            )
            self.dispatch_parent_bound_messages_thread.start()

        self.health_check_task = self.loop.create_task(self._do_periodic_health_check())

    async def destroy_module(self, reason: Exception):
        """
        Destroy the module. This is called when the module is unhealthy.

        async because we need to set exceptions to the futures.

        Params:
        - reason: the exception that caused the module to be destroyed. Propagated to
          active requests so they can be failed.
        """
        self.incarnation += 1
        self.next_request_id = 0
        self.process.terminate()
        self.process = None

        for active_request in self.active_requests.pop_all().values():
            active_request.response_fut.set_exception(reason)
        self.parent_bound_queue.close()
        self.parent_bound_queue = None

        self.child_bound_queue.close()
        self.child_bound_queue = None

        # dispatch_parent_bound_messages_thread is daemon so we don't need to join it.
        self.dispatch_parent_bound_messages_thread = None

        self.health_check_task.cancel()
        self.health_check_task = None

    async def health_check(self) -> aiohttp.web.Response:
        """
        Do internal health check. The module should respond immediately with a 200 OK.
        This can be used to measure module responsiveness in RTT, it also indicates
        subprocess event loop lag.

        Currently you get a 200 OK with body = b'ok!'. Later if we want we can add more
        observability payloads.
        """
        return await self.send_request("_internal_health_check", request=None)

    async def _do_once_health_check(self):
        """
        Do a health check once. We check for:
        1. if the process exits, it's considered died.

        # TODO(ryw): also do `await self.health_check()` and define a policy to
        # determine if the process is dead.
        """
        if self.process.exitcode is not None:
            raise RuntimeError(f"Process exited with code {self.process.exitcode}")

    async def _do_periodic_health_check(self):
        """
        Every 1s, do a health check. If the module is unhealthy:
        1. log the exception
        2. log the last N lines of the log file
        3. fail all active requests
        4. restart the module
        """
        while True:
            try:
                await self._do_once_health_check()
            except Exception as e:
                filename = module_logging_filename(
                    self.module_cls.__name__, self.config.logging_filename
                )
                logger.exception(
                    f"Module {self.module_cls.__name__} is unhealthy. Please refer to"
                    f"{self.config.log_dir}/{filename} "
                    "for more details. Failing all active requests."
                )
                await self.destroy_module(e)
                self.start_module()
                return
            await asyncio.sleep(1)

    async def send_request(
        self, method_name: str, request: Optional[aiohttp.web.Request]
    ) -> Awaitable[aiohttp.web.StreamResponse]:
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
        response_fut: Awaitable[aiohttp.web.StreamResponse],
    ) -> None:
        try:
            response = await asyncio.wrap_future(prev_fut)
            await response.write_eof()
            response_fut.set_result(response)
        except Exception as e:
            response_fut.set_exception(e)

    @staticmethod
    async def handle_stream_response_error(
        prev_fut: Awaitable[aiohttp.web.StreamResponse],
        exception: Exception,
        response_fut: Awaitable[aiohttp.web.StreamResponse],
    ) -> None:
        """
        When the async iterator in the module raises an error, we need to propagate it
        to the client and close the stream. However, we already sent a 200 OK to the
        client and can't change that to a 500. We can't just raise an exception here to
        aiohttp because that causes it to abruptly close the connection and the client
        will raise a ClientPayloadError(TransferEncodingError).

        Instead, we write exception to the stream and close the stream.
        """
        try:
            response = await asyncio.wrap_future(prev_fut)
            await response.write(str(exception).encode())
            await response.write_eof()
            response_fut.set_result(response)
        except Exception as e:
            response_fut.set_exception(e)

    def handle_parent_bound_message(self, message: ParentBoundMessage):
        """Handles a message from the parent bound queue. This function must run on a
        dedicated thread, called by dispatch_parent_bound_messages."""
        loop = self.loop
        if isinstance(message, UnaryResponseMessage):
            active_request = self.active_requests.pop_or_raise(message.request_id)
            # set_result is not thread safe.
            loop.call_soon_threadsafe(
                active_request.response_fut.set_result,
                aiohttp.web.Response(
                    status=message.status,
                    body=message.body,
                ),
            )
        elif isinstance(message, StreamResponseStartMessage):
            active_request = self.active_requests.get_or_raise(message.request_id)
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
            active_request = self.active_requests.get_or_raise(message.request_id)
            assert active_request.stream_response is not None
            active_request.stream_response = asyncio.run_coroutine_threadsafe(
                SubprocessModuleHandle.handle_stream_response_data(
                    active_request.stream_response, message.body
                ),
                loop,
            )
        elif isinstance(message, StreamResponseEndMessage):
            active_request = self.active_requests.pop_or_raise(message.request_id)
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
            active_request = self.active_requests.pop_or_raise(message.request_id)
            if active_request.stream_response is not None:
                asyncio.run_coroutine_threadsafe(
                    SubprocessModuleHandle.handle_stream_response_error(
                        active_request.stream_response,
                        message.error,
                        active_request.response_fut,
                    ),
                    loop,
                )
            else:
                loop.call_soon_threadsafe(
                    active_request.response_fut.set_exception, message.error
                )
        else:
            raise ValueError(f"Unknown message type: {type(message)}")

    def dispatch_parent_bound_messages(self):
        """
        Dispatch Messages from the module. This function should be run in a separate thread
        from the asyncio loop of the parent process.
        """
        assert_not_in_asyncio_loop()
        incarnation = self.incarnation
        pid = self.process.pid if self.process else None
        self_str = self.str_for_state(incarnation, pid)

        queue = self.parent_bound_queue
        # Exit if the module has restarted.
        while incarnation == self.incarnation:
            message = None
            try:
                message = queue.get(timeout=1)
            except multiprocessing.queues.Empty:
                # Empty is normal.
                continue
            except ValueError:
                # queue is closed.
                break
            except Exception:
                logger.exception(
                    f"Error unpickling parent bound message from {self_str}."
                    " This may result in a http request never being responded to."
                )
                continue
            try:
                self.handle_parent_bound_message(message)
            except Exception:
                logger.exception(
                    f"Error handling parent bound message from {self_str}."
                    " This may result in a http request never being responded to."
                )

        logger.info(f"dispatch_parent_bound_messages thread for {self_str} is exiting")
