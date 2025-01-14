import abc
import asyncio
import multiprocessing
import threading
from dataclasses import dataclass

from ray._private.utils import get_or_create_event_loop
from ray.dashboard.subprocesses.message import (
    ChildBoundMessage,
    RequestMessage,
    ResponseMessage,
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
        child_bound_queue: multiprocessing.Queue[ChildBoundMessage],
        parent_bound_queue: multiprocessing.Queue[ParentBoundMessage],
    ):
        """
        Initialize current module when DashboardHead loading modules.
        :param dashboard_head: The DashboardHead instance.
        """
        self._config = config
        self._child_bound_queue = child_bound_queue
        self._parent_bound_queue = parent_bound_queue

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

    async def _internal_health_check(
        self, message: RequestMessage, parent_bound_queue: multiprocessing.Queue
    ) -> None:
        """
        Internal health check. Sends back a response to the parent queue.

        Note this is NOT registered as a route, so an external HTTP request will not
        trigger this.
        """
        try:
            parent_bound_queue.put(
                ResponseMessage(id=message.id, status=200, body=b"ok!")
            )
        except Exception as e:
            print(f"Error sending response: {e}")


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
