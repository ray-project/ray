import abc
import asyncio
import logging
import multiprocessing
import threading
from dataclasses import dataclass

from ray.dashboard.subprocesses.message import (
    ChildBoundMessage,
    RequestMessage,
    UnaryResponseMessage,
)
from ray.dashboard.subprocesses.utils import (
    assert_not_in_asyncio_loop,
    module_logging_filename,
)
from ray._private.ray_logging import setup_component_logger

logger = logging.getLogger(__name__)


@dataclass
class SubprocessModuleConfig:
    """
    Configuration for a SubprocessModule.
    Pickleable.
    """

    # Logger configs. Will be set up in subprocess entrypoint `run_module`.
    logging_level: str
    logging_format: str
    log_dir: str
    # Name of the "base" log file. Its stem is appended with the Module.__name__.
    # e.g. when logging_filename = "dashboard.log", and Module is JobHead,
    # we will set up logger with name "dashboard-JobHead.log". This name will again be
    # appended with .1 and .2 for rotation.
    logging_filename: str
    logging_rotate_bytes: int
    logging_rotate_backup_count: int


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

    @staticmethod
    def is_minimal_module():
        """
        Currently all SubprocessModule classes should be non-minimal.

        We require this because SubprocessModuleHandle tracks aiohttp requests and
        responses. To ease this, we can define another SubprocessModuleMinimalHandle
        that doesn't track requests and responses, but still provides Queue interface
        and health check.
        TODO(ryw): If needed, create SubprocessModuleMinimalHandle.
        """
        return False

    @abc.abstractmethod
    async def init(self):
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
            except Exception:
                logger.exception(
                    f"Error handling child bound message {message}. This request will hang forever."
                )

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
                UnaryResponseMessage(
                    request_id=message.request_id, status=200, body=b"ok!"
                )
            )
        except Exception as e:
            logger.error(
                f"Error sending response: {e}. This means we will never reply the parent's health check request. The parent will think the module is dead."
            )


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
    `module.init()`.
    """
    module_name = cls.__name__
    logging_filename = module_logging_filename(module_name, config.logging_filename)
    setup_component_logger(
        logging_level=config.logging_level,
        logging_format=config.logging_format,
        log_dir=config.log_dir,
        filename=logging_filename,
        max_bytes=config.logging_rotate_bytes,
        backup_count=config.logging_rotate_backup_count,
    )

    assert_not_in_asyncio_loop()

    loop = asyncio.new_event_loop()
    module = cls(config, child_bound_queue, parent_bound_queue)

    loop.run_until_complete(module.init())

    dispatch_child_bound_messages_thread = threading.Thread(
        name=f"{module_name}-dispatch_child_bound_messages_thread",
        target=module.dispatch_child_bound_messages,
        args=(loop,),
        daemon=True,
    )
    dispatch_child_bound_messages_thread.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        # TODO: do graceful shutdown.
        # 1. define a stop token.
        # 2. dispatch_child_bound_messages_thread will stop listening.
        # 3. join the loop to wait for all pending tasks to finish, up until a timeout.
        # 4. close the loop and exit.
        loop.stop()
    finally:
        loop.close()
