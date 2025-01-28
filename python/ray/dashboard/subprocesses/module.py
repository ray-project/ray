import abc
import asyncio
import logging
import multiprocessing
import threading
import sys
from dataclasses import dataclass
from typing import Dict
import os
import setproctitle
from packaging.version import Version
import json

import ray
from ray._private.gcs_utils import GcsAioClient
from ray.dashboard.subprocesses.message import (
    ChildBoundMessage,
    RequestMessage,
    UnaryResponseMessage,
    ErrorMessage,
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

    cluster_id_hex: str
    gcs_address: str
    session_name: str
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


@dataclass
class SubprocessModuleRequest:
    """
    Request type for an endpoint handler in SubprocessModule. Modeled after
    aiohttp.web.Request which is not pickleable.
    """

    # Not really importing because we want to support minimal Ray later.
    # MultiMapping is just a dict, with `getall()` method to give (K -> List[V]).
    query: "multidict.MultiMapping[str, str]"  # noqa: F821
    headers: "multidict.MultiMapping[str, str]"  # noqa: F821
    body: bytes
    # If the route has a match_info, it will be stored here.
    # e.g. @SubprocessRouteTable.get("/api/data/datasets/{job_id}")
    # match_info = {"job_id": "123"}
    match_info: Dict[str, str]

    async def json(self):
        return json.loads(self.body)

    async def read(self):
        return self.body


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
        parent_process_pid: int,
    ):
        """
        Initialize current module when DashboardHead loading modules.
        :param dashboard_head: The DashboardHead instance.
        """
        self._config = config
        self._child_bound_queue = child_bound_queue
        self._parent_bound_queue = parent_bound_queue
        self._parent_process_pid = parent_process_pid
        # Lazy init
        self._gcs_aio_client = None
        self._parent_process_death_detection_task = None
        self._http_session = None

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

    @property
    def session_name(self):
        # Ray session name. It's not related to the aiohttp client session.
        return self._config.session_name

    @property
    def log_dir(self):
        return self._config.log_dir

    @property
    def http_session(self):
        # Assumes non minimal Ray.
        from ray.dashboard.optional_deps import aiohttp
        from ray.dashboard.utils import get_or_create_event_loop

        if self._http_session is not None:
            return self._http_session
        # Create a http session for this module.
        # aiohttp<4.0.0 uses a 'loop' variable, aiohttp>=4.0.0 doesn't anymore
        if Version(aiohttp.__version__) < Version("4.0.0"):
            self._http_session = aiohttp.ClientSession(loop=get_or_create_event_loop())
        else:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    @property
    def gcs_address(self):
        return self._config.gcs_address

    @property
    def gcs_aio_client(self):
        if self._gcs_aio_client is None:
            self._gcs_aio_client = GcsAioClient(
                address=self._config.gcs_address,
                nums_reconnect_retry=0,
                cluster_id=self._config.cluster_id_hex,
            )
        return self._gcs_aio_client

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
            try:
                method = getattr(self, message.method_name)
            except Exception as e:
                logger.exception(
                    f"Error getting method {message.method_name} from module {self.__class__.__name__}"
                )
                msg = ErrorMessage(request_id=message.request_id, error=e)
                self._parent_bound_queue.put(msg)
                return

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
            try:
                message = self._child_bound_queue.get()
            except Exception:
                # This can happen if the parent process died, and getting from the queue
                # can have EOFError.
                logger.exception(
                    "Error getting message from child bound queue. This module will exit."
                )
                loop.call_soon_threadsafe(sys.exit)
                break
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

    async def _detect_parent_process_death(self):
        """
        Detect parent process death by checking if ppid is still the same.
        """
        while True:
            ppid = os.getppid()
            if ppid != self._parent_process_pid:
                logger.warning(
                    f"Parent process {self._parent_process_pid} died because ppid changed to {ppid}. Exiting..."
                )
                sys.exit()
            await asyncio.sleep(1)


async def run_module_inner(
    child_bound_queue: multiprocessing.Queue,
    parent_bound_queue: multiprocessing.Queue,
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
    incarnation: int,
    parent_process_pid: int,
):

    module_name = cls.__name__

    logger.info(
        f"Starting module {module_name} with incarnation {incarnation} and config {config}"
    )

    try:
        module = cls(config, child_bound_queue, parent_bound_queue, parent_process_pid)
        module._parent_process_death_detection_task = asyncio.create_task(
            module._detect_parent_process_death()
        )
        # First init the module, then start dispatching messages.
        await module.init()
        logger.info(f"Module {module_name} initialized, receiving messages...")
    except Exception as e:
        logger.exception(f"Error creating module {module_name}")
        raise e
    loop = asyncio.get_running_loop()
    dispatch_child_bound_messages_thread = threading.Thread(
        name=f"{module_name}-dispatch_child_bound_messages_thread",
        target=module.dispatch_child_bound_messages,
        args=(loop,),
        daemon=True,
    )
    dispatch_child_bound_messages_thread.start()


def run_module(
    child_bound_queue: multiprocessing.Queue,
    parent_bound_queue: multiprocessing.Queue,
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
    incarnation: int,
    parent_process_pid: int,
):
    """
    Entrypoint for a subprocess module.
    Creates a dedicated thread to listen from the the parent queue and dispatch messages
    to the module. Only listen to the parent queue AFTER the module is prepared by
    `module.init()`.

    parent_process_pid: Used to detect if the parent process died every 1s. If it does,
    the module will exit.
    """
    module_name = cls.__name__
    current_proctitle = setproctitle.getproctitle()
    setproctitle.setproctitle(
        f"ray-dashboard-{module_name}-{incarnation} ({current_proctitle})"
    )
    logging_filename = module_logging_filename(
        module_name, incarnation, config.logging_filename
    )
    setup_component_logger(
        logging_level=config.logging_level,
        logging_format=config.logging_format,
        log_dir=config.log_dir,
        filename=logging_filename,
        max_bytes=config.logging_rotate_bytes,
        backup_count=config.logging_rotate_backup_count,
    )

    loop = asyncio.new_event_loop()
    loop.create_task(
        run_module_inner(
            child_bound_queue,
            parent_bound_queue,
            cls,
            config,
            incarnation,
            parent_process_pid,
        )
    )
    # TODO: do graceful shutdown.
    # 1. define a stop token.
    # 2. dispatch_child_bound_messages_thread will stop listening.
    # 3. join the loop to wait for all pending tasks to finish, up until a timeout.
    # 4. close the loop and exit.

    def sigterm_handler(signum, frame):
        logger.warning(f"Exiting with signal {signum} immediately...")
        sys.exit(signum)

    ray._private.utils.set_sigterm_handler(sigterm_handler)

    loop.run_forever()
