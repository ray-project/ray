import abc
import asyncio
import aiohttp
import inspect
import logging
import sys
import os
from dataclasses import dataclass
import setproctitle
import multiprocessing

import ray
from ray._raylet import GcsClient
from ray._private.gcs_utils import GcsAioClient
from ray._private.ray_logging import configure_log_file
from ray._private.utils import open_log
from ray.dashboard.subprocesses.utils import (
    module_logging_filename,
    get_socket_path,
    get_named_pipe_path,
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
    # The directory where the socket file will be created.
    socket_dir: str


class SubprocessModule(abc.ABC):
    """
    A Dashboard Head Module that runs in a subprocess as a standalone aiohttp server.
    """

    def __init__(
        self,
        config: SubprocessModuleConfig,
    ):
        """
        Initialize current module when DashboardHead loading modules.
        :param dashboard_head: The DashboardHead instance.
        """
        self._config = config
        self._parent_process = multiprocessing.parent_process()
        # Lazy init
        self._gcs_client = None
        self._gcs_aio_client = None
        self._parent_process_death_detection_task = None
        self._http_session = None

    async def _detect_parent_process_death(self):
        """
        Detect parent process liveness. Only returns when parent process is dead.
        """
        while True:
            if not self._parent_process.is_alive():
                logger.warning(
                    f"Parent process {self._parent_process.pid} died. Exiting..."
                )
                return
            await asyncio.sleep(1)

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

    async def run(self):
        """
        Start running the module.
        This method should be called first before the module starts receiving requests.
        """
        app = aiohttp.web.Application()
        routes: list[aiohttp.web.RouteDef] = [
            aiohttp.web.get("/api/healthz", self._internal_module_health_check)
        ]
        handlers = inspect.getmembers(
            self,
            lambda x: (
                inspect.ismethod(x)
                and hasattr(x, "__route_method__")
                and hasattr(x, "__route_path__")
            ),
        )
        for _, handler in handlers:
            routes.append(
                aiohttp.web.route(
                    handler.__route_method__,
                    handler.__route_path__,
                    handler,
                )
            )
        app.add_routes(routes)
        runner = aiohttp.web.AppRunner(app, access_log=None)
        await runner.setup()

        module_name = self.__class__.__name__
        if sys.platform == "win32":
            named_pipe_path = get_named_pipe_path(module_name)
            site = aiohttp.web.NamedPipeSite(runner, named_pipe_path)
            logger.info(f"Started aiohttp server over {named_pipe_path}.")
        else:
            socket_path = get_socket_path(self._config.socket_dir, module_name)
            site = aiohttp.web.UnixSite(runner, socket_path)
            logger.info(f"Started aiohttp server over {socket_path}.")
        await site.start()

    @property
    def gcs_aio_client(self):
        if self._gcs_aio_client is None:
            self._gcs_aio_client = GcsAioClient(
                address=self._config.gcs_address,
                cluster_id=self._config.cluster_id_hex,
            )
        return self._gcs_aio_client

    @property
    def gcs_client(self):
        if self._gcs_client is None:
            if not ray.experimental.internal_kv._internal_kv_initialized():
                gcs_client = GcsClient(
                    address=self._config.gcs_address,
                    cluster_id=self._config.cluster_id_hex,
                )
                ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
            self._gcs_client = ray.experimental.internal_kv.internal_kv_get_gcs_client()
        return self._gcs_client

    @property
    def session_name(self):
        """
        Return the Ray session name. It's not related to the aiohttp session.
        """
        return self._config.session_name

    @property
    def http_session(self):
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    @property
    def gcs_address(self):
        return self._config.gcs_address

    async def _internal_module_health_check(self, request):
        return aiohttp.web.Response(
            text="success",
            content_type="application/text",
        )


async def run_module_inner(
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
    incarnation: int,
    ready_event: multiprocessing.Event,
):

    module_name = cls.__name__

    logger.info(
        f"Starting module {module_name} with incarnation {incarnation} and config {config}"
    )

    try:
        module = cls(config)
        module._parent_process_death_detection_task = asyncio.create_task(
            module._detect_parent_process_death()
        )
        module._parent_process_death_detection_task.add_done_callback(
            lambda _: sys.exit()
        )
        await module.run()
        ready_event.set()
        logger.info(f"Module {module_name} initialized, receiving messages...")
    except Exception as e:
        logger.exception(f"Error creating module {module_name}")
        raise e


def run_module(
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
    incarnation: int,
    ready_event: multiprocessing.Event,
):
    """
    Entrypoint for a subprocess module.
    """
    module_name = cls.__name__
    current_proctitle = setproctitle.getproctitle()
    setproctitle.setproctitle(
        f"ray-dashboard-{module_name}-{incarnation} ({current_proctitle})"
    )
    logging_filename = module_logging_filename(module_name, config.logging_filename)
    setup_component_logger(
        logging_level=config.logging_level,
        logging_format=config.logging_format,
        log_dir=config.log_dir,
        filename=logging_filename,
        max_bytes=config.logging_rotate_bytes,
        backup_count=config.logging_rotate_backup_count,
    )

    stderr_filename = module_logging_filename(
        module_name, config.logging_filename, is_stderr=True
    )
    err_file = open_log(os.path.join(config.log_dir, stderr_filename), unbuffered=True)
    configure_log_file(err_file, err_file)

    loop = asyncio.new_event_loop()
    task = loop.create_task(
        run_module_inner(
            cls,
            config,
            incarnation,
            ready_event,
        )
    )
    # TODO: do graceful shutdown.
    # 1. define a stop token.
    # 2. join the loop to wait for all pending tasks to finish, up until a timeout.
    # 3. close the loop and exit.

    def sigterm_handler(signum, frame):
        logger.warning(f"Exiting with signal {signum} immediately...")
        sys.exit(signum)

    ray._private.utils.set_sigterm_handler(sigterm_handler)

    loop.run_until_complete(task)
    loop.run_forever()
