import abc
import asyncio
import aiohttp
import inspect
import logging
import sys
from dataclasses import dataclass
import os
import multiprocessing

import ray
from ray.dashboard.subprocesses.utils import (
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

    async def start_server(self):
        """
        Start the aiohttp server.
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
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()

        socket_path = os.path.join(
            self._config.socket_dir, "dashboard_" + self.__class__.__name__
        )
        if sys.platform == "win32":
            site = aiohttp.web.NamedPipeSite(runner, socket_path)
        else:
            site = aiohttp.web.UnixSite(runner, socket_path)
        await site.start()
        logger.info(f"Started aiohttp server over {socket_path}.")

    async def _internal_module_health_check(self, request):
        return aiohttp.web.Response(
            text="success",
            content_type="application/text",
        )


async def run_module_inner(
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
    ready_event: multiprocessing.Event,
):

    module_name = cls.__name__

    logger.info(f"Starting module {module_name} with config {config}")

    try:
        module = cls(config)
        # First init the module, then start the aiohttp server.
        await module.init()
        await module.start_server()
        ready_event.set()
        logger.info(f"Module {module_name} initialized, receiving messages...")
    except Exception as e:
        logger.exception(f"Error creating module {module_name}")
        raise e


def run_module(
    cls: type[SubprocessModule],
    config: SubprocessModuleConfig,
    ready_event: multiprocessing.Event,
):
    """
    Entrypoint for a subprocess module.
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

    loop = asyncio.new_event_loop()
    loop.create_task(
        run_module_inner(
            cls,
            config,
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

    loop.run_forever()
