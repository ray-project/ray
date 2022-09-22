import asyncio
import logging
import os
import threading
from concurrent.futures import Future
from queue import Queue

import ray._private.services
import ray._private.utils
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.experimental.internal_kv as internal_kv
from ray._private import ray_constants
from ray.dashboard.utils import DashboardHeadModule
from ray._private.gcs_pubsub import GcsAioErrorSubscriber, GcsAioLogSubscriber
from ray._private.gcs_utils import GcsClient, GcsAioClient, check_health
from ray.dashboard.datacenter import DataOrganizer
from ray.dashboard.utils import async_loop_forever

from typing import Optional, Set

try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc


logger = logging.getLogger(__name__)

aiogrpc.init_grpc_aio()
GRPC_CHANNEL_OPTIONS = (
    *ray_constants.GLOBAL_GRPC_OPTIONS,
    ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
)


class GCSHealthCheckThread(threading.Thread):
    def __init__(self, gcs_address: str):
        self.gcs_address = gcs_address
        self.work_queue = Queue()

        super().__init__(daemon=True)

    def run(self) -> None:
        while True:
            future = self.work_queue.get()
            check_result = check_health(self.gcs_address)
            future.set_result(check_result)

    async def check_once(self) -> bool:
        """Ask the thread to perform a health check."""
        assert (
            threading.current_thread != self
        ), "caller shouldn't be from the same thread as GCSHealthCheckThread."

        future = Future()
        self.work_queue.put(future)
        return await asyncio.wrap_future(future)


class DashboardHead:
    def __init__(
        self,
        http_host: str,
        http_port: int,
        http_port_retries: int,
        gcs_address: str,
        log_dir: str,
        temp_dir: str,
        session_dir: str,
        minimal: bool,
        modules_to_load: Optional[Set[str]] = None,
    ):
        """
        Args:
            http_host: The host address for the Http server.
            http_port: The port for the Http server.
            http_port_retries: The maximum retry to bind ports for the Http server.
            gcs_address: The GCS address in the {address}:{port} format.
            log_dir: The log directory. E.g., /tmp/session_latest/logs.
            temp_dir: The temp directory. E.g., /tmp.
            session_dir: The session directory. E.g., tmp/session_latest.
            minimal: Whether or not it will load the minimal modules.
            modules_to_load: A set of module name in string to load.
                By default (None), it loads all available modules.
                Note that available modules could be changed depending on
                minimal flags.
        """
        self.minimal = minimal
        self.health_check_thread: GCSHealthCheckThread = None
        self._gcs_rpc_error_counter = 0
        # Public attributes are accessible for all head modules.
        # Walkaround for issue: https://github.com/ray-project/ray/issues/7084
        self.http_host = "127.0.0.1" if http_host == "localhost" else http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries
        self._modules_to_load = modules_to_load

        self.gcs_address = None
        assert gcs_address is not None
        self.gcs_address = gcs_address
        self.log_dir = log_dir
        self.temp_dir = temp_dir
        self.session_dir = session_dir
        self.aiogrpc_gcs_channel = None
        self.gcs_aio_client = None
        self.gcs_error_subscriber = None
        self.gcs_log_subscriber = None
        self.ip = ray.util.get_node_ip_address()
        ip, port = gcs_address.split(":")

        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        grpc_ip = "127.0.0.1" if self.ip == "127.0.0.1" else "0.0.0.0"
        self.grpc_port = ray._private.tls_utils.add_port_to_grpc_server(
            self.server, f"{grpc_ip}:0"
        )
        logger.info("Dashboard head grpc address: %s:%s", grpc_ip, self.grpc_port)
        # If the dashboard is started as non-minimal version, http server should
        # be configured to expose APIs.
        self.http_server = None

    async def _configure_http_server(self, modules):
        from ray.dashboard.http_server_head import HttpServerDashboardHead

        http_server = HttpServerDashboardHead(
            self.ip,
            self.http_host,
            self.http_port,
            self.http_port_retries,
            self.gcs_address,
            self.gcs_client,
        )
        await http_server.run(modules)
        return http_server

    @property
    def http_session(self):
        assert self.http_server, "Accessing unsupported API in a minimal ray."
        return self.http_server.http_session

    @async_loop_forever(dashboard_consts.GCS_CHECK_ALIVE_INTERVAL_SECONDS)
    async def _gcs_check_alive(self):
        check_future = self.health_check_thread.check_once()

        # NOTE(simon): making sure the check procedure doesn't timeout itself.
        # Otherwise, the dashboard will always think that gcs is alive.
        try:
            is_alive = await asyncio.wait_for(
                check_future, dashboard_consts.GCS_CHECK_ALIVE_RPC_TIMEOUT + 1
            )
        except asyncio.TimeoutError:
            logger.error("Failed to check gcs health, client timed out.")
            is_alive = False

        if is_alive:
            self._gcs_rpc_error_counter = 0
        else:
            self._gcs_rpc_error_counter += 1
            if (
                self._gcs_rpc_error_counter
                > dashboard_consts.GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR
            ):
                logger.error(
                    "Dashboard exiting because it received too many GCS RPC "
                    "errors count: %s, threshold is %s.",
                    self._gcs_rpc_error_counter,
                    dashboard_consts.GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR,
                )
                # TODO(fyrestone): Do not use ray.state in
                # PrometheusServiceDiscoveryWriter.
                # Currently, we use os._exit() here to avoid hanging at the ray
                # shutdown(). Please refer to:
                # https://github.com/ray-project/ray/issues/16328
                os._exit(-1)

    def _load_modules(self, modules_to_load: Optional[Set[str]] = None):
        """Load dashboard head modules.

        Args:
            modules: A list of module names to load. By default (None),
                it loads all modules.
        """
        modules = []
        head_cls_list = dashboard_utils.get_all_modules(DashboardHeadModule)

        # Select modules to load.
        modules_to_load = modules_to_load or {m.__name__ for m in head_cls_list}
        logger.info("Modules to load: %s", modules_to_load)

        for cls in head_cls_list:
            logger.info("Loading %s: %s", DashboardHeadModule.__name__, cls)
            if cls.__name__ in modules_to_load:
                c = cls(self)
                modules.append(c)

        # Verify modules are loaded as expected.
        loaded_modules = {type(m).__name__ for m in modules}
        if loaded_modules != modules_to_load:
            assert False, (
                "Actual loaded modules, {}, doesn't match the requested modules "
                "to load, {}".format(loaded_modules, modules_to_load)
            )

        logger.info("Loaded %d modules. %s", len(modules), modules)
        return modules

    async def run(self):
        gcs_address = self.gcs_address

        # Dashboard will handle connection failure automatically
        self.gcs_client = GcsClient(address=gcs_address, nums_reconnect_retry=0)
        internal_kv._initialize_internal_kv(self.gcs_client)
        self.gcs_aio_client = GcsAioClient(address=gcs_address, nums_reconnect_retry=0)
        self.aiogrpc_gcs_channel = self.gcs_aio_client.channel.channel()

        self.gcs_error_subscriber = GcsAioErrorSubscriber(address=gcs_address)
        self.gcs_log_subscriber = GcsAioLogSubscriber(address=gcs_address)
        await self.gcs_error_subscriber.subscribe()
        await self.gcs_log_subscriber.subscribe()

        self.health_check_thread = GCSHealthCheckThread(gcs_address)
        self.health_check_thread.start()

        # Start a grpc asyncio server.
        await self.server.start()

        async def _async_notify():
            """Notify signals from queue."""
            while True:
                co = await dashboard_utils.NotifyQueue.get()
                try:
                    await co
                except Exception:
                    logger.exception(f"Error notifying coroutine {co}")

        modules = self._load_modules(self._modules_to_load)

        http_host, http_port = self.http_host, self.http_port
        if not self.minimal:
            self.http_server = await self._configure_http_server(modules)
            http_host, http_port = self.http_server.get_address()
        internal_kv._internal_kv_put(
            ray_constants.DASHBOARD_ADDRESS,
            f"{http_host}:{http_port}",
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        )

        # TODO: Use async version if performance is an issue
        # Write the dashboard head port to gcs kv.
        internal_kv._internal_kv_put(
            dashboard_consts.DASHBOARD_RPC_ADDRESS,
            f"{self.ip}:{self.grpc_port}",
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        )

        # Freeze signal after all modules loaded.
        dashboard_utils.SignalManager.freeze()
        concurrent_tasks = [
            self._gcs_check_alive(),
            _async_notify(),
            DataOrganizer.purge(),
            DataOrganizer.organize(),
        ]
        await asyncio.gather(*concurrent_tasks, *(m.run(self.server) for m in modules))
        await self.server.wait_for_termination()

        if self.http_server:
            await self.http_server.cleanup()
