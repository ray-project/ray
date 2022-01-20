import os
import sys
import socket
import asyncio
import logging
import ipaddress
import threading
from concurrent.futures import Future
from queue import Queue

from distutils.version import LooseVersion
import grpc
try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

import ray.experimental.internal_kv as internal_kv
import ray._private.utils
from ray._private.gcs_utils import GcsClient, use_gcs_for_bootstrap
import ray._private.services
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
from ray import ray_constants
from ray._private.gcs_pubsub import gcs_pubsub_enabled, \
    GcsAioErrorSubscriber, GcsAioLogSubscriber
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.dashboard.datacenter import DataOrganizer
from ray.dashboard.utils import async_loop_forever

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp, hdrs

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()
GRPC_CHANNEL_OPTIONS = (
    ("grpc.enable_http_proxy", 0),
    ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    ("grpc.max_receive_message_length",
     ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
)


async def get_gcs_address_with_retry(redis_client) -> str:
    while True:
        try:
            gcs_address = (await redis_client.get(
                dashboard_consts.GCS_SERVER_ADDRESS)).decode()
            if not gcs_address:
                raise Exception("GCS address not found.")
            logger.info("Connect to GCS at %s", gcs_address)
            return gcs_address
        except Exception as ex:
            logger.error("Connect to GCS failed: %s, retry...", ex)
            await asyncio.sleep(
                dashboard_consts.GCS_RETRY_CONNECT_INTERVAL_SECONDS)


class GCSHealthCheckThread(threading.Thread):
    def __init__(self, gcs_address: str):
        self.grpc_gcs_channel = ray._private.utils.init_grpc_channel(
            gcs_address, options=GRPC_CHANNEL_OPTIONS)
        self.gcs_heartbeat_info_stub = (
            gcs_service_pb2_grpc.HeartbeatInfoGcsServiceStub(
                self.grpc_gcs_channel))
        self.work_queue = Queue()

        super().__init__(daemon=True)

    def run(self) -> None:
        while True:
            future = self.work_queue.get()
            check_result = self._check_once_synchrounously()
            future.set_result(check_result)

    def _check_once_synchrounously(self) -> bool:
        request = gcs_service_pb2.CheckAliveRequest()
        try:
            reply = self.gcs_heartbeat_info_stub.CheckAlive(
                request, timeout=dashboard_consts.GCS_CHECK_ALIVE_RPC_TIMEOUT)
            if reply.status.code != 0:
                logger.exception(
                    f"Failed to CheckAlive: {reply.status.message}")
                return False
        except grpc.RpcError:  # Deadline Exceeded
            logger.exception("Got RpcError when checking GCS is alive")
            return False
        return True

    async def check_once(self) -> bool:
        """Ask the thread to perform a healthcheck."""
        assert threading.current_thread != self, (
            "caller shouldn't be from the same thread as GCSHealthCheckThread."
        )

        future = Future()
        self.work_queue.put(future)
        return await asyncio.wrap_future(future)


class DashboardHead:
    def __init__(self, http_host, http_port, http_port_retries, gcs_address,
                 redis_address, redis_password, log_dir):
        self.health_check_thread: GCSHealthCheckThread = None
        self._gcs_rpc_error_counter = 0
        # Public attributes are accessible for all head modules.
        # Walkaround for issue: https://github.com/ray-project/ray/issues/7084
        self.http_host = "127.0.0.1" if http_host == "localhost" else http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries

        if use_gcs_for_bootstrap():
            assert gcs_address is not None
            self.gcs_address = gcs_address
        else:
            self.redis_address = dashboard_utils.address_tuple(redis_address)
            self.redis_password = redis_password

        self.log_dir = log_dir
        self.aioredis_client = None
        self.aiogrpc_gcs_channel = None
        self.gcs_error_subscriber = None
        self.gcs_log_subscriber = None
        self.http_session = None
        self.ip = ray.util.get_node_ip_address()
        if not use_gcs_for_bootstrap():
            ip, port = redis_address.split(":")
        else:
            ip, port = gcs_address.split(":")

        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0), ))
        grpc_ip = "127.0.0.1" if self.ip == "127.0.0.1" else "0.0.0.0"
        self.grpc_port = ray._private.tls_utils.add_port_to_grpc_server(
            self.server, f"{grpc_ip}:0")
        logger.info("Dashboard head grpc address: %s:%s", grpc_ip,
                    self.grpc_port)

    @async_loop_forever(dashboard_consts.GCS_CHECK_ALIVE_INTERVAL_SECONDS)
    async def _gcs_check_alive(self):
        check_future = self.health_check_thread.check_once()

        # NOTE(simon): making sure the check procedure doesn't timeout itself.
        # Otherwise, the dashboard will always think that gcs is alive.
        try:
            is_alive = await asyncio.wait_for(
                check_future, dashboard_consts.GCS_CHECK_ALIVE_RPC_TIMEOUT + 1)
        except asyncio.TimeoutError:
            logger.error("Failed to check gcs health, client timed out.")
            is_alive = False

        if is_alive:
            self._gcs_rpc_error_counter = 0
        else:
            self._gcs_rpc_error_counter += 1
            if self._gcs_rpc_error_counter > \
                    dashboard_consts.GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR:
                logger.error(
                    "Dashboard exiting because it received too many GCS RPC "
                    "errors count: %s, threshold is %s.",
                    self._gcs_rpc_error_counter,
                    dashboard_consts.GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR)
                # TODO(fyrestone): Do not use ray.state in
                # PrometheusServiceDiscoveryWriter.
                # Currently, we use os._exit() here to avoid hanging at the ray
                # shutdown(). Please refer to:
                # https://github.com/ray-project/ray/issues/16328
                os._exit(-1)

    def _load_modules(self):
        """Load dashboard head modules."""
        modules = []
        head_cls_list = dashboard_utils.get_all_modules(
            dashboard_utils.DashboardHeadModule)
        for cls in head_cls_list:
            logger.info("Loading %s: %s",
                        dashboard_utils.DashboardHeadModule.__name__, cls)
            c = cls(self)
            dashboard_utils.ClassMethodRouteTable.bind(c)
            modules.append(c)
        logger.info("Loaded %d modules.", len(modules))
        return modules

    async def get_gcs_address(self):
        # Create an aioredis client for all modules.
        if use_gcs_for_bootstrap():
            return self.gcs_address
        else:
            try:
                self.aioredis_client = \
                    await dashboard_utils.get_aioredis_client(
                        self.redis_address, self.redis_password,
                        dashboard_consts.CONNECT_REDIS_INTERNAL_SECONDS,
                        dashboard_consts.RETRY_REDIS_CONNECTION_TIMES)
            except (socket.gaierror, ConnectionError):
                logger.error(
                    "Dashboard head exiting: "
                    "Failed to connect to redis at %s", self.redis_address)
                sys.exit(-1)
            return await get_gcs_address_with_retry(self.aioredis_client)

    async def run(self):

        # Create a http session for all modules.
        # aiohttp<4.0.0 uses a 'loop' variable, aiohttp>=4.0.0 doesn't anymore
        if LooseVersion(aiohttp.__version__) < LooseVersion("4.0.0"):
            self.http_session = aiohttp.ClientSession(
                loop=asyncio.get_event_loop())
        else:
            self.http_session = aiohttp.ClientSession()

        gcs_address = await self.get_gcs_address()

        # Dashboard will handle connection failure automatically
        self.gcs_client = GcsClient(
            address=gcs_address, nums_reconnect_retry=0)
        internal_kv._initialize_internal_kv(self.gcs_client)
        self.aiogrpc_gcs_channel = ray._private.utils.init_grpc_channel(
            gcs_address, GRPC_CHANNEL_OPTIONS, asynchronous=True)
        if gcs_pubsub_enabled():
            self.gcs_error_subscriber = GcsAioErrorSubscriber(
                address=gcs_address)
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

        modules = self._load_modules()

        # Http server should be initialized after all modules loaded.
        # working_dir uploads for job submission can be up to 100MiB.
        app = aiohttp.web.Application(client_max_size=100 * 1024**2)
        app.add_routes(routes=routes.bound_routes())

        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        last_ex = None
        for i in range(1 + self.http_port_retries):
            try:
                site = aiohttp.web.TCPSite(runner, self.http_host,
                                           self.http_port)
                await site.start()
                break
            except OSError as e:
                last_ex = e
                self.http_port += 1
                logger.warning("Try to use port %s: %s", self.http_port, e)
        else:
            raise Exception(f"Failed to find a valid port for dashboard after "
                            f"{self.http_port_retries} retries: {last_ex}")
        http_host, http_port, *_ = site._server.sockets[0].getsockname()
        http_host = self.ip if ipaddress.ip_address(
            http_host).is_unspecified else http_host
        logger.info("Dashboard head http address: %s:%s", http_host, http_port)

        # TODO: Use async version if performance is an issue
        # Write the dashboard head port to gcs kv.
        internal_kv._internal_kv_put(
            ray_constants.DASHBOARD_ADDRESS,
            f"{http_host}:{http_port}",
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD)
        internal_kv._internal_kv_put(
            dashboard_consts.DASHBOARD_RPC_ADDRESS,
            f"{self.ip}:{self.grpc_port}",
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD)

        # Dump registered http routes.
        dump_routes = [
            r for r in app.router.routes() if r.method != hdrs.METH_HEAD
        ]
        for r in dump_routes:
            logger.info(r)
        logger.info("Registered %s routes.", len(dump_routes))

        # Freeze signal after all modules loaded.
        dashboard_utils.SignalManager.freeze()
        concurrent_tasks = [
            self._gcs_check_alive(),
            _async_notify(),
            DataOrganizer.purge(),
            DataOrganizer.organize(),
        ]
        await asyncio.gather(*concurrent_tasks,
                             *(m.run(self.server) for m in modules))
        await self.server.wait_for_termination()
