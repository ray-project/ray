import os
import sys
import socket
import asyncio
import logging
import ipaddress
import threading

from grpc.experimental import aio as aiogrpc

import ray._private.services
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
from ray import ray_constants
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import DataOrganizer
from ray.new_dashboard.utils import async_loop_forever
from ray._raylet import connect_to_gcs

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.new_dashboard.optional_deps import aiohttp, hdrs

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()


async def make_gcs_grpc_channel(redis_client):
    while True:
        try:
            gcs_address = await redis_client.get(
                dashboard_consts.REDIS_KEY_GCS_SERVER_ADDRESS)
            if not gcs_address:
                raise Exception("GCS address not found.")
            logger.info("Connect to GCS at %s", gcs_address)
            options = (("grpc.enable_http_proxy", 0), )
            channel = aiogrpc.insecure_channel(gcs_address, options=options)
            return channel
        except Exception as ex:
            logger.error("Connect to GCS failed: %s, retry...", ex)
            await asyncio.sleep(
                dashboard_consts.GCS_RETRY_CONNECT_INTERVAL_SECONDS)


class GCSHealthCheckThread(threading.Thread):
    def __init__(self, redis_client):
        self.thread_local_loop = asyncio.new_event_loop()
        self.aiogrpc_gcs_channel = None
        self.gcs_heartbeat_info_stub = None

        async def on_startup():
            aiogrpc.init_grpc_aio()
            self.aiogrpc_gcs_channel = await (
                make_gcs_grpc_channel(redis_client))
            self.gcs_heartbeat_info_stub = (
                gcs_service_pb2_grpc.HeartbeatInfoGcsServiceStub(
                    self.aiogrpc_gcs_channel))

        self.startup_task = self.thread_local_loop.create_task(on_startup())

        super().__init__(daemon=True)

    async def _check_once(self) -> bool:
        request = gcs_service_pb2.CheckAliveRequest()
        try:
            reply = await self.gcs_heartbeat_info_stub.CheckAlive(
                request, timeout=dashboard_consts.GCS_CHECK_ALIVE_RPC_TIMEOUT)
            if reply.status.code != 0:
                logger.exception(
                    f"Failed to CheckAlive: {reply.status.message}")
                return False
        except aiogrpc.AioRpcError:  # Deadline Exceeded
            logger.exception("Got AioRpcError when checking GCS is alive")
            return False
        return True

    async def check_once(self) -> bool:
        # Make sure startup is complete
        if not self.startup_task.done():
            return False
        # Make the grpc call inside the thread loop so it's not blocked by
        # potentially busy main loop.
        return await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(self._check_once(),
                                             self.thread_local_loop))

    def run(self) -> None:
        self.thread_local_loop.run_forever()


class DashboardHead:
    def __init__(self, http_host, http_port, http_port_retries, redis_address,
                 redis_password, log_dir):
        self.health_check_thread: GCSHealthCheckThread = None
        self._gcs_rpc_error_counter = 0
        # Public attributes are accessible for all head modules.
        # Walkaround for issue: https://github.com/ray-project/ray/issues/7084
        self.http_host = "127.0.0.1" if http_host == "localhost" else http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries
        self.redis_address = dashboard_utils.address_tuple(redis_address)
        self.redis_password = redis_password
        self.log_dir = log_dir
        self.aioredis_client = None
        self.aiogrpc_gcs_channel = None
        self.http_session = None
        self.ip = ray.util.get_node_ip_address()
        ip, port = redis_address.split(":")
        self.gcs_client = connect_to_gcs(ip, int(port), redis_password)
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0), ))
        self.grpc_port = self.server.add_insecure_port("[::]:0")
        logger.info("Dashboard head grpc address: %s:%s", self.ip,
                    self.grpc_port)

    @async_loop_forever(dashboard_consts.GCS_CHECK_ALIVE_INTERVAL_SECONDS)
    async def _gcs_check_alive(self):
        is_alive = await self.health_check_thread.check_once()
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

    async def run(self):
        # Create an aioredis client for all modules.
        try:
            self.aioredis_client = await dashboard_utils.get_aioredis_client(
                self.redis_address, self.redis_password,
                dashboard_consts.CONNECT_REDIS_INTERNAL_SECONDS,
                dashboard_consts.RETRY_REDIS_CONNECTION_TIMES)
        except (socket.gaierror, ConnectionError):
            logger.error(
                "Dashboard head exiting: "
                "Failed to connect to redis at %s", self.redis_address)
            sys.exit(-1)

        # Create a http session for all modules.
        self.http_session = aiohttp.ClientSession(
            loop=asyncio.get_event_loop())

        # Waiting for GCS is ready.
        self.aiogrpc_gcs_channel = await make_gcs_grpc_channel(
            self.aioredis_client)

        self.health_check_thread = GCSHealthCheckThread(
            redis_client=self.aioredis_client)
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
        app = aiohttp.web.Application()
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

        # Write the dashboard head port to redis.
        await self.aioredis_client.set(ray_constants.REDIS_KEY_DASHBOARD,
                                       f"{http_host}:{http_port}")
        await self.aioredis_client.set(
            dashboard_consts.REDIS_KEY_DASHBOARD_RPC,
            f"{self.ip}:{self.grpc_port}")

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
