import sys
import socket
import json
import asyncio
import logging

import aiohttp
import aiohttp.web
from aiohttp import hdrs
from grpc.experimental import aio as aiogrpc

import ray._private.services
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import DataSource, DataOrganizer

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()


def gcs_node_info_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {"nodeId"}, including_default_value_fields=True)


class DashboardHead:
    def __init__(self, http_host, http_port, redis_address, redis_password,
                 log_dir):
        # NodeInfoGcsService
        self._gcs_node_info_stub = None
        self._gcs_rpc_error_counter = 0
        # Public attributes are accessible for all head modules.
        self.http_host = http_host
        self.http_port = http_port
        self.redis_address = dashboard_utils.address_tuple(redis_address)
        self.redis_password = redis_password
        self.log_dir = log_dir
        self.aioredis_client = None
        self.aiogrpc_gcs_channel = None
        self.http_session = None
        self.ip = ray._private.services.get_node_ip_address()
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0), ))
        self.grpc_port = self.server.add_insecure_port("[::]:0")
        logger.info("Dashboard head grpc address: %s:%s", self.ip,
                    self.grpc_port)
        logger.info("Dashboard head http address: %s:%s", self.http_host,
                    self.http_port)

    async def _get_nodes(self):
        """Read the client table.

        Returns:
            A dict of information about the nodes in the cluster.
        """
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(
            request, timeout=2)
        if reply.status.code == 0:
            result = {}
            for node_info in reply.node_info_list:
                node_info_dict = gcs_node_info_to_dict(node_info)
                result[node_info_dict["nodeId"]] = node_info_dict
            return result
        else:
            logger.error("Failed to GetAllNodeInfo: %s", reply.status.message)

    async def _update_nodes(self):
        while True:
            try:
                nodes = await self._get_nodes()

                alive_node_ids = []
                alive_node_infos = []
                node_id_to_ip = {}
                node_id_to_hostname = {}
                for node in nodes.values():
                    node_id = node["nodeId"]
                    ip = node["nodeManagerAddress"]
                    hostname = node["nodeManagerHostname"]
                    node_id_to_ip[node_id] = ip
                    node_id_to_hostname[node_id] = hostname
                    assert node["state"] in ["ALIVE", "DEAD"]
                    if node["state"] == "ALIVE":
                        alive_node_ids.append(node_id)
                        alive_node_infos.append(node)

                agents = dict(DataSource.agents)
                for node_id in alive_node_ids:
                    key = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}" \
                          f"{node_id}"
                    agent_port = await self.aioredis_client.get(key)
                    if agent_port:
                        agents[node_id] = json.loads(agent_port)
                for node_id in agents.keys() - set(alive_node_ids):
                    agents.pop(node_id, None)

                DataSource.node_id_to_ip.reset(node_id_to_ip)
                DataSource.node_id_to_hostname.reset(node_id_to_hostname)
                DataSource.agents.reset(agents)
                DataSource.nodes.reset(nodes)

                self._gcs_rpc_error_counter = 0
            except aiogrpc.AioRpcError:
                logger.exception("Got AioRpcError when updating nodes.")
                self._gcs_rpc_error_counter += 1
                if self._gcs_rpc_error_counter > \
                        dashboard_consts.MAX_COUNT_OF_GCS_RPC_ERROR:
                    logger.error(
                        "Dashboard suicide, the GCS RPC error count %s > %s",
                        self._gcs_rpc_error_counter,
                        dashboard_consts.MAX_COUNT_OF_GCS_RPC_ERROR)
                    sys.exit(-1)
            except Exception:
                logger.exception("Error updating nodes.")
            finally:
                await asyncio.sleep(
                    dashboard_consts.UPDATE_NODES_INTERVAL_SECONDS)

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
        while True:
            try:
                gcs_address = await self.aioredis_client.get(
                    dashboard_consts.REDIS_KEY_GCS_SERVER_ADDRESS)
                if not gcs_address:
                    raise Exception("GCS address not found.")
                logger.info("Connect to GCS at %s", gcs_address)
                options = (("grpc.enable_http_proxy", 0), )
                channel = aiogrpc.insecure_channel(
                    gcs_address, options=options)
            except Exception as ex:
                logger.error("Connect to GCS failed: %s, retry...", ex)
                await asyncio.sleep(
                    dashboard_consts.CONNECT_GCS_INTERVAL_SECONDS)
            else:
                self.aiogrpc_gcs_channel = channel
                break

        # Create a NodeInfoGcsServiceStub.
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            self.aiogrpc_gcs_channel)

        # Start a grpc asyncio server.
        await self.server.start()

        # Write the dashboard head port to redis.
        await self.aioredis_client.set(dashboard_consts.REDIS_KEY_DASHBOARD,
                                       self.ip + ":" + str(self.http_port))
        await self.aioredis_client.set(
            dashboard_consts.REDIS_KEY_DASHBOARD_RPC,
            self.ip + ":" + str(self.grpc_port))

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
        web_server = aiohttp.web._run_app(
            app, host=self.http_host, port=self.http_port)

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
            self._update_nodes(),
            _async_notify(),
            DataOrganizer.purge(),
            DataOrganizer.organize(),
            web_server,
        ]
        await asyncio.gather(*concurrent_tasks,
                             *(m.run(self.server) for m in modules))
        await self.server.wait_for_termination()
