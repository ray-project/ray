import asyncio
import logging

import aioredis
from grpc.experimental import aio as aiogrpc

import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.gcs_utils
import ray.services
from ray.new_dashboard.datacenter import DataSource, DataOrganizer

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aiogrpc.init_grpc_aio()


class DashboardMaster:
    def __init__(self, redis_address, redis_password):
        # Scan and import master modules for collecting http routes.
        self._master_cls_list = dashboard_utils.get_all_modules(
            dashboard_consts.TYPE_MASTER)
        ip, port = redis_address.split(":")
        # Public attributes are accessible for all master modules.
        self.redis_address = (ip, int(port))
        self.redis_password = redis_password
        self.aioredis_client = None
        self.aiogrpc_gcs_channel = None

    async def _get_nodes(self):
        """Read the client table.

        Returns:
            A list of information about the nodes in the cluster.
        """
        message = await self.aioredis_client.execute(
            "RAY.TABLE_LOOKUP", ray.gcs_utils.TablePrefix.Value("CLIENT"), "",
            ray.ClientID.nil().binary())

        # Handle the case where no clients are returned. This should only
        # occur potentially immediately after the cluster is started.
        if message is None:
            return []

        results = []
        node_id_set = set()
        gcs_entry = ray.gcs_utils.GcsEntry.FromString(message)

        # Since GCS entries are append-only, we override so that
        # only the latest entries are kept.
        for entry in gcs_entry.entries:
            item = ray.gcs_utils.GcsNodeInfo.FromString(entry)
            if item.node_id in node_id_set:
                continue
            node_id_set.add(item.node_id)
            node_info = dashboard_utils.message_to_dict(
                item, including_default_value_fields=True)
            results.append(node_info)

        return results

    async def _update_nodes(self):
        while True:
            try:
                nodes = await self._get_nodes()
                node_ips = [node["nodeManagerAddress"] for node in nodes]

                agents = {}
                for node in nodes:
                    node_ip = node["nodeManagerAddress"]
                    if node_ip not in DataSource.agents:
                        key = "{}{}".format(
                            dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX,
                            node_ip)
                        agent_port = await self.aioredis_client.get(key)
                        if agent_port:
                            agents[node_ip] = agent_port

                DataSource.agents.reset(agents)
                DataSource.nodes.reset(
                    dict(
                        zip(node_ips,
                            [node["nodeManagerPort"] for node in nodes])))
                DataSource.hostname_to_ip.reset(
                    dict(
                        zip([node["nodeManagerHostname"] for node in nodes],
                            node_ips)))
                DataSource.ip_to_hostname.reset(
                    dict(
                        zip(node_ips,
                            [node["nodeManagerHostname"] for node in nodes])))
            except Exception as ex:
                logger.exception(ex)
            finally:
                await asyncio.sleep(
                    dashboard_consts.UPDATE_NODES_INTERVAL_SECONDS)

    def _load_modules(self):
        """Load dashboard master modules."""
        modules = []
        for cls in self._master_cls_list:
            logger.info("Load %s module: %s", dashboard_consts.TYPE_MASTER,
                        cls)
            c = cls(self)
            dashboard_utils.ClassMethodRouteTable.bind(c)
            modules.append(c)
        return modules

    async def run(self):
        # Create aioredis client for all modules.
        self.aioredis_client = await aioredis.create_redis_pool(
            address=self.redis_address, password=self.redis_password)
        # Waiting for GCS is ready.
        while True:
            try:
                gcs_address = await self.aioredis_client.get(
                    dashboard_consts.REDIS_KEY_GCS_SERVER_ADDRESS)
                if not gcs_address:
                    raise Exception("GCS address not found.")
                channel = aiogrpc.insecure_channel(gcs_address)
            except Exception as ex:
                logger.error("Connect to GCS failed: %s, retry...", ex)
                await asyncio.sleep(
                    dashboard_consts.CONNECT_GCS_INTERVAL_SECONDS)
            else:
                self.aiogrpc_gcs_channel = channel
                break

        async def _async_notify():
            """Notify signals from queue."""
            while True:
                co = await dashboard_utils.NotifyQueue.get()
                try:
                    await co
                except Exception as e:
                    logger.exception(e)

        async def _purge_data():
            """Purge data in datacenter."""
            while True:
                await asyncio.sleep(
                    dashboard_consts.PURGE_DATA_INTERVAL_SECONDS)
                try:
                    DataOrganizer.purge()
                except Exception as e:
                    logger.exception(e)

        modules = self._load_modules()
        # Freeze signal after all modules loaded.
        dashboard_utils.NotifyQueue.freeze_signal()
        await asyncio.gather(self._update_nodes(), _async_notify(),
                             _purge_data(), *(m.run() for m in modules))
