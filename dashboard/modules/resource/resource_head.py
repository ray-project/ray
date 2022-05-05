import logging

import aiohttp.web
from ray.dashboard.datacenter import DataSource
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.optional_utils import rest_response
from ray.dashboard.state_aggregator import StateAPIManager
from ray.experimental.state.state_manager import StateDataSourceClient

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ResourceHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

        self._state_api_data_source_client = None
        self._state_api = None
        DataSource.nodes.signal.append(self._update_raylet_stubs)

    async def _update_raylet_stubs(self, change: dashboard_utils.Change):
        """Callback that's called when a new raylet is added to Datasource.

        Datasource is a api-server-specific module that's updated whenever
        api server adds/removes a new node.

        Args:
            change: The change object. Whenever a new node is added
                or removed, this callback is invoked.
                When new node is added: information is in `change.new`.
                When a node is removed: information is in `change.old`.
                When a node id is overwritten by a new node with the same node id:
                    `change.old` contains the old node info, and
                    `change.new` contains the new node info.
        """
        if change.old:
            # When a node is deleted from the DataSource or it is overwritten.
            node_id, node_info = change.old
            self._state_api_data_source_client.unregister_raylet_client(node_id)
        if change.new:
            # When a new node information is written to DataSource.
            node_id, node_info = change.new
            self._state_api_data_source_client.register_raylet_client(
                node_id,
                node_info["nodeManagerAddress"],
                int(node_info["nodeManagerPort"]),
            )

    @routes.get("/api/v0/resources/summary/cluster")
    async def get_resource_summary_cluster(self, request) -> aiohttp.web.Response:
        data = await self._state_api.get_resource_summary()
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    @routes.get("/api/v0/resources/summary/nodes")
    async def get_resource_summary_nodes(self, request) -> aiohttp.web.Response:
        data = await self._state_api.get_resource_summary(per_node=True)
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    @routes.get("/api/v0/resources/usage/cluster")
    async def get_detailed_resource_usage_cluster(
        self, request
    ) -> aiohttp.web.Response:
        data = await self._state_api.get_detailed_resource_usage()
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    @routes.get("/api/v0/resources/usage/nodes")
    async def get_detailed_resource_usage_nodes(self, request) -> aiohttp.web.Response:
        data = await self._state_api.get_detailed_resource_usage(per_node=True)
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(gcs_channel)
        self._state_api = StateAPIManager(self._state_api_data_source_client)

    @staticmethod
    def is_minimal_module():
        return False
