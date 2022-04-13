import logging

import aiohttp.web

import ray
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import node_manager_pb2
from ray.dashboard.datacenter import DataSource
from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ObjectHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # ActorInfoGcsService
        self._gcs_actor_info_stub = None
        DataSource.nodes.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._stubs.pop(node_id)
        if change.new:
            # TODO(fyrestone): Handle exceptions.
            node_id, node_info = change.new
            address = "{}:{}".format(
                node_info["nodeManagerAddress"], int(node_info["nodeManagerPort"])
            )
            options = (("grpc.enable_http_proxy", 0),)
            channel = ray._private.utils.init_grpc_channel(
                address, options, asynchronous=True
            )
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[node_id] = stub

    @routes.get("/api/v0/objects")
    @dashboard_optional_utils.aiohttp_cache
    async def get_objects(self, req) -> aiohttp.web.Response:
        from ray.dashboard.memory_utils import memory_summary

        return rest_response(
            success=True,
            message="Objects fetched",
            memory=memory_summary(ray.state.state),
        )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
