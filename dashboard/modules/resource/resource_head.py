from typing import Dict, List
import logging

from ray._private.utils import init_grpc_channel, binary_to_hex
import aiohttp.web
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.datacenter import DataSource
from ray.dashboard.utils import Change
from ray.core.generated.node_manager_pb2_grpc import NodeManagerServiceStub
from ray.core.generated.gcs_service_pb2_grpc import (
    NodeInfoGcsServiceStub,
    NodeResourceInfoGcsServiceStub,
)
import ray.core.generated.gcs_service_pb2 as gcs_service_pb2
import ray.core.generated.node_manager_pb2 as node_manager_pb2

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ResourceHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

        options = (("grpc.enable_http_proxy", 0),)
        gcs_channel = init_grpc_channel(
            dashboard_head.gcs_address, options=options, asynchronous=True
        )
        self._gcs_node_info_stub = NodeInfoGcsServiceStub(gcs_channel)
        self._gcs_node_resource_info_stub = NodeResourceInfoGcsServiceStub(gcs_channel)
        self._raylet_stubs = {}
        DataSource.nodes.signal.append(self._update_raylet_stubs)

    async def _update_raylet_stubs(self, change: Change):
        """Callback that's called new raylet is added to Datasource.
        Args:
            change: The change object. Whenever a new node is added
                or removed, this callback is invoked.
                When new node is added: information is in `change.new`.
                When a node is removed: information is in `change.old`.
                When a node is overwritten: `change.old` contains the
                    old node info, and `change.new` contains the
                    new node info.
        """
        if change.old:
            # When a node is deleted from the DataSource or it is overwritten.
            node_id, node_info = change.old
            self.unregister_raylet_stub(node_id)
        if change.new:
            # When a new node information is written to DataSource.
            node_id, node_info = change.new
            stub = self._make_raylet_stub(
                node_info["nodeManagerAddress"], int(node_info["nodeManagerPort"])
            )
            self.register_raylet_stub(node_id, stub)

    def register_raylet_stub(self, node_id: str, stub: NodeManagerServiceStub):
        self._raylet_stubs[node_id] = stub

    def unregister_raylet_stub(self, node_id):
        self._raylet_stubs.pop(node_id)

    def _make_raylet_stub(self, addr: str, port):
        full_addr = f"{addr}:{port}"
        options = (("grpc.enable_http_proxy", 0),)
        channel = init_grpc_channel(full_addr, options, asynchronous=True)
        return NodeManagerServiceStub(channel)

    @routes.get("/api/experimental/resources/available/nodes")
    async def get_available_resources_nodes(self, request) -> aiohttp.web.Response:
        # Getting the total resources
        response = {}
        node_resources = (
            await self._gcs_node_resource_info_stub.GetAllAvailableResources(
                gcs_service_pb2.GetAllAvailableResourcesRequest()
            )
        )
        for node in node_resources.resources_list:
            node_id = binary_to_hex(node.node_id)
            response[node_id] = {k: v for k, v in node.resources_available.items()}

        return aiohttp.web.json_response(response)

    @routes.get("/api/experimental/resources/available/cluster")
    async def get_available_resources_cluster(self, request) -> aiohttp.web.Response:
        # Getting the total resources
        response = {}
        node_resources = (
            await self._gcs_node_resource_info_stub.GetAllAvailableResources(
                gcs_service_pb2.GetAllAvailableResourcesRequest()
            )
        )
        for node in node_resources.resources_list:
            for k, v in node.resources_available.items():
                if k in response:
                    response[k] += v
                else:
                    response[k] = v

        return aiohttp.web.json_response(response)

    @routes.get("/api/experimental/resources/total/nodes")
    async def get_total_resources_nodes(self, request) -> aiohttp.web.Response:
        # Getting the total resources
        response = {}
        all_node_info = await self._gcs_node_info_stub.GetAllNodeInfo(
            gcs_service_pb2.GetAllNodeInfoRequest()
        )
        for node in all_node_info.node_info_list:
            node_id = binary_to_hex(node.node_id)
            response[node_id] = {k: v for k, v in node.resources_total.items()}

        return aiohttp.web.json_response(response)

    @routes.get("/api/experimental/resources/total/cluster")
    async def get_total_resources_cluster(self, request) -> aiohttp.web.Response:
        # Getting the total resources
        response = {}
        all_node_info = await self._gcs_node_info_stub.GetAllNodeInfo(
            gcs_service_pb2.GetAllNodeInfoRequest()
        )
        for node in all_node_info.node_info_list:
            for k, v in node.resources_total.items():
                if k in response:
                    response[k] += v
                else:
                    response[k] = v

        return aiohttp.web.json_response(response)

    @routes.get("/api/experimental/resources/usage/nodes")
    async def get_resource_usage_nodes(self, request) -> aiohttp.web.Response:
        node_id_query = request.query.get("node_id", None)

        response = {}
        for node_id in self._raylet_stubs:
            if node_id_query is None or node_id == node_id_query:
                node_mapping = {}
                await self._fill_resource_usage_by_task_for_node(node_id, node_mapping)
                response[node_id] = node_mapping

        return aiohttp.web.json_response(response)

    @routes.get("/api/experimental/resources/usage/cluster")
    async def get_resource_usage_cluster(self, request) -> aiohttp.web.Response:
        response = {}
        for node_id in self._raylet_stubs:
            await self._fill_resource_usage_by_task_for_node(node_id, response)

        return aiohttp.web.json_response(response)

    async def _fill_resource_usage_by_task_for_node(
        self,
        node_id: str,
        task_to_resource_usage: Dict,
    ):
        """
        Mutates task_to_resource_usage in place, filling data for task names and
        their respective resource usages.
        """
        resource_usage = await self._raylet_stubs[node_id].GetResourceUsageByTask(
            node_manager_pb2.GetResourceUsageByTaskRequest(), timeout=10
        )

        for task in resource_usage.task_resource_usage:
            task_name = get_task_name(task)
            resource_set = {}
            for k, v in task.resource_usage.items():
                resource_set[k] = v
            add_resource_usage_to_task(task_name, resource_set, task_to_resource_usage)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False


ResourceSet = Dict[str, float]
ResourceCountList = List[Dict]


def append_or_create(dic, key, entry):
    if key in dic:
        dic[key].append(entry)
    else:
        dic[key] = [entry]


def add_resource_usage_to_task(
    task: str,
    resource_set: ResourceSet,
    task_resource_count_mapping: Dict[str, ResourceCountList],
):

    logger.info(f"mapping before: {resource_set}, {task_resource_count_mapping}")
    if task in task_resource_count_mapping:
        add_to_resources_list(
            resource_set,
            task_resource_count_mapping[task],
        )
    else:
        task_resource_count_mapping[task] = [{"resource_set": resource_set, "count": 1}]
    logger.info(f"mapping after: {resource_set}, {task_resource_count_mapping}")


def add_to_resources_list(
    resource_set: ResourceSet,
    resource_count_list: ResourceCountList,
):
    found = False
    for idx, resource_count in enumerate(resource_count_list):
        logger.info(f"RESOURCE {resource_count}, {resource_count_list}")
        if resource_count["resource_set"] == resource_set:
            resource_count_list[idx] = {
                "resource_set": resource_set,
                "count": resource_count["count"] + 1,
            }
            found = True
            break
    if not found:
        resource_count_list.append({"resource_set": resource_set, "count": 1})
    logger.info(f"RESOURCE after {resource_count}, {resource_count_list}")


def get_task_name(task: node_manager_pb2.TaskResourceUsage):
    if hasattr(task.function_descriptor, "python_function_descriptor"):
        fd = task.function_descriptor.python_function_descriptor
    elif hasattr(task.function_descriptor, "java_function_descriptor"):
        fd = task.function_descriptor.java_function_descriptor
    elif hasattr(task.function_descriptor, "cpp_function_descriptor"):
        fd = task.function_descriptor.cpp_function_descriptor
    else:
        return None
    if task.is_actor:
        task_name = fd.class_name
    else:
        task_name = fd.function_name
    return task_name
