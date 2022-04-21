from typing import Dict, List, Tuple
import logging

from ray._private.utils import init_grpc_channel
import aiohttp.web
import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.datacenter import DataSource, GlobalSignals
from ray.dashboard.utils import Change
from ray.core.generated.node_manager_pb2_grpc import NodeManagerServiceStub
from ray.core.generated.node_manager_pb2 import GetResourceUsageByTaskRequest, GetGcsServerAddressRequest, GetNodeStatsRequest
from ray.core.generated.gcs_service_pb2_grpc import ActorInfoGcsServiceStub
from ray.core.generated.gcs_service_pb2 import GetAllActorInfoRequest
import ray.core.generated.gcs_pb2 as gcs_pb2

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ResourceHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

        options = (("grpc.enable_http_proxy", 0),)
        gcs_channel = init_grpc_channel(
            dashboard_head.gcs_address, options=options, asynchronous=True
        )
        self._gcs_actor_info_stub = ActorInfoGcsServiceStub(
            gcs_channel
        )
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
        channel = init_grpc_channel(
            full_addr, options, asynchronous=True
        )
        return NodeManagerServiceStub(channel)

    @routes.get("/api/experimental/resources")
    async def get_resources(self, request) -> aiohttp.web.Response:
        group_by_args = request.query.get("group_by", "").split(",")

        # If group by node_id is requested, we need to sort the
        group_by_node_id = "node_id" in group_by_args
        group_by_actor_class = "name" in group_by_args

# actor_info = await self._gcs_actor_info_stub.GetAllActorInfo(
#     GetAllActorInfoRequest)
# for actor_data in actor_info.actor_table_data:
#     if gcs_pb2.ActorTableData.ActorState.DESCRIPTOR.values_by_number[
#         actor_data.state
#     ].name == "ALIVE":
#         raylet_id = actor_data.address.raylet_id.hex()
#         if raylet_id in response:
#             append_or_create(
#                 response[raylet_id], actor_data.class_name, str(actor_data.resource_mapping))
#         else:
#             response[raylet_id] = {
#                 actor_data.class_name: [
#                     f"{actor_data.resource_mapping}"
#                 ]
#             }
        # if group_by_node_id and group_by_actor_class:

        response = {}
        for node_id in self._raylet_stubs:
            if group_by_node_id:
                mapping_for_node = {}
            resource_usage = await self._raylet_stubs[node_id].GetResourceUsageByTask(
                GetResourceUsageByTaskRequest(), timeout=10)
            logger.info(f"{resource_usage.task_resource_usage}")
            for task in resource_usage.task_resource_usage:
                if hasattr(task.function_descriptor, "python_function_descriptor"):
                    fd = task.function_descriptor.python_function_descriptor
                    if task.is_actor:
                        task_name = fd.class_name
                    else:
                        task_name = fd.function_name
                else:
                    task_name = None
                resource_set = {}
                for k, v in task.resource_usage.items():
                    resource_set[k] = v
                if group_by_node_id:
                    add_resource_usage_to_task(
                        task_name, resource_set, mapping_for_node)
                else:
                    add_resource_usage_to_task(task_name, resource_set, response)
            if group_by_node_id:
                response[node_id] = mapping_for_node

        return aiohttp.web.json_response(response)

    # def aggregate_resources_by_task_name():

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False


ResourceSet = Dict[str, float]
ResourceCountList = List[Tuple[ResourceSet, int]]


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
        task_resource_count_mapping[task] = [[resource_set, 1]]
    logger.info(f"mapping after: {resource_set}, {task_resource_count_mapping}")


def add_to_resources_list(
    resource_set: ResourceSet,
    resources_list: ResourceCountList,
):
    found = False
    for idx, resource in enumerate(resources_list):
        logger.info(f"RESOURCE {resource}, {resources_list}")
        if resource[0] == resource_set:
            resources_list[idx] = [resource[0], resource[1]+1]
            found = True
            break
    if not found:
        resources_list.append([resource_set, 1])
    logger.info(f"RESOURCE after {resource}, {resources_list}")
