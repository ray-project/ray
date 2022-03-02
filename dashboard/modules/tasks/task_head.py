import asyncio
import logging
import aiohttp.web
import ray._private.utils

from collections import defaultdict

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.optional_utils import rest_response
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import node_manager_pb2
from ray.dashboard.datacenter import DataSource
from ray._raylet import TaskID

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class TaskHead(dashboard_utils.DashboardHeadModule):
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

    @routes.get("/tasks/get")
    @dashboard_optional_utils.aiohttp_cache
    async def get_tasks(self, req) -> aiohttp.web.Response:
        async def get_node_stats(stub):
            reply = await stub.GetNodeStats(
                node_manager_pb2.GetNodeStatsRequest(
                    include_memory_info=False, exclude_stats=True, include_tasks=True
                ),
                timeout=10,
            )
            return reply

        replies = await asyncio.gather(
            *[get_node_stats(stub) for stub in self._stubs.values()]
        )

        tasks = defaultdict(dict)
        for reply in replies:
            entries = reply.task_info_entries
            for e in entries:
                logger.info(e.task_id)
                task_id = TaskID(e.task_id).hex()
                tasks[task_id].update(
                    dashboard_utils.message_to_dict(
                        e,
                        {"taskId", "jobId", "parentTaskId"},
                    )
                )

            scheduling_states = reply.detailed_scheduling_states
            for state in scheduling_states:
                logger.info(state.task_id)
                task_id = TaskID(state.task_id).hex()
                tasks[task_id][
                    "SchedulingStateDetail"
                ] = dashboard_utils.message_to_dict(state)["schedulingState"]

        return rest_response(success=True, message="Tasks fetched", tasks=tasks)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
