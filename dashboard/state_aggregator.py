import asyncio
import logging

from typing import List, Dict
from itertools import islice

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.memory_utils as memory_utils
from ray.dashboard.modules.job.common import JobInfo

from ray.experimental.state.common import (
    filter_fields,
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
    TaskState,
    ObjectState,
    ResourceSummary,
    RuntimeEnvState,
    ListApiOptions,
    DEFAULT_RPC_TIMEOUT,
)
from ray.experimental.state.utils import (
    get_task_name,
    aggregate_resource_usage_for_task,
)
from ray.experimental.state.state_manager import StateDataSourceClient
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)


# TODO(sang): Move the class to state/state_manager.py.
# TODO(sang): Remove *State and replaces with Pydantic or protobuf
# (depending on API interface standardization).
class StateAPIManager:
    """A class to query states from data source, caches, and post-processes
    the entries.
    """

    def __init__(self, state_data_source_client: StateDataSourceClient):
        self._client = state_data_source_client

    @property
    def data_source_client(self):
        return self._client

    async def list_actors(self, *, option: ListApiOptions) -> dict:
        """List all actor information from the cluster.

        Returns:
            {actor_id -> actor_data_in_dict}
            actor_data_in_dict's schema is in ActorState
        """
        reply = await self._client.get_all_actor_info(timeout=option.timeout)
        result = []
        for message in reply.actor_table_data:
            data = self._message_to_dict(message=message, fields_to_decode=["actor_id"])
            data = filter_fields(data, ActorState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["actor_id"])
        return {d["actor_id"]: d for d in islice(result, option.limit)}

    async def list_placement_groups(self, *, option: ListApiOptions) -> dict:
        """List all placement group information from the cluster.

        Returns:
            {pg_id -> pg_data_in_dict}
            pg_data_in_dict's schema is in PlacementGroupState
        """
        reply = await self._client.get_all_placement_group_info(timeout=option.timeout)
        result = []
        for message in reply.placement_group_table_data:

            data = self._message_to_dict(
                message=message,
                fields_to_decode=["placement_group_id"],
            )
            data = filter_fields(data, PlacementGroupState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["placement_group_id"])
        return {d["placement_group_id"]: d for d in islice(result, option.limit)}

    async def list_nodes(self, *, option: ListApiOptions) -> dict:
        """List all node information from the cluster.

        Returns:
            {node_id -> node_data_in_dict}
            node_data_in_dict's schema is in NodeState
        """
        reply = await self._client.get_all_node_info(timeout=option.timeout)
        result = []
        for message in reply.node_info_list:
            data = self._message_to_dict(message=message, fields_to_decode=["node_id"])
            data = filter_fields(data, NodeState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["node_id"])
        return {d["node_id"]: d for d in islice(result, option.limit)}

    async def list_workers(self, *, option: ListApiOptions) -> dict:
        """List all worker information from the cluster.

        Returns:
            {worker_id -> worker_data_in_dict}
            worker_data_in_dict's schema is in WorkerState
        """
        reply = await self._client.get_all_worker_info(timeout=option.timeout)
        result = []
        for message in reply.worker_table_data:
            data = self._message_to_dict(
                message=message, fields_to_decode=["worker_id"]
            )
            data["worker_id"] = data["worker_address"]["worker_id"]
            data = filter_fields(data, WorkerState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["worker_id"])
        return {d["worker_id"]: d for d in islice(result, option.limit)}

    def list_jobs(self, *, option: ListApiOptions) -> Dict[str, JobInfo]:
        # TODO(sang): Support limit & timeout & async calls.
        return self._client.get_job_info()

    async def list_tasks(self, *, option: ListApiOptions) -> dict:
        """List all task information from the cluster.

        Returns:
            {task_id -> task_data_in_dict}
            task_data_in_dict's schema is in TaskState
        """
        replies = await asyncio.gather(
            *[
                self._client.get_task_info(node_id, timeout=option.timeout)
                for node_id in self._client.get_all_registered_raylet_ids()
            ]
        )

        result = []
        for reply in replies:
            tasks = reply.task_info_entries
            for task in tasks:
                data = self._message_to_dict(
                    message=task,
                    fields_to_decode=["task_id"],
                )
                data = filter_fields(data, TaskState)
                result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["task_id"])
        return {d["task_id"]: d for d in islice(result, option.limit)}

    async def list_objects(self, *, option: ListApiOptions) -> dict:
        """List all object information from the cluster.

        Returns:
            {object_id -> object_data_in_dict}
            object_data_in_dict's schema is in ObjectState
        """
        replies = await asyncio.gather(
            *[
                self._client.get_object_info(node_id, timeout=option.timeout)
                for node_id in self._client.get_all_registered_raylet_ids()
            ]
        )

        worker_stats = []
        for reply in replies:
            for core_worker_stat in reply.core_workers_stats:
                # NOTE: Set preserving_proto_field_name=False here because
                # `construct_memory_table` requires a dictionary that has
                # modified protobuf name
                # (e.g., workerId instead of worker_id) as a key.
                worker_stats.append(
                    self._message_to_dict(
                        message=core_worker_stat,
                        fields_to_decode=["object_id"],
                        preserving_proto_field_name=False,
                    )
                )

        result = []
        memory_table = memory_utils.construct_memory_table(worker_stats)
        for entry in memory_table.table:
            data = entry.as_dict()
            # `construct_memory_table` returns object_ref field which is indeed
            # object_id. We do transformation here.
            # TODO(sang): Refactor `construct_memory_table`.
            data["object_id"] = data["object_ref"]
            del data["object_ref"]
            data = filter_fields(data, ObjectState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["object_id"])
        return {d["object_id"]: d for d in islice(result, option.limit)}

    async def list_runtime_envs(self, *, option: ListApiOptions) -> List[dict]:
        """List all runtime env information from the cluster.

        Returns:
            A list of runtime env information in the cluster.
            The schema of returned "dict" is equivalent to the
            `RuntimeEnvState` protobuf message.
            We don't have id -> data mapping like other API because runtime env
            doesn't have unique ids.
        """
        replies = await asyncio.gather(
            *[
                self._client.get_runtime_envs_info(node_id, timeout=option.timeout)
                for node_id in self._client.get_all_registered_agent_ids()
            ]
        )
        result = []
        for node_id, reply in zip(self._client.get_all_registered_agent_ids(), replies):
            states = reply.runtime_env_states
            for state in states:
                data = self._message_to_dict(message=state, fields_to_decode=[])
                # Need to deseiralize this field.
                data["runtime_env"] = RuntimeEnv.deserialize(
                    data["runtime_env"]
                ).to_dict()
                data["node_id"] = node_id
                data = filter_fields(data, RuntimeEnvState)
                result.append(data)

        # Sort to make the output deterministic.
        def sort_func(entry):
            # If creation time is not there yet (runtime env is failed
            # to be created or not created yet, they are the highest priority.
            # Otherwise, "bigger" creation time is coming first.
            if "creation_time_ms" not in entry:
                return float("inf")
            elif entry["creation_time_ms"] is None:
                return float("inf")
            else:
                return float(entry["creation_time_ms"])

        result.sort(key=sort_func, reverse=True)
        return list(islice(result, option.limit))

    async def get_resource_summary(self, per_node: bool = False) -> dict:
        """Summarizes the total/available resources in the cluster.

        Returns:
            if per_node == True:
                {node_id -> resource_summary_dict}
            else:
                resource_summary_dict
            resource_summary_dict's schema is in ResourceSummary
        """
        resources_available = {}
        resources_total = {}

        def fill_resources(resources: dict, data_field: str, node_list: list):
            for node in node_list:
                data = self._message_to_dict(
                    message=node,
                    fields_to_decode=["node_id"],
                )
                if per_node:
                    resources[data["node_id"]] = data[data_field]
                else:
                    for k, v in data[data_field].items():
                        if k in resources:
                            resources[k] += v
                        else:
                            resources[k] = v

        async def fill_available_resources():
            reply = await self.data_source_client.get_all_available_resources(
                timeout=DEFAULT_RPC_TIMEOUT
            )
            fill_resources(
                resources_available, "resources_available", reply.resources_list
            )

        async def fill_total_resources():
            reply = await self.data_source_client.get_all_node_info(
                timeout=DEFAULT_RPC_TIMEOUT
            )
            fill_resources(resources_total, "resources_total", reply.node_info_list)

        await asyncio.gather(*[fill_total_resources(), fill_available_resources()])
        if per_node:
            # Combine resources_total and resources_available into
            # ResourceSummary per node
            response = {}
            all_node_ids = set(resources_available).union(set(resources_total))
            for node_id in all_node_ids:
                response[node_id] = filter_fields(
                    {
                        "total": resources_total.get(node_id) or {},
                        "available": resources_available.get(node_id) or {},
                    },
                    ResourceSummary,
                )  # sanity check
        else:
            response = filter_fields(
                {"total": resources_total, "available": resources_available},
                ResourceSummary,
            )  # sanity check
        return response

    async def get_detailed_resource_usage(self, per_node: bool = False):
        """Returns the resources usage per task/actor type in the cluster.

        Returns:
            if per_node == True:
                {node_id -> detailed_resource_usage_dict}
            else:
                detailed_resource_usage_dict
            detailed_resource_usage_dict's schema is in DetailedResourceUsage
        """

        async def _fill_for_node(
            node_id: str, total: dict, available: dict, resource_usage: dict
        ):
            reply = await self._client.get_resource_usage(
                node_id, timeout=DEFAULT_RPC_TIMEOUT
            )

            def insert_or_accumulate(existing: dict, new: dict):
                # merge intersection
                for key in existing:
                    if key in new:
                        existing[key] += new[key]
                # merge difference
                for key in new:
                    if key not in existing:
                        existing[key] = new[key]

            insert_or_accumulate(total, reply.total)
            insert_or_accumulate(available, reply.available)

            for task in reply.task_or_actor_resource_usage:
                task_name = get_task_name(task)
                resource_set = {k: v for k, v in task.resource_usage.items()}
                aggregate_resource_usage_for_task(
                    task_name, resource_set, resource_usage
                )

        if per_node:
            result = {}

            async def _fill_result(node_id: str):
                total, available, resource_usage = {}, {}, {}
                await _fill_for_node(node_id, total, available, resource_usage)
                result[node_id] = {
                    "summary": {
                        "total": total,
                        "available": available,
                    },
                    "usage": resource_usage,
                }

            await asyncio.gather(
                *[
                    _fill_result(node_id)
                    for node_id in self._client.get_all_registered_raylet_ids()
                ]
            )
        else:
            result = {}
            total, available, resource_usage = {}, {}, {}
            await asyncio.gather(
                *[
                    _fill_for_node(node_id, total, available, resource_usage)
                    for node_id in self._client.get_all_registered_raylet_ids()
                ]
            )
            result = {
                "summary": {
                    "total": total,
                    "available": available,
                },
                "usage": resource_usage,
            }
        return result

    def _message_to_dict(
        self,
        *,
        message,
        fields_to_decode: List[str] = None,
        preserving_proto_field_name: bool = True,
    ) -> dict:
        return dashboard_utils.message_to_dict(
            message,
            fields_to_decode,
            including_default_value_fields=True,
            preserving_proto_field_name=preserving_proto_field_name,
        )
