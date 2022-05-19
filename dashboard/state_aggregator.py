import asyncio
import logging

from dataclasses import dataclass
from itertools import islice
from typing import List, Dict, Union

from ray.core.generated.common_pb2 import TaskStatus
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
    RuntimeEnvState,
    ListApiOptions,
)
from ray.experimental.state.state_manager import StateDataSourceClient
from ray.runtime_env import RuntimeEnv
from ray._private.utils import binary_to_hex

logger = logging.getLogger(__name__)


@dataclass(init=True)
class StateApiResult:
    # Returned data.
    data: Union[dict, List[dict], Dict[str, JobInfo]] = None
    # A list of warnings generated from the API that should be delivered to users.
    warnings: List[str] = None


GCS_QUERY_FAILURE_WARNING = (
    "Failed to query data from GCS. It is due to "
    "(1) GCS is unexpectedly failed. "
    "(2) GCS is overloaded by lots of work. "
    "(3) There's an uexpected network issues. Please check the GCS logs to "
    "find the root cause."
)
RAYLET_QUERY_FAILURE_WARNING = (
    "Failed to query data from some raylets. You might have data loss. "
    "Queryed {total} raylets "
    "and {network_failures} raylets failed to reply. It is due to "
    "(1) Raylet is unexpectedly failed. "
    "(2) Raylet is overloaded by lots of work. "
    "(3) There's an uexpected network issues. Please check the Raylet logs to "
    "find the root cause."
)
AGENT_QUERY_FAILURE_WARNING = (
    "Failed to query data from some Ray agents. You might have data loss. "
    "Queryed {total} Ray agents "
    "and {network_failures} Ray agents failed to reply. It is due to "
    "(1) Ray agent is unexpectedly failed. "
    "(2) Ray agent is overloaded by lots of work. "
    "(3) There's an uexpected network issues. Please check the Ray agent logs to "
    "find the root cause."
)


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

    async def list_actors(self, *, option: ListApiOptions) -> StateApiResult:
        """List all actor information from the cluster.

        Returns:
            {actor_id -> actor_data_in_dict}
            actor_data_in_dict's schema is in ActorState
        """
        reply = await self._client.get_all_actor_info(timeout=option.timeout)
        if not reply:
            return StateApiResult(warnings=[GCS_QUERY_FAILURE_WARNING])

        result = []
        for message in reply.actor_table_data:
            data = self._message_to_dict(message=message, fields_to_decode=["actor_id"])
            data = filter_fields(data, ActorState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["actor_id"])
        return StateApiResult(
            data={d["actor_id"]: d for d in islice(result, option.limit)}
        )

    async def list_placement_groups(self, *, option: ListApiOptions) -> StateApiResult:
        """List all placement group information from the cluster.

        Returns:
            {pg_id -> pg_data_in_dict}
            pg_data_in_dict's schema is in PlacementGroupState
        """
        reply = await self._client.get_all_placement_group_info(timeout=option.timeout)
        if not reply:
            return StateApiResult(warnings=[GCS_QUERY_FAILURE_WARNING])

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
        return StateApiResult(
            data={d["placement_group_id"]: d for d in islice(result, option.limit)}
        )

    async def list_nodes(self, *, option: ListApiOptions) -> StateApiResult:
        """List all node information from the cluster.

        Returns:
            {node_id -> node_data_in_dict}
            node_data_in_dict's schema is in NodeState
        """
        reply = await self._client.get_all_node_info(timeout=option.timeout)
        if not reply:
            return StateApiResult(warnings=[GCS_QUERY_FAILURE_WARNING])

        result = []
        for message in reply.node_info_list:
            data = self._message_to_dict(message=message, fields_to_decode=["node_id"])
            data = filter_fields(data, NodeState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["node_id"])
        return StateApiResult(
            data={d["node_id"]: d for d in islice(result, option.limit)}
        )

    async def list_workers(self, *, option: ListApiOptions) -> StateApiResult:
        """List all worker information from the cluster.

        Returns:
            {worker_id -> worker_data_in_dict}
            worker_data_in_dict's schema is in WorkerState
        """
        reply = await self._client.get_all_worker_info(timeout=option.timeout)
        if not reply:
            return StateApiResult(warnings=[GCS_QUERY_FAILURE_WARNING])

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
        return StateApiResult(
            data={d["worker_id"]: d for d in islice(result, option.limit)}
        )

    def list_jobs(self, *, option: ListApiOptions) -> StateApiResult:
        # TODO(sang): Support limit & timeout & async calls.
        result = self._client.get_job_info()
        if not result:
            return StateApiResult(warnings=[GCS_QUERY_FAILURE_WARNING])
        return StateApiResult(data=result)

    async def list_tasks(self, *, option: ListApiOptions) -> StateApiResult:
        """List all task information from the cluster.

        Returns:
            {task_id -> task_data_in_dict}
            task_data_in_dict's schema is in TaskState
        """
        raylet_ids = self._client.get_all_registered_raylet_ids()
        replies = await asyncio.gather(
            *[
                self._client.get_task_info(node_id, timeout=option.timeout)
                for node_id in raylet_ids
            ],
        )

        network_failures = 0
        running_task_id = set()
        for reply in replies:
            if not reply:
                network_failures += 1
                continue
            for task_id in reply.running_task_ids:
                running_task_id.add(binary_to_hex(task_id))

        result = []
        for reply in replies:
            if not reply:
                # We don't increment network failures here
                # becaues it has been already counted.
                continue
            tasks = reply.owned_task_info_entries
            for task in tasks:
                data = self._message_to_dict(
                    message=task,
                    fields_to_decode=["task_id"],
                )
                if data["task_id"] in running_task_id:
                    data["scheduling_state"] = TaskStatus.DESCRIPTOR.values_by_number[
                        TaskStatus.RUNNING
                    ].name
                data = filter_fields(data, TaskState)
                result.append(data)

        warnings = (
            [
                RAYLET_QUERY_FAILURE_WARNING.format(
                    total=len(raylet_ids), network_failures=network_failures
                )
            ]
            if network_failures
            else None
        )
        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["task_id"])
        return StateApiResult(
            data={d["task_id"]: d for d in islice(result, option.limit)},
            warnings=warnings,
        )

    async def list_objects(self, *, option: ListApiOptions) -> StateApiResult:
        """List all object information from the cluster.

        Returns:
            {object_id -> object_data_in_dict}
            object_data_in_dict's schema is in ObjectState
        """
        raylet_ids = self._client.get_all_registered_raylet_ids()
        replies = await asyncio.gather(
            *[
                self._client.get_object_info(node_id, timeout=option.timeout)
                for node_id in raylet_ids
            ]
        )

        network_failures = 0
        worker_stats = []
        for reply in replies:
            if not reply:
                network_failures += 1
                continue

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

        warnings = (
            [
                RAYLET_QUERY_FAILURE_WARNING.format(
                    total=len(raylet_ids), network_failures=network_failures
                )
            ]
            if network_failures
            else None
        )
        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["object_id"])
        return StateApiResult(
            data={d["object_id"]: d for d in islice(result, option.limit)},
            warnings=warnings,
        )

    async def list_runtime_envs(self, *, option: ListApiOptions) -> StateApiResult:
        """List all runtime env information from the cluster.

        Returns:
            A list of runtime env information in the cluster.
            The schema of returned "dict" is equivalent to the
            `RuntimeEnvState` protobuf message.
            We don't have id -> data mapping like other API because runtime env
            doesn't have unique ids.
        """
        agent_ids = self._client.get_all_registered_agent_ids()
        replies = await asyncio.gather(
            *[
                self._client.get_runtime_envs_info(node_id, timeout=option.timeout)
                for node_id in agent_ids
            ]
        )

        result = []
        network_failures = 0
        for node_id, reply in zip(self._client.get_all_registered_agent_ids(), replies):
            if not reply:
                network_failures += 1
                continue

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

        warnings = (
            [
                AGENT_QUERY_FAILURE_WARNING.format(
                    total=len(agent_ids), network_failures=network_failures
                )
            ]
            if network_failures
            else None
        )

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
        return StateApiResult(
            data=list(islice(result, option.limit)), warnings=warnings
        )

    def _message_to_dict(
        self,
        *,
        message,
        fields_to_decode: List[str],
        preserving_proto_field_name: bool = True,
    ) -> dict:
        return dashboard_utils.message_to_dict(
            message,
            fields_to_decode,
            including_default_value_fields=True,
            preserving_proto_field_name=preserving_proto_field_name,
        )
