import asyncio
import logging

from itertools import islice
from typing import List

from ray.core.generated.common_pb2 import TaskStatus
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.memory_utils as memory_utils

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
    ListApiResponse,
)
from ray.experimental.state.state_manager import (
    StateDataSourceClient,
    DataSourceUnavailable,
)
from ray.runtime_env import RuntimeEnv
from ray._private.utils import binary_to_hex

logger = logging.getLogger(__name__)

GCS_QUERY_FAILURE_WARNING = (
    "Failed to query data from GCS. It is due to "
    "(1) GCS is unexpectedly failed. "
    "(2) GCS is overloaded. "
    "(3) There's an unexpected network issue. "
    "Please check the gcs_server.out log to find the root cause."
)
NODE_QUERY_FAILURE_WARNING = (
    "Failed to query data from {type}. "
    "Queryed {total} {type} "
    "and {network_failures} {type} failed to reply. It is due to "
    "(1) {type} is unexpectedly failed. "
    "(2) {type} is overloaded. "
    "(3) There's an unexpected network issue. Please check the "
    "{log_command} to find the root cause."
)


# TODO(sang): Move the class to state/state_manager.py.
# TODO(sang): Remove *State and replaces with Pydantic or protobuf.
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

    async def list_actors(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all actor information from the cluster.

        Returns:
            {actor_id -> actor_data_in_dict}
            actor_data_in_dict's schema is in ActorState

        """
        try:
            reply = await self._client.get_all_actor_info(timeout=option.timeout)
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        result = []
        for message in reply.actor_table_data:
            data = self._message_to_dict(message=message, fields_to_decode=["actor_id"])
            data = filter_fields(data, ActorState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["actor_id"])
        return ListApiResponse(
            result={d["actor_id"]: d for d in islice(result, option.limit)}
        )

    async def list_placement_groups(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all placement group information from the cluster.

        Returns:
            {pg_id -> pg_data_in_dict}
            pg_data_in_dict's schema is in PlacementGroupState
        """
        try:
            reply = await self._client.get_all_placement_group_info(
                timeout=option.timeout
            )
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

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
        return ListApiResponse(
            result={d["placement_group_id"]: d for d in islice(result, option.limit)}
        )

    async def list_nodes(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all node information from the cluster.

        Returns:
            {node_id -> node_data_in_dict}
            node_data_in_dict's schema is in NodeState
        """
        try:
            reply = await self._client.get_all_node_info(timeout=option.timeout)
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        result = []
        for message in reply.node_info_list:
            data = self._message_to_dict(message=message, fields_to_decode=["node_id"])
            data = filter_fields(data, NodeState)
            result.append(data)

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["node_id"])
        return ListApiResponse(
            result={d["node_id"]: d for d in islice(result, option.limit)}
        )

    async def list_workers(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all worker information from the cluster.

        Returns:
            {worker_id -> worker_data_in_dict}
            worker_data_in_dict's schema is in WorkerState
        """
        try:
            reply = await self._client.get_all_worker_info(timeout=option.timeout)
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

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
        return ListApiResponse(
            result={d["worker_id"]: d for d in islice(result, option.limit)}
        )

    def list_jobs(self, *, option: ListApiOptions) -> ListApiResponse:
        # TODO(sang): Support limit & timeout & async calls.
        try:
            result = self._client.get_job_info()
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)
        return ListApiResponse(result=result)

    async def list_tasks(self, *, option: ListApiOptions) -> ListApiResponse:
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
            return_exceptions=True,
        )

        unresponsive_nodes = 0
        running_task_id = set()
        successful_replies = []
        for reply in replies:
            if isinstance(reply, DataSourceUnavailable):
                unresponsive_nodes += 1
                continue
            elif isinstance(reply, Exception):
                raise reply

            successful_replies.append(reply)
            for task_id in reply.running_task_ids:
                running_task_id.add(binary_to_hex(task_id))

        partial_failure_warning = None
        if len(raylet_ids) > 0 and unresponsive_nodes > 0:
            warning_msg = NODE_QUERY_FAILURE_WARNING.format(
                type="raylet",
                total=len(raylet_ids),
                network_failures=unresponsive_nodes,
                log_command="raylet.out",
            )
            if unresponsive_nodes == len(raylet_ids):
                raise DataSourceUnavailable(warning_msg)
            partial_failure_warning = (
                f"The returned data may contain incomplete result. {warning_msg}"
            )

        result = []
        for reply in successful_replies:
            assert not isinstance(reply, Exception)
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

        # Sort to make the output deterministic.
        result.sort(key=lambda entry: entry["task_id"])
        return ListApiResponse(
            result={d["task_id"]: d for d in islice(result, option.limit)},
            partial_failure_warning=partial_failure_warning,
        )

    async def list_objects(self, *, option: ListApiOptions) -> ListApiResponse:
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
            ],
            return_exceptions=True,
        )

        unresponsive_nodes = 0
        worker_stats = []
        for reply, node_id in zip(replies, raylet_ids):
            if isinstance(reply, DataSourceUnavailable):
                unresponsive_nodes += 1
                continue
            elif isinstance(reply, Exception):
                raise reply

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

        partial_failure_warning = None
        if len(raylet_ids) > 0 and unresponsive_nodes > 0:
            warning_msg = NODE_QUERY_FAILURE_WARNING.format(
                type="raylet",
                total=len(raylet_ids),
                network_failures=unresponsive_nodes,
                log_command="raylet.out",
            )
            if unresponsive_nodes == len(raylet_ids):
                raise DataSourceUnavailable(warning_msg)
            partial_failure_warning = (
                f"The returned data may contain incomplete result. {warning_msg}"
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
        return ListApiResponse(
            result={d["object_id"]: d for d in islice(result, option.limit)},
            partial_failure_warning=partial_failure_warning,
        )

    async def list_runtime_envs(self, *, option: ListApiOptions) -> ListApiResponse:
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
            ],
            return_exceptions=True,
        )

        result = []
        unresponsive_nodes = 0
        for node_id, reply in zip(self._client.get_all_registered_agent_ids(), replies):
            if isinstance(reply, DataSourceUnavailable):
                unresponsive_nodes += 1
                continue
            elif isinstance(reply, Exception):
                raise reply

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

        partial_failure_warning = None
        if len(agent_ids) > 0 and unresponsive_nodes > 0:
            warning_msg = NODE_QUERY_FAILURE_WARNING.format(
                type="agent",
                total=len(agent_ids),
                network_failures=unresponsive_nodes,
                log_command="dashboard_agent.log",
            )
            if unresponsive_nodes == len(agent_ids):
                raise DataSourceUnavailable(warning_msg)
            partial_failure_warning = (
                f"The returned data may contain incomplete result. {warning_msg}"
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
        return ListApiResponse(
            result=list(islice(result, option.limit)),
            partial_failure_warning=partial_failure_warning,
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
