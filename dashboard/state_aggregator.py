import asyncio
import logging

from collections import defaultdict
from typing import List

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.memory_utils as memory_utils

from ray.dashboard.datacenter import DataSource
from ray.dashboard.utils import Change
from ray.experimental.state.common import (
    filter_fields,
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
    TaskState,
    ObjectState,
    ListApiOptions,
)
from ray.experimental.state.state_manager import StateDataSourceClient

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30
DEFAULT_LIMIT = 1000


# TODO(sang): Move the class to state/state_manager.py.
# TODO(sang): Remove *State and replaces with Pydantic or protobuf
# (depending on API interface standardization).
class StateAPIManager:
    """A class to query states from data source, caches, and post-processes
    the entries.
    """

    def __init__(self, state_data_source_client: StateDataSourceClient):
        self._client = state_data_source_client
        DataSource.nodes.signal.append(self._update_raylet_stubs)

    async def _update_raylet_stubs(self, change: Change):
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
        # TODO(sang): Move this function out of this class.
        if change.old:
            # When a node is deleted from the DataSource or it is overwritten.
            node_id, node_info = change.old
            self._client.unregister_raylet_client(node_id)
        if change.new:
            # When a new node information is written to DataSource.
            node_id, node_info = change.new
            self._client.register_raylet_client(
                node_id,
                node_info["nodeManagerAddress"],
                int(node_info["nodeManagerPort"]),
            )

    @property
    def data_source_client(self):
        return self._client

    async def get_actors(self, *, option: ListApiOptions) -> dict:
        """List all actor information from the cluster.

        Returns:
            {actor_id -> actor_data_in_dict}
            actor_data_in_dict's schema is in ActorState
        """
        reply = await self._client.get_all_actor_info(timeout=DEFAULT_RPC_TIMEOUT)
        result = {}
        for i, message in enumerate(reply.actor_table_data):
            logger.info(i)
            logger.info(option.limit)
            if i == option.limit:
                break

            data = self._message_to_dict(message=message, fields_to_decode=["actor_id"])
            data = filter_fields(data, ActorState)
            result[data["actor_id"]] = data
        return result

    async def get_placement_groups(self, *, option: ListApiOptions) -> dict:
        """List all placement group information from the cluster.

        Returns:
            {pg_id -> pg_data_in_dict}
            pg_data_in_dict's schema is in PlacementGroupState
        """
        reply = await self._client.get_all_placement_group_info(
            timeout=DEFAULT_RPC_TIMEOUT
        )
        result = {}
        for i, message in enumerate(reply.placement_group_table_data):
            if i == option.limit:
                break

            data = self._message_to_dict(
                message=message,
                fields_to_decode=["placement_group_id"],
            )
            data = filter_fields(data, PlacementGroupState)
            result[data["placement_group_id"]] = data
        return result

    async def get_nodes(self, *, option: ListApiOptions) -> dict:
        """List all node information from the cluster.

        Returns:
            {node_id -> node_data_in_dict}
            node_data_in_dict's schema is in NodeState
        """
        reply = await self._client.get_all_node_info(timeout=DEFAULT_RPC_TIMEOUT)
        result = {}
        for i, message in enumerate(reply.node_info_list):
            if i == option.limit:
                break

            data = self._message_to_dict(message=message, fields_to_decode=["node_id"])
            data = filter_fields(data, NodeState)
            result[data["node_id"]] = data
        return result

    async def get_workers(self, *, option: ListApiOptions) -> dict:
        """List all worker information from the cluster.

        Returns:
            {worker_id -> worker_data_in_dict}
            worker_data_in_dict's schema is in WorkerState
        """
        reply = await self._client.get_all_worker_info(timeout=DEFAULT_RPC_TIMEOUT)
        result = {}
        for i, message in enumerate(reply.worker_table_data):
            if i == option.limit:
                break

            data = self._message_to_dict(
                message=message, fields_to_decode=["worker_id"]
            )
            data["worker_id"] = data["worker_address"]["worker_id"]
            data = filter_fields(data, WorkerState)
            result[data["worker_id"]] = data
        return result

    async def get_tasks(self, *, option: ListApiOptions) -> dict:
        """List all task information from the cluster.

        Returns:
            {task_id -> task_data_in_dict}
            task_data_in_dict's schema is in TaskState
        """
        replies = await asyncio.gather(
            *[
                self._client.get_task_info(node_id, timeout=DEFAULT_RPC_TIMEOUT)
                for node_id in self._client.get_all_registered_raylet_ids()
            ]
        )

        result = defaultdict(dict)
        entries = 0
        for reply in replies:
            tasks = reply.task_info_entries
            for task in tasks:
                if entries == option.limit:
                    break

                data = self._message_to_dict(
                    message=task,
                    fields_to_decode=["task_id"],
                )
                data = filter_fields(data, TaskState)
                result[data["task_id"]] = data
                entries += 1
        return result

    async def get_objects(self, *, option: ListApiOptions) -> dict:
        """List all object information from the cluster.

        Returns:
            {object_id -> object_data_in_dict}
            object_data_in_dict's schema is in ObjectState
        """
        replies = await asyncio.gather(
            *[
                self._client.get_object_info(node_id, timeout=DEFAULT_RPC_TIMEOUT)
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

        result = {}
        entries = 0
        memory_table = memory_utils.construct_memory_table(worker_stats)
        for entry in memory_table.table:
            if entries == option.limit:
                break

            data = entry.as_dict()
            # `construct_memory_table` returns object_ref field which is indeed
            # object_id. We do transformation here.
            # TODO(sang): Refactor `construct_memory_table`.
            data["object_id"] = data["object_ref"]
            del data["object_ref"]
            data = filter_fields(data, ObjectState)
            result[data["object_id"]] = data
            entries += 1
        return result

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
