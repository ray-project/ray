import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from typing import List, Optional

from ray import NodeID
import ray.dashboard.memory_utils as memory_utils
from ray._private.profiling import chrome_tracing_dump
from ray._private.ray_constants import env_integer
from ray._private.utils import get_or_create_event_loop
from ray.dashboard.state_api_utils import do_filter
from ray.dashboard.utils import compose_state_message
from ray.runtime_env import RuntimeEnv
from ray.util.state.common import (
    RAY_MAX_LIMIT_FROM_API_SERVER,
    ActorState,
    ActorSummaries,
    JobState,
    ListApiOptions,
    ListApiResponse,
    NodeState,
    ObjectState,
    ObjectSummaries,
    PlacementGroupState,
    RuntimeEnvState,
    StateSummary,
    SummaryApiOptions,
    SummaryApiResponse,
    TaskState,
    TaskSummaries,
    WorkerState,
    protobuf_message_to_dict,
    protobuf_to_task_state_dict,
)
from ray.util.state.state_manager import DataSourceUnavailable, StateDataSourceClient

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
    "Queried {total} {type} "
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

    def __init__(
        self,
        state_data_source_client: StateDataSourceClient,
        thread_pool_executor: ThreadPoolExecutor,
    ):
        self._client = state_data_source_client
        self._thread_pool_executor = thread_pool_executor

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
            reply = await self._client.get_all_actor_info(
                timeout=option.timeout, filters=option.filters
            )
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        def transform(reply) -> ListApiResponse:
            result = []
            for message in reply.actor_table_data:
                # Note: this is different from actor_table_data_to_dict in actor_head.py
                # because we set preserving_proto_field_name=True so fields are
                # snake_case, while actor_table_data_to_dict in actor_head.py is
                # camelCase.
                # TODO(ryw): modify actor_table_data_to_dict to use snake_case, and
                # consolidate the code.
                data = protobuf_message_to_dict(
                    message=message,
                    fields_to_decode=[
                        "actor_id",
                        "owner_id",
                        "job_id",
                        "node_id",
                        "placement_group_id",
                    ],
                )
                result.append(data)

            num_after_truncation = len(result) + reply.num_filtered
            result = do_filter(result, option.filters, ActorState, option.detail)
            num_filtered = len(result)

            # Sort to make the output deterministic.
            result.sort(key=lambda entry: entry["actor_id"])
            result = list(islice(result, option.limit))
            return ListApiResponse(
                result=result,
                total=reply.total,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, reply
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

        def transform(reply) -> ListApiResponse:
            result = []
            for message in reply.placement_group_table_data:
                data = protobuf_message_to_dict(
                    message=message,
                    fields_to_decode=[
                        "placement_group_id",
                        "creator_job_id",
                        "node_id",
                    ],
                )
                result.append(data)
            num_after_truncation = len(result)

            result = do_filter(
                result, option.filters, PlacementGroupState, option.detail
            )
            num_filtered = len(result)
            # Sort to make the output deterministic.
            result.sort(key=lambda entry: entry["placement_group_id"])
            return ListApiResponse(
                result=list(islice(result, option.limit)),
                total=reply.total,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, reply
        )

    async def list_nodes(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all node information from the cluster.

        Returns:
            {node_id -> node_data_in_dict}
            node_data_in_dict's schema is in NodeState
        """
        try:
            reply = await self._client.get_all_node_info(
                timeout=option.timeout, filters=option.filters
            )
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        def transform(reply) -> ListApiResponse:
            result = []
            for message in reply.node_info_list:
                data = protobuf_message_to_dict(
                    message=message, fields_to_decode=["node_id"]
                )
                data["node_ip"] = data["node_manager_address"]
                data["start_time_ms"] = int(data["start_time_ms"])
                data["end_time_ms"] = int(data["end_time_ms"])
                death_info = data.get("death_info", {})
                data["state_message"] = compose_state_message(
                    death_info.get("reason", None),
                    death_info.get("reason_message", None),
                )

                result.append(data)

            num_after_truncation = len(result) + reply.num_filtered
            result = do_filter(result, option.filters, NodeState, option.detail)
            num_filtered = len(result)

            # Sort to make the output deterministic.
            result.sort(key=lambda entry: entry["node_id"])
            result = list(islice(result, option.limit))
            return ListApiResponse(
                result=result,
                total=reply.total,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, reply
        )

    async def list_workers(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all worker information from the cluster.

        Returns:
            {worker_id -> worker_data_in_dict}
            worker_data_in_dict's schema is in WorkerState
        """
        try:
            reply = await self._client.get_all_worker_info(
                timeout=option.timeout,
                filters=option.filters,
            )
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        def transform(reply) -> ListApiResponse:

            result = []
            for message in reply.worker_table_data:
                data = protobuf_message_to_dict(
                    message=message, fields_to_decode=["worker_id", "raylet_id"]
                )
                data["worker_id"] = data["worker_address"]["worker_id"]
                data["node_id"] = data["worker_address"]["raylet_id"]
                data["ip"] = data["worker_address"]["ip_address"]
                data["start_time_ms"] = int(data["start_time_ms"])
                data["end_time_ms"] = int(data["end_time_ms"])
                data["worker_launch_time_ms"] = int(data["worker_launch_time_ms"])
                data["worker_launched_time_ms"] = int(data["worker_launched_time_ms"])
                result.append(data)

            num_after_truncation = len(result) + reply.num_filtered
            result = do_filter(result, option.filters, WorkerState, option.detail)
            num_filtered = len(result)
            # Sort to make the output deterministic.
            result.sort(key=lambda entry: entry["worker_id"])
            result = list(islice(result, option.limit))
            return ListApiResponse(
                result=result,
                total=reply.total,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, reply
        )

    async def list_jobs(self, *, option: ListApiOptions) -> ListApiResponse:
        try:
            reply = await self._client.get_job_info(timeout=option.timeout)
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        def transform(reply) -> ListApiResponse:
            result = [job.dict() for job in reply]
            total = len(result)
            result = do_filter(result, option.filters, JobState, option.detail)
            num_filtered = len(result)
            result.sort(key=lambda entry: entry["job_id"] or "")
            result = list(islice(result, option.limit))
            return ListApiResponse(
                result=result,
                total=total,
                num_after_truncation=total,
                num_filtered=num_filtered,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, reply
        )

    async def list_tasks(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all task information from the cluster.

        Returns:
            {task_id -> task_data_in_dict}
            task_data_in_dict's schema is in TaskState
        """
        try:
            reply = await self._client.get_all_task_info(
                timeout=option.timeout,
                filters=option.filters,
                exclude_driver=option.exclude_driver,
            )
        except DataSourceUnavailable:
            raise DataSourceUnavailable(GCS_QUERY_FAILURE_WARNING)

        def transform(reply) -> ListApiResponse:
            """
            Transforms from proto to dict, applies filters, sorts, and truncates.
            This function is executed in a separate thread.
            """
            result = [
                protobuf_to_task_state_dict(message) for message in reply.events_by_task
            ]

            # Num pre-truncation is the number of tasks returned from
            # source + num filtered on source
            num_after_truncation = len(result)
            num_total = len(result) + reply.num_status_task_events_dropped

            # Only certain filters are done on GCS, so here the filter function is still
            # needed to apply all the filters
            result = do_filter(result, option.filters, TaskState, option.detail)
            num_filtered = len(result)

            result.sort(key=lambda entry: entry["task_id"])
            result = list(islice(result, option.limit))

            # TODO(rickyx): we could do better with the warning logic. It's messy now.
            return ListApiResponse(
                result=result,
                total=num_total,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
            )

        # In the error case
        if reply.status.code != 0:
            return ListApiResponse(
                result=[],
                total=0,
                num_after_truncation=0,
                num_filtered=0,
                warnings=[reply.status.message],
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, reply
        )

    async def list_objects(self, *, option: ListApiOptions) -> ListApiResponse:
        """List all object information from the cluster.

        Returns:
            {object_id -> object_data_in_dict}
            object_data_in_dict's schema is in ObjectState
        """
        all_node_info_reply = await self._client.get_all_node_info(
            timeout=option.timeout,
            limit=None,
            filters=[("state", "=", "ALIVE")],
        )
        tasks = [
            self._client.get_object_info(
                node_info.node_manager_address,
                node_info.node_manager_port,
                timeout=option.timeout,
            )
            for node_info in all_node_info_reply.node_info_list
        ]

        replies = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        def transform(replies) -> ListApiResponse:
            unresponsive_nodes = 0
            worker_stats = []
            total_objects = 0
            for reply in replies:
                if isinstance(reply, DataSourceUnavailable):
                    unresponsive_nodes += 1
                    continue
                elif isinstance(reply, Exception):
                    raise reply

                total_objects += reply.total
                for core_worker_stat in reply.core_workers_stats:
                    # NOTE: Set preserving_proto_field_name=False here because
                    # `construct_memory_table` requires a dictionary that has
                    # modified protobuf name
                    # (e.g., workerId instead of worker_id) as a key.
                    worker_stats.append(
                        protobuf_message_to_dict(
                            message=core_worker_stat,
                            fields_to_decode=["object_id"],
                            preserving_proto_field_name=False,
                        )
                    )

            partial_failure_warning = None
            if len(tasks) > 0 and unresponsive_nodes > 0:
                warning_msg = NODE_QUERY_FAILURE_WARNING.format(
                    type="raylet",
                    total=len(tasks),
                    network_failures=unresponsive_nodes,
                    log_command="raylet.out",
                )
                if unresponsive_nodes == len(tasks):
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
                data["ip"] = data["node_ip_address"]
                del data["node_ip_address"]
                data["type"] = data["type"].upper()
                data["task_status"] = (
                    "NIL" if data["task_status"] == "-" else data["task_status"]
                )
                result.append(data)

            # Add callsite warnings if it is not configured.
            callsite_warning = []
            callsite_enabled = env_integer("RAY_record_ref_creation_sites", 0)
            if not callsite_enabled:
                callsite_warning.append(
                    "Callsite is not being recorded. "
                    "To record callsite information for each ObjectRef created, set "
                    "env variable RAY_record_ref_creation_sites=1 during `ray start` "
                    "and `ray.init`."
                )

            num_after_truncation = len(result)
            result = do_filter(result, option.filters, ObjectState, option.detail)
            num_filtered = len(result)
            # Sort to make the output deterministic.
            result.sort(key=lambda entry: entry["object_id"])
            result = list(islice(result, option.limit))
            return ListApiResponse(
                result=result,
                partial_failure_warning=partial_failure_warning,
                total=total_objects,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
                warnings=callsite_warning,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, replies
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
        live_node_info_reply = await self._client.get_all_node_info(
            timeout=option.timeout,
            limit=None,
            filters=[("state", "=", "ALIVE")],
        )
        node_infos = [
            node_info
            for node_info in live_node_info_reply.node_info_list
            if node_info.runtime_env_agent_port is not None
        ]
        tasks = [
            self._client.get_runtime_envs_info(
                node_info.node_manager_address,
                node_info.runtime_env_agent_port,
                timeout=option.timeout,
            )
            for node_info in node_infos
        ]

        replies = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        def transform(replies) -> ListApiResponse:
            result = []
            unresponsive_nodes = 0
            total_runtime_envs = 0
            for node_info, reply in zip(node_infos, replies):
                if isinstance(reply, DataSourceUnavailable):
                    unresponsive_nodes += 1
                    continue
                elif isinstance(reply, Exception):
                    raise reply

                total_runtime_envs += reply.total
                states = reply.runtime_env_states
                for state in states:
                    data = protobuf_message_to_dict(message=state, fields_to_decode=[])
                    # Need to deserialize this field.
                    data["runtime_env"] = RuntimeEnv.deserialize(
                        data["runtime_env"]
                    ).to_dict()
                    data["node_id"] = NodeID(node_info.node_id).hex()
                    result.append(data)

            partial_failure_warning = None
            if len(tasks) > 0 and unresponsive_nodes > 0:
                warning_msg = NODE_QUERY_FAILURE_WARNING.format(
                    type="agent",
                    total=len(tasks),
                    network_failures=unresponsive_nodes,
                    log_command="dashboard_agent.log",
                )
                if unresponsive_nodes == len(tasks):
                    raise DataSourceUnavailable(warning_msg)
                partial_failure_warning = (
                    f"The returned data may contain incomplete result. {warning_msg}"
                )
            num_after_truncation = len(result)
            result = do_filter(result, option.filters, RuntimeEnvState, option.detail)
            num_filtered = len(result)

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
            result = list(islice(result, option.limit))
            return ListApiResponse(
                result=result,
                partial_failure_warning=partial_failure_warning,
                total=total_runtime_envs,
                num_after_truncation=num_after_truncation,
                num_filtered=num_filtered,
            )

        return await get_or_create_event_loop().run_in_executor(
            self._thread_pool_executor, transform, replies
        )

    async def summarize_tasks(self, option: SummaryApiOptions) -> SummaryApiResponse:
        summary_by = option.summary_by or "func_name"
        if summary_by not in ["func_name", "lineage"]:
            raise ValueError('summary_by must be one of "func_name" or "lineage".')

        # For summary, try getting as many entries as possible to minimze data loss.
        result = await self.list_tasks(
            option=ListApiOptions(
                timeout=option.timeout,
                limit=RAY_MAX_LIMIT_FROM_API_SERVER,
                filters=option.filters,
                detail=summary_by == "lineage",
            )
        )

        if summary_by == "func_name":
            summary_results = TaskSummaries.to_summary_by_func_name(tasks=result.result)
        else:
            # We will need the actors info for actor tasks.
            actors = await self.list_actors(
                option=ListApiOptions(
                    timeout=option.timeout,
                    limit=RAY_MAX_LIMIT_FROM_API_SERVER,
                    detail=True,
                )
            )
            summary_results = TaskSummaries.to_summary_by_lineage(
                tasks=result.result, actors=actors.result
            )
        summary = StateSummary(node_id_to_summary={"cluster": summary_results})
        warnings = result.warnings
        if (
            summary_results.total_actor_scheduled
            + summary_results.total_actor_tasks
            + summary_results.total_tasks
            < result.num_filtered
        ):
            warnings = warnings or []
            warnings.append(
                "There is missing data in this aggregation. "
                "Possibly due to task data being evicted to preserve memory."
            )
        return SummaryApiResponse(
            total=result.total,
            result=summary,
            partial_failure_warning=result.partial_failure_warning,
            warnings=warnings,
            num_after_truncation=result.num_after_truncation,
            num_filtered=result.num_filtered,
        )

    async def summarize_actors(self, option: SummaryApiOptions) -> SummaryApiResponse:
        # For summary, try getting as many entries as possible to minimze data loss.
        result = await self.list_actors(
            option=ListApiOptions(
                timeout=option.timeout,
                limit=RAY_MAX_LIMIT_FROM_API_SERVER,
                filters=option.filters,
            )
        )
        summary = StateSummary(
            node_id_to_summary={
                "cluster": ActorSummaries.to_summary(actors=result.result)
            }
        )
        return SummaryApiResponse(
            total=result.total,
            result=summary,
            partial_failure_warning=result.partial_failure_warning,
            warnings=result.warnings,
            num_after_truncation=result.num_after_truncation,
            num_filtered=result.num_filtered,
        )

    async def summarize_objects(self, option: SummaryApiOptions) -> SummaryApiResponse:
        # For summary, try getting as many entries as possible to minimize data loss.
        result = await self.list_objects(
            option=ListApiOptions(
                timeout=option.timeout,
                limit=RAY_MAX_LIMIT_FROM_API_SERVER,
                filters=option.filters,
            )
        )
        summary = StateSummary(
            node_id_to_summary={
                "cluster": ObjectSummaries.to_summary(objects=result.result)
            }
        )
        return SummaryApiResponse(
            total=result.total,
            result=summary,
            partial_failure_warning=result.partial_failure_warning,
            warnings=result.warnings,
            num_after_truncation=result.num_after_truncation,
            num_filtered=result.num_filtered,
        )

    async def generate_task_timeline(self, job_id: Optional[str]) -> List[dict]:
        filters = [("job_id", "=", job_id)] if job_id else None
        result = await self.list_tasks(
            option=ListApiOptions(detail=True, filters=filters, limit=10000)
        )
        return chrome_tracing_dump(result.result)
