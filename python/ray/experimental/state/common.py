import logging
from abc import ABC
from dataclasses import dataclass, field, fields
from enum import Enum, unique
from typing import Dict, List, Optional, Set, Tuple, Union

from ray._private.ray_constants import env_integer
from ray.core.generated.common_pb2 import TaskType
from ray.dashboard.modules.job.common import JobInfo
from ray.experimental.state.custom_types import (
    TypeActorStatus,
    TypeNodeStatus,
    TypePlacementGroupStatus,
    TypeReferenceType,
    TypeTaskStatus,
    TypeTaskType,
    TypeWorkerExitType,
    TypeWorkerType,
)

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30
DEFAULT_LIMIT = 100
DEFAULT_LOG_LIMIT = 1000

# Max number of entries from API server to the client
RAY_MAX_LIMIT_FROM_API_SERVER = env_integer(
    "RAY_MAX_LIMIT_FROM_API_SERVER", 10 * 1000
)  # 10k

# Max number of entries from data sources (rest will be truncated at the
# data source, e.g. raylet)
RAY_MAX_LIMIT_FROM_DATA_SOURCE = env_integer(
    "RAY_MAX_LIMIT_FROM_DATA_SOURCE", 10 * 1000
)  # 10k

STATE_OBS_ALPHA_FEEDBACK_MSG = [
    "\n==========ALPHA, FEEDBACK NEEDED ===============",
    "State Observability APIs is currently in Alpha. ",
    "If you have any feedback, you could do so at either way as below:",
    "    1. Report bugs/issues with details: https://forms.gle/gh77mwjEskjhN8G46",
    "    2. Follow up in #ray-state-observability-dogfooding slack channel of Ray: "
    "https://tinyurl.com/2pm26m4a",
    "==========================================================",
]


@unique
class StateResource(Enum):
    ACTORS = "actors"
    JOBS = "jobs"
    PLACEMENT_GROUPS = "placement_groups"
    NODES = "nodes"
    WORKERS = "workers"
    TASKS = "tasks"
    OBJECTS = "objects"
    RUNTIME_ENVS = "runtime_envs"
    CLUSTER_EVENTS = "cluster_events"


@unique
class SummaryResource(Enum):
    ACTORS = "actors"
    TASKS = "tasks"
    OBJECTS = "objects"


SupportedFilterType = Union[str, bool, int, float]


PredicateType = str  # Literal["=", "!="]


@dataclass(init=True)
class ListApiOptions:
    # Maximum number of entries to return
    limit: int = DEFAULT_LIMIT
    # The timeout for the API call.
    timeout: int = DEFAULT_RPC_TIMEOUT
    # If True, more detailed output will be printed.
    # The API could query more sources than detail == False
    # to get more data in detail.
    detail: bool = False
    # Filters. Each tuple pair (key, predicate, value) means key predicate value.
    # If there's more than 1 filter, it means AND.
    # E.g., [(key, "=", val), (key2, "!=" val2)] means (key=val) AND (key2!=val2)
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = field(
        default_factory=list
    )
    # When the request is processed on the server side,
    # we should apply multiplier so that server side can finish
    # processing a request within timeout. Otherwise,
    # timeout will always lead Http timeout.
    _server_timeout_multiplier: float = 0.8

    # TODO(sang): Use Pydantic instead.
    def __post_init__(self):
        assert isinstance(self.limit, int)
        assert isinstance(self.timeout, int)
        assert isinstance(self.detail, bool)
        assert isinstance(self.filters, list) or self.filters is None, (
            "filters must be a list type. Given filters: "
            f"{self.filters} type: {type(self.filters)}. "
            "Provide a list of tuples instead. "
            "e.g., list_actors(filters=[('name', '=', 'ABC')])"
        )
        # To return the data to users, when there's a partial failure
        # we need to have a timeout that's smaller than the users' timeout.
        # 80% is configured arbitrarily.
        self.timeout = int(self.timeout * self._server_timeout_multiplier)
        assert self.timeout != 0, "0 second timeout is not supported."
        if self.filters is None:
            self.filters = []

        for filter in self.filters:
            _, filter_predicate, _ = filter
            if filter_predicate != "=" and filter_predicate != "!=":
                raise ValueError(
                    f"Unsupported filter predicate {filter_predicate} is given. "
                    "Available predicates: =, !=."
                )


@dataclass(init=True)
class GetApiOptions:
    # Timeout for the HTTP request
    timeout: int = DEFAULT_RPC_TIMEOUT


@dataclass(init=True)
class SummaryApiOptions:
    # Timeout for the HTTP request
    timeout: int = DEFAULT_RPC_TIMEOUT


def state_column(*, filterable: bool, detail: bool = False, **kwargs):
    """A wrapper around dataclass.field to add additional metadata.

    The metadata is used to define detail / filterable option of
    each column.

    Args:
        detail: If True, the column is used when detail == True
        filterable: If True, the column can be used for filtering.
        kwargs: The same kwargs for the `dataclasses.field` function.
    """
    m = {"detail": detail, "filterable": filterable}
    if "metadata" in kwargs:
        kwargs["metadata"].update(m)
    else:
        kwargs["metadata"] = m
    return field(**kwargs)


class StateSchema(ABC):
    """Schema class for Ray resource abstraction.

    The child class must be dataclass. All child classes
    - perform runtime type checking upon initialization.
    - are supposed to use `state_column` instead of `field`.
        It will allow the class to return filterable/detail columns.
        If `state_column` is not specified, that column is not filterable
        and for non-detail output.

    For example,
    ```
    @dataclass
    class State(StateSchema):
        column_a: str
        column_b: int = state_column(detail=True, filterable=True)

    s = State(column_a="abc", b=1)
    # Returns {"column_b"}
    s.filterable_columns()
    # Returns {"column_a"}
    s.base_columns()
    # Returns {"column_a", "column_b"}
    s.columns()
    ```
    """

    @classmethod
    def list_columns(cls) -> List[str]:
        """Return a list of columns."""
        cols = []
        for f in fields(cls):
            cols.append(f.name)
        return cols

    @classmethod
    def columns(cls) -> Set[str]:
        """Return a set of all columns."""
        return set(cls.list_columns())

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        """Return a list of filterable columns"""
        filterable = set()
        for f in fields(cls):
            if f.metadata.get("filterable", False):
                filterable.add(f.name)
        return filterable

    @classmethod
    def base_columns(cls) -> Set[str]:
        """Return a list of base columns.

        Base columns mean columns to return when detail == False.
        """
        base = set()
        for f in fields(cls):
            if not f.metadata.get("detail", False):
                base.add(f.name)
        return base

    @classmethod
    def detail_columns(cls) -> Set[str]:
        """Return a list of detail columns.

        Detail columns mean columns to return when detail == True.
        """
        detail = set()
        for f in fields(cls):
            if f.metadata.get("detail", False):
                detail.add(f.name)
        return detail

    def __post_init__(self):
        for f in fields(self):
            v = getattr(self, f.name)
            assert isinstance(getattr(self, f.name), f.type), (
                f"The field {f.name} has a wrong type {type(v)}. "
                f"Expected type: {f.type}"
            )


def filter_fields(data: dict, state_dataclass: StateSchema, detail: bool) -> dict:
    """Filter the given data's columns based on the given schema.

    Args:
        data: A single data entry to filter columns.
        state_dataclass: The schema to filter data.
        detail: Whether or not it should include columns for detail output.
    """
    filtered_data = {}
    columns = state_dataclass.columns() if detail else state_dataclass.base_columns()
    for col in columns:
        if col in data:
            filtered_data[col] = data[col]
        else:
            filtered_data[col] = None
    return filtered_data


@dataclass(init=True)
class GetLogOptions:
    timeout: int
    node_id: Optional[str] = None
    node_ip: Optional[str] = None
    # One of {file, stream}. File means it will return the whole log.
    # stream means it will keep the connection and streaming the log.
    media_type: str = "file"
    # The file name of the log.
    filename: Optional[str] = None
    # The actor id of the log. It is used only for worker logs.
    actor_id: Optional[str] = None
    # The task id of the log. It is used only for worker logs.
    # This is currently not working. TODO(sang): Support task log.
    task_id: Optional[str] = None
    # The pid of the log. It is used only for worker logs.
    pid: Optional[int] = None
    # Total log lines to return.
    lines: int = 1000
    # The interval where new logs are streamed to.
    # Should be used only when media_type == stream.
    interval: Optional[float] = None
    # The suffix of the log file if file resolution not through filename directly.
    suffix: Optional[str] = None

    def __post_init__(self):
        if self.pid:
            self.pid = int(self.pid)
        if self.interval:
            self.interval = float(self.interval)
        self.lines = int(self.lines)

        if self.task_id:
            raise NotImplementedError("task_id is not supported yet.")

        if self.media_type == "file":
            assert self.interval is None
        if self.media_type not in ["file", "stream"]:
            raise ValueError(f"Invalid media type: {self.media_type}")
        if not (self.node_id or self.node_ip) and not (self.actor_id or self.task_id):
            raise ValueError(
                "node_id or node_ip must be provided as constructor arguments when no "
                "actor or task_id is supplied as arguments."
            )
        if self.node_id and self.node_ip:
            raise ValueError(
                "Both node_id and node_ip are given. Only one of them can be provided. "
                f"Given node id: {self.node_id}, given node ip: {self.node_ip}"
            )
        if not (self.actor_id or self.task_id or self.pid or self.filename):
            raise ValueError(
                "None of actor_id, task_id, pid, or filename is provided. "
                "At least one of them is required to fetch logs."
            )
        if self.filename and self.suffix:
            raise ValueError("suffix should not be provided together with filename.")


# See the ActorTableData message in gcs.proto for all potential options that
# can be included in this class.
# TODO(sang): Replace it with Pydantic or gRPC schema (once interface is finalized).
@dataclass(init=True)
class ActorState(StateSchema):
    """Actor State"""

    #: The id of the actor.
    actor_id: str = state_column(filterable=True)
    #: The class name of the actor.
    class_name: str = state_column(filterable=True)
    #: The state of the actor.
    #:
    #: - DEPENDENCIES_UNREADY: Actor is waiting for dependency to be ready.
    #:   E.g., a new actor is waiting for object ref that's created from
    #:   other remote task.
    #: - PENDING_CREATION: Actor's dependency is ready, but it is not created yet.
    #:   It could be because there are not enough resources, too many actor
    #:   entries in the scheduler queue, or the actor creation is slow
    #:   (e.g., slow runtime environment creation,
    #:   slow worker startup, or etc.).
    #: - ALIVE: The actor is created, and it is alive.
    #: - RESTARTING: The actor is dead, and it is restarting.
    #:   It is equivalent to `PENDING_CREATION`,
    #:   but means the actor was dead more than once.
    #: - DEAD: The actor is permanatly dead.
    state: TypeActorStatus = state_column(filterable=True)
    #: The job id of this actor.
    job_id: str = state_column(filterable=True)
    #: The name of the actor given by the `name` argument.
    name: Optional[str] = state_column(filterable=True)
    #: The node id of this actor.
    #: If the actor is restarting, it could be the node id
    #: of the dead actor (and it will be re-updated when
    #: the actor is successfully restarted).
    node_id: str = state_column(filterable=True)
    #: The pid of the actor. 0 if it is not created yet.
    pid: int = state_column(filterable=True)
    #: The namespace of the actor.
    ray_namespace: str = state_column(filterable=True)
    #: The runtime environment information of the actor.
    serialized_runtime_env: str = state_column(filterable=False, detail=True)
    #: The resource requirement of the actor.
    required_resources: dict = state_column(filterable=False, detail=True)
    #: Actor's death information in detail. None if the actor is not dead yet.
    death_cause: Optional[dict] = state_column(filterable=False, detail=True)
    #: True if the actor is detached. False otherwise.
    is_detached: bool = state_column(filterable=False, detail=True)


@dataclass(init=True)
class PlacementGroupState(StateSchema):
    """PlacementGroup State"""

    #: The id of the placement group.
    placement_group_id: str = state_column(filterable=True)
    #: The name of the placement group if it is given by the name argument.
    name: str = state_column(filterable=True)
    #: The state of the placement group.
    #:
    #: - PENDING: The placement group creation is pending scheduling.
    #:   It could be because there's not enough resources, some of creation
    #:   stage has failed (e.g., failed to commit placement gropus because
    #:   the node is dead).
    #: - CREATED: The placement group is created.
    #: - REMOVED: The placement group is removed.
    #: - RESCHEDULING: The placement group is rescheduling because some of
    #:   bundles are dead because they were on dead nodes.
    state: TypePlacementGroupStatus = state_column(filterable=True)
    #: The bundle specification of the placement group.
    bundles: dict = state_column(filterable=False, detail=True)
    #: True if the placement group is detached. False otherwise.
    is_detached: bool = state_column(filterable=True, detail=True)
    #: The scheduling stats of the placement group.
    stats: dict = state_column(filterable=False, detail=True)


@dataclass(init=True)
class NodeState(StateSchema):
    """Node State"""

    #: The id of the node.
    node_id: str = state_column(filterable=True)
    #: The ip address of the node.
    node_ip: str = state_column(filterable=True)
    #: The state of the node.
    #:
    #: ALIVE: The node is alive.
    #: DEAD: The node is dead.
    state: TypeNodeStatus = state_column(filterable=True)
    #: The name of the node if it is given by the name argument.
    node_name: str = state_column(filterable=True)
    #: The total resources of the node.
    resources_total: dict = state_column(filterable=False)


class JobState(JobInfo, StateSchema):
    """The state of the job that's submitted by Ray's Job APIs"""

    @classmethod
    def list_columns(cls) -> List[str]:
        cols = ["job_id"]
        for f in fields(cls):
            cols.append(f.name)
        return cols

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"status", "entrypoint", "error_type"}


@dataclass(init=True)
class WorkerState(StateSchema):
    """Worker State"""

    #: The id of the worker.
    worker_id: str = state_column(filterable=True)
    #: Whether or not if the worker is alive.
    is_alive: bool = state_column(filterable=True)
    #: The type of the worker.
    #:
    #: - WORKER: The regular Ray worker process that executes tasks or
    #    instantiates an actor.
    #: - DRIVER: The driver (Python script that calls `ray.init`).
    #: - SPILL_WORKER: The worker that spills objects.
    #: - RESTORE_WORKER: The worker that restores objects.
    worker_type: TypeWorkerType = state_column(filterable=True)
    #: The exit type of the worker if the worker is dead.
    #:
    #: - SYSTEM_ERROR: Worker exit due to system level failures (i.e. worker crash).
    #: - INTENDED_SYSTEM_EXIT: System-level exit that is intended. E.g.,
    #:   Workers are killed because they are idle for a long time.
    #: - USER_ERROR: Worker exits because of user error.
    #:   E.g., execptions from the actor initialization.
    #: - INTENDED_USER_EXIT: Intended exit from users (e.g., users exit
    #:   workers with exit code 0 or exit initated by Ray API such as ray.kill).
    exit_type: Optional[TypeWorkerExitType] = state_column(filterable=True)
    #: The node id of the worker.
    node_id: str = state_column(filterable=True)
    #: The ip address of the worker.
    ip: str = state_column(filterable=True)
    #: The pid of the worker.
    pid: str = state_column(filterable=True)
    #: The exit detail of the worker if the worker is dead.
    exit_detail: Optional[str] = state_column(detail=True, filterable=False)


@dataclass(init=True)
class ClusterEventState(StateSchema):
    severity: str = state_column(filterable=True)
    time: int = state_column(filterable=False)
    source_type: str = state_column(filterable=True)
    message: str = state_column(filterable=False)
    event_id: int = state_column(filterable=True)
    custom_fields: dict = state_column(filterable=False, detail=True)


@dataclass(init=True)
class TaskState(StateSchema):
    """Task State"""

    #: The id of the task.
    task_id: str = state_column(filterable=True)
    #: The name of the task if it is given by the name argument.
    name: str = state_column(filterable=True)
    #: The state of the task.
    #:
    #: Refer to src/ray/protobuf/common.proto for a detailed explanation of the state
    #: breakdowns and typical state transition flow.
    #:
    scheduling_state: TypeTaskStatus = state_column(filterable=True)
    #: The job id of this task.
    job_id: str = state_column(filterable=True)
    #: Id of the node that runs the task. If the task is retried, it could
    #: contain the node id of the previous executed task.
    #: If empty, it means the task hasn't been scheduled yet.
    node_id: str = state_column(filterable=True)
    #: The actor id that's associated with this task.
    #: It is empty if there's no relevant actors.
    actor_id: str = state_column(filterable=True)
    #: The type of the task.
    #:
    #: - NORMAL_TASK: Tasks created by `func.remote()``
    #: - ACTOR_CREATION_TASK: Actors created by `class.remote()`
    #: - ACTOR_TASK: Actor tasks submitted by `actor.method.remote()`
    #: - DRIVER_TASK: Driver (A script that calls `ray.init`).
    type: TypeTaskType = state_column(filterable=True)
    #: The name of the task. If is the name of the function
    #: if the type is a task or an actor task.
    #: It is the name of the class if it is a actor scheduling task.
    func_or_class_name: str = state_column(filterable=True)
    #: The language of the task. E.g., Python, Java, or Cpp.
    language: str = state_column(detail=True, filterable=True)
    #: The required resources to execute the task.
    required_resources: dict = state_column(detail=True, filterable=False)
    #: The runtime environment information for the task.
    runtime_env_info: str = state_column(detail=True, filterable=False)


@dataclass(init=True)
class ObjectState(StateSchema):
    """Object State"""

    #: The id of the object.
    object_id: str = state_column(filterable=True)
    #: The size of the object in mb.
    object_size: int = state_column(filterable=True)
    #: The status of the task that creates the object.
    #:
    #: - NIL: We don't have a status for this task because we are not the owner or the
    #:   task metadata has already been deleted.
    #: - WAITING_FOR_DEPENDENCIES: The task is waiting for its dependencies
    #:   to be created.
    #: - SCHEDULED: All dependencies have been created and the task is
    #:   scheduled to execute.
    #:   It could be because the task is waiting for resources,
    #:   runtime environmenet creation, fetching dependencies to the
    #:   local node, and etc..
    #: - FINISHED: The task finished successfully.
    #: - WAITING_FOR_EXECUTION: The task is scheduled properly and
    #:   waiting for execution. It includes time to deliver the task
    #:   to the remote worker + queueing time from the execution side.
    #: - RUNNING: The task that is running.
    task_status: TypeTaskStatus = state_column(filterable=True)
    #: The reference type of the object.
    #: See :ref:`Debugging with Ray Memory <debug-with-ray-memory>` for more details.
    #:
    #: - ACTOR_HANDLE: The reference is an actor handle.
    #: - PINNED_IN_MEMORY: The object is pinned in memory, meaning there's
    #:   in-flight `ray.get` on this reference.
    #: - LOCAL_REFERENCE: There's a local reference (e.g., Python reference)
    #:   to this object reference. The object won't be GC'ed until all of them is gone.
    #: - USED_BY_PENDING_TASK: The object reference is passed to other tasks. E.g.,
    #:   `a = ray.put()` -> `task.remote(a)`. In this case, a is used by a
    #:   pending task `task`.
    #: - CAPTURED_IN_OBJECT: The object is serialized by other objects. E.g.,
    #:   `a = ray.put(1)` -> `b = ray.put([a])`. a is serialized within a list.
    #: - UNKNOWN_STATUS: The object ref status is unkonwn.
    reference_type: TypeReferenceType = state_column(filterable=True)
    #: The callsite of the object.
    call_site: str = state_column(filterable=True)
    #: The worker type that creates the object.
    #:
    #: - WORKER: The regular Ray worker process that executes tasks or
    #:   instantiates an actor.
    #: - DRIVER: The driver (Python script that calls `ray.init`).
    #: - SPILL_WORKER: The worker that spills objects.
    #: - RESTORE_WORKER: The worker that restores objects.
    type: TypeWorkerType = state_column(filterable=True)
    #: The pid of the owner.
    pid: int = state_column(filterable=True)
    #: The ip address of the owner.
    ip: str = state_column(filterable=True)


@dataclass(init=True)
class RuntimeEnvState(StateSchema):
    """Runtime Environment State"""

    #: The runtime environment spec.
    runtime_env: str = state_column(filterable=True)
    #: Whether or not the runtime env creation has succeeded.
    success: bool = state_column(filterable=True)
    #: The latency of creating the runtime environment.
    #: Available if the runtime env is successfully created.
    creation_time_ms: Optional[float] = state_column(filterable=False)
    #: The node id of this runtime environment.
    node_id: str = state_column(filterable=True)
    #: The number of actors and tasks that use this runtime environment.
    ref_cnt: int = state_column(detail=True, filterable=False)
    #: The error message if the runtime environment creation has failed.
    #: Available if the runtime env is failed to be created.
    error: Optional[str] = state_column(detail=True, filterable=True)


AVAILABLE_STATES = [
    ActorState,
    PlacementGroupState,
    NodeState,
    WorkerState,
    JobState,
    TaskState,
    ObjectState,
    RuntimeEnvState,
]


for state in AVAILABLE_STATES:
    if len(state.filterable_columns()) > 0:
        filterable_cols = "\n\n    ".join(state.filterable_columns())
        state.__doc__ += f"""
\nBelow columns can be used for the `--filter` option.
\n
    {filterable_cols}
\n
"""

    if len(state.detail_columns()) > 0:
        detail_cols = "\n\n    ".join(state.detail_columns())
        state.__doc__ += f"""
\nBelow columns are available only when `get` API is used,
\n`--detail` is specified through CLI, or `detail=True` is given to Python APIs.
\n
\n
    {detail_cols}
\n
"""


@dataclass(init=True)
class ListApiResponse:
    # NOTE(rickyyx): We currently perform hard truncation when querying
    # resources which could have a large number (e.g. asking raylets for
    # the number of all objects).
    # The returned of resources seen by the user will go through from the
    # below funnel:
    # - total
    #      |  With truncation at the data source if the number of returned
    #      |  resource exceeds `RAY_MAX_LIMIT_FROM_DATA_SOURCE`
    #      v
    # - num_after_truncation
    #      |  With filtering at the state API server
    #      v
    # - num_filtered
    #      |  With limiting,
    #      |  set by min(`RAY_MAX_LIMIT_FROM_API_SERER`, <user-supplied limit>)
    #      v
    # - len(result)

    # Total number of the available resource from the cluster.
    total: int
    # Number of resources returned by data sources after truncation
    num_after_truncation: int
    # Number of resources after filtering
    num_filtered: int
    # Returned data. None if no data is returned.
    result: List[
        Union[
            ActorState,
            PlacementGroupState,
            NodeState,
            JobState,
            WorkerState,
            TaskState,
            ObjectState,
            RuntimeEnvState,
        ]
    ]
    # List API can have a partial failure if queries to
    # all sources fail. For example, getting object states
    # require to ping all raylets, and it is possible some of
    # them fails. Note that it is impossible to guarantee high
    # availability of data because ray's state information is
    # not replicated.
    partial_failure_warning: str = ""
    # A list of warnings to print.
    warnings: Optional[List[str]] = None

    def __post_init__(self):
        assert self.total is not None
        assert self.num_after_truncation is not None
        assert self.num_filtered is not None
        assert self.result is not None
        assert isinstance(self.result, list)


"""
Summary API schema
"""


@dataclass(init=True)
class TaskSummaryPerFuncOrClassName:
    #: The function or class name of this task.
    func_or_class_name: str
    #: The type of the class. Equivalent to protobuf TaskType.
    type: str
    #: State name to the count dict. State name is equivalent to
    #: the protobuf TaskStatus.
    state_counts: Dict[TypeTaskStatus, int] = field(default_factory=dict)


@dataclass
class TaskSummaries:
    #: Group key -> summary.
    #: Right now, we only have func_class_name as a key.
    # TODO(sang): Support the task group abstraction.
    summary: Dict[str, TaskSummaryPerFuncOrClassName]
    #: Total Ray tasks.
    total_tasks: int
    #: Total actor tasks.
    total_actor_tasks: int
    #: Total scheduled actors.
    total_actor_scheduled: int
    summary_by: str = "func_name"

    @classmethod
    def to_summary(cls, *, tasks: List[Dict]):
        # NOTE: The argument tasks contains a list of dictionary
        # that have the same k/v as TaskState.
        summary = {}
        total_tasks = 0
        total_actor_tasks = 0
        total_actor_scheduled = 0

        for task in tasks:
            key = task["func_or_class_name"]
            if key not in summary:
                summary[key] = TaskSummaryPerFuncOrClassName(
                    func_or_class_name=task["func_or_class_name"],
                    type=task["type"],
                )
            task_summary = summary[key]

            state = task["scheduling_state"]
            if state not in task_summary.state_counts:
                task_summary.state_counts[state] = 0
            task_summary.state_counts[state] += 1

            type_enum = TaskType.DESCRIPTOR.values_by_name[task["type"]].number
            if type_enum == TaskType.NORMAL_TASK:
                total_tasks += 1
            elif type_enum == TaskType.ACTOR_CREATION_TASK:
                total_actor_scheduled += 1
            elif type_enum == TaskType.ACTOR_TASK:
                total_actor_tasks += 1

        return TaskSummaries(
            summary=summary,
            total_tasks=total_tasks,
            total_actor_tasks=total_actor_tasks,
            total_actor_scheduled=total_actor_scheduled,
        )


@dataclass(init=True)
class ActorSummaryPerClass:
    #: The class name of the actor.
    class_name: str
    #: State name to the count dict. State name is equivalent to
    #: the protobuf ActorState.
    state_counts: Dict[TypeActorStatus, int] = field(default_factory=dict)


@dataclass
class ActorSummaries:
    #: Group key (actor class name) -> summary
    summary: Dict[str, ActorSummaryPerClass]
    #: Total number of actors
    total_actors: int
    summary_by: str = "class"

    @classmethod
    def to_summary(cls, *, actors: List[Dict]):
        # NOTE: The argument tasks contains a list of dictionary
        # that have the same k/v as ActorState.
        summary = {}
        total_actors = 0

        for actor in actors:
            key = actor["class_name"]
            if key not in summary:
                summary[key] = ActorSummaryPerClass(
                    class_name=actor["class_name"],
                )
            actor_summary = summary[key]

            state = actor["state"]
            if state not in actor_summary.state_counts:
                actor_summary.state_counts[state] = 0
            actor_summary.state_counts[state] += 1

            total_actors += 1

        return ActorSummaries(
            summary=summary,
            total_actors=total_actors,
        )


@dataclass(init=True)
class ObjectSummaryPerKey:
    #: Total number of objects of the type.
    total_objects: int
    #: Total size in mb.
    total_size_mb: float
    #: Total number of workers that reference the type of objects.
    total_num_workers: int
    #: Total number of nodes that reference the type of objects.
    total_num_nodes: int
    #: State name to the count dict. State name is equivalent to
    #: ObjectState.
    task_state_counts: Dict[TypeTaskStatus, int] = field(default_factory=dict)
    #: Ref count type to the count dict. State name is equivalent to
    #: ObjectState.
    ref_type_counts: Dict[TypeReferenceType, int] = field(default_factory=dict)


@dataclass
class ObjectSummaries:
    #: Group key (actor class name) -> summary
    summary: Dict[str, ObjectSummaryPerKey]
    #: Total number of referenced objects in the cluster.
    total_objects: int
    #: Total size of referenced objects in the cluster in MB.
    total_size_mb: float
    #: Whether or not the callsite collection is enabled.
    callsite_enabled: bool
    summary_by: str = "callsite"

    @classmethod
    def to_summary(cls, *, objects: List[Dict]):
        # NOTE: The argument tasks contains a list of dictionary
        # that have the same k/v as ObjectState.
        summary = {}
        total_objects = 0
        total_size_mb = 0
        key_to_workers = {}
        key_to_nodes = {}
        callsite_enabled = True

        for object in objects:
            key = object["call_site"]
            if key == "disabled":
                callsite_enabled = False
            if key not in summary:
                summary[key] = ObjectSummaryPerKey(
                    total_objects=0,
                    total_size_mb=0,
                    total_num_workers=0,
                    total_num_nodes=0,
                )
                key_to_workers[key] = set()
                key_to_nodes[key] = set()

            object_summary = summary[key]

            task_state = object["task_status"]
            if task_state not in object_summary.task_state_counts:
                object_summary.task_state_counts[task_state] = 0
            object_summary.task_state_counts[task_state] += 1

            ref_type = object["reference_type"]
            if ref_type not in object_summary.ref_type_counts:
                object_summary.ref_type_counts[ref_type] = 0
            object_summary.ref_type_counts[ref_type] += 1
            object_summary.total_objects += 1
            total_objects += 1

            size_bytes = object["object_size"]
            # object_size's unit is byte by default. It is -1, if the size is
            # unknown.
            if size_bytes != -1:
                object_summary.total_size_mb += size_bytes / 1024**2
                total_size_mb += size_bytes / 1024**2

            key_to_workers[key].add(object["pid"])
            key_to_nodes[key].add(object["ip"])

        # Convert set of pid & node ips to length.
        for key, workers in key_to_workers.items():
            summary[key].total_num_workers = len(workers)
        for key, nodes in key_to_nodes.items():
            summary[key].total_num_nodes = len(nodes)

        return ObjectSummaries(
            summary=summary,
            total_objects=total_objects,
            total_size_mb=total_size_mb,
            callsite_enabled=callsite_enabled,
        )


@dataclass(init=True)
class StateSummary:
    #: Node ID -> summary per node
    #: If the data is not required to be orgnized per node, it will contain
    #: a single key, "cluster".
    node_id_to_summary: Dict[str, Union[TaskSummaries, ActorSummaries, ObjectSummaries]]


@dataclass(init=True)
class SummaryApiResponse:
    # Carried over from ListApiResponse
    # We currently use list API for listing the resources
    total: int
    # Carried over from ListApiResponse
    # Number of resources returned by data sources after truncation
    num_after_truncation: int
    # Number of resources after filtering
    num_filtered: int
    result: StateSummary = None
    partial_failure_warning: str = ""
    # A list of warnings to print.
    warnings: Optional[List[str]] = None


def resource_to_schema(resource: StateResource) -> StateSchema:
    if resource == StateResource.ACTORS:
        return ActorState
    elif resource == StateResource.JOBS:
        return JobState
    elif resource == StateResource.NODES:
        return NodeState
    elif resource == StateResource.OBJECTS:
        return ObjectState
    elif resource == StateResource.PLACEMENT_GROUPS:
        return PlacementGroupState
    elif resource == StateResource.RUNTIME_ENVS:
        return RuntimeEnvState
    elif resource == StateResource.TASKS:
        return TaskState
    elif resource == StateResource.WORKERS:
        return WorkerState
    elif resource == StateResource.CLUSTER_EVENTS:
        return ClusterEventState
    else:
        assert False, "Unreachable"
