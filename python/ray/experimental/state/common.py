import logging
from abc import ABC
from dataclasses import dataclass, field, fields
from enum import Enum, unique
from typing import Dict, List, Optional, Set, Tuple, Union

from ray.core.generated.common_pb2 import TaskType
from ray.dashboard.modules.job.common import JobInfo

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30
DEFAULT_LIMIT = 100
DEFAULT_LOG_LIMIT = 1000
MAX_LIMIT = 10000

STATE_OBS_ALPHA_FEEDBACK_MSG = [
    "\n==========ALPHA PREVIEW, FEEDBACK NEEDED ===============",
    "State Observability APIs is currently in Alpha-Preview. ",
    "If you have any feedback, you could do so at either way as below:",
    "  1. Report bugs/issues with details: https://forms.gle/gh77mwjEskjhN8G46",
    "  2. Follow up in #ray-state-observability-dogfooding slack channel of Ray: "
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
        # To return the data to users, when there's a partial failure
        # we need to have a timeout that's smaller than the users' timeout.
        # 80% is configured arbitrarily.
        self.timeout = int(self.timeout * self._server_timeout_multiplier)
        assert self.timeout != 0, "0 second timeout is not supported."
        if self.filters is None:
            self.filters = []

        if self.limit > MAX_LIMIT:
            raise ValueError(
                f"Given limit {self.limit} exceeds the supported "
                f"limit {MAX_LIMIT}. Use a lower limit."
            )

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


def state_column(*, filterable, detail=False, **kwargs):
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
    def columns(cls) -> Set[str]:
        """Return a list of all columns"""
        cols = set()
        for f in fields(cls):
            cols.add(f.name)
        return cols

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
        detail = set()
        for f in fields(cls):
            if not f.metadata.get("detail", False):
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
                "node_id or node_ip should be provided."
                "Please provide at least one of them."
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


# TODO(sang): Replace it with Pydantic or gRPC schema (once interface is finalized).
@dataclass(init=True)
class ActorState(StateSchema):
    actor_id: str = state_column(filterable=True)
    state: str = state_column(filterable=True)
    class_name: str = state_column(filterable=True)
    name: str = state_column(filterable=True)
    pid: int = state_column(filterable=True)
    serialized_runtime_env: str = state_column(filterable=False, detail=True)
    resource_mapping: dict = state_column(filterable=False, detail=True)
    death_cause: dict = state_column(filterable=False, detail=True)
    is_detached: bool = state_column(filterable=False, detail=True)


@dataclass(init=True)
class PlacementGroupState(StateSchema):
    placement_group_id: str = state_column(filterable=True)
    state: str = state_column(filterable=True)
    name: str = state_column(filterable=True)
    bundles: dict = state_column(filterable=False, detail=True)
    is_detached: bool = state_column(filterable=True, detail=True)
    stats: dict = state_column(filterable=False, detail=True)


@dataclass(init=True)
class NodeState(StateSchema):
    node_id: str = state_column(filterable=True)
    node_ip: str = state_column(filterable=True)
    state: str = state_column(filterable=True)
    node_name: str = state_column(filterable=True)
    resources_total: dict = state_column(filterable=False)


class JobState(JobInfo, StateSchema):
    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"status", "entrypoint", "error_type"}


@dataclass(init=True)
class WorkerState(StateSchema):
    worker_id: str = state_column(filterable=True)
    is_alive: bool = state_column(filterable=True)
    worker_type: str = state_column(filterable=True)
    exit_type: str = state_column(filterable=True)
    node_id: str = state_column(filterable=True)
    ip: str = state_column(filterable=True)
    pid: str = state_column(filterable=True)
    exit_detail: str = state_column(detail=True, filterable=False)
    worker_info: dict = state_column(detail=True, filterable=False)


@dataclass(init=True)
class TaskState(StateSchema):
    task_id: str = state_column(filterable=True)
    name: str = state_column(filterable=True)
    scheduling_state: str = state_column(filterable=True)
    type: str = state_column(filterable=True)
    func_or_class_name: str = state_column(filterable=True)
    language: str = state_column(detail=True, filterable=True)
    required_resources: dict = state_column(detail=True, filterable=False)
    runtime_env_info: str = state_column(detail=True, filterable=False)


@dataclass(init=True)
class ObjectState(StateSchema):
    object_id: str = state_column(filterable=True)
    pid: int = state_column(filterable=True)
    ip: str = state_column(filterable=True)
    object_size: int = state_column(filterable=True)
    reference_type: str = state_column(filterable=True)
    call_site: str = state_column(filterable=True)
    task_status: str = state_column(filterable=True)
    type: str = state_column(filterable=True)


@dataclass(init=True)
class RuntimeEnvState(StateSchema):
    runtime_env: str = state_column(filterable=True)
    success: bool = state_column(filterable=True)
    node_id: str = state_column(filterable=True)
    creation_time_ms: float = state_column(filterable=False)
    ref_cnt: int = state_column(detail=True, filterable=False)
    error: str = state_column(detail=True, filterable=True)


@dataclass(init=True)
class ListApiResponse:
    # Total number of the resource from the cluster.
    # Note that this value can be larger than `result`
    # because `result` can be truncated.
    total: int
    # Returned data. None if no data is returned.
    result: List[
        Union[
            ActorState,
            PlacementGroupState,
            NodeState,
            JobInfo,
            WorkerState,
            TaskState,
            ObjectState,
            RuntimeEnvState,
        ]
    ] = None
    # List API can have a partial failure if queries to
    # all sources fail. For example, getting object states
    # require to ping all raylets, and it is possible some of
    # them fails. Note that it is impossible to guarantee high
    # availability of data because ray's state information is
    # not replicated.
    partial_failure_warning: str = ""


"""
Summary API schema
"""


@dataclass(init=True)
class TaskSummaryPerFuncOrClassName:
    # The function or class name of this task.
    func_or_class_name: str
    # The type of the class. Equivalent to protobuf TaskType.
    type: str
    # State name to the count dict. State name is equivalent to
    # the protobuf TaskStatus.
    state_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class TaskSummaries:
    # Group key -> summary
    # Right now, we only have func_class_name as a key.
    # TODO(sang): Support the task group abstraction.
    summary: Dict[str, TaskSummaryPerFuncOrClassName]
    # Total Ray tasks
    total_tasks: int
    # Total actor tasks
    total_actor_tasks: int
    # Total scheduling actors
    total_actor_scheduled: int
    summary_by: str = "func_name"

    @classmethod
    def to_summary(cls, *, tasks: List[Dict]):
        """
        NOTE: The argument tasks contains a list of dictionary
        that have the same k/v as TaskState.
        TODO(sang): Refactor this to use real dataclass.
        """
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
    # The class name of the actor.
    class_name: str
    # State name to the count dict. State name is equivalent to
    # the protobuf ActorState.
    state_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class ActorSummaries:
    # Group key (actor class name) -> summary
    summary: Dict[str, ActorSummaryPerClass]
    # Total number of actors
    total_actors: int
    summary_by: str = "class"

    @classmethod
    def to_summary(cls, *, actors: List[Dict]):
        """
        NOTE: The argument tasks contains a list of dictionary
        that have the same k/v as ActorState.
        TODO(sang): Refactor this to use real dataclass.
        """
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
    # Total number of objects of the type.
    total_objects: int
    # Total size in mb.
    total_size_mb: float
    # Total number of workers that reference the type of objects.
    total_num_workers: int
    # Total number of nodes that reference the type of objects.
    total_num_nodes: int
    # State name to the count dict. State name is equivalent to
    # ObjectState.
    task_state_counts: Dict[str, int] = field(default_factory=dict)
    # Ref count type to the count dict. State name is equivalent to
    # ObjectState.
    ref_type_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class ObjectSummaries:
    # Group key (actor class name) -> summary
    summary: Dict[str, ObjectSummaryPerKey]
    # Total number of referenced objects in the cluster.
    total_objects: int
    # Total size of referenced objects in the cluster in MB.
    total_size_mb: float
    # Whether or not the callsite collection is enabled.
    callsite_enabled: bool
    summary_by: str = "callsite"

    @classmethod
    def to_summary(cls, *, objects: List[Dict]):
        """
        NOTE: The argument tasks contains a list of dictionary
        that have the same k/v as ObjectState.
        TODO(sang): Refactor this to use real dataclass.
        """
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
                object_summary.total_size_mb += size_bytes / 1024 ** 2
                total_size_mb += size_bytes / 1024 ** 2

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
    # Node ID -> summary per node
    # If the data is not required to be orgnized per node, it will contain
    # a single key, "cluster".
    node_id_to_summary: Dict[str, Union[TaskSummaries, ActorSummaries, ObjectSummaries]]


@dataclass(init=True)
class SummaryApiResponse:
    result: StateSummary = None
    partial_failure_warning: str = ""
