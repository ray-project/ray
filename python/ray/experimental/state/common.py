import logging
from abc import ABC
from dataclasses import dataclass, fields, field
from enum import Enum, unique
from typing import List, Optional, Set, Tuple, Union, Dict

from ray.dashboard.modules.job.common import JobInfo
from ray.core.generated.common_pb2 import TaskType

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30
DEFAULT_LIMIT = 10000


def filter_fields(data: dict, state_dataclass) -> dict:
    """Filter the given data using keys from a given state dataclass."""
    filtered_data = {}
    for f in fields(state_dataclass):
        if f.name in data:
            filtered_data[f.name] = data[f.name]
    return filtered_data


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


@dataclass(init=True)
class ListApiOptions:
    limit: int = DEFAULT_LIMIT
    timeout: int = DEFAULT_RPC_TIMEOUT
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None
    # When the request is processed on the server side,
    # we should apply multiplier so that server side can finish
    # processing a request within timeout. Otherwise,
    # timeout will always lead Http timeout.
    _server_timeout_multiplier: float = 0.8

    # TODO(sang): Use Pydantic instead.
    def __post_init__(self):
        assert isinstance(self.limit, int)
        assert isinstance(self.timeout, int)
        # To return the data to users, when there's a partial failure
        # we need to have a timeout that's smaller than the users' timeout.
        # 80% is configured arbitrarily.
        self.timeout = int(self.timeout * self._server_timeout_multiplier)
        if self.filters is None:
            self.filters = []


@dataclass(init=True)
class SummaryApiOptions:
    timeout: int = DEFAULT_RPC_TIMEOUT


class StateSchema(ABC):
    @classmethod
    def filterable_columns(cls) -> Set[str]:
        """Return a set of columns that support filtering.

        NOTE: Currently, only bool, str, int, and float
            types are supported for filtering.
        TODO(sang): Filter on the source side instead for optimization.
        """
        raise NotImplementedError


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
    actor_id: str
    state: str
    class_name: str
    name: str
    pid: int
    serialized_runtime_env: str
    resource_mapping: dict
    death_cause: dict
    is_detached: bool

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"actor_id", "state", "class_name", "name", "pid"}


@dataclass(init=True)
class PlacementGroupState(StateSchema):
    placement_group_id: str
    state: str
    name: str
    bundles: dict
    is_detached: bool
    stats: dict

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"placement_group_id", "state", "name"}


@dataclass(init=True)
class NodeState(StateSchema):
    node_id: str
    node_ip: str
    state: str
    node_name: str
    resources_total: dict

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"node_id", "state", "node_ip", "node_name"}


class JobState(JobInfo, StateSchema):
    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"status", "entrypoint", "error_type"}


@dataclass(init=True)
class WorkerState(StateSchema):
    worker_id: str
    is_alive: str
    worker_type: str
    exit_type: str
    exit_detail: str
    pid: str
    worker_info: dict

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"worker_id", "is_alive", "worker_type", "exit_type", "pid"}


@dataclass(init=True)
class TaskState(StateSchema):
    task_id: str
    name: str
    scheduling_state: str
    type: str
    language: str
    func_or_class_name: str
    required_resources: dict
    runtime_env_info: str

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {"task_id", "name", "scheduling_state", "type", "func_or_class_name"}


@dataclass(init=True)
class ObjectState(StateSchema):
    object_id: str
    pid: int
    node_ip_address: str
    object_size: int
    reference_type: str
    call_site: str
    task_status: str
    type: str

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {
            "object_id",
            "node_ip_address",
            "reference_type",
            "task_status",
            "type",
            "pid",
            "call_site",
        }


@dataclass(init=True)
class RuntimeEnvState(StateSchema):
    runtime_env: str
    ref_cnt: int
    success: bool
    error: str
    creation_time_ms: float
    node_id: str

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        return {
            "node_id",
            "runtime_env",
            "success",
        }


@dataclass(init=True)
class ListApiResponse:
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
    # Total actor scheduling tasks
    total_actor_scheduling_tasks: int
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
        total_actor_scheduling_tasks = 0

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
                total_actor_scheduling_tasks += 1
            elif type_enum == TaskType.ACTOR_TASK:
                total_actor_tasks += 1

        return TaskSummaries(
            summary=summary,
            total_tasks=total_tasks,
            total_actor_tasks=total_actor_tasks,
            total_actor_scheduling_tasks=total_actor_scheduling_tasks,
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
            key_to_nodes[key].add(object["node_ip_address"])

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
