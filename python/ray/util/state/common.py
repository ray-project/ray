import datetime
import json
import logging
import sys
from abc import ABC
from dataclasses import asdict, field, fields
from enum import Enum, unique
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import env_integer
from ray.core.generated.common_pb2 import TaskStatus, TaskType
from ray.core.generated.gcs_pb2 import TaskEvents
from ray.util.state.custom_types import (
    TypeActorStatus,
    TypeNodeStatus,
    TypePlacementGroupStatus,
    TypeReferenceType,
    TypeTaskStatus,
    TypeTaskType,
    TypeWorkerExitType,
    TypeWorkerType,
)
from ray.util.state.exception import RayStateApiException
from ray.dashboard.modules.job.pydantic_models import JobDetails

# TODO(aguo): Instead of a version check, modify the below models
# to use pydantic BaseModel instead of dataclass.
# In pydantic 2, dataclass no longer needs the `init=True` kwarg to
# generate an __init__ method. Additionally, it will raise an error if
# it detects `init=True` to be set.
from ray._private.pydantic_compat import IS_PYDANTIC_2

try:
    from pydantic.dataclasses import dataclass


except ImportError:
    # pydantic is not available in the dashboard.
    # We will use the dataclass from the standard library.
    from dataclasses import dataclass


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


class Humanify:
    """A class containing default methods to
    convert units into a human readable string."""

    def timestamp(x: float):
        """Converts milliseconds to a datetime object."""
        return str(datetime.datetime.fromtimestamp(x / 1000))

    def memory(x: int):
        """Converts raw bytes to a human readable memory size."""
        if x >= 2**30:
            return str(format(x / (2**30), ".3f")) + " GiB"
        elif x >= 2**20:
            return str(format(x / (2**20), ".3f")) + " MiB"
        elif x >= 2**10:
            return str(format(x / (2**10), ".3f")) + " KiB"
        return str(format(x, ".3f")) + " B"

    def duration(x: int):
        """Converts milliseconds to a human readable duration."""
        return str(datetime.timedelta(milliseconds=x))

    def events(events: List[dict]):
        """Converts a list of task events into a human readable format."""
        for event in events:
            if "created_ms" in event:
                event["created_ms"] = Humanify.timestamp(event["created_ms"])
        return events

    def node_resources(resources: dict):
        """Converts a node's resources into a human readable format."""
        for resource in resources:
            if "memory" in resource:
                resources[resource] = Humanify.memory(resources[resource])
        return resources


@dataclass(init=not IS_PYDANTIC_2)
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
    # [only tasks] If driver tasks should be excluded.
    exclude_driver: bool = True
    # When the request is processed on the server side,
    # we should apply multiplier so that server side can finish
    # processing a request within timeout. Otherwise,
    # timeout will always lead Http timeout.
    server_timeout_multiplier: float = 0.8

    def __post_init__(self):
        # To return the data to users, when there's a partial failure
        # we need to have a timeout that's smaller than the users' timeout.
        # 80% is configured arbitrarily.
        self.timeout = int(self.timeout * self.server_timeout_multiplier)
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


@dataclass(init=not IS_PYDANTIC_2)
class GetApiOptions:
    # Timeout for the HTTP request
    timeout: int = DEFAULT_RPC_TIMEOUT


@dataclass(init=not IS_PYDANTIC_2)
class SummaryApiOptions:
    # Timeout for the HTTP request
    timeout: int = DEFAULT_RPC_TIMEOUT

    # Filters. Each tuple pair (key, predicate, value) means key predicate value.
    # If there's more than 1 filter, it means AND.
    # E.g., [(key, "=", val), (key2, "!=" val2)] means (key=val) AND (key2!=val2)
    # For summary endpoints that call list under the hood, we'll pass
    # these filters directly into the list call.
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = field(
        default_factory=list
    )

    # Change out to summarize the output. There is a summary_by value for each entity.
    # Tasks: by func_name
    # Actors: by class
    # Objects: by callsite
    summary_by: Optional[str] = None


def state_column(*, filterable: bool, detail: bool = False, format_fn=None, **kwargs):
    """A wrapper around dataclass.field to add additional metadata.

    The metadata is used to define detail / filterable option of
    each column.

    Args:
        detail: If True, the column is used when detail == True
        filterable: If True, the column can be used for filtering.
        kwargs: The same kwargs for the `dataclasses.field` function.
    """
    m = {"detail": detail, "filterable": filterable, "format_fn": format_fn}
    # Default for detail field is None since it could be missing.
    if detail and "default" not in kwargs:
        kwargs["default"] = None

    if "metadata" in kwargs:
        # Metadata explicitly specified, so add detail and filterable if missing.
        kwargs["metadata"].update(m)
    else:
        # Metadata not explicitly specified, so add it.
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

    In addition, the schema also provides a humanify abstract method to
    convert the state object into something human readable, ready for printing.

    Subclasses should override this method, providing logic to convert its own fields
    to something human readable, packaged and returned in a dict.

    Each field that wants to be humanified should include a 'format_fn' key in its
    metadata dictionary.
    """

    @classmethod
    def humanify(cls, state: dict) -> dict:
        """Convert the given state object into something human readable."""
        for f in fields(cls):
            if (
                f.metadata.get("format_fn") is not None
                and f.name in state
                and state[f.name] is not None
            ):
                try:
                    state[f.name] = f.metadata["format_fn"](state[f.name])
                except Exception as e:
                    logger.error(f"Failed to format {f.name}:{state[f.name]} with {e}")
        return state

    @classmethod
    def list_columns(cls, detail: bool = True) -> List[str]:
        """Return a list of columns."""
        cols = []
        for f in fields(cls):
            if detail:
                cols.append(f.name)
            elif not f.metadata.get("detail", False):
                cols.append(f.name)

        return cols

    @classmethod
    def columns(cls) -> Set[str]:
        """Return a set of all columns."""
        return set(cls.list_columns(detail=True))

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
        return set(cls.list_columns(detail=False))

    @classmethod
    def detail_columns(cls) -> Set[str]:
        """Return a list of detail columns.

        Detail columns mean columns to return when detail == True.
        """
        return set(cls.list_columns(detail=True))

    def asdict(self):
        return asdict(self)

    # Allow dict like access on the class directly for backward compatibility.
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)


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


@dataclass(init=not IS_PYDANTIC_2)
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
    # The task id of the log.
    task_id: Optional[str] = None
    # The attempt number of the task.
    attempt_number: int = 0
    # The pid of the log. It is used only for worker logs.
    pid: Optional[int] = None
    # Total log lines to return.
    lines: int = 1000
    # The interval where new logs are streamed to.
    # Should be used only when media_type == stream.
    interval: Optional[float] = None
    # The suffix of the log file if file resolution not through filename directly.
    # Default to "out".
    suffix: str = "out"
    # The job submission id for submission job. This doesn't work for driver job
    # since Ray doesn't log driver logs to file in the ray logs directory.
    submission_id: Optional[str] = None

    def __post_init__(self):
        if self.pid:
            self.pid = int(self.pid)
        if self.interval:
            self.interval = float(self.interval)
        self.lines = int(self.lines)

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
        if not (
            self.actor_id
            or self.task_id
            or self.pid
            or self.filename
            or self.submission_id
        ):
            raise ValueError(
                "None of actor_id, task_id, pid, submission_id or filename "
                "is provided. At least one of them is required to fetch logs."
            )

        if self.suffix not in ["out", "err"]:
            raise ValueError(
                f"Invalid suffix: {self.suffix}. Must be one of 'out' or 'err'."
            )


# See the ActorTableData message in gcs.proto for all potential options that
# can be included in this class.
@dataclass(init=not IS_PYDANTIC_2)
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
    node_id: Optional[str] = state_column(filterable=True)
    #: The pid of the actor. 0 if it is not created yet.
    pid: Optional[int] = state_column(filterable=True)
    #: The namespace of the actor.
    ray_namespace: Optional[str] = state_column(filterable=True)
    #: The runtime environment information of the actor.
    serialized_runtime_env: Optional[str] = state_column(filterable=False, detail=True)
    #: The resource requirement of the actor.
    required_resources: Optional[dict] = state_column(filterable=False, detail=True)
    #: Actor's death information in detail. None if the actor is not dead yet.
    death_cause: Optional[dict] = state_column(filterable=False, detail=True)
    #: True if the actor is detached. False otherwise.
    is_detached: Optional[bool] = state_column(filterable=False, detail=True)
    #: The placement group id that's associated with this actor.
    placement_group_id: Optional[str] = state_column(detail=True, filterable=True)
    #: Actor's repr name if a customized __repr__ method exists, else empty string.
    repr_name: Optional[str] = state_column(detail=True, filterable=True)
    #: Number of restarts that has been tried on this actor.
    num_restarts: int = state_column(filterable=False, detail=True)
    #: Number of times this actor is restarted due to lineage reconstructions.
    num_restarts_due_to_lineage_reconstruction: int = state_column(
        filterable=False, detail=True
    )


@dataclass(init=not IS_PYDANTIC_2)
class PlacementGroupState(StateSchema):
    """PlacementGroup State"""

    #: The id of the placement group.
    placement_group_id: str = state_column(filterable=True)
    #: The name of the placement group if it is given by the name argument.
    name: str = state_column(filterable=True)
    #: The job id of the placement group.
    creator_job_id: str = state_column(filterable=True)
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
    bundles: Optional[List[dict]] = state_column(filterable=False, detail=True)
    #: True if the placement group is detached. False otherwise.
    is_detached: Optional[bool] = state_column(filterable=True, detail=True)
    #: The scheduling stats of the placement group.
    stats: Optional[dict] = state_column(filterable=False, detail=True)


@dataclass(init=not IS_PYDANTIC_2)
class NodeState(StateSchema):
    """Node State"""

    #: The id of the node.
    node_id: str = state_column(filterable=True)
    #: The ip address of the node.
    node_ip: str = state_column(filterable=True)
    #: If this is a head node.
    is_head_node: bool = state_column(filterable=True)
    #: The state of the node.
    #:
    #: ALIVE: The node is alive.
    #: DEAD: The node is dead.
    state: TypeNodeStatus = state_column(filterable=True)
    #: The state message of the node.
    #: This provides more detailed information about the node's state.
    state_message: Optional[str] = state_column(filterable=False)
    #: The name of the node if it is given by the name argument.
    node_name: str = state_column(filterable=True)
    #: The total resources of the node.
    resources_total: dict = state_column(
        filterable=False, format_fn=Humanify.node_resources
    )
    #: The labels of the node.
    labels: dict = state_column(filterable=False)
    #: The time when the node (raylet) starts.
    start_time_ms: Optional[int] = state_column(
        filterable=False, detail=True, format_fn=Humanify.timestamp
    )
    #: The time when the node exits. The timestamp could be delayed
    #: if the node is dead unexpectedly (could be delayed
    # up to 30 seconds).
    end_time_ms: Optional[int] = state_column(
        filterable=False, detail=True, format_fn=Humanify.timestamp
    )


# NOTE: Declaring this as dataclass would make __init__ not being called properly.
# NOTE: `JobDetails` will be `None` in the minimal install because Pydantic is not
#       installed. Inheriting from `None` raises an exception.
class JobState(StateSchema, JobDetails if JobDetails is not None else object):
    """The state of the job that's submitted by Ray's Job APIs or driver jobs"""

    def __init__(self, **kwargs):
        JobDetails.__init__(self, **kwargs)

    @classmethod
    def filterable_columns(cls) -> Set[str]:
        # We are not doing any filtering since filtering is currently done
        # at the backend.
        return {"job_id", "type", "status", "submission_id"}

    @classmethod
    def humanify(cls, state: dict) -> dict:
        return state

    @classmethod
    def list_columns(cls, detail: bool = True) -> List[str]:
        if not detail:
            return [
                "job_id",
                "submission_id",
                "entrypoint",
                "type",
                "status",
                "message",
                "error_type",
                "driver_info",
            ]
        if JobDetails is None:
            # We don't have pydantic in the dashboard. This is because
            # we call this method at module import time, so we need to
            # check if the class is a pydantic model.
            return []

        # TODO(aguo): Once we only support pydantic 2, we can remove this if check.
        # In pydantic 2.0, `__fields__` has been renamed to `model_fields`.
        return (
            list(JobDetails.model_fields.keys())
            if hasattr(JobDetails, "model_fields")
            else list(JobDetails.__fields__.keys())
        )

    def asdict(self):
        return JobDetails.dict(self)

    @classmethod
    def schema_dict(cls) -> Dict[str, Any]:
        schema_types = cls.schema()["properties"]
        # Get type name to actual type mapping.
        return {
            k: v["type"] for k, v in schema_types.items() if v.get("type") is not None
        }


@dataclass(init=not IS_PYDANTIC_2)
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
    pid: int = state_column(filterable=True)
    #: The exit detail of the worker if the worker is dead.
    exit_detail: Optional[str] = state_column(detail=True, filterable=False)
    #: The time worker is first launched.
    #: -1 if the value doesn't exist.
    #: The lifecycle of worker is as follow.
    #: worker_launch_time_ms (process startup requested).
    #: -> worker_launched_time_ms (process started).
    #: -> start_time_ms (worker is ready to be used).
    #: -> end_time_ms (worker is destroyed).
    worker_launch_time_ms: Optional[int] = state_column(
        filterable=False,
        detail=True,
        format_fn=lambda x: "" if x == -1 else Humanify.timestamp(x),
    )
    #: The time worker is succesfully launched
    #: -1 if the value doesn't exist.
    worker_launched_time_ms: Optional[int] = state_column(
        filterable=False,
        detail=True,
        format_fn=lambda x: "" if x == -1 else Humanify.timestamp(x),
    )
    #: The time when the worker is started and initialized.
    #: 0 if the value doesn't exist.
    start_time_ms: Optional[int] = state_column(
        filterable=False, detail=True, format_fn=Humanify.timestamp
    )
    #: The time when the worker exits. The timestamp could be delayed
    #: if the worker is dead unexpectedly.
    #: 0 if the value doesn't exist.
    end_time_ms: Optional[int] = state_column(
        filterable=False, detail=True, format_fn=Humanify.timestamp
    )
    # the debugger port of the worker
    debugger_port: Optional[int] = state_column(filterable=True, detail=True)
    # the number of threads paused in this worker
    num_paused_threads: Optional[int] = state_column(filterable=True, detail=True)


@dataclass(init=not IS_PYDANTIC_2)
class ClusterEventState(StateSchema):
    severity: str = state_column(filterable=True)
    time: str = state_column(filterable=False)
    source_type: str = state_column(filterable=True)
    message: str = state_column(filterable=False)
    event_id: str = state_column(filterable=True)
    custom_fields: Optional[dict] = state_column(filterable=False, detail=True)


@dataclass(init=not IS_PYDANTIC_2)
class TaskState(StateSchema):
    """Task State"""

    #: The id of the task.
    task_id: str = state_column(filterable=True)
    #: The attempt (retry) number of the task.
    attempt_number: int = state_column(filterable=True)
    #: The name of the task if it is given by the name argument.
    name: str = state_column(filterable=True)
    #: The state of the task.
    #:
    #: Refer to src/ray/protobuf/common.proto for a detailed explanation of the state
    #: breakdowns and typical state transition flow.
    #:
    state: TypeTaskStatus = state_column(filterable=True)
    #: The job id of this task.
    job_id: str = state_column(filterable=True)
    #: The actor id that's associated with this task.
    #: It is empty if there's no relevant actors.
    actor_id: Optional[str] = state_column(filterable=True)
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
    #: The parent task id. If the parent is a normal task, it will be the task's id.
    #: If the parent runs in a concurrent actor (async actor or threaded actor),
    #: it will be the actor's creation task id.
    parent_task_id: str = state_column(filterable=True)
    #: Id of the node that runs the task. If the task is retried, it could
    #: contain the node id of the previous executed task.
    #: If empty, it means the task hasn't been scheduled yet.
    node_id: Optional[str] = state_column(filterable=True)
    #: The worker id that's associated with this task.
    worker_id: Optional[str] = state_column(filterable=True)
    #: The worker's pid that's associated with this task.
    worker_pid: Optional[int] = state_column(filterable=True)
    #: Task error type.
    error_type: Optional[str] = state_column(filterable=True)
    #: The language of the task. E.g., Python, Java, or Cpp.
    language: Optional[str] = state_column(detail=True, filterable=True)
    #: The required resources to execute the task.
    required_resources: Optional[dict] = state_column(detail=True, filterable=False)
    #: The runtime environment information for the task.
    runtime_env_info: Optional[dict] = state_column(detail=True, filterable=False)
    #: The placement group id that's associated with this task.
    placement_group_id: Optional[str] = state_column(detail=True, filterable=True)
    #: The list of events of the given task.
    #: Refer to src/ray/protobuf/common.proto for a detailed explanation of the state
    #: breakdowns and typical state transition flow.
    events: Optional[List[dict]] = state_column(
        detail=True, filterable=False, format_fn=Humanify.events
    )
    #: The list of profile events of the given task.
    profiling_data: Optional[dict] = state_column(detail=True, filterable=False)
    #: The time when the task is created. A Unix timestamp in ms.
    creation_time_ms: Optional[int] = state_column(
        detail=True,
        filterable=False,
        format_fn=Humanify.timestamp,
    )
    #: The time when the task starts to run. A Unix timestamp in ms.
    start_time_ms: Optional[int] = state_column(
        detail=True,
        filterable=False,
        format_fn=Humanify.timestamp,
    )
    #: The time when the task is finished or failed. A Unix timestamp in ms.
    end_time_ms: Optional[int] = state_column(
        detail=True, filterable=False, format_fn=Humanify.timestamp
    )
    #: The task logs info, e.g. offset into the worker log file when the task
    #: starts/finishes.
    #: None if the task is from a concurrent actor (e.g. async actor or threaded actor)
    task_log_info: Optional[dict] = state_column(detail=True, filterable=False)
    #: Task error detail info.
    error_message: Optional[str] = state_column(detail=True, filterable=False)
    # Is task paused by the debugger
    is_debugger_paused: Optional[bool] = state_column(detail=True, filterable=True)


@dataclass(init=not IS_PYDANTIC_2)
class ObjectState(StateSchema):
    """Object State"""

    #: The id of the object.
    object_id: str = state_column(filterable=True)
    #: The size of the object in mb.
    object_size: int = state_column(filterable=True, format_fn=Humanify.memory)
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
    #: The number of times the task has been executed (including the current execution)
    attempt_number: int = state_column(filterable=True)
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


@dataclass(init=not IS_PYDANTIC_2)
class RuntimeEnvState(StateSchema):
    """Runtime Environment State"""

    #: The runtime environment spec.
    runtime_env: dict = state_column(filterable=True)
    #: Whether or not the runtime env creation has succeeded.
    success: bool = state_column(filterable=True)
    #: The latency of creating the runtime environment.
    #: Available if the runtime env is successfully created.
    creation_time_ms: Optional[float] = state_column(
        filterable=False, format_fn=Humanify.timestamp
    )
    #: The node id of this runtime environment.
    node_id: str = state_column(filterable=True)
    #: The number of actors and tasks that use this runtime environment.
    ref_cnt: Optional[int] = state_column(detail=True, filterable=False)
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


@dataclass(init=not IS_PYDANTIC_2)
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
    result: List[Dict]
    # List API can have a partial failure if queries to
    # all sources fail. For example, getting object states
    # require to ping all raylets, and it is possible some of
    # them fails. Note that it is impossible to guarantee high
    # availability of data because ray's state information is
    # not replicated.
    partial_failure_warning: Optional[str] = ""
    # A list of warnings to print.
    warnings: Optional[List[str]] = None


"""
Summary API schema
"""

DRIVER_TASK_ID_PREFIX = "ffffffffffffffffffffffffffffffffffffffff"


@dataclass(init=not IS_PYDANTIC_2)
class TaskSummaryPerFuncOrClassName:
    #: The function or class name of this task.
    func_or_class_name: str
    #: The type of the class. Equivalent to protobuf TaskType.
    type: str
    #: State name to the count dict. State name is equivalent to
    #: the protobuf TaskStatus.
    state_counts: Dict[TypeTaskStatus, int] = field(default_factory=dict)


@dataclass
class Link:
    #: The type of entity to link to
    type: str
    #: The id of the entity to link to
    id: str


@dataclass(init=not IS_PYDANTIC_2)
class NestedTaskSummary:
    #: The name of this task group
    name: str
    #: A unique identifier for this group
    key: str
    #: The type of the class. Equivalent to protobuf TaskType,
    #: "ACTOR" if it represents an Actor, or "GROUP" if it's a grouping of tasks.
    type: str
    #: Unix timestamp to use to sort the task group.
    timestamp: Optional[int] = None
    #: State name to the count dict. State name is equivalent to
    #: the protobuf TaskStatus.
    state_counts: Dict[TypeTaskStatus, int] = field(default_factory=dict)
    #: The child
    children: List["NestedTaskSummary"] = field(default_factory=list)
    #: A link to more details about this summary.
    link: Optional[Link] = None


@dataclass
class TaskSummaries:
    #: Group key -> summary.
    #: Right now, we only have func_class_name as a key.
    # TODO(sang): Support the task group abstraction.
    summary: Union[Dict[str, TaskSummaryPerFuncOrClassName], List[NestedTaskSummary]]
    #: Total Ray tasks.
    total_tasks: int
    #: Total actor tasks.
    total_actor_tasks: int
    #: Total scheduled actors.
    total_actor_scheduled: int
    summary_by: str = "func_name"

    @classmethod
    def to_summary_by_func_name(cls, *, tasks: List[Dict]) -> "TaskSummaries":
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

            state = task["state"]
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
            summary_by="func_name",
        )

    @classmethod
    def to_summary_by_lineage(
        cls, *, tasks: List[Dict], actors: List[Dict]
    ) -> "TaskSummaries":
        """
        This summarizes tasks by lineage.
        i.e. A task will be grouped with another task if they have the
        same parent.

        This does things in 4 steps.
        Step 1: Iterate through all tasks and keep track of them by id and ownership
        Step 2: Put the tasks in a tree structure based on ownership
        Step 3: Merge together siblings in the tree if there are more
        than one with the same name.
        Step 4: Sort by running and then errored and then successful tasks
        Step 5: Total the children

        This can probably be more efficient if we merge together some steps to
        reduce the amount of iterations but this algorithm produces very easy to
        understand code. We can optimize in the future.
        """
        # NOTE: The argument tasks contains a list of dictionary
        # that have the same k/v as TaskState.

        tasks_by_id = {}
        task_group_by_id = {}
        actor_creation_task_id_for_actor_id = {}
        summary = []
        total_tasks = 0
        total_actor_tasks = 0
        total_actor_scheduled = 0

        # Step 1
        # We cannot assume that a parent task always comes before the child task
        # So we need to keep track of all tasks by ids so we can quickly find the
        # parent.
        # We also track the actor creation tasks so we can quickly figure out the
        # ownership of actors.
        for task in tasks:
            tasks_by_id[task["task_id"]] = task
            type_enum = TaskType.DESCRIPTOR.values_by_name[task["type"]].number
            if type_enum == TaskType.ACTOR_CREATION_TASK:
                actor_creation_task_id_for_actor_id[task["actor_id"]] = task["task_id"]

        actor_dict = {actor["actor_id"]: actor for actor in actors}

        def get_or_create_task_group(task_id: str) -> Optional[NestedTaskSummary]:
            """
            Gets an already created task_group
            OR
            Creates a task group and puts it in the right place under its parent.
            For actor tasks, the parent is the Actor that owns it. For all other
            tasks, the owner is the driver or task that created it.

            Returns None if there is missing data about the task or one of its parents.

            For task groups that represents actors, the id is in the
            format actor:{actor_id}
            """
            if task_id in task_group_by_id:
                return task_group_by_id[task_id]

            task = tasks_by_id.get(task_id)
            if not task:
                logger.debug(f"We're missing data about {task_id}")
                # We're missing data about this parent. So we're dropping the whole
                # tree at that node.
                return None

            # Use name first which allows users to customize the name of
            # their remote function call using the name option.
            func_name = task["name"] or task["func_or_class_name"]
            task_id = task["task_id"]
            type_enum = TaskType.DESCRIPTOR.values_by_name[task["type"]].number

            task_group_by_id[task_id] = NestedTaskSummary(
                name=func_name,
                key=task_id,
                type=task["type"],
                timestamp=task["creation_time_ms"],
                link=Link(type="task", id=task_id),
            )

            # Set summary in right place under parent
            if (
                type_enum == TaskType.ACTOR_TASK
                or type_enum == TaskType.ACTOR_CREATION_TASK
            ):
                # For actor tasks, the parent is the actor and not the parent task.
                parent_task_group = get_or_create_actor_task_group(task["actor_id"])
                if parent_task_group:
                    parent_task_group.children.append(task_group_by_id[task_id])
            else:
                parent_task_id = task["parent_task_id"]
                if not parent_task_id or parent_task_id.startswith(
                    DRIVER_TASK_ID_PREFIX
                ):
                    summary.append(task_group_by_id[task_id])
                else:
                    parent_task_group = get_or_create_task_group(parent_task_id)
                    if parent_task_group:
                        parent_task_group.children.append(task_group_by_id[task_id])

            return task_group_by_id[task_id]

        def get_or_create_actor_task_group(
            actor_id: str,
        ) -> Optional[NestedTaskSummary]:
            """
            Gets an existing task group that represents an actor.
            OR
            Creates a task group that represents an actor. The owner of the actor is
            the parent of the creation_task that created that actor.

            Returns None if there is missing data about the actor or one of its parents.
            """
            key = f"actor:{actor_id}"
            actor = actor_dict.get(actor_id)
            if key not in task_group_by_id:
                creation_task_id = actor_creation_task_id_for_actor_id.get(actor_id)
                creation_task = tasks_by_id.get(creation_task_id)

                if not creation_task:
                    logger.debug(f"We're missing data about actor {actor_id}")
                    # We're missing data about the parent. So we're dropping the whole
                    # tree at that node.
                    return None

                # TODO(rickyx)
                # We are using repr name for grouping actors if exists,
                # else use class name. We should be using some group_name in the future.
                if actor is None:
                    logger.debug(
                        f"We are missing actor info for actor {actor_id}, "
                        f"even though creation task exists: {creation_task}"
                    )
                    [actor_name, *rest] = creation_task["func_or_class_name"].split(".")
                else:
                    actor_name = (
                        actor["repr_name"]
                        if actor["repr_name"]
                        else actor["class_name"]
                    )

                task_group_by_id[key] = NestedTaskSummary(
                    name=actor_name,
                    key=key,
                    type="ACTOR",
                    timestamp=task["creation_time_ms"],
                    link=Link(type="actor", id=actor_id),
                )

                parent_task_id = creation_task["parent_task_id"]
                if not parent_task_id or parent_task_id.startswith(
                    DRIVER_TASK_ID_PREFIX
                ):
                    summary.append(task_group_by_id[key])
                else:
                    parent_task_group = get_or_create_task_group(parent_task_id)
                    if parent_task_group:
                        parent_task_group.children.append(task_group_by_id[key])

            return task_group_by_id[key]

        # Step 2: Create the tree structure based on ownership
        for task in tasks:
            task_id = task["task_id"]

            task_group = get_or_create_task_group(task_id)

            if not task_group:
                # We are probably missing data about this task or one of its parents.
                continue

            state = task["state"]
            if state not in task_group.state_counts:
                task_group.state_counts[state] = 0
            task_group.state_counts[state] += 1

            type_enum = TaskType.DESCRIPTOR.values_by_name[task["type"]].number
            if type_enum == TaskType.NORMAL_TASK:
                total_tasks += 1
            elif type_enum == TaskType.ACTOR_CREATION_TASK:
                total_actor_scheduled += 1
            elif type_enum == TaskType.ACTOR_TASK:
                total_actor_tasks += 1

        def merge_sibings_for_task_group(
            siblings: List[NestedTaskSummary],
        ) -> Tuple[List[NestedTaskSummary], Optional[int]]:
            """
            Merges task summaries with the same name into a group if there are more than
            one child with that name.

            Args:
                siblings: A list of NestedTaskSummary's to merge together

            Returns
                Index 0: A list of NestedTaskSummary's which have been merged
                Index 1: The smallest timestamp amongst the siblings
            """
            if not len(siblings):
                return siblings, None

            # Group by name
            groups = {}
            min_timestamp = None

            for child in siblings:
                child.children, child_min_timestamp = merge_sibings_for_task_group(
                    child.children
                )
                if child_min_timestamp and child_min_timestamp < (
                    child.timestamp or sys.maxsize
                ):
                    child.timestamp = child_min_timestamp

                if child.name not in groups:
                    groups[child.name] = NestedTaskSummary(
                        name=child.name,
                        key=child.name,
                        type="GROUP",
                    )
                groups[child.name].children.append(child)
                if child.timestamp and child.timestamp < (
                    groups[child.name].timestamp or sys.maxsize
                ):
                    groups[child.name].timestamp = child.timestamp
                    if child.timestamp < (min_timestamp or sys.maxsize):
                        min_timestamp = child.timestamp

            # Take the groups that have more than one children and return it.
            # For groups with just one child, return the child itself instead of
            # creating a group.
            return [
                group if len(group.children) > 1 else group.children[0]
                for group in groups.values()
            ], min_timestamp

        # Step 3
        summary, _ = merge_sibings_for_task_group(summary)

        def get_running_tasks_count(task_group: NestedTaskSummary) -> int:
            return (
                task_group.state_counts.get("RUNNING", 0)
                + task_group.state_counts.get("RUNNING_IN_RAY_GET", 0)
                + task_group.state_counts.get("RUNNING_IN_RAY_WAIT", 0)
            )

        def get_pending_tasks_count(task_group: NestedTaskSummary) -> int:
            return (
                task_group.state_counts.get("PENDING_ARGS_AVAIL", 0)
                + task_group.state_counts.get("PENDING_NODE_ASSIGNMENT", 0)
                + task_group.state_counts.get("PENDING_OBJ_STORE_MEM_AVAIL", 0)
                + task_group.state_counts.get("PENDING_ARGS_FETCH", 0)
            )

        def sort_task_groups(task_groups: List[NestedTaskSummary]) -> None:
            # Sort by running tasks, pending tasks, failed tasks, timestamp,
            # and actor_creation_task
            # Put actor creation tasks above other tasks with the same timestamp
            task_groups.sort(key=lambda x: 0 if x.type == "ACTOR_CREATION_TASK" else 1)
            task_groups.sort(key=lambda x: x.timestamp or sys.maxsize)
            task_groups.sort(
                key=lambda x: x.state_counts.get("FAIELD", 0), reverse=True
            )
            task_groups.sort(key=get_pending_tasks_count, reverse=True)
            task_groups.sort(key=get_running_tasks_count, reverse=True)

        def calc_total_for_task_group(
            task_group: NestedTaskSummary,
        ) -> NestedTaskSummary:
            """
            Calculates the total of a group as the sum of all children.
            Sorts children by timestamp
            """
            if not len(task_group.children):
                return task_group

            for child in task_group.children:
                totaled = calc_total_for_task_group(child)

                for state, count in totaled.state_counts.items():
                    task_group.state_counts[state] = (
                        task_group.state_counts.get(state, 0) + count
                    )

            sort_task_groups(task_group.children)

            return task_group

        # Step 4
        summary = [calc_total_for_task_group(task_group) for task_group in summary]
        sort_task_groups(summary)

        return TaskSummaries(
            summary=summary,
            total_tasks=total_tasks,
            total_actor_tasks=total_actor_tasks,
            total_actor_scheduled=total_actor_scheduled,
            summary_by="lineage",
        )


@dataclass(init=not IS_PYDANTIC_2)
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


@dataclass(init=not IS_PYDANTIC_2)
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
    #: Attempt number to the count dict. The attempt number include the current
    #: execution
    task_attempt_number_counts: Dict[str, int] = field(default_factory=dict)
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

            attempt_number = str(object["attempt_number"])
            if attempt_number not in object_summary.task_attempt_number_counts:
                object_summary.task_attempt_number_counts[attempt_number] = 0
            object_summary.task_attempt_number_counts[attempt_number] += 1

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


@dataclass(init=not IS_PYDANTIC_2)
class StateSummary:
    #: Node ID -> summary per node
    #: If the data is not required to be orgnized per node, it will contain
    #: a single key, "cluster".
    node_id_to_summary: Dict[str, Union[TaskSummaries, ActorSummaries, ObjectSummaries]]


@dataclass(init=not IS_PYDANTIC_2)
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
    partial_failure_warning: Optional[str] = ""
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


def protobuf_message_to_dict(
    message,
    fields_to_decode: List[str],
    preserving_proto_field_name: bool = True,
) -> dict:
    """Convert a protobuf message to dict

    Args:
        fields_to_decode: field names which will be decoded from binary to hex.
        preserving_proto_field_name: a pass-through option for protobuf message
            method. See google.protobuf MessageToDict

    Return:
        Dictionary of the converted rpc protobuf.
    """
    return dashboard_utils.message_to_dict(
        message,
        fields_to_decode,
        always_print_fields_with_no_presence=True,
        preserving_proto_field_name=preserving_proto_field_name,
    )


def protobuf_to_task_state_dict(message: TaskEvents) -> dict:
    """
    Convert a TaskEvents to a dic repr of `TaskState`
    """
    task_attempt = protobuf_message_to_dict(
        message=message,
        fields_to_decode=[
            "task_id",
            "job_id",
            "node_id",
            "actor_id",
            "parent_task_id",
            "worker_id",
            "placement_group_id",
            "component_id",
        ],
    )

    task_state = {}
    task_info = task_attempt.get("task_info", {})
    state_updates = task_attempt.get("state_updates", {})
    profiling_data = task_attempt.get("profile_events", {})
    if profiling_data:
        for event in profiling_data["events"]:
            # End/start times are recorded in ns. We convert them to ms.
            event["end_time"] = int(event["end_time"]) / 1e6
            event["start_time"] = int(event["start_time"]) / 1e6
            event["extra_data"] = json.loads(event["extra_data"])
    task_state["profiling_data"] = profiling_data

    # Convert those settable fields
    mappings = [
        (
            task_info,
            [
                "task_id",
                "name",
                "actor_id",
                "type",
                "func_or_class_name",
                "language",
                "required_resources",
                "runtime_env_info",
                "parent_task_id",
                "placement_group_id",
            ],
        ),
        (task_attempt, ["task_id", "attempt_number", "job_id"]),
        (
            state_updates,
            [
                "node_id",
                "worker_id",
                "task_log_info",
                "actor_repr_name",
                "worker_pid",
                "is_debugger_paused",
            ],
        ),
    ]
    for src, keys in mappings:
        for key in keys:
            task_state[key] = src.get(key)

    task_state["creation_time_ms"] = None
    task_state["start_time_ms"] = None
    task_state["end_time_ms"] = None
    events = []

    if "state_ts_ns" in state_updates:
        state_ts_ns = state_updates["state_ts_ns"]
        for state_name, state in TaskStatus.items():
            # state_ts_ns is Map[str, str] after protobuf MessageToDict
            key = str(state)
            if key in state_ts_ns:
                # timestamp is recorded as nanosecond from the backend.
                # We need to convert it to the second.
                ts_ms = int(state_ts_ns[key]) // 1e6
                events.append(
                    {
                        "state": state_name,
                        "created_ms": ts_ms,
                    }
                )
                if state == TaskStatus.PENDING_ARGS_AVAIL:
                    task_state["creation_time_ms"] = ts_ms
                if state == TaskStatus.RUNNING:
                    task_state["start_time_ms"] = ts_ms
                if state == TaskStatus.FINISHED or state == TaskStatus.FAILED:
                    task_state["end_time_ms"] = ts_ms

    task_state["events"] = events
    if len(events) > 0:
        latest_state = events[-1]["state"]
    else:
        latest_state = "NIL"
    task_state["state"] = latest_state

    # Parse error info
    if latest_state == "FAILED":
        error_info = state_updates.get("error_info", None)
        if error_info:
            # We captured colored error message printed to console, e.g.
            # "\x1b[31mTraceback (most recent call last):\x1b[0m",
            # this is to remove the ANSI escape codes.
            task_state["error_message"] = remove_ansi_escape_codes(
                error_info.get("error_message", "")
            )
            task_state["error_type"] = error_info.get("error_type", "")

    # Parse actor task name for actor with repr name.
    if (
        state_updates.get("actor_repr_name")
        and task_state["type"] == "ACTOR_TASK"
        and task_state["name"]
        == task_state["func_or_class_name"]  # no name option provided.
    ):
        # If it's an actor task with no name override, and has repr name defined
        # for the actor, we override the name.
        method_name = task_state["name"].split(".")[-1]
        actor_repr_task_name = f"{state_updates['actor_repr_name']}.{method_name}"
        task_state["name"] = actor_repr_task_name

    return task_state


def remove_ansi_escape_codes(text: str) -> str:
    """Remove ANSI escape codes from a string."""
    import re

    return re.sub(r"\x1b[^m]*m", "", text)


def dict_to_state(d: Dict, state_resource: StateResource) -> StateSchema:

    """Convert a dict to a state schema.

    Args:
        d: a dict to convert.
        state_resource: the state resource to convert to.

    Returns:
        A state schema.
    """
    try:
        return resource_to_schema(state_resource)(**d)

    except Exception as e:
        raise RayStateApiException(f"Failed to convert {d} to StateSchema: {e}") from e
