import logging

from dataclasses import dataclass, fields

from ray.dashboard.modules.job.common import JobInfo

logger = logging.getLogger(__name__)


def filter_fields(data: dict, state_dataclass) -> dict:
    """Filter the given data using keys from a given state dataclass."""
    filtered_data = {}
    for field in fields(state_dataclass):
        filtered_data[field.name] = data[field.name]
    return filtered_data


@dataclass(init=True)
class ListApiOptions:
    limit: int
    timeout: int


# TODO(sang): Replace it with Pydantic or gRPC schema (once interface is finalized).
@dataclass(init=True)
class ActorState:
    actor_id: str
    state: str
    class_name: str


@dataclass(init=True)
class PlacementGroupState:
    placement_group_id: str
    state: str


@dataclass(init=True)
class NodeState:
    node_id: str
    state: str


JobState = JobInfo


@dataclass(init=True)
class WorkerState:
    worker_id: str
    is_alive: str
    worker_type: str


@dataclass(init=True)
class TaskState:
    task_id: str
    name: str
    scheduling_state: str


@dataclass(init=True)
class ObjectState:
    object_id: str
