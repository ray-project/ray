import logging

from dataclasses import dataclass, fields

from ray.dashboard.modules.job.common import JobInfo

logger = logging.getLogger(__name__)


def data_from_schema(schema, data: dict):
    filtered_data = {}
    for field in fields(schema):
        filtered_data[field.name] = data[field.name]
    return filtered_data


@dataclass(init=True)
class ListApiOptions:
    limit: int
    timeout: int


# TODO(sang): Replace it with Pydantic after the migration.
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
