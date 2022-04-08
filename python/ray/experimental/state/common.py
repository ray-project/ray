import logging

from dataclasses import dataclass

from ray.dashboard.modules.job.common import JobInfo

logger = logging.getLogger(__name__)


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
