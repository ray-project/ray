import logging

from dataclasses import dataclass, fields

from ray.dashboard.modules.job.common import JobInfo

logger = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 30
DEFAULT_LIMIT = 1000


def filter_fields(data: dict, state_dataclass) -> dict:
    """Filter the given data using keys from a given state dataclass."""
    filtered_data = {}
    for field in fields(state_dataclass):
        if field.name in data:
            filtered_data[field.name] = data[field.name]
    return filtered_data


@dataclass(init=True)
class ListApiOptions:
    limit: int
    timeout: int

    # TODO(sang): Use Pydantic instead.
    def __post_init__(self):
        assert isinstance(self.limit, int)
        assert isinstance(self.timeout, int)


# TODO(sang): Replace it with Pydantic or gRPC schema (once interface is finalized).
@dataclass(init=True)
class ActorState:
    actor_id: str
    state: str
    class_name: str
    name: str
    pid: int
    serialized_runtime_env: str
    resource_mapping: dict
    death_cause: dict
    is_detached: bool


@dataclass(init=True)
class PlacementGroupState:
    placement_group_id: str
    state: str
    name: str
    bundles: dict
    is_detached: bool
    stats: dict


@dataclass(init=True)
class NodeState:
    node_id: str
    state: str
    node_name: str
    resources_total: dict


JobState = JobInfo


@dataclass(init=True)
class WorkerState:
    worker_id: str
    is_alive: str
    worker_type: str
    exit_type: str
    worker_info: dict


@dataclass(init=True)
class TaskState:
    task_id: str
    name: str
    scheduling_state: str
    type: str
    language: str
    func_or_class_name: str
    required_resources: dict
    runtime_env_info: str


@dataclass(init=True)
class ObjectState:
    object_id: str
    pid: int
    node_ip_address: str
    object_size: int
    reference_type: str
    call_site: str
    task_status: str
    local_ref_count: int
    pinned_in_memory: int
    submitted_task_ref_count: int
    contained_in_owned: int
    type: str


@dataclass(init=True)
class RuntimeEnvState:
    runtime_env: str
    ref_cnt: int
    success: bool
    error: str
    created_time_ms: float
    retry_cnt: int
