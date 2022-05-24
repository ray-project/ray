import logging

from dataclasses import dataclass, fields
from typing import List, Dict, Union

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
    exit_type: str
    exit_detail: str
    pid: str


@dataclass(init=True)
class TaskState:
    task_id: str
    name: str
    scheduling_state: str


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
    creation_time_ms: float
    node_id: str


@dataclass(init=True)
class ListApiResponse:
    # Returned data. None if no data is returned.
    result: Union[
        Dict[
            str,
            Union[
                ActorState,
                PlacementGroupState,
                NodeState,
                JobInfo,
                WorkerState,
                TaskState,
                ObjectState,
            ],
        ],
        List[RuntimeEnvState],
    ] = None
    # List API can have a partial failure if queries to
    # all sources fail. For example, getting object states
    # require to ping all raylets, and it is possible some of
    # them fails. Note that it is impossible to guarantee high
    # availability of data because ray's state information is
    # not replicated.
    partial_failure_warning: str = ""
