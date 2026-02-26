"""Shared mock infrastructure for deployment state unit tests.

This module contains MockReplicaActorWrapper and its associated module-level
globals. It exists as a separate module (rather than living in conftest.py)
because bazel's test sandboxing can create duplicate module copies of conftest,
causing `from conftest import` to yield different objects than the ones pytest
auto-loads. By placing the mutable state and mock class here, both conftest.py
and test files get the same objects via Python's standard import caching.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import Mock

from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.deployment_scheduler import ReplicaSchedulingRequest
from ray.serve._private.deployment_state import (
    DeploymentVersion,
    ReplicaStartupStatus,
)
from ray.serve._private.test_utils import MockActorHandle
from ray.serve.gang import GangContext
from ray.serve.schema import ReplicaRank


# Global variable that is fetched during controller recovery that
# marks (simulates) which replicas have died since controller first
# recovered a list of live replica names.
# NOTE(zcin): This is necessary because the replica's `recover()` method
# is called in the controller's init function, instead of in the control
# loop, so we can't "mark" a replica dead through a method. This global
# state is cleared after each test that uses the fixtures in this file.
dead_replicas_context = set()
replica_rank_context: Dict[str, ReplicaRank] = {}


class MockReplicaActorWrapper:
    def __init__(
        self,
        replica_id: ReplicaID,
        version: DeploymentVersion,
    ):
        self._replica_id = replica_id
        self._actor_name = replica_id.to_full_id_str()
        # Will be set when `start()` is called.
        self.started = False
        # Will be set when `recover()` is called.
        self.recovering = False
        # Will be set when `start()` is called.
        self.version = version
        # Initial state for a replica is PENDING_ALLOCATION.
        self.status = ReplicaStartupStatus.PENDING_ALLOCATION
        # Will be set when `graceful_stop()` is called.
        self.stopped = False
        # Expected to be set in the test.
        self.done_stopping = False
        # Will be set when `force_stop()` is called.
        self.force_stopped_counter = 0
        # Will be set when `check_health()` is called.
        self.health_check_called = False
        # Returned by the health check.
        self.healthy = True
        self._is_cross_language = False
        self._actor_handle = MockActorHandle()
        self._node_id = None
        self._node_ip = None
        self._node_instance_id = None
        self._node_id_is_set = False
        self._actor_id = None
        self._internal_grpc_port = None
        self._pg_bundles = None
        self._initialization_latency_s = -1
        self._docs_path = None
        self._rank = replica_rank_context.get(replica_id.unique_id, None)
        self._assign_rank_callback = None
        self._ingress = False
        self._gang_context = None
        self._gang_pg_index = None

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def deployment_name(self) -> str:
        return self._replica_id.deployment_id.name

    @property
    def actor_handle(self) -> MockActorHandle:
        return self._actor_handle

    @property
    def max_ongoing_requests(self) -> int:
        return self.version.deployment_config.max_ongoing_requests

    @property
    def graceful_shutdown_timeout_s(self) -> float:
        return self.version.deployment_config.graceful_shutdown_timeout_s

    @property
    def health_check_period_s(self) -> float:
        return self.version.deployment_config.health_check_period_s

    @property
    def health_check_timeout_s(self) -> float:
        return self.version.deployment_config.health_check_timeout_s

    @property
    def pid(self) -> Optional[int]:
        return None

    @property
    def actor_id(self) -> Optional[str]:
        return self._actor_id

    @property
    def worker_id(self) -> Optional[str]:
        return None

    @property
    def node_id(self) -> Optional[str]:
        if self._node_id_is_set:
            return self._node_id
        if self.status == ReplicaStartupStatus.SUCCEEDED or self.started:
            return "node-id"
        return None

    @property
    def availability_zone(self) -> Optional[str]:
        return None

    @property
    def node_ip(self) -> Optional[str]:
        return None

    @property
    def node_instance_id(self) -> Optional[str]:
        return None

    @property
    def log_file_path(self) -> Optional[str]:
        return None

    @property
    def grpc_port(self) -> Optional[int]:
        return None

    @property
    def placement_group_bundles(self) -> Optional[List[Dict[str, float]]]:
        return None

    @property
    def initialization_latency_s(self) -> float:
        return self._initialization_latency_s

    @property
    def reconfigure_start_time(self) -> Optional[float]:
        return None

    @property
    def last_health_check_latency_ms(self) -> Optional[float]:
        return None

    @property
    def last_health_check_failed(self) -> bool:
        return False

    def set_docs_path(self, docs_path: str):
        self._docs_path = docs_path

    @property
    def docs_path(self) -> Optional[str]:
        return self._docs_path

    def set_status(self, status: ReplicaStartupStatus):
        self.status = status

    def set_ready(self, version: DeploymentVersion = None):
        self.status = ReplicaStartupStatus.SUCCEEDED
        if version:
            self.version_to_be_fetched_from_actor = version
        else:
            self.version_to_be_fetched_from_actor = self.version

    def set_failed_to_start(self):
        self.status = ReplicaStartupStatus.FAILED

    def set_done_stopping(self):
        self.done_stopping = True

    def set_unhealthy(self):
        self.healthy = False

    def set_starting_version(self, version: DeploymentVersion):
        """Mocked deployment_worker return version from reconfigure()"""
        self.starting_version = version

    def set_node_id(self, node_id: str):
        self._node_id = node_id
        self._node_id_is_set = True

    def set_actor_id(self, actor_id: str):
        self._actor_id = actor_id

    def start(
        self,
        deployment_info: DeploymentInfo,
        assign_rank_callback: Callable[[ReplicaID], ReplicaRank],
        gang_placement_group=None,
        gang_pg_index=None,
        gang_context=None,
    ):
        self.started = True
        self._gang_context = gang_context
        self._gang_pg_index = gang_pg_index
        self._assign_rank_callback = assign_rank_callback
        self._rank = assign_rank_callback(self._replica_id.unique_id, node_id=-1)
        replica_rank_context[self._replica_id.unique_id] = self._rank

        def _on_scheduled_stub(*args, **kwargs):
            pass

        return ReplicaSchedulingRequest(
            replica_id=self._replica_id,
            actor_def=Mock(),
            actor_resources={},
            actor_options={"name": "placeholder"},
            actor_init_args=(),
            placement_group_bundles=(
                deployment_info.replica_config.placement_group_bundles
            ),
            on_scheduled=_on_scheduled_stub,
            gang_placement_group=gang_placement_group,
            gang_pg_index=gang_pg_index,
        )

    @property
    def rank(self) -> Optional[ReplicaRank]:
        return self._rank

    def reconfigure(
        self,
        version: DeploymentVersion,
        rank: ReplicaRank = None,
    ):
        self.started = True
        updating = self.version.requires_actor_reconfigure(version)
        self.version = version
        self._rank = rank
        replica_rank_context[self._replica_id.unique_id] = rank
        return updating

    def recover(self, ingress: bool = False):
        if self.replica_id in dead_replicas_context:
            return False

        self._ingress = ingress
        self.recovering = True
        self.started = False
        self._rank = replica_rank_context.get(self._replica_id.unique_id, None)
        return True

    def check_ready(self) -> ReplicaStartupStatus:
        ready = self.status
        self.status = ReplicaStartupStatus.PENDING_INITIALIZATION
        if ready == ReplicaStartupStatus.SUCCEEDED and self.recovering:
            self.recovering = False
            self.started = True
            self.version = self.version_to_be_fetched_from_actor
        return ready, None

    def resource_requirements(self) -> Tuple[str, str]:
        assert self.started
        return str({"REQUIRED_RESOURCE": 1.0}), str({"AVAILABLE_RESOURCE": 1.0})

    @property
    def actor_resources(self) -> Dict[str, float]:
        return {"CPU": 0.1}

    @property
    def available_resources(self) -> Dict[str, float]:
        # Only used to print a warning.
        return {}

    def graceful_stop(self) -> None:
        assert self.started
        self.stopped = True
        return self.graceful_shutdown_timeout_s

    def check_stopped(self) -> bool:
        return self.done_stopping

    def force_stop(self, log_shutdown_message: bool = False):
        self.force_stopped_counter += 1

    def check_health(self):
        self.health_check_called = True
        return self.healthy

    def get_routing_stats(self) -> Dict[str, Any]:
        return {}

    def get_outbound_deployments(self) -> Optional[List[DeploymentID]]:
        return getattr(self, "_outbound_deployments", None)

    @property
    def route_patterns(self) -> Optional[List[str]]:
        return None

    @property
    def gang_context(self) -> Optional[GangContext]:
        return self._gang_context
