import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    TargetCapacityDirection,
)
from ray.serve._private.constants import (
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.metrics_utils import InMemoryMetricsStore
from ray.serve._private.utils import get_capacity_adjusted_num_replicas

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class HandleMetadata:
    """Report from a deployment handle on queued and ongoing requests.

    Args:
        actor_id: If the deployment handle (from which this metric was
            sent) lives on an actor, the actor ID of that actor.
        handle_source: Describes what kind of entity holds this
            deployment handle: a Serve proxy, a Serve replica, or
            unknown.
    """

    actor_id: Optional[str]
    handle_source: DeploymentHandleSource

    @property
    def is_serve_component_source(self) -> bool:
        """Whether the handle source is a Serve actor.

        More specifically, this returns whether a Serve actor tracked
        by the controller holds the deployment handle that sent this
        report. If the deployment handle lives on a driver, a Ray task,
        or an actor that's not a Serve replica, then this returns False.
        """
        return self.handle_source in [
            DeploymentHandleSource.PROXY,
            DeploymentHandleSource.REPLICA,
        ]


@dataclass
class ReplicaMetricReport:
    """Report from a replica on ongoing requests.

    Args:
        running_requests: Average number of running requests at the
            replica.
        timestamp: The time at which this report was received.
    """

    running_requests: float
    timestamp: float


class AutoscalingState:
    """Manages autoscaling for a single deployment."""

    def __init__(self, deployment_id: DeploymentID):
        self._deployment_id = deployment_id

        self._metrics_store = InMemoryMetricsStore()

        # Map from handle ID to handle request metric report. Metrics
        # are removed from this dict either when the actor on which the
        # handle lived dies, or after a period of no updates.
        # self._handle_requests: Dict[str, HandleMetricReport] = dict()
        self._handle_metadata: Dict[str, HandleMetadata] = dict()
        # Map from replica ID to replica request metric report. Metrics
        # are removed from this dict when a replica is stopped.
        self._replica_requests: Dict[ReplicaID, ReplicaMetricReport] = dict()

        self._deployment_info = None
        self._config = None
        self._policy = None
        self._running_replicas: List[ReplicaID] = []
        self._target_capacity: Optional[float] = None
        self._target_capacity_direction: Optional[TargetCapacityDirection] = None

    def register(self, info: DeploymentInfo, curr_target_num_replicas: int) -> int:
        """Registers an autoscaling deployment's info.

        Returns the number of replicas the target should be set to.
        """

        config = info.deployment_config.autoscaling_config
        if (
            self._deployment_info is None or self._deployment_info.config_changed(info)
        ) and config.initial_replicas is not None:
            target_num_replicas = config.initial_replicas
        else:
            target_num_replicas = curr_target_num_replicas

        self._deployment_info = info
        self._config = config
        self._policy = self._config.get_policy()
        self._target_capacity = info.target_capacity
        self._target_capacity_direction = info.target_capacity_direction
        self._policy_state = {}

        return self.apply_bounds(target_num_replicas)

    def on_replica_stopped(self, replica_id: ReplicaID):
        if replica_id in self._replica_requests:
            del self._replica_requests[replica_id]

    def get_num_replicas_lower_bound(self) -> int:
        if self._config.initial_replicas is not None and (
            self._target_capacity_direction == TargetCapacityDirection.UP
        ):
            return get_capacity_adjusted_num_replicas(
                self._config.initial_replicas,
                self._target_capacity,
            )
        else:
            return get_capacity_adjusted_num_replicas(
                self._config.min_replicas,
                self._target_capacity,
            )

    def get_num_replicas_upper_bound(self) -> int:
        return get_capacity_adjusted_num_replicas(
            self._config.max_replicas,
            self._target_capacity,
        )

    def update_running_replica_ids(self, running_replicas: List[ReplicaID]):
        """Update cached set of running replica IDs for this deployment."""
        self._running_replicas = running_replicas

    def is_within_bounds(self, num_replicas_running_at_target_version: int):
        """Whether or not this deployment is within the autoscaling bounds.

        Returns: True if the number of running replicas for the current
            deployment version is within the autoscaling bounds. False
            otherwise.
        """

        return (
            self.apply_bounds(num_replicas_running_at_target_version)
            == num_replicas_running_at_target_version
        )

    def apply_bounds(self, num_replicas: int) -> int:
        """Clips a replica count with current autoscaling bounds.

        This takes into account target capacity.
        """

        return max(
            self.get_num_replicas_lower_bound(),
            min(self.get_num_replicas_upper_bound(), num_replicas),
        )

    def record_request_metrics_for_replica(
        self, replica_id: ReplicaID, window_avg: Optional[float], send_timestamp: float
    ) -> None:
        """Records average number of ongoing requests at a replica."""

        if window_avg is None:
            return

        if (
            replica_id not in self._replica_requests
            or send_timestamp > self._replica_requests[replica_id].timestamp
        ):
            self._replica_requests[replica_id] = ReplicaMetricReport(
                running_requests=window_avg,
                timestamp=send_timestamp,
            )

    def record_request_metrics_for_handle(
        self,
        *,
        handle_id: str,
        actor_id: Optional[str],
        handle_source: DeploymentHandleSource,
        # queued_requests: float,
        # running_requests: Dict[ReplicaID, float],
        metrics: List,
        # send_timestamp: float,
    ) -> None:
        """Records average number of queued and running requests at a handle for this
        deployment.
        """

        if handle_id not in self._handle_metadata:
            self._handle_metadata[handle_id] = HandleMetadata(actor_id, handle_source)

        for metric in metrics:
            self._metrics_store.add_metrics_point({handle_id: metric[0]}, metric[1])

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        """Drops handle metrics that are no longer valid.

        This includes handles that live on Serve Proxy or replica actors
        that have died AND handles from which the controller hasn't
        received an update for too long.
        """

        self._metrics_store.prune_keys_and_compact_data(
            time.time() - self._config.look_back_period_s
        )
        for handle_id in list(self._handle_metadata):
            if handle_id not in self._metrics_store.data:
                del self._handle_metadata[handle_id]

    def get_decision_num_replicas(
        self, curr_target_num_replicas: int, _skip_bound_check: bool = False
    ) -> int:
        """Decide the target number of replicas to autoscale to.

        The decision is based off of the number of requests received
        for this deployment. After the decision number of replicas is
        returned by the policy, it is then bounded by the bounds min
        and max adjusted by the target capacity and returned. If
        `_skip_bound_check` is True, then the bounds are not applied.
        """

        decision_num_replicas = self._policy(
            curr_target_num_replicas=curr_target_num_replicas,
            total_num_requests=self.get_total_num_requests(),
            num_running_replicas=len(self._running_replicas),
            config=self._config,
            capacity_adjusted_min_replicas=self.get_num_replicas_lower_bound(),
            capacity_adjusted_max_replicas=self.get_num_replicas_upper_bound(),
            policy_state=self._policy_state,
        )

        if _skip_bound_check:
            return decision_num_replicas

        return self.apply_bounds(decision_num_replicas)

    def get_total_num_requests(self) -> float:
        """Get average total number of requests aggregated over the past
        `look_back_period_s` number of seconds.

        If there are 0 running replicas, then returns the total number
        of requests queued at handles

        If the flag RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE is
        set to 1, the returned average includes both queued and ongoing
        requests. Otherwise, the returned average includes only ongoing
        requests.
        """

        total_requests = 0

        if (
            RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE
            or len(self._running_replicas) == 0
        ):
            for handle_id in self._metrics_store.data:  # noqa: E502
                handle_avg = self._metrics_store.window_average(
                    handle_id, time.time() - self._config.look_back_period_s
                )
                if handle_avg:
                    total_requests += handle_avg
        else:
            for id in self._running_replicas:
                if id in self._replica_requests:
                    total_requests += self._replica_requests[id].running_requests

        return total_requests


class AutoscalingStateManager:
    """Manages all things autoscaling related.

    Keeps track of request metrics for each deployment and decides on
    the target number of replicas to autoscale to based on those metrics.
    """

    def __init__(self):
        self._autoscaling_states: Dict[DeploymentID, AutoscalingState] = dict()

    def register_deployment(
        self,
        deployment_id: DeploymentID,
        info: DeploymentInfo,
        curr_target_num_replicas: int,
    ) -> int:
        """Register autoscaling deployment info."""
        assert info.deployment_config.autoscaling_config
        if deployment_id not in self._autoscaling_states:
            self._autoscaling_states[deployment_id] = AutoscalingState(deployment_id)
        return self._autoscaling_states[deployment_id].register(
            info, curr_target_num_replicas
        )

    def deregister_deployment(self, deployment_id: DeploymentID):
        """Remove deployment from tracking."""
        self._autoscaling_states.pop(deployment_id, None)

    def update_running_replica_ids(
        self, deployment_id: DeploymentID, running_replicas: List[ReplicaID]
    ):
        self._autoscaling_states[deployment_id].update_running_replica_ids(
            running_replicas
        )

    def on_replica_stopped(self, replica_id: ReplicaID):
        deployment_id = replica_id.deployment_id
        if deployment_id in self._autoscaling_states:
            self._autoscaling_states[deployment_id].on_replica_stopped(replica_id)

    def get_metrics(self) -> Dict[DeploymentID, float]:
        return {
            deployment_id: self.get_total_num_requests(deployment_id)
            for deployment_id in self._autoscaling_states
        }

    def get_target_num_replicas(
        self, deployment_id: DeploymentID, curr_target_num_replicas: int
    ) -> int:
        return self._autoscaling_states[deployment_id].get_decision_num_replicas(
            curr_target_num_replicas=curr_target_num_replicas,
        )

    def get_total_num_requests(self, deployment_id: DeploymentID) -> float:
        return self._autoscaling_states[deployment_id].get_total_num_requests()

    def is_within_bounds(
        self, deployment_id: DeploymentID, num_replicas_running_at_target_version: int
    ) -> bool:
        return self._autoscaling_states[deployment_id].is_within_bounds(
            num_replicas_running_at_target_version
        )

    def record_request_metrics_for_replica(
        self, replica_id: ReplicaID, window_avg: Optional[float], send_timestamp: float
    ) -> None:
        deployment_id = replica_id.deployment_id
        # Defensively guard against delayed replica metrics arriving
        # after the deployment's been deleted
        if deployment_id in self._autoscaling_states:
            self._autoscaling_states[deployment_id].record_request_metrics_for_replica(
                replica_id=replica_id,
                window_avg=window_avg,
                send_timestamp=send_timestamp,
            )

    def record_request_metrics_for_handle(
        self,
        *,
        deployment_id: DeploymentID,
        handle_id: str,
        actor_id: Optional[str],
        handle_source: DeploymentHandleSource,
        metrics,
    ) -> None:
        """Update request metric for a specific handle."""

        if deployment_id in self._autoscaling_states:
            self._autoscaling_states[deployment_id].record_request_metrics_for_handle(
                handle_id=handle_id,
                actor_id=actor_id,
                handle_source=handle_source,
                metrics=metrics,
            )

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        """Drops handle metrics that are no longer valid.

        This includes handles that live on Serve Proxy or replica actors
        that have died AND handles from which the controller hasn't
        received an update for too long.
        """

        for autoscaling_state in self._autoscaling_states.values():
            autoscaling_state.drop_stale_handle_metrics(alive_serve_actor_ids)
