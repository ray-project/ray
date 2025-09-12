import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    AutoscalingSnapshotError,
    DeploymentID,
    DeploymentSnapshot,
    DeploymentStatusTrigger,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TargetCapacityDirection,
)
from ray.serve._private.constants import (
    AUTOSCALER_SUMMARIZER_DECISION_HISTORY_MAX,
    RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.utils import get_capacity_adjusted_num_replicas

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class _DecisionRecord:
    timestamp_s: str
    prev_num_replicas: int
    curr_num_replicas: int
    reason: str
    policy_name: Optional[str] = None


@dataclass
class AutoscalingContext:
    """Rich context provided to custom autoscaling policies."""

    # Deployment information
    deployment_id: DeploymentID
    deployment_name: str
    app_name: Optional[str]

    # Current state
    current_num_replicas: int
    target_num_replicas: int
    running_replicas: List[ReplicaID]

    # Built-in metrics
    total_num_requests: float
    queued_requests: Optional[float]
    requests_per_replica: Dict[ReplicaID, float]

    # Custom metrics
    aggregated_metrics: Dict[str, Dict[ReplicaID, float]]
    raw_metrics: Dict[str, Dict[ReplicaID, List[float]]]

    # Capacity and bounds
    capacity_adjusted_min_replicas: int
    capacity_adjusted_max_replicas: int

    # Policy state
    policy_state: Dict[str, Any]

    # Timing
    last_scale_up_time: Optional[float]
    last_scale_down_time: Optional[float]
    current_time: Optional[float]

    # Config
    config: Optional[Any]


class AutoscalingState:
    """Manages autoscaling for a single deployment."""

    def __init__(self, deployment_id: DeploymentID):
        self._deployment_id = deployment_id

        # Map from handle ID to handle request metric report. Metrics
        # are removed from this dict either when the actor on which the
        # handle lived dies, or after a period of no updates.
        self._handle_requests: Dict[str, HandleMetricReport] = dict()
        # Map from replica ID to replica request metric report. Metrics
        # are removed from this dict when a replica is stopped.
        self._replica_requests: Dict[ReplicaID, ReplicaMetricReport] = dict()

        self._deployment_info = None
        self._config = None
        self._policy = None
        self._running_replicas: List[ReplicaID] = []
        self._target_capacity: Optional[float] = None
        self._target_capacity_direction: Optional[TargetCapacityDirection] = None
        self._cached_deployment_snapshot: Optional[DeploymentSnapshot] = None
        self._decision_history: List[_DecisionRecord] = []

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
        self, replica_metric_report: ReplicaMetricReport
    ) -> None:
        """Records average number of ongoing requests at a replica."""

        replica_id = replica_metric_report.replica_id
        send_timestamp = replica_metric_report.timestamp
        if (
            replica_id not in self._replica_requests
            or send_timestamp > self._replica_requests[replica_id].timestamp
        ):
            self._replica_requests[replica_id] = replica_metric_report

    def record_request_metrics_for_handle(
        self,
        handle_metric_report: HandleMetricReport,
    ) -> None:
        """Records average number of queued and running requests at a handle for this
        deployment.
        """
        handle_id = handle_metric_report.handle_id
        send_timestamp = handle_metric_report.timestamp
        if (
            handle_id not in self._handle_requests
            or send_timestamp > self._handle_requests[handle_id].timestamp
        ):
            self._handle_requests[handle_id] = handle_metric_report

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        """Drops handle metrics that are no longer valid.

        This includes handles that live on Serve Proxy or replica actors
        that have died AND handles from which the controller hasn't
        received an update for too long.
        """

        timeout_s = max(
            2 * self._config.metrics_interval_s,
            RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
        )
        for handle_id, handle_metric in list(self._handle_requests.items()):
            # Drop metrics for handles that are on Serve proxy/replica
            # actors that have died
            if (
                handle_metric.is_serve_component_source
                and handle_metric.actor_id is not None
                and handle_metric.actor_id not in alive_serve_actor_ids
            ):
                del self._handle_requests[handle_id]
                if handle_metric.total_requests > 0:
                    logger.debug(
                        f"Dropping metrics for handle '{handle_id}' because the Serve "
                        f"actor it was on ({handle_metric.actor_id}) is no longer "
                        f"alive. It had {handle_metric.total_requests} ongoing requests"
                    )
            # Drop metrics for handles that haven't sent an update in a while.
            # This is expected behavior for handles that were on replicas or
            # proxies that have been shut down.
            elif time.time() - handle_metric.timestamp >= timeout_s:
                del self._handle_requests[handle_id]
                if handle_metric.total_requests > 0:
                    actor_id = handle_metric.actor_id
                    actor_info = f"on actor '{actor_id}' " if actor_id else ""
                    logger.info(
                        f"Dropping stale metrics for handle '{handle_id}' {actor_info}"
                        f"because no update was received for {timeout_s:.1f}s. "
                        f"Ongoing requests was: {handle_metric.total_requests}."
                    )

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

        total_requests = self.get_total_num_requests()
        ctx: AutoscalingContext = AutoscalingContext(
            deployment_id=self._deployment_id,
            deployment_name=self._deployment_id.name,
            app_name=self._deployment_id.app_name,
            current_num_replicas=len(self._running_replicas),
            target_num_replicas=curr_target_num_replicas,
            running_replicas=self._running_replicas,
            total_num_requests=total_requests,
            capacity_adjusted_min_replicas=self.get_num_replicas_lower_bound(),
            capacity_adjusted_max_replicas=self.get_num_replicas_upper_bound(),
            policy_state=self._policy_state.copy(),
            current_time=time.time(),
            config=self._config,
            queued_requests=None,
            requests_per_replica=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )

        decision_num_replicas, self._policy_state = self._policy(ctx)

        if _skip_bound_check:
            target_for_record = decision_num_replicas
        else:
            target_for_record = self.apply_bounds(decision_num_replicas)
        self._decision_history.append(
            _DecisionRecord(
                timestamp_s=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                prev_num_replicas=int(ctx.current_num_replicas),
                curr_num_replicas=int(target_for_record),
                reason=f"current={ctx.current_num_replicas}, target={target_for_record}",
                policy_name=getattr(ctx.config.policy, "name", None),
            )
        )
        if len(self._decision_history) > AUTOSCALER_SUMMARIZER_DECISION_HISTORY_MAX:
            self._decision_history = self._decision_history[
                -AUTOSCALER_SUMMARIZER_DECISION_HISTORY_MAX:
            ]

        self._cached_deployment_snapshot = self._create_deployment_snapshot(
            ctx=ctx,
            target_replicas=target_for_record,
        )
        return target_for_record

    def get_recent_decisions(self) -> List[_DecisionRecord]:
        return self._decision_history

    def get_total_num_requests(self) -> float:
        """Get average total number of requests aggregated over the past
        `look_back_period_s` number of seconds.

        If there are 0 running replicas, then returns the total number
        of requests queued at handles

        This code assumes that the metrics are either emmited on handles
        or on replicas, but not both. Its the responsibility of the writer
        to ensure enclusivity of the metrics.
        """

        total_requests = 0

        for id in self._running_replicas:
            if id in self._replica_requests:
                total_requests += self._replica_requests[id].aggregated_metrics.get(
                    RUNNING_REQUESTS_KEY
                )

        metrics_collected_on_replicas = total_requests > 0
        for handle_metric in self._handle_requests.values():
            total_requests += handle_metric.queued_requests

            if not metrics_collected_on_replicas:
                for replica_id in self._running_replicas:
                    if replica_id in handle_metric.aggregated_metrics.get(
                        RUNNING_REQUESTS_KEY
                    ):
                        total_requests += handle_metric.aggregated_metrics.get(
                            RUNNING_REQUESTS_KEY
                        ).get(replica_id)

        return total_requests

    def _create_deployment_snapshot(
        self,
        *,
        ctx: AutoscalingContext,
        target_replicas: int,
    ) -> DeploymentSnapshot:
        """Create a fully-populated DeploymentSnapshot using data already available in
        AutoscalingState and the provided context.
        """
        current_replicas = ctx.current_num_replicas
        min_replicas = ctx.capacity_adjusted_min_replicas
        max_replicas = ctx.capacity_adjusted_max_replicas

        # Aggregate queued requests (best-effort)
        if self._handle_requests:
            queued_requests = sum(
                h.queued_requests for h in self._handle_requests.values()
            )
        else:
            queued_requests = 0.0

        timestamps = [r.timestamp for r in self._replica_requests.values()]
        timestamps.extend(h.timestamp for h in self._handle_requests.values())
        if timestamps:
            time_since_last_collected_metrics_s = time.time() - max(timestamps)
        else:
            time_since_last_collected_metrics_s = None

        if target_replicas > current_replicas:
            scaling_status_raw = DeploymentStatusTrigger.AUTOSCALING_UPSCALE
        elif target_replicas < current_replicas:
            scaling_status_raw = DeploymentStatusTrigger.AUTOSCALING_DOWNSCALE
        else:
            scaling_status_raw = DeploymentStatusTrigger.AUTOSCALING_STABLE
        scaling_status = DeploymentSnapshot.format_scaling_status(scaling_status_raw)

        look_back_period_s = getattr(self._config, "look_back_period_s", None)
        metrics_health = DeploymentSnapshot.format_metrics_health_text(
            time_since_last_collected_metrics_s=(
                None
                if time_since_last_collected_metrics_s is None
                else float(time_since_last_collected_metrics_s)
            ),
            look_back_period_s=look_back_period_s,
        )

        decisions_summary = DeploymentSnapshot.summarize_decisions(
            self._decision_history
        )
        errors: List[str] = []

        if time_since_last_collected_metrics_s is None:
            errors.append(AutoscalingSnapshotError.METRICS_UNAVAILABLE)

        return DeploymentSnapshot(
            timestamp_s=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            app=self._deployment_id.app_name,
            deployment=self._deployment_id.name,
            current_replicas=int(current_replicas),
            target_replicas=int(target_replicas),
            min_replicas=int(min_replicas) if min_replicas is not None else None,
            max_replicas=int(max_replicas) if max_replicas is not None else None,
            scaling_status=scaling_status,
            policy_name=getattr(ctx.config.policy, "name", None),
            look_back_period_s=look_back_period_s,
            queued_requests=float(queued_requests),
            total_requests=float(ctx.total_num_requests),
            metrics_health=metrics_health,
            errors=errors,
            decisions=decisions_summary,
        )

    def get_deployment_snapshot(self) -> Optional[DeploymentSnapshot]:
        """
        Return the cached deployment snapshot if available.
        """
        return self._cached_deployment_snapshot


class AutoscalingStateManager:
    """Manages all things autoscaling related.

    Keeps track of request metrics for each deployment and decides on
    the target number of replicas to autoscale to based on those metrics.
    """

    def __init__(self):
        self._autoscaling_states: Dict[DeploymentID, AutoscalingState] = {}

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
        self, replica_metric_report: ReplicaMetricReport
    ) -> None:
        deployment_id = replica_metric_report.replica_id.deployment_id
        # Defensively guard against delayed replica metrics arriving
        # after the deployment's been deleted
        if deployment_id in self._autoscaling_states:
            self._autoscaling_states[deployment_id].record_request_metrics_for_replica(
                replica_metric_report
            )

    def record_request_metrics_for_handle(
        self,
        handle_metric_report: HandleMetricReport,
    ) -> None:
        """Update request metric for a specific handle."""

        deployment_id = handle_metric_report.deployment_id
        if deployment_id in self._autoscaling_states:
            self._autoscaling_states[deployment_id].record_request_metrics_for_handle(
                handle_metric_report
            )

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        """Drops handle metrics that are no longer valid.

        This includes handles that live on Serve Proxy or replica actors
        that have died AND handles from which the controller hasn't
        received an update for too long.
        """

        for autoscaling_state in self._autoscaling_states.values():
            autoscaling_state.drop_stale_handle_metrics(alive_serve_actor_ids)

    def get_deployment_snapshot(
        self, deployment_id: DeploymentID
    ) -> Optional[DeploymentSnapshot]:
        state = self._autoscaling_states.get(deployment_id)
        return state.get_deployment_snapshot() if state else None

    def get_recent_decisions(self, deployment_id: DeploymentID):
        state = self._autoscaling_states.get(deployment_id)
        return state.get_recent_decisions() if state else []
