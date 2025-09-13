import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    ApplicationName,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TargetCapacityDirection,
)
from ray.serve._private.constants import (
    RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.config import AutoscalingPolicy
from ray.serve.schema import DeploymentDetails, ServeApplicationSchema

logger = logging.getLogger(SERVE_LOGGER_NAME)


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


class DeploymentAutoscalingState:
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
        self._is_part_of_autoscaling_application: bool = False

    def register(
        self,
        info: DeploymentInfo,
        curr_target_num_replicas: int,
        is_part_of_autoscaling_application: bool = False,
    ) -> int:
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
        self._target_capacity = info.target_capacity
        self._target_capacity_direction = info.target_capacity_direction
        self._is_part_of_autoscaling_application = is_part_of_autoscaling_application
        if not is_part_of_autoscaling_application:
            self._policy = self._config.get_policy()
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

        assert not self._is_part_of_autoscaling_application

        autoscaling_context: AutoscalingContext = AutoscalingContext(
            deployment_id=self._deployment_id,
            deployment_name=self._deployment_id.name,
            app_name=self._deployment_id.app_name,
            current_num_replicas=len(self._running_replicas),
            target_num_replicas=curr_target_num_replicas,
            running_replicas=self._running_replicas,
            total_num_requests=self.get_total_num_requests(),
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

        decision_num_replicas, self._policy_state = self._policy(autoscaling_context)

        if _skip_bound_check:
            return decision_num_replicas

        return self.apply_bounds(decision_num_replicas)

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


class ApplicationAutoscalingState:
    """Manages autoscaling for a single application."""

    def __init__(
        self,
        app_name: ApplicationName,
        deployment_autoscaling_states: Dict[DeploymentID, DeploymentAutoscalingState],
    ):
        self._app_name = app_name
        self._deployment_autoscaling_states = deployment_autoscaling_states
        self._config: ServeApplicationSchema = None
        self._policy: AutoscalingPolicy = None
        self._policy_state: Dict[str, Dict] = None

    def register(
        self,
        config: ServeApplicationSchema,
    ):
        self._config = config
        self._policy = self._config.get_autoscaling_policy()
        self._policy_state = {}

    def get_scaling_decisions(
        self, deployments: Dict[str, DeploymentDetails], _skip_bound_check: bool = False
    ) -> Dict[str, int]:
        autoscaling_contexts: Dict[str, AutoscalingContext] = {}
        decisions: Dict[str, int] = {}
        for name, deployment_detail in deployments.items():
            deployment_id: DeploymentID = DeploymentID(
                name=name, app_name=self._app_name
            )
            deployment_autoscaling_state = self._deployment_autoscaling_states[
                deployment_id
            ]
            autoscaling_context: AutoscalingContext = AutoscalingContext(
                deployment_id=deployment_id,
                deployment_name=name,
                app_name=self._app_name,
                current_num_replicas=len(
                    deployment_autoscaling_state._running_replicas
                ),
                target_num_replicas=deployment_detail.target_num_replicas,
                running_replicas=deployment_autoscaling_state._running_replicas,
                total_num_requests=deployment_autoscaling_state.get_total_num_requests(),
                capacity_adjusted_min_replicas=deployment_autoscaling_state.get_num_replicas_lower_bound(),
                capacity_adjusted_max_replicas=deployment_autoscaling_state.get_num_replicas_upper_bound(),
                policy_state=self._policy_state.copy(),
                current_time=time.time(),
                config=deployment_autoscaling_state._config,
                queued_requests=None,
                requests_per_replica=None,
                aggregated_metrics=None,
                raw_metrics=None,
                last_scale_up_time=None,
                last_scale_down_time=None,
            )

            autoscaling_contexts[name] = autoscaling_context

        decisions, self._policy_state = self._policy(autoscaling_contexts)

        updated_decisions = {}

        for deployment, decision_num_replicas in decisions.items():
            deployment_id: DeploymentID = DeploymentID(
                name=deployment, app_name=self._app_name
            )
            deployment_autoscaling_state = self._deployment_autoscaling_states[
                deployment_id
            ]
            if not _skip_bound_check:
                updated_decisions[
                    deployment_id
                ] = deployment_autoscaling_state.apply_bounds(decision_num_replicas)

        return updated_decisions


class AutoscalingStateManager:
    """Manages all things autoscaling related.

    Keeps track of request metrics for each deployment and decides on
    the target number of replicas to autoscale to based on those metrics.
    """

    def __init__(self):
        self._deployment_autoscaling_states: Dict[
            DeploymentID, DeploymentAutoscalingState
        ] = {}
        self._app_autoscaling_states: Dict[
            ApplicationName, ApplicationAutoscalingState
        ] = {}

    def register_deployment(
        self,
        deployment_id: DeploymentID,
        info: DeploymentInfo,
        curr_target_num_replicas: int,
    ) -> int:
        """Register autoscaling deployment info."""
        assert info.deployment_config.autoscaling_config
        if deployment_id not in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                deployment_id
            ] = DeploymentAutoscalingState(deployment_id)

        if self.is_part_of_autoscaling_application(deployment_id):
            logger.warning(
                f"Deployment '{deployment_id}' is part of an autoscaling application. "
                "Deployment scaling policy may be overridden by application-level autoscaling policy."
            )

        return self._deployment_autoscaling_states[deployment_id].register(
            info,
            curr_target_num_replicas,
            is_part_of_autoscaling_application=self.is_part_of_autoscaling_application(
                deployment_id
            ),
        )

    def deregister_deployment(self, deployment_id: DeploymentID):
        """Remove deployment from tracking."""
        self._deployment_autoscaling_states.pop(deployment_id, None)

    def register_application(
        self,
        app_name: ApplicationName,
        config: ServeApplicationSchema,
        deployment_infos: Dict[str, DeploymentInfo],
    ):
        if app_name not in self._app_autoscaling_states:
            deployment_autoscaling_states = {}
            for deployment_name in config.deployment_names:
                deployment_id = DeploymentID(deployment_name, app_name)
                if deployment_id not in self._deployment_autoscaling_states:
                    deployment_autoscaling_state = DeploymentAutoscalingState(
                        deployment_id
                    )
                    if (
                        deployment_infos is not None
                        and deployment_name in deployment_infos
                    ):
                        deployment_info = deployment_infos[deployment_name]
                        target_num_replicas = get_capacity_adjusted_num_replicas(
                            deployment_info.deployment_config.num_replicas,
                            deployment_info.target_capacity,
                        )
                        deployment_autoscaling_state.register(
                            deployment_info,
                            target_num_replicas,
                            is_part_of_autoscaling_application=True,
                        )
                    self._deployment_autoscaling_states[
                        deployment_id
                    ] = deployment_autoscaling_state

                deployment_autoscaling_states[
                    deployment_id
                ] = self._deployment_autoscaling_states[deployment_id]

            self._app_autoscaling_states[app_name] = ApplicationAutoscalingState(
                app_name, deployment_autoscaling_states
            )

        self._app_autoscaling_states[app_name].register(config)

    def deregister_application(
        self, app_name: ApplicationName, deployment_names: List[str]
    ):
        """Remove application from tracking."""
        for deployment_name in deployment_names:
            deployment_id = DeploymentID(deployment_name, app_name)
            self._deployment_autoscaling_states.pop(deployment_id, None)
        self._app_autoscaling_states.pop(app_name, None)

    def get_scaling_decisions_for_application(
        self, app_name: ApplicationName, deployments: Dict[str, DeploymentDetails]
    ) -> Dict[str, int]:
        return self._app_autoscaling_states[app_name].get_scaling_decisions(deployments)

    def is_part_of_autoscaling_application(self, deployment_id: DeploymentID):
        return deployment_id.app_name in self._app_autoscaling_states

    def update_running_replica_ids(
        self, deployment_id: DeploymentID, running_replicas: List[ReplicaID]
    ):
        self._deployment_autoscaling_states[deployment_id].update_running_replica_ids(
            running_replicas
        )

    def on_replica_stopped(self, replica_id: ReplicaID):
        deployment_id = replica_id.deployment_id
        if deployment_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[deployment_id].on_replica_stopped(
                replica_id
            )

    def get_metrics(self) -> Dict[DeploymentID, float]:
        return {
            deployment_id: self.get_total_num_requests(deployment_id)
            for deployment_id in self._deployment_autoscaling_states
        }

    def get_scaling_decision_for_deployment(
        self, deployment_id: DeploymentID, curr_target_num_replicas: int
    ) -> int:
        return self._deployment_autoscaling_states[
            deployment_id
        ].get_decision_num_replicas(
            curr_target_num_replicas=curr_target_num_replicas,
        )

    def get_total_num_requests(self, deployment_id: DeploymentID) -> float:
        return self._deployment_autoscaling_states[
            deployment_id
        ].get_total_num_requests()

    def is_within_bounds(
        self, deployment_id: DeploymentID, num_replicas_running_at_target_version: int
    ) -> bool:
        return self._deployment_autoscaling_states[deployment_id].is_within_bounds(
            num_replicas_running_at_target_version
        )

    def record_request_metrics_for_replica(
        self, replica_metric_report: ReplicaMetricReport
    ) -> None:
        deployment_id = replica_metric_report.replica_id.deployment_id
        # Defensively guard against delayed replica metrics arriving
        # after the deployment's been deleted
        if deployment_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                deployment_id
            ].record_request_metrics_for_replica(replica_metric_report)

    def record_request_metrics_for_handle(
        self,
        handle_metric_report: HandleMetricReport,
    ) -> None:
        """Update request metric for a specific handle."""

        deployment_id = handle_metric_report.deployment_id
        if deployment_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                deployment_id
            ].record_request_metrics_for_handle(handle_metric_report)

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        """Drops handle metrics that are no longer valid.

        This includes handles that live on Serve Proxy or replica actors
        that have died AND handles from which the controller hasn't
        received an update for too long.
        """

        for autoscaling_state in self._deployment_autoscaling_states.values():
            autoscaling_state.drop_stale_handle_metrics(alive_serve_actor_ids)
