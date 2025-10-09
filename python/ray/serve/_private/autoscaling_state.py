import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from ray.serve._private.common import (
    ONGOING_REQUESTS_KEY,
    RUNNING_REQUESTS_KEY,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TargetCapacityDirection,
    TimeStampedValue,
)
from ray.serve._private.constants import (
    RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER,
    RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.metrics_utils import (
    merge_timeseries_dicts,
    time_weighted_average,
)
from ray.serve._private.utils import get_capacity_adjusted_num_replicas

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
        # Prometheus + Custom metrics from each replica are also included
        self._replica_metrics: Dict[ReplicaID, ReplicaMetricReport] = dict()

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
        if replica_id in self._replica_metrics:
            del self._replica_metrics[replica_id]

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
            replica_id not in self._replica_metrics
            or send_timestamp > self._replica_metrics[replica_id].timestamp
        ):
            self._replica_metrics[replica_id] = replica_metric_report

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

    def _collect_replica_running_requests(self) -> List[List[TimeStampedValue]]:
        """Collect running requests timeseries from replicas for aggregation.

        Returns:
            List of timeseries data (List[TimeStampedValue]).
        """
        timeseries_list = []

        for replica_id in self._running_replicas:
            replica_metric_report = self._replica_metrics.get(replica_id, None)
            if (
                replica_metric_report is not None
                and RUNNING_REQUESTS_KEY in replica_metric_report.metrics
            ):
                timeseries_list.append(
                    replica_metric_report.metrics[RUNNING_REQUESTS_KEY]
                )

        return timeseries_list

    def _collect_handle_queued_requests(self) -> List[List[TimeStampedValue]]:
        """Collect queued requests timeseries from all handles.

        Returns:
            List of timeseries data (List[TimeStampedValue]).
        """
        timeseries_list = []
        for handle_metric_report in self._handle_requests.values():
            timeseries_list.append(handle_metric_report.queued_requests)
        return timeseries_list

    def _collect_handle_running_requests(self) -> List[List[TimeStampedValue]]:
        """Collect running requests timeseries from handles when not collected on replicas.

        Returns:
            List of timeseries data (List[TimeStampedValue]).

        Example:
            If there are 2 handles, each managing 2 replicas, and the running requests metrics are:
            - Handle 1: Replica 1: 5, Replica 2: 7
            - Handle 2: Replica 1: 3, Replica 2: 1
            and the timestamp is 0.1 and 0.2 respectively
            Then the returned list will be:
            [
                [TimeStampedValue(timestamp=0.1, value=5.0)],
                [TimeStampedValue(timestamp=0.2, value=7.0)],
                [TimeStampedValue(timestamp=0.1, value=3.0)],
                [TimeStampedValue(timestamp=0.2, value=1.0)]
            ]
        """
        timeseries_list = []

        for handle_metric in self._handle_requests.values():
            for replica_id in self._running_replicas:
                if (
                    RUNNING_REQUESTS_KEY not in handle_metric.metrics
                    or replica_id not in handle_metric.metrics[RUNNING_REQUESTS_KEY]
                ):
                    continue
                timeseries_list.append(
                    handle_metric.metrics[RUNNING_REQUESTS_KEY][replica_id]
                )

        return timeseries_list

    def _aggregate_ongoing_requests(
        self, metrics_timeseries_dicts: List[Dict[str, List[TimeStampedValue]]]
    ) -> float:
        """Aggregate and average ongoing requests from timeseries data using instantaneous merge.

        Args:
            metrics_timeseries_dicts: A list of dictionaries, each containing a key-value pair:
                - The key is the name of the metric (ONGOING_REQUESTS_KEY)
                - The value is a list of TimeStampedValue objects, each representing a single measurement of the metric
                this list is sorted by timestamp ascending

        Returns:
            The time-weighted average of the ongoing requests

        Example:
            If the metrics_timeseries_dicts is:
            [
                {
                    "ongoing_requests": [
                        TimeStampedValue(timestamp=0.1, value=5.0),
                        TimeStampedValue(timestamp=0.2, value=7.0),
                    ]
                },
                {
                    "ongoing_requests": [
                        TimeStampedValue(timestamp=0.2, value=3.0),
                        TimeStampedValue(timestamp=0.3, value=1.0),
                    ]
                }
            ]
            Then the returned value will be:
            (5.0*0.1 + 7.0*0.2 + 3.0*0.2 + 1.0*0.3) / (0.1 + 0.2 + 0.2 + 0.3) = 4.5 / 0.8 = 5.625
        """

        if not metrics_timeseries_dicts:
            return 0.0

        # Use instantaneous merge approach - no arbitrary windowing needed
        aggregated_metrics = merge_timeseries_dicts(*metrics_timeseries_dicts)
        ongoing_requests_timeseries = aggregated_metrics.get(ONGOING_REQUESTS_KEY, [])
        if ongoing_requests_timeseries:
            # assume that the last recorded metric is valid for last_window_s seconds
            last_metric_time = ongoing_requests_timeseries[-1].timestamp
            # we dont want to make any assumption about how long the last metric will be valid
            # only conclude that the last metric is valid for last_window_s seconds that is the
            # difference between the current time and the last metric recorded time
            last_window_s = time.time() - last_metric_time
            # adding a check to negative values caused by clock skew
            # between replicas and controller. Also add a small epsilon to avoid division by zero
            if last_window_s <= 0:
                last_window_s = 1e-3
            # Calculate the time-weighted average of the running requests
            avg_ongoing = time_weighted_average(
                ongoing_requests_timeseries, last_window_s=last_window_s
            )
            return avg_ongoing if avg_ongoing is not None else 0.0

        return 0.0

    def _calculate_total_requests_aggregate_mode(self) -> float:
        """Calculate total requests using aggregate metrics mode with timeseries data.

        This method works with raw timeseries metrics data and performs aggregation
        at the controller level, providing more accurate and stable metrics compared
        to simple mode.

        Processing Steps:
            1. Collect raw timeseries data (eg: running request) from replicas (if available)
            2. Collect queued requests from handles (always tracked at handle level)
            3. Collect raw timeseries data (eg: running request) from handles (if not available from replicas)
            4. Merge timeseries using instantaneous approach for mathematically correct totals
            5. Calculate time-weighted average running requests from the merged timeseries

        Key Differences from Simple Mode:
            - Uses raw timeseries data instead of pre-aggregated metrics
            - Performs instantaneous merging for exact gauge semantics
            - Aggregates at the controller level rather than using pre-computed averages
            - Uses time-weighted averaging over the look_back_period_s interval for accurate calculations

        Metrics Collection:
            Running requests are collected with either replica-level or handle-level metrics.

            Queued requests are always collected from handles regardless of where
            running requests are collected.

        Timeseries Aggregation:
            Raw timeseries data from multiple sources is merged using an instantaneous
            approach that treats gauges as right-continuous step functions. This provides
            mathematically correct totals without arbitrary windowing bias.

        Example with Numbers:
            Assume metrics_interval_s = 0.5s, current time = 2.0s

            Step 1: Collect raw timeseries from 2 replicas (r1, r2)
            replica_metrics = [
                {"running_requests": [(t=0.2, val=5), (t=0.8, val=7), (t=1.5, val=6)]},  # r1
                {"running_requests": [(t=0.1, val=3), (t=0.9, val=4), (t=1.4, val=8)]}   # r2
            ]

            Step 2: Collect queued requests from handles
            handle_queued = 2 + 3 = 5  # total from all handles

            Step 3: No handle metrics needed (replica metrics available)
            handle_metrics = []

            Step 4: Merge timeseries using instantaneous approach
            # Create delta events: r1 starts at 5 (t=0.2), changes to 7 (t=0.8), then 6 (t=1.5)
            #                      r2 starts at 3 (t=0.1), changes to 4 (t=0.9), then 8 (t=1.4)
            # Merged instantaneous total: [(t=0.1, val=3), (t=0.2, val=8), (t=0.8, val=10), (t=0.9, val=11), (t=1.4, val=15), (t=1.5, val=14)]
            merged_timeseries = {"running_requests": [(0.1, 3), (0.2, 8), (0.8, 10), (0.9, 11), (1.4, 15), (1.5, 14)]}

            Step 5: Calculate time-weighted average over full timeseries (t=0.1 to t=1.5+0.5=2.0)
            # Time-weighted calculation: (3*0.1 + 8*0.6 + 10*0.1 + 11*0.5 + 15*0.1 + 14*0.5) / 2.0 = 10.05
            avg_running = 10.05

            Final result: total_requests = avg_running + queued = 10.05 + 5 = 15.05

        Returns:
            Total number of requests (average running + queued) calculated from
            timeseries data aggregation.
        """
        # Collect replica-based running requests (returns List[List[TimeStampedValue]])
        replica_timeseries = self._collect_replica_running_requests()
        metrics_collected_on_replicas = len(replica_timeseries) > 0

        # Collect queued requests from handles (returns List[List[TimeStampedValue]])
        queued_timeseries = self._collect_handle_queued_requests()

        if not metrics_collected_on_replicas:
            # Collect handle-based running requests if not collected on replicas
            handle_timeseries = self._collect_handle_running_requests()
        else:
            handle_timeseries = []

        # Create minimal dictionary objects only when needed
        ongoing_requests_metrics = []

        # Add replica timeseries with minimal dict wrapping
        for timeseries in replica_timeseries:
            ongoing_requests_metrics.append({ONGOING_REQUESTS_KEY: timeseries})

        # Add handle timeseries if replica metrics weren't collected
        if not metrics_collected_on_replicas:
            for timeseries in handle_timeseries:
                ongoing_requests_metrics.append({ONGOING_REQUESTS_KEY: timeseries})

        # Add queued timeseries with minimal dict wrapping
        for timeseries in queued_timeseries:
            ongoing_requests_metrics.append({ONGOING_REQUESTS_KEY: timeseries})
        # Aggregate and add running requests to total
        ongoing_requests = self._aggregate_ongoing_requests(ongoing_requests_metrics)

        return ongoing_requests

    def _calculate_total_requests_simple_mode(self) -> float:
        """Calculate total requests using simple aggregated metrics mode.

        This method works with pre-aggregated metrics that are computed by averaging
        (or other functions) over the past look_back_period_s seconds.

        Metrics Collection:
            Metrics can be collected at two levels:
            1. Replica level: Each replica reports one aggregated metric value
            2. Handle level: Each handle reports metrics for multiple replicas

        Replica-Level Metrics Example:
            For 3 replicas (r1, r2, r3), metrics might look like:
            {
                "r1": 10,
                "r2": 20,
                "r3": 30
            }
            Total requests = 10 + 20 + 30 = 60

        Handle-Level Metrics Example:
            For 3 handles (h1, h2, h3), each managing 2 replicas:
            - h1 manages r1, r2
            - h2 manages r2, r3
            - h3 manages r3, r1

            Metrics structure:
            {
                "h1": {"r1": 10, "r2": 20},
                "h2": {"r2": 20, "r3": 30},
                "h3": {"r3": 30, "r1": 10}
            }

            Total requests = 10 + 20 + 20 + 30 + 30 + 10 = 120

            Note: We can safely sum all handle metrics because each unique request
            is counted only once across all handles (no double-counting).

        Queued Requests:
            Queued request metrics are always tracked at the handle level, regardless
            of whether running request metrics are collected at replicas or handles.

        Returns:
            Total number of requests (running + queued) across all replicas/handles.
        """
        total_requests = 0

        for id in self._running_replicas:
            if id in self._replica_metrics:
                total_requests += self._replica_metrics[id].aggregated_metrics.get(
                    RUNNING_REQUESTS_KEY, 0
                )

        metrics_collected_on_replicas = total_requests > 0

        # Add handle metrics
        for handle_metric in self._handle_requests.values():
            total_requests += handle_metric.aggregated_queued_requests
            # Add running requests from handles if not collected on replicas
            if not metrics_collected_on_replicas:
                for replica_id in self._running_replicas:
                    if replica_id in handle_metric.aggregated_metrics.get(
                        RUNNING_REQUESTS_KEY, {}
                    ):
                        total_requests += handle_metric.aggregated_metrics.get(
                            RUNNING_REQUESTS_KEY
                        ).get(replica_id)
        return total_requests

    def get_total_num_requests(self) -> float:
        """Get average total number of requests aggregated over the past
        `look_back_period_s` number of seconds.

        If there are 0 running replicas, then returns the total number
        of requests queued at handles

        This code assumes that the metrics are either emmited on handles
        or on replicas, but not both. Its the responsibility of the writer
        to ensure enclusivity of the metrics.
        """
        if RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER:
            return self._calculate_total_requests_aggregate_mode()
        else:
            return self._calculate_total_requests_simple_mode()

    def get_replica_metrics(self, agg_func: str) -> Dict[ReplicaID, List[Any]]:
        """Get the raw replica metrics dict."""
        # arcyleung TODO: pass agg_func from autoscaling policy https://github.com/ray-project/ray/pull/51905
        # Dummy implementation of mean agg_func across all values of the same metrics key

        metric_values = defaultdict(list)
        for id in self._running_replicas:
            if id in self._replica_metrics and self._replica_metrics[id].metrics:
                for k, v in self._replica_metrics[id].metrics.items():
                    metric_values[k].append(v)

        return metric_values


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

    def get_all_metrics(
        self, agg_func="mean"
    ) -> Dict[DeploymentID, Dict[ReplicaID, List[Any]]]:
        return {
            deployment_id: self._autoscaling_states[deployment_id].get_replica_metrics(
                agg_func
            )
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
