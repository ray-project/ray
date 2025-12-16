import logging
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    ApplicationName,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TargetCapacityDirection,
    TimeSeries,
)
from ray.serve._private.constants import (
    RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER,
    RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.metrics_utils import (
    aggregate_timeseries,
    merge_instantaneous_total,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.config import AutoscalingContext, AutoscalingPolicy
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)


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
        # Prometheus + Custom metrics from each replica are also included
        self._replica_metrics: Dict[ReplicaID, ReplicaMetricReport] = dict()

        self._deployment_info = None
        self._config = None
        self._policy: Optional[
            Callable[[AutoscalingContext], Tuple[int, Optional[Dict[str, Any]]]]
        ] = None
        # user defined policy returns a dictionary of state that is persisted between autoscaling decisions
        # content of the dictionary is determined by the user defined policy
        self._policy_state: Optional[Dict[str, Any]] = None
        self._running_replicas: List[ReplicaID] = []
        self._target_capacity: Optional[float] = None
        self._target_capacity_direction: Optional[TargetCapacityDirection] = None
        # Track timestamps of last scale up and scale down events
        self._last_scale_up_time: Optional[float] = None
        self._last_scale_down_time: Optional[float] = None

        self.autoscaling_decision_gauge = metrics.Gauge(
            "serve_autoscaling_desired_replicas",
            description=(
                "The raw autoscaling decision (number of replicas) from the autoscaling "
                "policy before applying min/max bounds."
            ),
            tag_keys=("deployment", "application"),
        )

        self.autoscaling_total_requests_gauge = metrics.Gauge(
            "serve_autoscaling_total_requests",
            description=(
                "Total number of requests as seen by the autoscaler. This is the input "
                "to the autoscaling decision."
            ),
            tag_keys=("deployment", "application"),
        )

        self.autoscaling_policy_execution_time_gauge = metrics.Gauge(
            "serve_autoscaling_policy_execution_time_ms",
            description=(
                "Time taken to execute the autoscaling policy in milliseconds. "
                "High values may indicate a slow or complex policy."
            ),
            tag_keys=("deployment", "application", "policy_scope"),
        )

    def register(self, info: DeploymentInfo, curr_target_num_replicas: int) -> int:
        """Registers an autoscaling deployment's info.

        Returns the number of replicas the target should be set to.
        """

        config = info.deployment_config.autoscaling_config
        if config is None:
            raise ValueError(
                f"Autoscaling config is not set for deployment {self._deployment_id}"
            )
        if (
            self._deployment_info is None or self._deployment_info.config_changed(info)
        ) and config.initial_replicas is not None:
            target_num_replicas = config.initial_replicas
        else:
            target_num_replicas = curr_target_num_replicas

        self._deployment_info = info
        self._config = config
        self._policy = self._config.policy.get_policy()
        self._target_capacity = info.target_capacity
        self._target_capacity_direction = info.target_capacity_direction
        self._policy_state = {}

        # Log when custom autoscaling policy is used for deployment
        if not self._config.policy.is_default_policy_function():
            logger.info(
                f"Using custom autoscaling policy '{self._config.policy.policy_function}' "
                f"for deployment '{self._deployment_id}'."
            )
            # Record telemetry for custom autoscaling policy usage
            ServeUsageTag.CUSTOM_AUTOSCALING_POLICY_USED.record("1")

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

    def record_scale_up(self):
        """Record a scale up event by updating the timestamp."""
        self._last_scale_up_time = time.time()

    def record_scale_down(self):
        """Record a scale down event by updating the timestamp."""
        self._last_scale_down_time = time.time()

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

    def record_autoscaling_metrics(
        self,
        decision_num_replicas: int,
        total_num_requests: float,
        policy_execution_time_ms: float,
        policy_scope: str,
    ):
        tags = {
            "deployment": self._deployment_id.name,
            "application": self._deployment_id.app_name,
        }
        self.autoscaling_decision_gauge.set(decision_num_replicas, tags=tags)
        self.autoscaling_total_requests_gauge.set(total_num_requests, tags=tags)
        self.autoscaling_policy_execution_time_gauge.set(
            policy_execution_time_ms, tags={**tags, "policy_scope": policy_scope}
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
        if self._policy is None:
            raise ValueError(f"Policy is not set for deployment {self._deployment_id}.")
        autoscaling_context = self.get_autoscaling_context(curr_target_num_replicas)

        # Time the policy execution
        start_time = time.time()
        decision_num_replicas, self._policy_state = self._policy(autoscaling_context)
        policy_execution_time_ms = (time.time() - start_time) * 1000

        self.record_autoscaling_metrics(
            decision_num_replicas,
            autoscaling_context.total_num_requests,
            policy_execution_time_ms,
            "deployment",
        )

        if _skip_bound_check:
            return decision_num_replicas

        return self.apply_bounds(decision_num_replicas)

    def get_autoscaling_context(
        self,
        curr_target_num_replicas,
        override_policy_state: Optional[Dict[str, Any]] = None,
    ) -> AutoscalingContext:
        # Adding this to overwrite policy state during application level autoscaling
        if override_policy_state is not None:
            current_policy_state = override_policy_state.copy()
        elif self._policy_state is not None:
            current_policy_state = self._policy_state.copy()
        else:
            current_policy_state = {}
        return AutoscalingContext(
            deployment_id=self._deployment_id,
            deployment_name=self._deployment_id.name,
            app_name=self._deployment_id.app_name,
            current_num_replicas=len(self._running_replicas),
            target_num_replicas=curr_target_num_replicas,
            running_replicas=self._running_replicas,
            total_num_requests=self.get_total_num_requests,
            capacity_adjusted_min_replicas=self.get_num_replicas_lower_bound(),
            capacity_adjusted_max_replicas=self.get_num_replicas_upper_bound(),
            policy_state=current_policy_state,
            current_time=time.time(),
            config=self._config,
            total_queued_requests=self._get_queued_requests,
            aggregated_metrics=self._get_aggregated_custom_metrics,
            raw_metrics=self._get_raw_custom_metrics,
            last_scale_up_time=self._last_scale_up_time,
            last_scale_down_time=self._last_scale_down_time,
        )

    def _collect_replica_running_requests(self) -> List[TimeSeries]:
        """Collect running requests timeseries from replicas for aggregation.

        Returns:
            List of timeseries data.
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

    def _collect_handle_queued_requests(self) -> List[TimeSeries]:
        """Collect queued requests timeseries from all handles.

        Returns:
            List of timeseries data.
        """
        timeseries_list = []
        for handle_metric_report in self._handle_requests.values():
            timeseries_list.append(handle_metric_report.queued_requests)
        return timeseries_list

    def _collect_handle_running_requests(self) -> List[TimeSeries]:
        """Collect running requests timeseries from handles when not collected on replicas.

        Returns:
            List of timeseries data.

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

    def _merge_and_aggregate_timeseries(
        self,
        timeseries_list: List[TimeSeries],
    ) -> float:
        """Aggregate and average a metric from timeseries data using instantaneous merge.

        Args:
            timeseries_list: A list of TimeSeries (TimeSeries), where each
                TimeSeries represents measurements from a single source (replica, handle, etc.).
                Each list is sorted by timestamp ascending.

        Returns:
            The time-weighted average of the metric

        Example:
            If the timeseries_list is:
            [
                [
                    TimeStampedValue(timestamp=0.1, value=5.0),
                    TimeStampedValue(timestamp=0.2, value=7.0),
                ],
                [
                    TimeStampedValue(timestamp=0.2, value=3.0),
                    TimeStampedValue(timestamp=0.3, value=1.0),
                ]
            ]
            Then the returned value will be:
            (5.0*0.1 + 7.0*0.2 + 3.0*0.2 + 1.0*0.3) / (0.1 + 0.2 + 0.2 + 0.3) = 4.5 / 0.8 = 5.625
        """

        if not timeseries_list:
            return 0.0

        # Use instantaneous merge approach - no arbitrary windowing needed
        merged_timeseries = merge_instantaneous_total(timeseries_list)
        if merged_timeseries:
            # assume that the last recorded metric is valid for last_window_s seconds
            last_metric_time = merged_timeseries[-1].timestamp
            # we dont want to make any assumption about how long the last metric will be valid
            # only conclude that the last metric is valid for last_window_s seconds that is the
            # difference between the current time and the last metric recorded time
            last_window_s = time.time() - last_metric_time
            # adding a check to negative values caused by clock skew
            # between replicas and controller. Also add a small epsilon to avoid division by zero
            if last_window_s <= 0:
                last_window_s = 1e-3
            # Calculate the aggregated metric value
            value = aggregate_timeseries(
                merged_timeseries,
                aggregation_function=self._config.aggregation_function,
                last_window_s=last_window_s,
            )
            return value if value is not None else 0.0

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
        # Collect replica-based running requests (returns List[TimeSeries])
        replica_timeseries = self._collect_replica_running_requests()
        metrics_collected_on_replicas = len(replica_timeseries) > 0

        # Collect queued requests from handles (returns List[TimeSeries])
        queued_timeseries = self._collect_handle_queued_requests()

        if not metrics_collected_on_replicas:
            # Collect handle-based running requests if not collected on replicas
            handle_timeseries = self._collect_handle_running_requests()
        else:
            handle_timeseries = []

        # Collect all timeseries for ongoing requests
        ongoing_requests_timeseries = []

        # Add replica timeseries
        ongoing_requests_timeseries.extend(replica_timeseries)

        # Add handle timeseries if replica metrics weren't collected
        if not metrics_collected_on_replicas:
            ongoing_requests_timeseries.extend(handle_timeseries)

        # Add queued timeseries
        ongoing_requests_timeseries.extend(queued_timeseries)

        # Aggregate and add running requests to total
        ongoing_requests = self._merge_and_aggregate_timeseries(
            ongoing_requests_timeseries
        )

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

    def get_replica_metrics(self) -> Dict[ReplicaID, List[TimeSeries]]:
        """Get the raw replica metrics dict."""
        metric_values = defaultdict(list)
        for id in self._running_replicas:
            if id in self._replica_metrics and self._replica_metrics[id].metrics:
                for k, v in self._replica_metrics[id].metrics.items():
                    metric_values[k].append(v)

        return metric_values

    def _get_queued_requests(self) -> float:
        """Calculate the total number of queued requests across all handles.

        Returns:
            Sum of queued requests at all handles. Uses aggregated values in simple mode,
            or aggregates timeseries data in aggregate mode.
        """
        if RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER:
            # Aggregate mode: collect and aggregate timeseries
            queued_timeseries = self._collect_handle_queued_requests()
            if not queued_timeseries:
                return 0.0

            return self._merge_and_aggregate_timeseries(queued_timeseries)
        else:
            # Simple mode: sum pre-aggregated values
            return sum(
                handle_metric.aggregated_queued_requests
                for handle_metric in self._handle_requests.values()
            )

    def _get_aggregated_custom_metrics(self) -> Dict[str, Dict[ReplicaID, float]]:
        """Aggregate custom metrics from replica metric reports.

        This method aggregates raw timeseries data from replicas on the controller,
        similar to how ongoing requests are aggregated.

        Returns:
            Dict mapping metric name to dict of replica ID to aggregated metric value.
        """
        aggregated_metrics = defaultdict(dict)

        for replica_id in self._running_replicas:
            replica_metric_report = self._replica_metrics.get(replica_id)
            if replica_metric_report is None:
                continue

            for metric_name, timeseries in replica_metric_report.metrics.items():
                # Aggregate the timeseries for this custom metric
                aggregated_value = self._merge_and_aggregate_timeseries([timeseries])
                aggregated_metrics[metric_name][replica_id] = aggregated_value

        return dict(aggregated_metrics)

    def _get_raw_custom_metrics(
        self,
    ) -> Dict[str, Dict[ReplicaID, TimeSeries]]:
        """Extract raw custom metric values from replica metric reports.

        Returns:
            Dict mapping metric name to dict of replica ID to raw metric timeseries.
        """
        raw_metrics = defaultdict(dict)

        for replica_id in self._running_replicas:
            replica_metric_report = self._replica_metrics.get(replica_id)
            if replica_metric_report is None:
                continue

            for metric_name, timeseries in replica_metric_report.metrics.items():
                # Extract values from TimeStampedValue list
                raw_metrics[metric_name][replica_id] = timeseries

        return dict(raw_metrics)


class ApplicationAutoscalingState:
    """Manages autoscaling for a single application."""

    def __init__(
        self,
        app_name: ApplicationName,
    ):
        self._app_name = app_name
        self._deployment_autoscaling_states: Dict[
            DeploymentID, DeploymentAutoscalingState
        ] = {}
        self._policy: Optional[
            Callable[
                [Dict[DeploymentID, AutoscalingContext]],
                Tuple[Dict[DeploymentID, int], Optional[Dict[DeploymentID, Dict]]],
            ]
        ] = None
        # user defined policy returns a dictionary of state that is persisted between autoscaling decisions
        # content of the dictionary is determined by the user defined policy but is keyed by deployment id
        self._policy_state: Optional[Dict[DeploymentID, Dict]] = None

    @property
    def deployments(self):
        return self._deployment_autoscaling_states.keys()

    def register(
        self,
        autoscaling_policy: AutoscalingPolicy,
    ):
        """Register or update application-level autoscaling config and deployments.

        This will overwrite the deployment-level policies with the application-level policy.

        Args:
            autoscaling_policy: The autoscaling policy to register.
        """
        self._policy = autoscaling_policy.get_policy()
        self._policy_state = {}

        # Log when custom autoscaling policy is used for application
        if not autoscaling_policy.is_default_policy_function():
            logger.info(
                f"Using custom autoscaling policy '{autoscaling_policy.policy_function}' "
                f"for application '{self._app_name}'."
            )
            # Record telemetry for custom autoscaling policy usage
            ServeUsageTag.CUSTOM_AUTOSCALING_POLICY_USED.record("1")

    def has_policy(self) -> bool:
        return self._policy is not None

    def register_deployment(
        self,
        deployment_id: DeploymentID,
        info: DeploymentInfo,
        curr_target_num_replicas: int,
    ) -> int:
        """Register a single deployment under this application."""
        if deployment_id not in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                deployment_id
            ] = DeploymentAutoscalingState(deployment_id)

        if info.deployment_config.autoscaling_config is None:
            raise ValueError(
                f"Autoscaling config is not set for deployment {deployment_id}"
            )

        # if the deployment-level policy is not the default policy, and the application has a policy,
        # warn the user that the application-level policy will take precedence
        if (
            not info.deployment_config.autoscaling_config.policy.is_default_policy_function()
            and self.has_policy()
        ):
            logger.warning(
                f"User provided both a deployment-level and an application-level policy for deployment {deployment_id}. "
                "The application-level policy will take precedence."
            )

        return self._deployment_autoscaling_states[deployment_id].register(
            info,
            curr_target_num_replicas,
        )

    def deregister_deployment(self, deployment_id: DeploymentID):
        if deployment_id not in self._deployment_autoscaling_states:
            logger.warning(
                f"Cannot deregister autoscaling state for deployment {deployment_id} because it is not registered"
            )
            return
        self._deployment_autoscaling_states.pop(deployment_id)

    def should_autoscale_deployment(self, deployment_id: DeploymentID):
        return deployment_id in self._deployment_autoscaling_states

    def _validate_policy_state(
        self, policy_state: Optional[Dict[DeploymentID, Dict[str, Any]]]
    ):
        """Validate that the returned policy_state from an application-level policy is correctly formatted."""
        if policy_state is None:
            return

        assert isinstance(
            policy_state, dict
        ), "Application-level autoscaling policy must return policy_state as Dict[DeploymentID, Dict[str, Any]]"

        # Check that all keys are valid deployment IDs
        for deployment_id in policy_state.keys():
            assert (
                deployment_id in self._deployment_autoscaling_states
            ), f"Policy state contains invalid deployment ID: {deployment_id}"
            assert isinstance(
                policy_state[deployment_id], dict
            ), f"Policy state for deployment {deployment_id} must be a dictionary, got {type(policy_state[deployment_id])}"

    def get_decision_num_replicas(
        self,
        deployment_to_target_num_replicas: Dict[DeploymentID, int],
        _skip_bound_check: bool = False,
    ) -> Dict[DeploymentID, int]:
        """
        Decide scaling for all deployments in this application by calling
        each deployment's autoscaling policy.
        """
        if self.has_policy():
            # Using app-level policy
            autoscaling_contexts = {
                deployment_id: state.get_autoscaling_context(
                    deployment_to_target_num_replicas[deployment_id],
                    self._policy_state.get(deployment_id, {})
                    if self._policy_state
                    else {},
                )
                for deployment_id, state in self._deployment_autoscaling_states.items()
            }
            # Time the policy execution
            start_time = time.time()
            # Policy returns decisions: {deployment_id -> decision} and
            # policy state: {deployment_id -> Dict}
            decisions, returned_policy_state = self._policy(autoscaling_contexts)
            policy_execution_time_ms = (time.time() - start_time) * 1000
            # Validate returned policy_state
            self._validate_policy_state(returned_policy_state)
            self._policy_state = returned_policy_state

            # Validate returned decisions
            assert (
                type(decisions) is dict
            ), "Autoscaling policy must return a dictionary of deployment_name -> decision_num_replicas"

            # assert that deployment_id is in decisions is valid
            for deployment_id in decisions.keys():
                assert (
                    deployment_id in self._deployment_autoscaling_states
                ), f"Deployment {deployment_id} is not registered"
                assert (
                    deployment_id in deployment_to_target_num_replicas
                ), f"Deployment {deployment_id} is invalid"

            results = {}
            for deployment_id, num_replicas in decisions.items():
                deployment_autoscaling_state = self._deployment_autoscaling_states[
                    deployment_id
                ]
                deployment_autoscaling_state.record_autoscaling_metrics(
                    num_replicas,
                    autoscaling_contexts[deployment_id].total_num_requests,
                    policy_execution_time_ms,
                    "application",
                )
                results[deployment_id] = (
                    self._deployment_autoscaling_states[deployment_id].apply_bounds(
                        num_replicas
                    )
                    if not _skip_bound_check
                    else num_replicas
                )
            return results
        else:
            # Using deployment-level policy
            return {
                deployment_id: deployment_autoscaling_state.get_decision_num_replicas(
                    curr_target_num_replicas=deployment_to_target_num_replicas[
                        deployment_id
                    ],
                    _skip_bound_check=_skip_bound_check,
                )
                for deployment_id, deployment_autoscaling_state in self._deployment_autoscaling_states.items()
            }

    def update_running_replica_ids(
        self, deployment_id: DeploymentID, running_replicas: List[ReplicaID]
    ):
        self._deployment_autoscaling_states[deployment_id].update_running_replica_ids(
            running_replicas
        )

    def record_scale_up(self, deployment_id: DeploymentID):
        """Record a scale up event for a deployment."""
        if deployment_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[deployment_id].record_scale_up()

    def record_scale_down(self, deployment_id: DeploymentID):
        """Record a scale down event for a deployment."""
        if deployment_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[deployment_id].record_scale_down()

    def on_replica_stopped(self, replica_id: ReplicaID):
        dep_id = replica_id.deployment_id
        if dep_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[dep_id].on_replica_stopped(replica_id)

    def get_total_num_requests_for_deployment(
        self, deployment_id: DeploymentID
    ) -> float:
        return self._deployment_autoscaling_states[
            deployment_id
        ].get_total_num_requests()

    def get_replica_metrics_by_deployment_id(self, deployment_id: DeploymentID):
        return self._deployment_autoscaling_states[deployment_id].get_replica_metrics()

    def is_within_bounds(
        self, deployment_id: DeploymentID, num_replicas_running_at_target_version: int
    ) -> bool:
        return self._deployment_autoscaling_states[deployment_id].is_within_bounds(
            num_replicas_running_at_target_version
        )

    def record_request_metrics_for_replica(
        self, replica_metric_report: ReplicaMetricReport
    ):
        dep_id = replica_metric_report.replica_id.deployment_id
        # Defensively guard against delayed replica metrics arriving
        # after the deployment's been deleted
        if dep_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                dep_id
            ].record_request_metrics_for_replica(replica_metric_report)

    def record_request_metrics_for_handle(
        self, handle_metric_report: HandleMetricReport
    ):
        dep_id = handle_metric_report.deployment_id
        if dep_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                dep_id
            ].record_request_metrics_for_handle(handle_metric_report)

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]):
        """Drops handle metrics that are no longer valid.

        This includes handles that live on Serve Proxy or replica actors
        that have died AND handles from which the controller hasn't
        received an update for too long.
        """
        for dep_state in self._deployment_autoscaling_states.values():
            dep_state.drop_stale_handle_metrics(alive_serve_actor_ids)


class AutoscalingStateManager:
    """Manages all things autoscaling related.

    Keeps track of request metrics for each application and its deployments,
    and decides on the target number of replicas to autoscale to.
    """

    def __init__(self):
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
        app_name = deployment_id.app_name
        app_state = self._app_autoscaling_states.setdefault(
            app_name, ApplicationAutoscalingState(app_name)
        )
        logger.info(f"Registering autoscaling state for deployment {deployment_id}")
        return app_state.register_deployment(
            deployment_id, info, curr_target_num_replicas
        )

    def deregister_deployment(self, deployment_id: DeploymentID):
        """Remove deployment from tracking."""
        app_state = self._app_autoscaling_states.get(deployment_id.app_name)
        if app_state:
            logger.info(
                f"Deregistering autoscaling state for deployment {deployment_id}"
            )
            app_state.deregister_deployment(deployment_id)

    def register_application(
        self,
        app_name: ApplicationName,
        autoscaling_policy: AutoscalingPolicy,
    ):
        app_state = self._app_autoscaling_states.setdefault(
            app_name, ApplicationAutoscalingState(app_name)
        )
        logger.info(f"Registering autoscaling state for application {app_name}")
        app_state.register(autoscaling_policy)

    def deregister_application(self, app_name: ApplicationName):
        """Remove application from tracking."""
        if app_name in self._app_autoscaling_states:
            logger.info(f"Deregistering autoscaling state for application {app_name}")
            self._app_autoscaling_states.pop(app_name, None)

    def _application_has_policy(self, app_name: ApplicationName) -> bool:
        return (
            app_name in self._app_autoscaling_states
            and self._app_autoscaling_states[app_name].has_policy()
        )

    def get_decision_num_replicas(
        self,
        app_name: ApplicationName,
        deployment_to_target_num_replicas: Dict[DeploymentID, int],
    ) -> Dict[DeploymentID, int]:
        """
        Decide scaling for all deployments in the application.

        Args:
            app_name: The name of the application.
            deployment_to_target_num_replicas: A dictionary of deployment_id to target number of replicas.

        Returns:
            A dictionary of deployment_id to decision number of replicas.
        """
        return self._app_autoscaling_states[app_name].get_decision_num_replicas(
            deployment_to_target_num_replicas
        )

    def should_autoscale_application(self, app_name: ApplicationName):
        return app_name in self._app_autoscaling_states

    def should_autoscale_deployment(self, deployment_id: DeploymentID):
        return (
            deployment_id.app_name in self._app_autoscaling_states
            and self._app_autoscaling_states[
                deployment_id.app_name
            ].should_autoscale_deployment(deployment_id)
        )

    def update_running_replica_ids(
        self, deployment_id: DeploymentID, running_replicas: List[ReplicaID]
    ):
        app_state = self._app_autoscaling_states.get(deployment_id.app_name)
        if app_state:
            app_state.update_running_replica_ids(deployment_id, running_replicas)

    def record_scale_up(self, deployment_id: DeploymentID):
        """Record a scale up event for a deployment.

        Args:
            deployment_id: The ID of the deployment being scaled up.
        """
        app_state = self._app_autoscaling_states.get(deployment_id.app_name)
        if app_state:
            app_state.record_scale_up(deployment_id)

    def record_scale_down(self, deployment_id: DeploymentID):
        """Record a scale down event for a deployment.

        Args:
            deployment_id: The ID of the deployment being scaled down.
        """
        app_state = self._app_autoscaling_states.get(deployment_id.app_name)
        if app_state:
            app_state.record_scale_down(deployment_id)

    def on_replica_stopped(self, replica_id: ReplicaID):
        app_state = self._app_autoscaling_states.get(replica_id.deployment_id.app_name)
        if app_state:
            app_state.on_replica_stopped(replica_id)

    def get_metrics_for_deployment(
        self, deployment_id: DeploymentID
    ) -> Dict[ReplicaID, List[TimeSeries]]:
        if deployment_id.app_name in self._app_autoscaling_states:
            return self._app_autoscaling_states[
                deployment_id.app_name
            ].get_replica_metrics_by_deployment_id(deployment_id)
        else:
            return {}

    def get_total_num_requests_for_deployment(
        self, deployment_id: DeploymentID
    ) -> float:
        if deployment_id.app_name in self._app_autoscaling_states:
            return self._app_autoscaling_states[
                deployment_id.app_name
            ].get_total_num_requests_for_deployment(deployment_id)
        else:
            return 0

    def is_within_bounds(
        self, deployment_id: DeploymentID, num_replicas_running_at_target_version: int
    ) -> bool:
        app_state = self._app_autoscaling_states[deployment_id.app_name]
        return app_state.is_within_bounds(
            deployment_id, num_replicas_running_at_target_version
        )

    def record_request_metrics_for_replica(
        self, replica_metric_report: ReplicaMetricReport
    ) -> None:
        app_state = self._app_autoscaling_states.get(
            replica_metric_report.replica_id.deployment_id.app_name
        )
        if app_state:
            app_state.record_request_metrics_for_replica(replica_metric_report)

    def record_request_metrics_for_handle(
        self,
        handle_metric_report: HandleMetricReport,
    ) -> None:
        """Update request metric for a specific handle."""
        app_state = self._app_autoscaling_states.get(
            handle_metric_report.deployment_id.app_name
        )
        if app_state:
            app_state.record_request_metrics_for_handle(handle_metric_report)

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        for app_state in self._app_autoscaling_states.values():
            app_state.drop_stale_handle_metrics(alive_serve_actor_ids)
