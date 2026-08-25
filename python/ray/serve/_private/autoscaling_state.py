import inspect
import logging
import math
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Union

if TYPE_CHECKING:
    import numpy as np
else:  # numpy is only on the columnar (opt-in) path; serve-minimal lacks it.
    try:
        import numpy as np
    except ModuleNotFoundError:
        np = None

from ray.serve._private import autoscaling_metrics_merge
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    ApplicationName,
    AsyncInferenceTaskQueueMetricReport,
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TargetCapacityDirection,
    TimeSeries,
    TimeStampedValue,
)
from ray.serve._private.constants import (
    RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.gang_scheduling_autoscaling_policy import (
    GangSchedulingAutoscalingPolicy,
)
from ray.serve._private.metrics_utils import (
    aggregate_timeseries,
    merge_instantaneous_total,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import get_capacity_adjusted_num_replicas
from ray.serve.autoscaling_policy import (
    _apply_app_level_autoscaling_config,
    _apply_autoscaling_config,
)
from ray.serve.config import AutoscalingContext, AutoscalingPolicy
from ray.util import metrics

if TYPE_CHECKING:
    from ray.serve.config import AutoscalingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _resolve_policy_callable(policy: AutoscalingPolicy) -> Callable:
    """Return a ready-to-call policy callable from an ``AutoscalingPolicy``.

    If the deserialized policy is a class (rather than a plain function),
    instantiate it once — forwarding any ``policy_kwargs`` — so that the
    framework invokes ``instance.__call__(ctx)`` on every autoscaling tick
    instead of ``Class(ctx)`` (which would create a new, stateless instance
    each time).
    """
    raw = policy.get_policy()
    if inspect.isclass(raw):
        logger.info(
            f"Instantiating class-callable autoscaling policy '{raw.__name__}' with kwargs: {policy.policy_kwargs}"
        )
        return raw(**policy.policy_kwargs)
    return raw


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
        # Columnar per-replica running-requests arrays (wire-detected; producers
        # choose the format via should_encode_columnar).
        self._replica_running_arrays: Dict[ReplicaID, tuple] = dict()
        # Non-running columnar metrics per replica (custom autoscaling metrics):
        # replica_id -> {metric_name: (ts_arr, val_arr)}.
        self._replica_custom_arrays: Dict[ReplicaID, Dict[str, tuple]] = dict()
        # Unified per-replica "last accepted report timestamp" across BOTH wire formats.
        # Gates the object AND columnar ingest paths so a delayed report in either format
        # can't overwrite fresher data the other wrote. Cleared only on replica stop --
        # NOT on a cross-format dedup write.
        self._replica_report_ts: Dict[ReplicaID, float] = dict()
        # Columnar per-handle arrays: metadata + per-replica running + queued
        # (filled whenever a columnar frame arrives).
        self._handle_arrays: Dict[str, dict] = dict()
        # Unified per-handle "last accepted report timestamp" (both wire formats) -- same
        # cross-format staleness guard as _replica_report_ts; pruned in
        # drop_stale_handle_metrics.
        self._handle_report_ts: Dict[str, float] = dict()
        # Async inference task queue length (from QueueMonitor).
        # QueueMonitor is a singleton per deployment i.e. we run a single QueueMonitor actor per task consumer (deployment).
        self._total_pending_async_requests: int = 0

        self._deployment_info: Optional[DeploymentInfo] = None
        # Set (non-None) by the first `update_config` call, which happens
        # before any of the methods that read it are called.
        self._config: "AutoscalingConfig" = None  # type: ignore[assignment]
        self._policy: Optional[
            Callable[
                [AutoscalingContext], Tuple[Union[int, float], Optional[Dict[str, Any]]]
            ]
        ] = None
        # user defined policy returns a dictionary of state that is persisted between autoscaling decisions
        # content of the dictionary is determined by the user defined policy
        self._policy_state: Optional[Dict[str, Any]] = None
        self._running_replicas: List[ReplicaID] = []
        self._cached_running_replica_strs: Set[str] = set()
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

        self.autoscaling_target_ongoing_requests_gauge = metrics.Gauge(
            "serve_autoscaling_target_ongoing_requests",
            description=(
                "The configured target number of ongoing requests per replica. "
                "For the default policy, this can be combined with "
                "serve_autoscaling_total_requests to compute the raw desired number "
                "of replicas (total_requests / target_ongoing_requests) and detect "
                "autoscaling regressions."
            ),
            tag_keys=("deployment", "application"),
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
        # Apply default autoscaling config to the policy
        self._policy = _apply_autoscaling_config(
            _resolve_policy_callable(self._config.policy)
        )
        gang_size = getattr(
            info.deployment_config.gang_scheduling_config, "gang_size", None
        )
        if gang_size is not None and gang_size > 1:
            self._policy = GangSchedulingAutoscalingPolicy(self._policy, gang_size)
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
        self._replica_running_arrays.pop(replica_id, None)
        self._replica_custom_arrays.pop(replica_id, None)
        self._replica_report_ts.pop(replica_id, None)

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
        self._cached_running_replica_strs = {
            r.to_full_id_str() for r in running_replicas
        }

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

        # Unified staleness gate across BOTH wire formats (see _replica_report_ts):
        # reject a report older than the last one accepted in EITHER format, so a delayed
        # cloudpickle report can't wipe fresher columnar data (or vice versa).
        last_ts = self._replica_report_ts.get(replica_id)
        if last_ts is None or send_timestamp > last_ts:
            self._replica_metrics[replica_id] = replica_metric_report
            self._replica_report_ts[replica_id] = send_timestamp
            # dedup-at-write: this source now reports via cloudpickle; drop any
            # columnar entries so the stores never double-count it.
            self._replica_running_arrays.pop(replica_id, None)
            self._replica_custom_arrays.pop(replica_id, None)

    def record_columnar_metrics_for_replica(
        self, replica_id, metric_arrays, timestamp
    ) -> None:
        """Store columnar per-metric arrays for a replica (no per-point objects).
        running_requests feeds the hot-path store; any other metrics feed the custom
        store used by custom autoscaling policies (the columnar decode is lossless)."""
        prev_ts = self._replica_report_ts.get(replica_id)
        if prev_ts is not None and timestamp <= prev_ts:
            return
        self._replica_report_ts[replica_id] = timestamp
        running = metric_arrays.get(RUNNING_REQUESTS_KEY)
        if running is not None:
            self._replica_running_arrays[replica_id] = (
                running[0],
                running[1],
                timestamp,
            )
        else:
            # A newer report that omits running_requests must drop the stale running
            # timeseries -- the object path replaces the whole report, so missing
            # running stops contributing there too.
            self._replica_running_arrays.pop(replica_id, None)
        custom = {m: a for m, a in metric_arrays.items() if m != RUNNING_REQUESTS_KEY}
        if custom:
            self._replica_custom_arrays[replica_id] = custom
        else:
            self._replica_custom_arrays.pop(replica_id, None)
        # dedup-at-write: drop any cloudpickle entry for this source.
        self._replica_metrics.pop(replica_id, None)

    def _columnar_aggregate_total_requests(self) -> float:
        """Aggregate-mode total over pure-columnar stores: replica (direct-ingress)
        running arrays when a RUNNING replica reported, else handle running arrays,
        plus queued -- one fused numpy merge (no per-replica Python objects)."""
        # Gate on whether a RUNNING replica actually reported, NOT on the store being
        # non-empty: a lingering stopped-replica array (before on_replica_stopped
        # clears it) must fall through to handle-running exactly like the object
        # path, else handle-collected running is dropped (total reads queued-only).
        replica_segments = self._replica_columnar_segments()
        if replica_segments:
            return self._aggregate_segments(
                replica_segments + self._queued_columnar_segments()
            )
        if not self._handle_arrays:
            return 0.0
        return self._aggregate_segments(
            self._handle_running_columnar_segments(self._cached_running_replica_strs)
            + self._queued_columnar_segments()
        )

    def _replica_columnar_segments(self):
        """RUNNING replicas' columnar running-request arrays as (ts, val) segments."""
        segs = []
        for replica_id in self._running_replicas:
            a = self._replica_running_arrays.get(replica_id)
            if a is not None and a[0].size:
                segs.append((a[0], a[1]))
        return segs

    def _queued_columnar_segments(self):
        """Columnar per-handle queued arrays as (ts, val) segments."""
        return [
            (hm["q_ts"], hm["q_val"])
            for hm in self._handle_arrays.values()
            if hm["q_ts"].size
        ]

    def _handle_running_columnar_segments(self, running):
        """Array-view segments of each columnar handle's running series, masked to
        replicas still in `running` (mirrors _collect_handle_running_requests). Sliced
        at write time; this runs on the 0.1s decision path."""
        return [
            (rts, rval)
            for hm in self._handle_arrays.values()
            for rkey, rts, rval in hm["running_segments"]
            if rkey in running
        ]

    def _series_to_segment(self, series):
        """Object timeseries -> (ts, val) float64 arrays (the cheap direction: object
        sources in a mixed fleet are the THIN ones, few points each)."""
        n = len(series)
        return (
            np.fromiter((p.timestamp for p in series), dtype=np.float64, count=n),
            np.fromiter((p.value for p in series), dtype=np.float64, count=n),
        )

    def _series_segments(self, series_list):
        """Thin object timeseries -> segments (empties dropped)."""
        return [self._series_to_segment(s) for s in series_list if s]

    def _aggregate_segments(self, segments) -> float:
        """One fused numpy merge over (ts, val) array segments. 0.0 when empty."""
        segments = [s for s in segments if s[0].size]
        if not segments:
            return 0.0
        offs = [0]
        for tarr, _ in segments:
            offs.append(offs[-1] + tarr.size)
        return autoscaling_metrics_merge.merge_and_aggregate_arrays(
            np.concatenate([t for t, _ in segments]),
            np.concatenate([v for _, v in segments]),
            np.array(offs, dtype="<i8"),
            time.time(),
            self._config.aggregation_function,
        )

    def record_request_metrics_for_handle(
        self,
        handle_metric_report: HandleMetricReport,
    ) -> None:
        """Records average number of queued and running requests at a handle for this
        deployment.
        """
        handle_id = handle_metric_report.handle_id
        send_timestamp = handle_metric_report.timestamp
        # Unified staleness gate across BOTH wire formats (see _handle_report_ts): a
        # handle flips object<->columnar when it crosses the columnar width gate, so
        # guard against a delayed report in either format wiping the other's data.
        last_ts = self._handle_report_ts.get(handle_id)
        if last_ts is None or send_timestamp > last_ts:
            self._handle_requests[handle_id] = handle_metric_report
            self._handle_report_ts[handle_id] = send_timestamp
            self._handle_arrays.pop(handle_id, None)

    def record_columnar_metrics_for_handle(self, payload: dict) -> None:
        """Store columnar handle metrics (no per-point objects)."""
        hid = payload["handle_id"]
        # Unified staleness gate across BOTH wire formats (see _handle_report_ts).
        last_ts = self._handle_report_ts.get(hid)
        if last_ts is None or payload["timestamp"] > last_ts:
            self._handle_report_ts[hid] = payload["timestamp"]
            mi = payload["mi"]
            # entries/replica_keys/mi are frozen once stored, so slice the running
            # segments here rather than rebuilding them on every 0.1s decision tick.
            p_ts, p_val = payload["ts"], payload["val"]
            running_segments = (
                [
                    (
                        payload["replica_keys"][int(r[1])],
                        p_ts[int(r[2]) : int(r[2]) + int(r[3])],
                        p_val[int(r[2]) : int(r[2]) + int(r[3])],
                    )
                    for r in payload["entries"]
                    if int(r[0]) == mi and int(r[3]) > 0
                ]
                if mi >= 0
                else []
            )
            self._handle_arrays[hid] = {
                "actor_id": payload["actor_id"],
                "is_component": payload["handle_source"]
                in (
                    DeploymentHandleSource.PROXY.value,
                    DeploymentHandleSource.REPLICA.value,
                ),
                "timestamp": payload["timestamp"],
                "ts": payload["ts"],
                "val": payload["val"],
                "entries": payload["entries"],
                "mi": payload["mi"],
                "replica_keys": payload["replica_keys"],
                "q_ts": payload["q_ts"],
                "q_val": payload["q_val"],
                "running_segments": running_segments,
            }
            self._handle_requests.pop(hid, None)

    def record_async_inference_task_queue_metrics(
        self, report: AsyncInferenceTaskQueueMetricReport
    ) -> None:
        """Records task queue length from QueueMonitor for async inference."""
        self._total_pending_async_requests = report.queue_length

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
        for _hid, _hm in list(self._handle_arrays.items()):
            if (
                _hm["is_component"]
                and _hm["actor_id"] is not None
                and _hm["actor_id"] not in alive_serve_actor_ids
            ):
                del self._handle_arrays[_hid]
            elif time.time() - _hm["timestamp"] >= timeout_s:
                del self._handle_arrays[_hid]
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

        # Prune the unified per-handle timestamp gate to handles still tracked in either
        # store (any dropped above no longer appear in _handle_arrays/_handle_requests).
        live_handles = set(self._handle_arrays) | set(self._handle_requests)
        for hid in list(self._handle_report_ts):
            if hid not in live_handles:
                del self._handle_report_ts[hid]

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
        self.autoscaling_target_ongoing_requests_gauge.set(
            self._config.get_target_ongoing_requests(), tags=tags
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
        # The policy can return a float value.
        if isinstance(decision_num_replicas, float):
            decision_num_replicas = math.ceil(decision_num_replicas)
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
            total_pending_async_requests=self._total_pending_async_requests,
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
            running_reqs = handle_metric.metrics.get(RUNNING_REQUESTS_KEY, {})
            for replica_str in self._cached_running_replica_strs:
                if replica_str not in running_reqs:
                    continue
                timeseries_list.append(running_reqs[replica_str])

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

            # Exclude early "partial" period: when series have misaligned start times,
            # late-starting series are implicitly 0 before their first data point, which
            # undercounts the total and biases aggregations. Start the window at the
            # timestamp when all series have contributed at least one point.
            # Use max(aligned_start, merged[0].timestamp) because merge rounds timestamps
            # to 10ms; if aligned_start is before the first merged point, the gap would
            # be treated as 0 and bias the average downward.
            window_start = None
            non_empty_series = [ts for ts in timeseries_list if ts]
            if len(non_empty_series) > 1:
                aligned_start = max(ts[0].timestamp for ts in non_empty_series)
                if aligned_start <= merged_timeseries[-1].timestamp:
                    window_start = max(aligned_start, merged_timeseries[0].timestamp)

            # Calculate the aggregated metric value
            value = aggregate_timeseries(
                merged_timeseries,
                # The field is declared `Union[str, AggregationFunction]`, but a
                # pydantic validator coerces it to `AggregationFunction`.
                aggregation_function=self._config.aggregation_function,  # type: ignore[arg-type]
                last_window_s=last_window_s,
                window_start=window_start,
            )
            return value if value is not None else 0.0

        return 0.0

    def _calculate_total_requests_aggregate_mode(self) -> float:
        """Calculate total requests using aggregate metrics mode with timeseries data.

        This method works with raw timeseries metrics data and performs aggregation
        at the controller level.

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
        has_columnar = bool(self._replica_running_arrays or self._handle_arrays)
        has_object = bool(self._replica_metrics or self._handle_requests)
        # Homogeneous fleets keep their native fast path. Columnar arrays are used
        # whenever present -- the controller wire-detects the format from the frame
        # magic, so it counts columnar reports regardless of how they were produced.
        if has_columnar and not has_object:
            return self._columnar_aggregate_total_requests()
        if has_object and not has_columnar:
            return self._object_aggregate_total_requests()
        if not has_columnar and not has_object:
            return 0.0
        # Mixed sources: merge ALL of them in one pass so the aggregation function is
        # exact (summing two separate aggregations is correct only for MEAN, not
        # MAX/MIN). A steady state, not just mid-rollout -- see
        # _mixed_aggregate_total_requests; the homogeneous fast paths above are
        # unaffected.
        return self._mixed_aggregate_total_requests()

    def _object_aggregate_total_requests(self) -> float:
        """Aggregate-mode total over the cloudpickle/object stores
        (_replica_metrics / _handle_requests). 0 when both are empty."""
        # Only replicas that carry actual running-request data count as "collected
        # on replicas"; an empty running series (no samples) must not suppress
        # handle-side running. Matches the columnar/mixed paths (equivalence).
        replica_timeseries = [
            ts for ts in self._collect_replica_running_requests() if ts
        ]
        metrics_collected_on_replicas = len(replica_timeseries) > 0
        queued_timeseries = self._collect_handle_queued_requests()
        if not metrics_collected_on_replicas:
            handle_timeseries = self._collect_handle_running_requests()
        else:
            handle_timeseries = []
        ongoing_requests_timeseries = []
        ongoing_requests_timeseries.extend(replica_timeseries)
        if not metrics_collected_on_replicas:
            ongoing_requests_timeseries.extend(handle_timeseries)
        ongoing_requests_timeseries.extend(queued_timeseries)
        if not ongoing_requests_timeseries:
            return 0.0
        return self._merge_and_aggregate_timeseries(ongoing_requests_timeseries)

    def _mixed_aggregate_total_requests(self) -> float:
        """Mixed columnar+object total: one fused ARRAY merge over all sources.

        Wide columnar sources are sliced as array views (never re-materialized into
        per-point objects -- mixing is a steady state, e.g. a thin driver handle
        alongside wide proxy handles, so this runs every tick); thin object sources
        are converted to small arrays. Empty object series are dropped so they
        cannot flip metrics_collected_on_replicas and suppress handle-side running
        (mirrors the columnar empty-skip). Disjoint by dedup-at-write."""
        segments = self._replica_columnar_segments()
        segments += self._series_segments(self._collect_replica_running_requests())
        metrics_collected_on_replicas = bool(segments)
        if not metrics_collected_on_replicas:
            segments += self._handle_running_columnar_segments(
                self._cached_running_replica_strs
            )
            segments += self._series_segments(self._collect_handle_running_requests())
        segments += self._queued_columnar_segments()
        segments += self._series_segments(self._collect_handle_queued_requests())
        return self._aggregate_segments(segments)

    def get_total_num_requests(self) -> float:
        """Get average total number of requests aggregated over the past
        `look_back_period_s` number of seconds.

        If there are 0 running replicas, then returns the total number
        of requests queued at handles

        This code assumes that the metrics are either emmited on handles
        or on replicas, but not both. Its the responsibility of the writer
        to ensure enclusivity of the metrics.
        """
        return self._calculate_total_requests_aggregate_mode()

    def get_replica_metrics(self) -> Dict[str, List[TimeSeries]]:
        """Get the raw replica metrics dict."""
        metric_values: Dict[str, List[TimeSeries]] = defaultdict(list)
        for id in self._running_replicas:
            if id in self._replica_metrics and self._replica_metrics[id].metrics:
                for k, v in self._replica_metrics[id].metrics.items():
                    metric_values[k].append(v)

        return metric_values

    def _get_queued_requests(self) -> float:
        """Calculate the total number of queued requests across all handles.

        Returns:
            Sum of queued requests at all handles, aggregated from handle timeseries.
        """
        queued_obj = self._collect_handle_queued_requests()
        if not self._handle_arrays:
            # Pure-object fleet: keep the numpy-free object kernel.
            if not queued_obj:
                return 0.0
            return self._merge_and_aggregate_timeseries(queued_obj)
        # Columnar present: one fused array merge over both queued sources
        # (disjoint by dedup-at-write) -- exact aggregation.
        return self._aggregate_segments(
            self._queued_columnar_segments() + self._series_segments(queued_obj)
        )

    def _aggregate_single_array(self, ts, val, now, agg) -> float:
        """Time-weighted aggregate of a single source's (ts, val) arrays."""
        return autoscaling_metrics_merge.merge_and_aggregate_arrays(
            ts, val, np.array([0, ts.size], dtype="<i8"), now, agg
        )

    def _get_aggregated_custom_metrics(self) -> Dict[str, Dict[ReplicaID, float]]:
        """Aggregate custom metrics from replica metric reports.

        This method aggregates raw timeseries data from replicas on the controller,
        similar to how ongoing requests are aggregated.

        Returns:
            Dict mapping metric name to dict of replica ID to aggregated metric value.
        """
        aggregated_metrics: Dict[str, Dict[ReplicaID, float]] = defaultdict(dict)
        now = time.time()
        agg = self._config.aggregation_function
        for replica_id in self._running_replicas:
            # A replica is in the object store OR the columnar stores (dedup-at-write).
            replica_metric_report = self._replica_metrics.get(replica_id)
            if replica_metric_report is not None:
                for metric_name, timeseries in replica_metric_report.metrics.items():
                    aggregated_metrics[metric_name][
                        replica_id
                    ] = self._merge_and_aggregate_timeseries([timeseries])
                continue
            running = self._replica_running_arrays.get(replica_id)
            if running is not None and running[0].size:
                aggregated_metrics[RUNNING_REQUESTS_KEY][
                    replica_id
                ] = self._aggregate_single_array(running[0], running[1], now, agg)
            custom = self._replica_custom_arrays.get(replica_id)
            if custom:
                for metric_name, (ts, val) in custom.items():
                    if ts.size:
                        aggregated_metrics[metric_name][
                            replica_id
                        ] = self._aggregate_single_array(ts, val, now, agg)
        return dict(aggregated_metrics)

    def _get_raw_custom_metrics(
        self,
    ) -> Dict[str, Dict[ReplicaID, TimeSeries]]:
        """Extract raw custom metric values from replica metric reports.

        Returns:
            Dict mapping metric name to dict of replica ID to raw metric timeseries.
        """
        raw_metrics: Dict[str, Dict[ReplicaID, TimeSeries]] = defaultdict(dict)
        for replica_id in self._running_replicas:
            replica_metric_report = self._replica_metrics.get(replica_id)
            if replica_metric_report is not None:
                for metric_name, timeseries in replica_metric_report.metrics.items():
                    raw_metrics[metric_name][replica_id] = timeseries
                continue
            running = self._replica_running_arrays.get(replica_id)
            if running is not None and running[0].size:
                raw_metrics[RUNNING_REQUESTS_KEY][replica_id] = [
                    TimeStampedValue(float(running[0][k]), float(running[1][k]))
                    for k in range(running[0].size)
                ]
            custom = self._replica_custom_arrays.get(replica_id)
            if custom:
                for metric_name, (ts, val) in custom.items():
                    raw_metrics[metric_name][replica_id] = [
                        TimeStampedValue(float(ts[k]), float(val[k]))
                        for k in range(ts.size)
                    ]
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
                Tuple[
                    Dict[DeploymentID, Union[int, float]],
                    Optional[Dict[DeploymentID, Dict]],
                ],
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
        # Apply default autoscaling config to the policy
        self._policy = _apply_app_level_autoscaling_config(  # type: ignore[assignment]
            _resolve_policy_callable(autoscaling_policy)
        )
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
            # `self._policy` is non-None here (guarded by `has_policy()` above).
            decisions, returned_policy_state = self._policy(  # type: ignore[misc]
                autoscaling_contexts
            )
            policy_execution_time_ms = (time.time() - start_time) * 1000
            # Validate returned policy_state
            self._validate_policy_state(returned_policy_state)
            self._policy_state = returned_policy_state

            # Validate returned decisions
            assert isinstance(
                decisions, dict
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
                    num_replicas,  # type: ignore[arg-type]
                    autoscaling_contexts[deployment_id].total_num_requests,
                    policy_execution_time_ms,
                    "application",
                )
                results[deployment_id] = (
                    self._deployment_autoscaling_states[deployment_id].apply_bounds(
                        math.ceil(num_replicas)
                    )
                    if not _skip_bound_check
                    else math.ceil(num_replicas)
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

    def record_columnar_metrics_for_replica(
        self, replica_id, metric_arrays, timestamp
    ) -> None:
        dep_id = replica_id.deployment_id
        if dep_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                dep_id
            ].record_columnar_metrics_for_replica(replica_id, metric_arrays, timestamp)

    def record_request_metrics_for_handle(
        self, handle_metric_report: HandleMetricReport
    ):
        dep_id = handle_metric_report.deployment_id
        if dep_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                dep_id
            ].record_request_metrics_for_handle(handle_metric_report)

    def record_columnar_metrics_for_handle(self, payload: dict) -> None:
        dep_id = payload["deployment_id"]
        if dep_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                dep_id
            ].record_columnar_metrics_for_handle(payload)

    def record_async_inference_task_queue_metrics(
        self, report: AsyncInferenceTaskQueueMetricReport
    ):
        """Record async inference task queue metrics for a deployment."""
        if report.deployment_id in self._deployment_autoscaling_states:
            self._deployment_autoscaling_states[
                report.deployment_id
            ].record_async_inference_task_queue_metrics(report)

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
    ) -> Dict[str, List[TimeSeries]]:
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

    def record_columnar_metrics_for_replica(
        self, replica_id, metric_arrays, timestamp
    ) -> None:
        app_state = self._app_autoscaling_states.get(replica_id.deployment_id.app_name)
        if app_state:
            app_state.record_columnar_metrics_for_replica(
                replica_id, metric_arrays, timestamp
            )

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

    def record_columnar_metrics_for_handle(self, payload: dict) -> None:
        app_state = self._app_autoscaling_states.get(payload["deployment_id"].app_name)
        if app_state:
            app_state.record_columnar_metrics_for_handle(payload)

    def record_async_inference_task_queue_metrics(
        self,
        report: AsyncInferenceTaskQueueMetricReport,
    ) -> None:
        """Record async inference task queue metrics from QueueMonitor."""
        app_state = self._app_autoscaling_states.get(report.deployment_id.app_name)
        if app_state:
            app_state.record_async_inference_task_queue_metrics(report)

    def drop_stale_handle_metrics(self, alive_serve_actor_ids: Set[str]) -> None:
        for app_state in self._app_autoscaling_states.values():
            app_state.drop_stale_handle_metrics(alive_serve_actor_ids)
