"""Background poller for cluster usage metrics.

A single process-global daemon thread samples the cluster metrics on an interval
and caches the latest values. Each execution records its own start baseline
(keyed by execution id) when it begins, and asks the poller to compute the delta
between the latest values and that baseline when it ends — so the usage
callback never blocks the execution path on a metric query and never has to
track start/end samples itself.
"""

import logging
import threading
import time
from collections import OrderedDict
from typing import Callable, Dict, Optional

from ray.data._internal.usage import collector, util

logger = logging.getLogger(__name__)

# Defaults to 10s to match Ray's Prometheus scrape interval
_POLL_INTERVAL_S = 10

# The value of a single cluster metric (None when its query failed / is unavailable).
MetricValue = Optional[int]

MetricFn = Callable[[], MetricValue]


class ClusterMetricsPoller:
    def __init__(
        self,
        metrics: Dict[str, MetricFn],
        interval_s: float = _POLL_INTERVAL_S,
    ):
        # The metrics to poll, keyed by name of metric defined in PipelinePerf
        self._metrics = metrics
        # The interval at which to poll the metrics
        self._interval_s = interval_s
        # Serializes access to poller state, since the poll thread writes
        # _latest while the driver thread reads _latest/_baselines.
        self._lock = threading.Lock()
        # The most recent poll result, refreshed every interval by the loop.
        self._latest: Dict[str, MetricValue] = {}
        # execution_id -> that execution's baseline to compute the cluster metric delta
        self._baselines: "OrderedDict[str, Dict[str, MetricValue]]" = OrderedDict()
        # The poll thread. Started on the first execution and runs as a daemon
        # until the driver process exits.
        self._thread: Optional[threading.Thread] = None

    def record_start(self, execution_id: str) -> None:
        """Called at an execution's start. Starts the poll loop and captures
        this execution's start baseline metric values in the background,
        so the driver never blocks on a metric query."""
        self._ensure_running()
        util.run_async(lambda: self._capture_baseline(execution_id))

    def compute_deltas(self, execution_id: str) -> Dict[str, MetricValue]:
        """Called at an execution's end. Return the per-metric delta between the
        latest polled values and this execution's start baseline. Missing on
        either side degrades that metric to None"""
        with self._lock:
            baseline = self._baselines.get(execution_id, {})
            latest = dict(self._latest)
        return {
            name: collector.compute_delta(baseline.get(name), latest.get(name))
            for name in self._metrics
        }

    def _capture_baseline(self, execution_id: str) -> None:
        """Sample every metric and store the values as ``execution_id``'s
        baseline, evicting the oldest baseline when at capacity."""
        values = self._sample_all_metrics()
        with self._lock:
            if (
                execution_id not in self._baselines
                and len(self._baselines) >= collector._MAX_EXECUTIONS_TO_TRACK
            ):
                self._baselines.popitem(last=False)
            self._baselines[execution_id] = values

    def _ensure_running(self) -> None:
        """Start the poll thread if it isn't already running. Called at each
        execution's start."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="data-usage-metrics-poller", daemon=True
            )
            self._thread.start()

    def _sample_all_metrics(self) -> Dict[str, MetricValue]:
        """Sample every metric concurrently and return the joined values.
        Shared firing logic for the poll loop and the per-execution baseline."""
        names = list(self._metrics)
        futures = [util.run_async(self._metrics[name]) for name in names]
        return dict(zip(names, util.join_async(futures)))

    def poll_once(self) -> None:
        """Sample every metric concurrently and publish the values as latest."""
        values = self._sample_all_metrics()
        with self._lock:
            self._latest = values

    def _run(self) -> None:
        while True:
            try:
                self.poll_once()
            except Exception:
                logger.debug("Cluster metrics poll failed", exc_info=True)
            time.sleep(self._interval_s)


_poller: Optional[ClusterMetricsPoller] = None
_poller_lock = threading.Lock()


def get_poller() -> ClusterMetricsPoller:
    """The cluster metrics poller (created on first use)."""
    global _poller
    with _poller_lock:
        if _poller is None:
            _poller = ClusterMetricsPoller(
                {
                    collector.METRIC_BYTES_SPILLED: collector.cluster_spilled_bytes,
                    collector.METRIC_NODE_DEATHS: collector.cluster_dead_node_count,
                    collector.METRIC_OOM_KILLS: collector.cluster_oom_kills,
                    collector.METRIC_UNEXPECTED_WORKER_KILLS: (
                        collector.cluster_unexpected_worker_kills
                    ),
                }
            )
        return _poller
