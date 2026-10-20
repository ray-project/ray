"""Background poller for cluster usage metrics.

A single daemon thread running in the driver process samples the cluster metrics on an interval
and caches the latest values of the metrics captured.
An execution captures a copy of the latest snapshot as its baseline when
it starts, then computes the delta between the latest snapshot and that
baseline when it ends.
"""

import logging
import threading
import time
from typing import Callable, Dict, Optional

from ray.data._internal.usage import collector

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
        # Serializes access to poller state, since the poll thread writes the
        # snapshots while the driver thread reads them.
        self._lock = threading.Lock()
        # The most recent poll result, refreshed every interval by the loop. Contains the most recent values of the cluster metrics.
        # None until the first poll completes.
        self._latest_snapshot: Optional[Dict[str, MetricValue]] = None
        # The first poll result the loop ever published. Used as a fallback
        # baseline for executions that start before any poll completes.
        self._first_snapshot: Optional[Dict[str, MetricValue]] = None
        # The poll thread. Started on the first execution and runs as a daemon
        # until the driver process exits.
        self._thread: Optional[threading.Thread] = None

    def _start_thread_if_not_running(self) -> None:
        """Start the poll thread if it isn't already running. Idempotent;
        called once when the poller is first created."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="data-usage-metrics-poller", daemon=True
            )
            self._thread.start()

    def latest_snapshot(self) -> Optional[Dict[str, MetricValue]]:
        """A copy of the most recent poll result, or None if no poll has
        completed yet."""
        with self._lock:
            return dict(self._latest_snapshot) if self._latest_snapshot else None

    def first_snapshot(self) -> Optional[Dict[str, MetricValue]]:
        """A copy of the first poll result the loop ever published, or None if no
        poll has completed yet."""
        with self._lock:
            return dict(self._first_snapshot) if self._first_snapshot else None

    def poll_once(self) -> None:
        """Sample every metric one at a time and publish the values as the latest
        snapshot (and as the first snapshot if this is the first poll)."""
        values = {name: fn() for name, fn in self._metrics.items()}
        with self._lock:
            self._latest_snapshot = values
            if self._first_snapshot is None:
                self._first_snapshot = values

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
            _poller._start_thread_if_not_running()
        return _poller
