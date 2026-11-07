"""Background poller for cluster usage metrics.

A single daemon thread running in the driver process samples the cluster metrics on an interval
and caches the latest values of the metrics captured.
An execution captures a copy of the latest snapshot as its baseline when
it starts, then computes the delta between the latest snapshot and that
baseline when it ends.

Every driver that has run a Dataset polls until it exits, and
``RAY_PROMETHEUS_HOST`` can be a metrics backend shared by many clusters.
Before adding a metric or shortening the interval, estimate the total query
rate (drivers * queries per poll / interval).
"""

import logging
import random
import threading
import time
from typing import Callable, Dict, Optional

from ray.data._internal.usage import collector

logger = logging.getLogger(__name__)

# Every driver that has run a Dataset polls for the rest of its life, so keep
# the interval long to bound the query load on the metrics backend.
_POLL_INTERVAL_S = 60

# Each sleep is randomized by this fraction of the interval so that drivers
# started together drift apart instead of polling in lockstep.
_POLL_JITTER = 0.1

# The value of a single cluster metric (None when its query failed / is unavailable).
MetricValue = Optional[int]

# Fetches the current value of every cluster metric, keyed by metric name.
MetricsFn = Callable[[], Dict[str, MetricValue]]


class ClusterMetricsPoller:
    def __init__(
        self,
        metrics_fn: MetricsFn,
        interval_s: float = _POLL_INTERVAL_S,
    ):
        # Fetches the metrics to poll, keyed by name of metric defined in PipelinePerf
        self._metrics_fn = metrics_fn
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
        """Fetch every metric and publish the values as the latest snapshot (and
        as the first snapshot if this is the first poll)."""
        values = self._metrics_fn()
        with self._lock:
            self._latest_snapshot = values
            if self._first_snapshot is None:
                self._first_snapshot = values

    def _run(self) -> None:
        # Poll right away so the execution that started the poller gets a
        # baseline. Then wait a random fraction of the interval to spread out
        # drivers that started at the same time.
        sleep_s = random.uniform(0, self._interval_s)
        while True:
            try:
                self.poll_once()
            except Exception:
                logger.debug("Cluster metrics poll failed", exc_info=True)
            time.sleep(sleep_s)
            sleep_s = self._interval_s * random.uniform(
                1 - _POLL_JITTER, 1 + _POLL_JITTER
            )


_poller: Optional[ClusterMetricsPoller] = None
_poller_lock = threading.Lock()


def get_poller() -> ClusterMetricsPoller:
    """The cluster metrics poller (created on first use)."""
    global _poller
    with _poller_lock:
        if _poller is None:
            _poller = ClusterMetricsPoller(collector.collect_cluster_metrics)
            _poller._start_thread_if_not_running()
        return _poller
