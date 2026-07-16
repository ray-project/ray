"""Background poller for cluster usage metrics.

A single process-global daemon thread samples the cluster metrics on an interval
and caches the latest snapshot, so the usage callback can read start/end
baselines without ever blocking the execution path on a metric query.
"""

import logging
import os
import threading
import time
from typing import Callable, Dict, Optional

from ray.data._internal.usage import collector, util

logger = logging.getLogger(__name__)

_POLL_INTERVAL_S = float(os.environ.get("RAY_DATA_USAGE_POLL_INTERVAL_S", "15"))
_POLL_IDLE_TIMEOUT_S = float(
    os.environ.get("RAY_DATA_USAGE_POLL_IDLE_TIMEOUT_S", "120")
)

# name -> zero-arg sampler returning the cumulative counter (None on failure).
SampleFn = Callable[[], Optional[int]]


class ClusterMetricsPoller:
    def __init__(
        self,
        samplers: Dict[str, SampleFn],
        interval_s: float = _POLL_INTERVAL_S,
        idle_timeout_s: float = _POLL_IDLE_TIMEOUT_S,
    ):
        # The metrics to poll, keyed by name of metric defined in PipelinePerf
        self._samplers = samplers
        # The interval at which to poll the metrics
        self._interval_s = interval_s
        # The timeout after which the poller will exit if no execution callback queries it
        self._idle_timeout_s = idle_timeout_s
        # Serializes access to poller state, since the poll thread writes them
        # while on_collection_start/end (the driver thread) read them.
        self._lock = threading.Lock()
        # The most recent poll result
        self._latest: Dict[str, Optional[int]] = {}
        # Current poll thread
        self._thread: Optional[threading.Thread] = None
        # Monotonic time of the last execution start/end (each calls
        # ensure_running). The poll thread exits once no execution has touched
        # the poller for idle_timeout_s.
        self._last_active_at = 0.0

    def ensure_running(self) -> None:
        """Called at each execution's start and end. Refreshes the idle deadline
        and starts the poll thread if it isn't already running."""
        with self._lock:
            self._last_active_at = time.monotonic()
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="data-usage-metrics-poller", daemon=True
            )
            self._thread.start()

    def latest(self) -> Dict[str, Optional[int]]:
        """A copy of the most recent snapshot (empty before the first poll)."""
        with self._lock:
            return dict(self._latest)

    def poll_once(self) -> None:
        """Sample every metric concurrently and publish the snapshot."""
        names = list(self._samplers)
        futures = [util.start_metric_sample(self._samplers[name]) for name in names]
        snapshot = dict(zip(names, util.join_samples(futures)))
        with self._lock:
            self._latest = snapshot

    def _run(self) -> None:
        while True:
            try:
                self.poll_once()
            except Exception:
                logger.debug("Cluster metrics poll failed", exc_info=True)
            time.sleep(self._interval_s)
            with self._lock:
                if time.monotonic() - self._last_active_at > self._idle_timeout_s:
                    self._thread = None
                    return


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
