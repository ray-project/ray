import logging
import threading
import time
from typing import Dict, FrozenSet, Optional, Set, Tuple

import ray
import ray._private.internal_api
import ray._private.state
from ray.data._internal.execution.node_trackers.actor_location import (
    get_or_create_actor_location_tracker,
)
from ray.data._internal.utils.cache import timed_cache

logger = logging.getLogger(__name__)


@timed_cache(ttl=1)
def get_local_ongoing_lineage_reconstruction_tasks():
    # ResourceManager.update_usages() calls extra_resource_usage() on map
    # operators. Unit tests exercise that path without ray.init(); lineage
    # reconstruction can only exist with a live worker, so treat offline as empty.
    if not ray.is_initialized():
        return []
    return ray._private.internal_api.get_local_ongoing_lineage_reconstruction_tasks()


@timed_cache(ttl=1)
def get_draining_nodes() -> Dict[str, int]:
    return ray._private.state.state.get_draining_nodes()


@timed_cache(ttl=1)
def get_actor_locations(logical_actor_ids: Tuple[str, ...]) -> Dict[str, str]:
    """Get the actor locations from logical actor ids.
    NOTE: This function is not thread-safe"""
    if not logical_actor_ids:
        return {}
    return ray.get(
        get_or_create_actor_location_tracker().get_actor_locations.remote(
            logical_actor_ids
        )
    )


# If we submit a task immediately before the deadline, Ray Core might not have
# enough time to launch the task and fetch objects before the node is terminated.
# To avoid this, we stop using inputs on such nodes some time before the deadline.
DRAIN_DEADLINE_BUFFER_TIME_MS = 5000


def get_drained_nodes() -> Set[str]:
    """Returns the set of nodes that are draining and are past their deadline.

    Deadline 0 is idle termination, which only drains nodes holding no objects.
    """
    now_ms = time.time() * 1000
    return {
        node_id
        for node_id, deadline_ms in get_draining_nodes().items()
        if deadline_ms > 0 and deadline_ms - DRAIN_DEADLINE_BUFFER_TIME_MS < now_ms
    }


# Dead nodes are polled less often than draining ones: a dead node's blocks are
# already being rebuilt by Core, so noticing a few seconds late only delays
# rotating its bundles, whereas the drain deadline has a 5 s buffer to honor.
@timed_cache(ttl=5)
def get_dead_node_ids() -> FrozenSet[str]:
    return frozenset(n["NodeID"] for n in ray.nodes() if not n["Alive"])


@timed_cache(ttl=1)
def get_lost_node_ids() -> FrozenSet[str]:
    """Nodes whose objects should be treated as gone: dead, or past their drain
    deadline."""
    return get_dead_node_ids() | frozenset(get_drained_nodes())


class _NodeLossTracker:
    """Counts node-loss events since the first observation.

    Version 0 means no node has been lost since this process started watching,
    which is the only state in which a freshly produced block is guaranteed to
    still be present without checking.

    The lost-node set is refreshed on a background thread while at least one
    executor is running, so callers on the scheduling loop never wait on a GCS
    round trip. The thread is tied to executor lifetime: calling into Ray's
    global state outside a live session can abort the process.
    """

    POLL_INTERVAL_S = 1.0

    def __init__(self):
        self._lock = threading.Lock()
        self._lost: Optional[FrozenSet[str]] = None
        self._version = 0
        self._active_executors = 0
        self._stop = threading.Event()
        self._poller: Optional[threading.Thread] = None

    def version(self) -> int:
        with self._lock:
            return self._version

    def start(self) -> None:
        with self._lock:
            self._active_executors += 1
            if self._poller is not None:
                return
        # Take the baseline before any queue can ask for the version, so a node
        # that dies after this point is counted rather than folded into the
        # baseline. One GCS round trip, once per executor start.
        self._poll_once()
        with self._lock:
            if self._poller is None and self._active_executors > 0:
                self._stop.clear()
                self._poller = threading.Thread(
                    target=self._poll_until_stopped,
                    name="ray-data-node-loss",
                    daemon=True,
                )
                self._poller.start()

    def stop(self) -> None:
        with self._lock:
            self._active_executors = max(0, self._active_executors - 1)
            if self._active_executors > 0 or self._poller is None:
                return
            poller, self._poller = self._poller, None
            self._stop.set()
        # Wait for an in-flight poll, so no GCS call outlives the executor and a
        # later start() can't leave two pollers running.
        poller.join()

    def _poll_once(self) -> None:
        try:
            if ray.is_initialized():
                self.refresh(get_lost_node_ids())
        except Exception as e:
            logger.debug(f"Node-loss poll failed: {e!r}")

    def _poll_until_stopped(self) -> None:
        while not self._stop.wait(self.POLL_INTERVAL_S):
            self._poll_once()

    def refresh(self, lost: FrozenSet[str]) -> int:
        """Record the current lost-node set; bumps the version if it changed."""
        with self._lock:
            if self._lost is None:
                self._lost = lost
            elif lost != self._lost:
                logger.info(
                    "Node loss detected: lost=%s, recovered=%s",
                    sorted(lost - self._lost),
                    sorted(self._lost - lost),
                )
                self._lost = lost
                self._version += 1
            return self._version


_node_loss_tracker = _NodeLossTracker()


def start_node_loss_polling() -> None:
    """Begin refreshing the lost-node set in the background (refcounted)."""
    _node_loss_tracker.start()


def stop_node_loss_polling() -> None:
    _node_loss_tracker.stop()


def get_node_loss_version() -> int:
    """Counter that increments whenever a node dies or passes its drain deadline.

    A queued block's primary copy is pinned, so it can only go missing through
    one of those events. Callers that saw a block exist at one version can skip
    re-checking it until the version changes.
    """
    return _node_loss_tracker.version()
