import logging
import os
from typing import TYPE_CHECKING, Dict, List, Optional

import ray
from ray.actor import ActorHandle
from ray.train.v2._internal.constants import (
    DEFAULT_PREEMPTION_POLL_INTERVAL_S,
    PREEMPTION_POLL_INTERVAL_S_ENV_VAR,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.preemption import PreemptionWatcher

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.worker_group import (
        WorkerGroup,
        WorkerGroupContext,
    )

logger = logging.getLogger(__name__)


class PreemptionCallback(WorkerGroupCallback):
    """Manages a :class:`PreemptionWatcher` across worker-group lifecycles.

    Spawns a fresh watcher in :meth:`after_worker_group_start` and stops it on
    every teardown path (shutdown and abort). Each worker group gets its own
    watcher and failure-domain map, so elastic resizes and restarts never
    leak stale state.
    """

    def __init__(self) -> None:
        self._poll_interval_s: float = float(
            os.getenv(
                PREEMPTION_POLL_INTERVAL_S_ENV_VAR,
                str(DEFAULT_PREEMPTION_POLL_INTERVAL_S),
            )
        )
        self._watcher: Optional[ActorHandle] = None

    def after_worker_group_start(self, worker_group: "WorkerGroup") -> None:
        # Tear down any watcher from a previous worker group first. Worker-group
        # startup can fail after this hook without running the shutdown hook, so
        # this also prevents leaking an orphaned watcher across a reschedule.
        self._stop_watcher()

        node_to_ranks: Dict[str, List[int]] = {}
        for w in worker_group.get_workers():
            node_to_ranks.setdefault(w.metadata.node_id, []).append(
                w.distributed_context.world_rank
            )

        watcher_cls = ray.remote(num_cpus=0, max_restarts=-1)(PreemptionWatcher)
        self._watcher = watcher_cls.remote(
            node_to_ranks=node_to_ranks,
            poll_interval_s=self._poll_interval_s,
        )

        logger.debug(
            "PreemptionCallback: started watcher for %d node(s).",
            len(node_to_ranks),
        )

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup") -> None:
        self._stop_watcher()

    def after_worker_group_abort(
        self, worker_group_context: "WorkerGroupContext"
    ) -> None:
        # abort() doesn't run the shutdown hook, so tear the watcher down here
        # too — otherwise it keeps polling GCS until the cluster reaps it.
        self._stop_watcher()

    def _stop_watcher(self) -> None:
        if self._watcher is None:
            return
        watcher = self._watcher
        self._watcher = None
        # Force-kill (non-blocking) rather than a synchronous graceful stop, so
        # we never block the controller's event loop. The watcher's daemon poll
        # thread dies with the actor process and holds no external resources.
        try:
            ray.kill(watcher)
        except Exception:
            logger.warning("Failed to kill PreemptionWatcher actor.", exc_info=True)
