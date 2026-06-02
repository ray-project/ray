import logging
import os
from typing import TYPE_CHECKING, Dict, List, Optional

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.train.v2._internal.constants import (
    DEFAULT_PREEMPTION_POLL_INTERVAL_S,
    PREEMPTION_POLL_INTERVAL_S_ENV_VAR,
    PREEMPTION_WATCHER_STOP_TIMEOUT_S,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.preemption import PreemptionWatcher

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class PreemptionCallback(WorkerGroupCallback):
    """Manages a :class:`PreemptionWatcher` across worker-group lifecycles.

    Spawns a fresh watcher in :meth:`after_worker_group_start` and stops it in
    :meth:`before_worker_group_shutdown`. Each worker group gets its own
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
        node_to_ranks: Dict[str, List[int]] = {}
        for w in worker_group.get_workers():
            if w.distributed_context is None:
                continue
            node_to_ranks.setdefault(w.metadata.node_id, []).append(
                w.distributed_context.world_rank
            )

        if not node_to_ranks:
            logger.debug(
                "PreemptionCallback: no ranks with node IDs; skipping watcher."
            )
            return

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
        if self._watcher is None:
            return
        watcher = self._watcher
        self._watcher = None
        try:
            ray.get(watcher.stop.remote(), timeout=PREEMPTION_WATCHER_STOP_TIMEOUT_S)
        except RayActorError:
            logger.debug("PreemptionWatcher already exited before stop; ignoring.")
        except Exception:
            logger.warning(
                "PreemptionWatcher.stop() did not complete within %.1fs; "
                "killing actor.",
                PREEMPTION_WATCHER_STOP_TIMEOUT_S,
                exc_info=True,
            )
        try:
            ray.kill(watcher)
        except Exception:
            logger.warning("Failed to kill PreemptionWatcher actor.", exc_info=True)
