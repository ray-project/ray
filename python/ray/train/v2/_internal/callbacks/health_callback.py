"""Controller-side wiring for the health loop.

Keeps the ``HealthManager`` alive across worker-group lifecycles and feeds it
from the hooks that already exist, so the Collect -> Decide half needs no new
RPC and no new control-loop state.

The Act half is deliberately split: this callback owns the one piece of Act
that can be expressed through an existing seam -- excluding evicted nodes from
the next worker group, via the label selector the controller already asks
callbacks for. Carrying out ``REATTEMPT`` / ``DIAGNOSE`` needs the controller's
own state machine and lives there.
"""
import logging
from typing import TYPE_CHECKING, Dict, List, Optional

from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.health.decision import Evict, HealthDecision
from ray.train.v2._internal.execution.health.manager import HealthManager
from ray.train.v2._internal.execution.health.policy import HealthConfig
from ray.train.v2._internal.execution.health.state import WorkerHealth

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.worker_group import (
        WorkerGroup,
        WorkerGroupPollStatus,
    )
    from ray.train.v2.api.config import ScalingConfig

logger = logging.getLogger(__name__)

NODE_ID_LABEL_KEY = "ray.io/node-id"


def build_node_exclusion_selector(
    excluded_node_ids: List[str],
    base: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, str]]:
    """A bundle label selector that keeps workers off ``excluded_node_ids``.

    Ray's label selectors take a ``!in(...)`` operator on ``ray.io/node-id``,
    so eviction needs no new scheduling primitive: the next worker group is
    simply scheduled with the bad nodes excluded. This is what lets ``EVICT``
    hand off to the existing worker-group sizing path instead of duplicating it.

    Returns ``None`` when nothing is excluded, which the controller reads as
    "no opinion" and leaves any user-supplied selector alone.
    """
    if not excluded_node_ids:
        return dict(base) if base else None

    selector = dict(base or {})
    existing = selector.get(NODE_ID_LABEL_KEY)
    if existing:
        # Don't silently drop what the user asked for.
        logger.warning(
            "Health eviction is overriding an existing %s selector %r.",
            NODE_ID_LABEL_KEY,
            existing,
        )
    selector[NODE_ID_LABEL_KEY] = f"!in({','.join(sorted(excluded_node_ids))})"
    return selector


class HealthCallback(ControllerCallback, WorkerGroupCallback):
    """Owns the ``HealthManager`` for a run."""

    def __init__(self, health_config: HealthConfig):
        self._manager = HealthManager(health_config.policies)
        self._worker_group: Optional["WorkerGroup"] = None
        self._pending: Optional[HealthDecision] = None

    @property
    def manager(self) -> HealthManager:
        return self._manager

    # -- Collect -------------------------------------------------------
    def after_worker_group_start(self, worker_group: "WorkerGroup") -> None:
        self._worker_group = worker_group
        self._manager.on_worker_group_start()
        self._pending = None

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup") -> None:
        self._worker_group = None

    def after_worker_group_poll_status(
        self, worker_group_status: "WorkerGroupPollStatus"
    ) -> None:
        """Runs on the controller's poll loop, so it must never raise."""
        if not self._manager.enabled:
            return
        try:
            for rank, status in worker_group_status.worker_statuses.items():
                health = getattr(status, "health", None)
                if isinstance(health, WorkerHealth):
                    self._manager.ingest_worker_health(health)

            node_ids = self._current_node_ids()
            if node_ids:
                self._manager.run_cluster_probes(node_ids)

            decision = self._manager.poll_decision()
            if decision is not None:
                logger.info(
                    "[Health] %s (%s): %s",
                    decision.action.name,
                    decision.cause.name,
                    decision.reason,
                )
                self._pending = decision
        except Exception:
            logger.exception("Health loop hit an unexpected error; skipping this poll.")

    def _current_node_ids(self) -> List[str]:
        if self._worker_group is None:
            return []
        try:
            return sorted(
                {w.metadata.node_id for w in self._worker_group.get_workers()}
            )
        except Exception:
            return []

    def consume_decision(self) -> Optional[HealthDecision]:
        """Hand the pending decision to the controller, exactly once."""
        decision, self._pending = self._pending, None
        return decision

    # -- Act (the part that fits an existing seam) ---------------------
    def on_controller_start_worker_group(
        self, scaling_config: "ScalingConfig", num_workers: int
    ) -> Optional[Dict[str, str]]:
        """Keep the next worker group off every node this run has evicted."""
        return build_node_exclusion_selector(self._manager.evicted_nodes)


def evicted_node_ids(decision: HealthDecision) -> List[str]:
    return list(decision.target_nodes) if isinstance(decision, Evict) else []
