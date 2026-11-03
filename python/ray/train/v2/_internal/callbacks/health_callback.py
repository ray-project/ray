from typing import TYPE_CHECKING

from ray.train.v2._internal.execution.callback import WorkerGroupCallback

if TYPE_CHECKING:
    from ray.train.health._internal.manager import HealthManager
    from ray.train.v2._internal.execution.worker_group import (
        WorkerGroup,
        WorkerGroupContext,
    )


class HealthCallback(WorkerGroupCallback):
    """Runs a ``HealthManager``'s checks on each attempt's worker group:
    monitors the attempt's nodes while its workers run.

    Args:
        health_manager: The run's ``HealthManager``.
    """

    def __init__(self, health_manager: "HealthManager"):
        self._health_manager = health_manager

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        node_ids = {worker.metadata.node_id for worker in worker_group.get_workers()}
        self._health_manager.reset(sorted(node_ids))

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup"):
        self._health_manager.shutdown()

    def before_worker_group_abort(self, worker_group_context: "WorkerGroupContext"):
        self._health_manager.shutdown()
