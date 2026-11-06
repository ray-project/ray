from typing import TYPE_CHECKING, Any, Dict, List

from ray.train.health import HealthCheck
from ray.train.health._internal.constants import DEFAULT_ON_DEMAND_PROBE_TIMEOUT_S
from ray.train.v2._internal.execution.callback import WorkerGroupCallback

if TYPE_CHECKING:
    from ray.train.health._internal.manager import HealthManager
    from ray.train.v2._internal.execution.worker_group import (
        Worker,
        WorkerGroup,
        WorkerGroupContext,
    )


class HealthCallback(WorkerGroupCallback):
    """Runs a ``HealthManager``'s checks on each attempt's worker group:
    gives each worker the in-flight WorkerProbes to sample, monitors the
    attempt's nodes while its workers run, and starts the pre-flight checks on
    demand once they start.

    Args:
        health_manager: The run's ``HealthManager``.
        preflight_checks: The checks to run once at the start of each attempt.
    """

    def __init__(
        self, health_manager: "HealthManager", preflight_checks: List[HealthCheck]
    ):
        self._health_manager = health_manager
        self._preflight_checks = preflight_checks

    def before_init_train_context(
        self, workers: List["Worker"]
    ) -> Dict[str, List[Any]]:
        worker_probes = self._health_manager.worker_probes
        if not worker_probes:
            return {}
        return {"worker_probes": [worker_probes] * len(workers)}

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        node_ids = {worker.metadata.node_id for worker in worker_group.get_workers()}
        self._health_manager.reset(sorted(node_ids), worker_group)
        if self._preflight_checks:
            self._health_manager.start_on_demand_checks(
                self._preflight_checks, timeout=DEFAULT_ON_DEMAND_PROBE_TIMEOUT_S
            )

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup"):
        self._health_manager.shutdown()

    def before_worker_group_abort(self, worker_group_context: "WorkerGroupContext"):
        self._health_manager.shutdown()
