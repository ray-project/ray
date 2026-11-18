from typing import TYPE_CHECKING, Any, Dict, List

from ray.train.health import Evict, HealthCheck, Reattempt
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2.api.exceptions import HealthCheckError

if TYPE_CHECKING:
    from ray.train.health._internal.manager import HealthManager
    from ray.train.v2._internal.execution.worker_group import (
        Worker,
        WorkerGroup,
        WorkerGroupContext,
    )


class HealthCallback(WorkerGroupCallback):
    """Runs a ``HealthManager``'s checks on each attempt's worker group: gives
    each worker the in-flight WorkerProbes to sample, runs the pre-flight
    checks as the workers start, and monitors the attempt's nodes while they
    run.

    Args:
        health_manager: The run's ``HealthManager``. The ``TrainController``
            creates it from ``RunConfig.health_config``, and evaluates its
            checks while the attempt runs.
        preflight_checks: The checks to run once at the start of each attempt.
    """

    def __init__(
        self, health_manager: "HealthManager", preflight_checks: List[HealthCheck]
    ):
        self._health_manager = health_manager
        self._preflight_checks = preflight_checks
        # Checks to run once at the start of the next attempt only.
        self._next_start_checks: List[HealthCheck] = []

    def run_at_next_start(self, checks: List[HealthCheck]) -> None:
        """Run some checks once at the start of the next attempt, with its
        pre-flight checks.

        Args:
            checks: The checks, such as those of a ``Diagnose`` that stop the
                workers.
        """
        self._next_start_checks.extend(checks)

    def before_init_train_context(
        self, workers: List["Worker"]
    ) -> Dict[str, List[Any]]:
        worker_probes = self._health_manager.worker_probes
        if not worker_probes:
            return {}
        return {"worker_probes": [worker_probes] * len(workers)}

    def after_worker_group_start(self, worker_group: "WorkerGroup") -> None:
        """Reset the ``HealthManager`` for the attempt, and run its pre-flight
        checks on demand. Training starts once the checks that stop the
        workers decide, alongside the other checks.

        Args:
            worker_group: The attempt's worker group.

        Raises:
            HealthCheckError: If the checks that require idle workers decide to
                ``Reattempt`` or ``Evict``, which keeps training from starting.
        """
        node_ids = {worker.metadata.node_id for worker in worker_group.get_workers()}
        self._health_manager.reset(sorted(node_ids), worker_group)

        checks = self._preflight_checks + self._next_start_checks
        self._next_start_checks = []
        blocking_checks = [c for c in checks if c.requires_idle_workers]
        if blocking_checks:
            decision = self._health_manager.start_and_evaluate_on_demand_checks(
                blocking_checks
            )
            if isinstance(decision, (Reattempt, Evict)):
                self._health_manager.shutdown()
                raise HealthCheckError(decision)

        other_checks = [c for c in checks if not c.requires_idle_workers]
        if other_checks:
            self._health_manager.start_on_demand_checks(other_checks)

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup"):
        self._health_manager.shutdown()

    def before_worker_group_abort(self, worker_group_context: "WorkerGroupContext"):
        self._health_manager.shutdown()
