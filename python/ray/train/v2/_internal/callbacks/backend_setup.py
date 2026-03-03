import logging

from ray.exceptions import RayActorError
from ray.train.backend import BackendConfig
from ray.train.v2._internal.execution.callback import (
    ReplicaGroupCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.worker_group import (
    ExecutionGroup,
    ReplicaGroup,
    WorkerGroup,
)

logger = logging.getLogger(__name__)


class BackendSetupCallback(ReplicaGroupCallback, WorkerGroupCallback):
    def __init__(self, backend_config: BackendConfig):
        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

    def _after_execution_group_start(self, execution_group: ExecutionGroup):
        """Shared logic for starting the backend on an execution group."""
        self._backend.on_start(execution_group, self._backend_config)
        self._backend.on_training_start(execution_group, self._backend_config)

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._after_execution_group_start(worker_group)

    def after_replica_group_start(self, replica_group: ReplicaGroup):
        self._after_execution_group_start(replica_group)

    def _before_execution_group_shutdown(self, execution_group: ExecutionGroup):
        """Shared logic for shutting down the backend on an execution group."""
        try:
            self._backend.on_shutdown(execution_group, self._backend_config)
        except RayActorError:
            logger.warning(
                "Graceful shutdown of backend failed. This is "
                "expected if one of the workers has crashed."
            )

    def before_worker_group_shutdown(self, worker_group: WorkerGroup):
        self._before_execution_group_shutdown(worker_group)

    def before_replica_group_shutdown(self, replica_group: ReplicaGroup):
        self._before_execution_group_shutdown(replica_group)
