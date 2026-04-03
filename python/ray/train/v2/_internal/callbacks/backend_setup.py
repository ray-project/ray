import logging

from ray.exceptions import RayActorError
from ray.train.backend import BackendConfig
from ray.train.v2._internal.execution.callback import (
    ReplicaGroupCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.worker_group import (
    ExecutionGroup,
)

logger = logging.getLogger(__name__)


class BackendSetupCallback(ReplicaGroupCallback, WorkerGroupCallback):
    def __init__(self, backend_config: BackendConfig):
        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

    def after_execution_group_start(self, execution_group: ExecutionGroup):
        self._backend.on_start(execution_group, self._backend_config)
        self._backend.on_training_start(execution_group, self._backend_config)

    def before_execution_group_shutdown(self, execution_group: ExecutionGroup):
        try:
            self._backend.on_shutdown(execution_group, self._backend_config)
        except RayActorError:
            logger.warning(
                "Graceful shutdown of backend failed. This is "
                "expected if one of the workers has crashed."
            )
