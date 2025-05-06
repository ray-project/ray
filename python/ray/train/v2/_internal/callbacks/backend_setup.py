import logging

from ray.exceptions import RayActorError
from ray.train.backend import BackendConfig
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class BackendSetupCallback(WorkerGroupCallback):
    def __init__(self, backend_config: BackendConfig):
        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._backend.on_start(worker_group, self._backend_config)
        self._backend.on_training_start(worker_group, self._backend_config)

    def before_worker_group_shutdown(self, worker_group: WorkerGroup):
        try:
            self._backend.on_shutdown(worker_group, self._backend_config)
        except RayActorError:
            logger.warning(
                "Graceful shutdown of backend failed. This is "
                "expected if one of the workers has crashed."
            )
