import logging

from ray.exceptions import RayActorError
from ray.train.backend import BackendConfig
from ray.train.v2._internal.execution.callback import SystemCallback
from ray.train.v2._internal.execution.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class BackendSetupCallback(SystemCallback):
    def __init__(self, backend_config: BackendConfig):
        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._backend.on_start(worker_group, self._backend_config)
        self._backend.on_training_start(worker_group, self._backend_config)

        # TODO: Executing this dummy function is a temporary workaround
        # to avoid a GIL segfault in the torch training loop.
        # This is most likely a bug with Ray threaded actors,
        # and the fix is to manage the training thread ourselves with
        # python threading, rather than relying on actor threading.
        worker_group.execute(lambda: None)

    def before_worker_group_shutdown(self, worker_group: WorkerGroup):
        try:
            self._backend.on_shutdown(worker_group, self._backend_config)
        except RayActorError:
            logger.warning(
                "Graceful shutdown of backend failed. This is "
                "expected if one of the workers has crashed."
            )
