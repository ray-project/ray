import logging
from dataclasses import dataclass
from typing import Optional

import ray
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@dataclass
class JaxConfig(BackendConfig):
    use_tpu: bool = False
    topology: Optional[str] = None
    accelerator_type: Optional[str] = None

    @property
    def backend_cls(self):
        return _JaxBackend


def _setup_jax_tpu_environment():
    """Set up distributed Jax training information.

    This function should be called on each worker.
    """
    import jax

    # coordinator_address, num_processes, process_id are
    # auto-detected in TPU environments
    jax.distributed.initialize()


class _JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        if backend_config.use_tpu:
            # Get setup tasks in order to throw errors on failure.
            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        _setup_jax_tpu_environment,
                    )
                )
            ray.get(setup_futures)
