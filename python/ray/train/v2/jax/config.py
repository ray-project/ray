import logging
import os
from dataclasses import dataclass

import ray
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@dataclass
class JaxConfig(BackendConfig):
    use_tpu: bool = False

    @property
    def backend_cls(self):
        return _JaxBackend


def _setup_jax_tpu_environment(
    master_addr_with_port: str, num_workers: int, index: int
):
    """Set up distributed Jax training information.

    This function should be called on each worker.
    """
    import jax

    jax_platforms = os.environ.get("JAX_PLATFORMS", "").lower()

    if "tpu" in jax_platforms.split(","):
        jax.distributed.initialize(master_addr_with_port, num_workers, index)


class _JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        if not backend_config.use_tpu:
            return

        master_addr, master_port = worker_group.execute_single(0, get_address_and_port)
        master_addr_with_port = f"{master_addr}:{master_port}"

        # Get setup tasks in order to throw errors on failure.
        setup_futures = []
        for i in range(len(worker_group)):
            setup_futures.append(
                worker_group.execute_single_async(
                    i,
                    _setup_jax_tpu_environment,
                    master_addr_with_port=master_addr_with_port,
                    num_workers=len(worker_group),
                    index=i,
                )
            )
        ray.get(setup_futures)
