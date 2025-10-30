import logging
import os
from dataclasses import dataclass

import ray
from ray._private import ray_constants
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.constants import (
    DEFAULT_JAX_DISTRIBUTED_SHUTDOWN_TIMEOUT_S,
    JAX_DISTRIBUTED_SHUTDOWN_TIMEOUT_S,
)
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@dataclass
class JaxConfig(BackendConfig):
    use_tpu: bool = False

    @property
    def backend_cls(self):
        return _JaxBackend


def _set_jax_env_vars(use_tpu: bool, use_gpu: bool, resources_per_worker: dict):
    """Set JAX environment variables based on configuration.

    If JAX_PLATFORMS is already set (by user or test), we trust that configuration
    and do nothing. Otherwise, if use_tpu=True, we set it to "tpu".
    """
    # If user/test already set JAX_PLATFORMS, respect their choice
    if os.environ.get("JAX_PLATFORMS"):
        return

    # Only set JAX_PLATFORMS if not already specified
    if use_tpu:
        if not os.environ.get("JAX_PLATFORMS"):
            os.environ["JAX_PLATFORMS"] = "tpu"
    if use_gpu:
        if not os.environ.get("JAX_PLATFORMS"):
            os.environ["JAX_PLATFORMS"] = "cuda"
            num_gpus = resources_per_worker.get("GPU", 0)
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in range(num_gpus))




def _setup_jax_distributed_environment(
    master_addr_with_port: str, num_workers: int, index: int, resources_per_worker: dict
):
    """Set up distributed Jax training information.

    This function should be called on each worker.
    """
    import jax

    jax_platforms = os.environ.get("JAX_PLATFORMS", "").lower()

    if "tpu" in jax_platforms.split(","):
        jax.distributed.initialize(master_addr_with_port, num_workers, index)

    if "cuda" in jax_platforms.split(","):
        num_gpus_per_worker = resources_per_worker.get("GPU", 0)
        if num_gpus_per_worker > 0:
            local_device_ids = list(range(num_gpus_per_worker))
        else:
            local_device_ids = 0
        jax.distributed.initialize(master_addr_with_port, num_workers, index, local_device_ids)
        print(f">>> Initialized JAX distributed with {num_gpus_per_worker} GPUs per worker")


def _shutdown_jax_distributed():
    """Shutdown JAX distributed environment.

    This function should be called on each worker during cleanup.
    If JAX distributed was not initialized, this is a no-op.
    """
    try:
        import jax

        jax.distributed.shutdown()
    except Exception as e:
        logger.warning(f"Error during JAX distributed shutdown: {e}")


class _JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        if not backend_config.use_tpu and not backend_config.use_gpu:
            return

        # Set JAX environment variables on all workers
        worker_group.execute(_set_jax_env_vars, use_tpu=backend_config.use_tpu, use_gpu=backend_config.use_gpu, resources_per_worker=worker_group.get_resources_per_worker())

        master_addr, master_port = worker_group.execute_single(0, get_address_and_port)
        master_addr_with_port = f"{master_addr}:{master_port}"

        # Get setup tasks in order to throw errors on failure.
        setup_futures = []
        for i in range(len(worker_group)):
            setup_futures.append(
                worker_group.execute_single_async(
                    i,
                    _setup_jax_distributed_environment,
                    master_addr_with_port=master_addr_with_port,
                    num_workers=len(worker_group),
                    index=i,
                )
            )
        ray.get(setup_futures)

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        """Cleanup JAX distributed resources when shutting down worker group."""
        if not backend_config.use_tpu:
            return

        # Shutdown JAX distributed on all workers
        shutdown_futures = worker_group.execute_async(_shutdown_jax_distributed)

        timeout_s = ray_constants.env_integer(
            JAX_DISTRIBUTED_SHUTDOWN_TIMEOUT_S,
            DEFAULT_JAX_DISTRIBUTED_SHUTDOWN_TIMEOUT_S,
        )
        try:
            ray.get(shutdown_futures, timeout=timeout_s)
            logger.debug("JAX distributed shutdown completed")
        except ray.exceptions.GetTimeoutError:
            logger.warning(
                f"JAX distributed shutdown timed out after {timeout_s} seconds. "
                "This may indicate workers are hung or unresponsive."
            )
        except Exception as e:
            logger.warning(f"Error during JAX distributed shutdown: {e}")
