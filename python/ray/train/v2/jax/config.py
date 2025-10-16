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


def _setup_jax_distributed_environment(
    master_addr_with_port: str, num_workers: int, index: int
):
    """Set up distributed Jax training information.

    This function should be called on each worker.
    """
    import jax

    jax.distributed.initialize(master_addr_with_port, num_workers, index)


def _shutdown_jax_distributed():
    """Shutdown JAX distributed environment.

    This function should be called on each worker during cleanup.
    """
    try:
        import jax

        # Only shutdown if JAX distributed was initialized
        if jax.process_count() > 1:
            jax.distributed.shutdown()
            logger.debug("JAX distributed shutdown completed")
    except Exception as e:
        # Log but don't raise - we want graceful degradation during shutdown
        logger.warning(f"Error during JAX distributed shutdown: {e}")         os.environ["JAX_PLATFORMS"] = "tpu," + existing_jax_platforms
        else:
            # No existing platforms, just set to "tpu"
            os.environ["JAX_PLATFORMS"] = "tpu"


def _setup_jax_distributed_environment(
    master_addr_with_port: str, num_workers: int, index: int
):
    """Set up distributed Jax training information.

    This function should be called on each worker.
    """
    import jax

    jax.distributed.initialize(master_addr_with_port, num_workers, index)


def _shutdown_jax_distributed():
    """Shutdown JAX distributed environment.

    This function should be called on each worker during cleanup.
    """
    try:
        import jax

        # Only shutdown if JAX distributed was initialized
        if jax.process_count() > 1:
            jax.distributed.shutdown()
            logger.debug("JAX distributed shutdown completed")
    except Exception as e:
        # Log but don't raise - we want graceful degradation during shutdown
        logger.warning(f"Error during JAX distributed shutdown: {e}")



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



class _JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        if not backend_config.use_tpu:
            return

        # Set JAX environment variables on all workers
        worker_group.execute(_set_jax_env_vars, use_tpu=backend_config.use_tpu)

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


    class _JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        if not backend_config.use_tpu:
            return

        # Set JAX environment variables on all workers
        worker_group.execute(_set_jax_env_vars, use_tpu=backend_config.use_tpu)

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
        """Cleanup JAX distributed resources when shutting down worker group.

        This is critical to prevent resource leaks and hanging workers.
        """
        if not backend_config.use_tpu:
            return

        # Shutdown JAX distributed on all workers
        shutdown_futures = worker_group.execute_async(_shutdown_jax_distributed)

        # Wait for shutdown to complete with a reasonable timeout
        timeout_s = 30  # JAX shutdown should be quick
        try:
            ray.get(shutdown_futures, timeout=timeout_s)
        except ray.exceptions.GetTimeoutError:
            logger.warning(
                f"JAX distributed shutdown timed out after {timeout_s} seconds. "
                "This may indicate workers are hung or unresponsive."
            )
        except Exception as e:
            logger.warning(f"Error during JAX distributed shutdown: {e}")xBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        if not backend_config.use_tpu:
            return

        # Set JAX environment variables on all workers
        worker_group.execute(_set_jax_env_vars, use_tpu=backend_config.use_tpu)

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
        """Cleanup JAX distributed resources when shutting down worker group.

        This is critical to prevent resource leaks and hanging workers.
        """
        if not backend_config.use_tpu:
            return

        # Shutdown JAX distributed on all workers
        shutdown_futures = worker_group.execute_async(_shutdown_jax_distributed)

        # Wait for shutdown to complete with a reasonable timeout
        timeout_s = 30  # JAX shutdown should be quick
        try:
            ray.get(shutdown_futures, timeout=timeout_s)
        except ray.exceptions.GetTimeoutError:
            logger.warning(
                f"JAX distributed shutdown timed out after {timeout_s} seconds. "
                "This may indicate workers are hung or unresponsive."
            )
        except Exception as e:
            logger.warning(f"Error during JAX distributed shutdown: {e}")
