import logging
from dataclasses import dataclass
import os

import ray
from ray.train.backend import BackendConfig, Backend
from ray.train.utils import get_address_and_port
from ray.train.worker_group import WorkerGroup
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
@dataclass
class JaxConfig(BackendConfig):
    @property
    def backend_cls(self):
        return JaxBackend


def setup_jax_environment(master_addr_with_port: str, num_workers: int, index: int):
    """Set up distributed jax training information.

    This function should be called on each worker. Only multi-gpu instances need
    to set up the distributed connections currently!

    Args:
        master_addr_with_port: The master work's address plus the port.
        num_workers: Total number of all the workers.
        index: Index (i.e. world rank) of the current worker.
    """
    use_gpu = len(ray.get_gpu_ids())
    if use_gpu:
        import jax

        # not import jax at the top to avoid tpulib_lockfile error
        jax.distributed.initialize(master_addr_with_port, num_workers, index)

    # TODO (jimmy)
    # cpu parallel: https://github.com/google/jax/issues/1408
    # os.environ["XLA_FLAGS"] = "--xla_force_host_platform_device_count=200"


def release_tpu_lock(RAY_TPU_DEV: bool = False):
    """release the tpulib lock file when using tpu for training.

    The jax process is unable to use tpu and fall back to cpu
    when there is the tpulib lock file, so we need to release it.
    # see https://github.com/google/jax/issues/10192


    To enable this hook, set RAY_TPU_DEV=1.

    Args:
        RAY_TPU_DEV (bool): whether to release the tpulib lock file.
            If set to True, the lock file will be removed. Otherwise,
            the user might manually release the tpu_lock or add the
            environment variable to release the lock file.
    """
    if RAY_TPU_DEV:
        import subprocess

        subprocess.run("sudo lsof -w /dev/accel0", shell=True)
        subprocess.run("sudo rm -f /tmp/libtpu_lockfile", shell=True)
    else:
        if os.path.isfile("/tmp/libtpu_lockfile"):
            import warnings

            warnings.warn(
                "The tpulib lock file is still there, you might"
                "be unable to use the tpu for training. please "
                "remove it (`/tmp/libtpu_lockfile`) manually or "
                "set `RAY_TPU_DEV=1` to release it."
            )


class JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        # get the master address
        master_addr, master_port = worker_group.execute_single(0, get_address_and_port)
        master_addr_with_port = f"{master_addr}:{master_port}"
        num_workers = len(worker_group)

        # Get setup tasks in order to throw errors on failure.
        setup_futures = []
        for i in range(len(worker_group)):
            setup_futures.append(
                worker_group.execute_single_async(
                    i,
                    setup_jax_environment,
                    master_addr_with_port=master_addr_with_port,
                    num_workers=num_workers,
                    index=i,
                )
            )
        ray.get(setup_futures)

        # case-insensitivize dict
        additional_resources_per_worker = worker_group.additional_resources_per_worker
        additional_resources_per_worker_lower = {
            k.lower(): v for k, v in additional_resources_per_worker.items()
        }
        use_tpu = additional_resources_per_worker_lower.pop("tpu", False)
        # Get setup tasks in order to throw errors on failure.

        if use_tpu:
            RAY_TPU_DEV = bool(os.environ.get("RAY_TPU_DEV"))

            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i, release_tpu_lock, RAY_TPU_DEV=RAY_TPU_DEV
                    )
                )
            ray.get(setup_futures)
