import logging
from dataclasses import dataclass
import os
import warnings
import subprocess

import ray
from ray.train.backend import BackendConfig, Backend
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.util import PublicAPI

logger = logging.getLogger(__name__)

RAY_TPU_DEV_ENV = "RAY_TPU_DEV"

@PublicAPI(stability="beta")
@dataclass
class JaxConfig(BackendConfig):
    @property
    def backend_cls(self):
        return _JaxBackend


def setup_jax_environment(master_addr_with_port: str, num_workers: int, index: int):
    """Set up distributed jax training information.

    This function should be called on each worker. Only multi-gpu instances need
    to set up the distributed connections currently!

    Args:
        master_addr_with_port: The master work's address plus the port.
        num_workers: Total number of all the workers.
        index: Index (i.e. world rank) of the current worker.
    """
    import jax
    # not import jax at the top to avoid tpulib_lockfile error
    jax.distributed.initialize(master_addr_with_port, num_workers, index)

def release_tpu_lock(try_remove_tpulib_lock: bool = False):
    """release the tpulib lock file when using tpu for training.

    The jax process is unable to use tpu and fall back to cpu
    when there is the tpulib lock file, so we need to release it.
    # see https://github.com/google/jax/issues/10192


    To enable this hook, set ``RAY_TPU_DEV=1`` env var.

    Args:
        try_remove_tpulib_lock: whether to release the tpulib lock file.
            If set to True, the lock file will be removed. Otherwise,
            the user might manually release the tpu_lock or add the
            environment variable to release the lock file.
    """
    if try_remove_tpulib_lock:
        # If one of the workers has an error during training, 
        # you will be left with processes that are using the TPUs on the other workers. 
        # This will stop you from restarting your job until those processes a terminated and release the TPU.
        # The following command should end all these processes. 
        # see more details: https://github.com/google/jax/issues/10192
        subprocess.run("sudo lsof -w /dev/accel0", shell=True) # kill all processes using the TPUs
        subprocess.run("sudo rm -f /tmp/libtpu_lockfile", shell=True) # remove the lock file
    else:
        if os.path.exists("/tmp/libtpu_lockfile"):

            warnings.warn(
                "The tpulib lock file exists, and you might"
                "be unable to use the tpu for training. Please "
                "remove it (`/tmp/libtpu_lockfile`) manually or "
                "set ``RAY_TPU_DEV=1`` to release it."
            )


class _JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        # get the master address
        master_addr, master_port = worker_group.execute_single(0, get_address_and_port)
        master_addr_with_port = f"{master_addr}:{master_port}"
        num_workers = len(worker_group)

        if worker_group.num_gpus_per_worker: 
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

        additional_resources_per_worker = worker_group.additional_resources_per_worker
        
        # in case where `use_tpu == True``: 
        if additional_resources_per_worker and additional_resources_per_worker.pop("TPU", False): 
            # Get setup tasks in order to throw errors on failure.
            try_remove_tpulib_lock = bool(os.environ.get(RAY_TPU_DEV_ENV, False))
            print(try_remove_tpulib_lock)
            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_async(
                        release_tpu_lock,
                        try_remove_tpulib_lock=try_remove_tpulib_lock,
                    )
                )
            ray.get(setup_futures)
