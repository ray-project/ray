import json
import logging
import os
from dataclasses import dataclass
from typing import List

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
        master_addr_with_port (str): The master work's address plus the port. 
        num_workers (int): Total number of all the workers.
        index (int): Index (i.e. world rank) of the current worker.
    """
    use_gpu = len(ray.get_gpu_ids())
    if use_gpu: 
        import jax
        jax.distributed.initialize(master_addr_with_port, num_workers, index)
    # TODO
    # cpu parallel: https://github.com/google/jax/issues/1408
    # os.environ["XLA_FLAGS"] = "--xla_force_host_platform_device_count=200"


def release_tpu_lock():
    # see https://github.com/google/jax/issues/10192
    import subprocess
    subprocess.run("sudo lsof -w /dev/accel0", shell=True)
    subprocess.run("sudo rm -f /tmp/libtpu_lockfile", shell=True)


class JaxBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: JaxConfig):
        # get the master address
        master_addr, master_port = worker_group.execute_single(
            0, get_address_and_port
        )
        master_addr_with_port = f"{master_addr}:{master_port}"
        num_workers = len(worker_group)

        # Get setup tasks in order to throw errors on failure.
        setup_futures = []
        for i in range(len(worker_group)):
            setup_futures.append(
                worker_group.execute_single_async(
                    i, setup_jax_environment, master_addr_with_port=master_addr_with_port, num_workers=num_workers, index=i
                )
            )
        ray.get(setup_futures)

        # case-insensitivize dict
        additional_resources_per_worker = worker_group.additional_resources_per_worker
        additional_resources_per_worker_lower = dict((k.lower(),v) for k,v in additional_resources_per_worker.items())
        use_tpu = additional_resources_per_worker_lower.pop("tpu", False)
        # Get setup tasks in order to throw errors on failure.
        
        if use_tpu: 
            # TODO: maybe throw exception or unsafe flag here
            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i, release_tpu_lock
                    )
                )
            ray.get(setup_futures)