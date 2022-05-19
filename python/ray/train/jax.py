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

import jax

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
@dataclass
class JaxConfig(BackendConfig):
    @property
    def backend_cls(self):
        return JaxBackend


def setup_jax_environment(master_addr_with_port: str, num_workers: int, index: int):
    """Set up distributed jax training information.

    This function should be called on each worker.

    Args:
        master_addr_with_port (str): The master work's address plus the port. 
        num_workers (int): Total number of all the workers.
        index (int): Index (i.e. world rank) of the current worker.
    """
    jax.distributed.initialize(master_addr_with_port, num_workers, index)
    # TODO
    # cpu parallel: https://github.com/google/jax/issues/1408
    # os.environ["XLA_FLAGS"] = "--xla_force_host_platform_device_count=200"



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
