import json
import logging
import os
from dataclasses import dataclass
from typing import List

import ray
from ray._common.network_utils import build_address
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
@dataclass
class TensorflowConfig(BackendConfig):
    @property
    def backend_cls(self):
        return _TensorflowBackend


def _setup_tensorflow_environment(worker_addresses: List[str], index: int):
    """Set up distributed Tensorflow training information.

    This function should be called on each worker.

    Args:
        worker_addresses: Addresses of all the workers.
        index: Index (i.e. world rank) of the current worker.
    """
    tf_config = {
        "cluster": {"worker": worker_addresses},
        "task": {"type": "worker", "index": index},
    }
    os.environ["TF_CONFIG"] = json.dumps(tf_config)


class _TensorflowBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TensorflowConfig):
        # Compute URL for initializing distributed setup.
        def get_url():
            address, port = get_address_and_port()
            return build_address(address, port)

        urls = worker_group.execute(get_url)

        # Get setup tasks in order to throw errors on failure.
        setup_futures = []
        for i in range(len(worker_group)):
            setup_futures.append(
                worker_group.execute_single_async(
                    i,
                    _setup_tensorflow_environment,
                    worker_addresses=urls,
                    index=i,
                )
            )
        ray.get(setup_futures)
