import json
import logging
import os
from dataclasses import dataclass
from typing import List

import ray
from ray.util.sgd.v2.backends.backend import BackendConfig, BackendInterface
from ray.util.sgd.v2.utils import get_address_and_port
from ray.util.sgd.v2.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


@dataclass
class TensorflowConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TensorflowBackend


def setup_tensorflow_environment(worker_addresses: List[str], index: int):
    """Set up distributed Tensorflow training information.

    This function should be called on each worker.

    Args:
        worker_addresses (list): Addresses of all the workers.
        index (int): Index (i.e. world rank) of the current worker.
    """
    tf_config = {
        "cluster": {
            "worker": worker_addresses
        },
        "task": {
            "type": "worker",
            "index": index
        }
    }
    os.environ["TF_CONFIG"] = json.dumps(tf_config)


class TensorflowBackend(BackendInterface):
    def on_start(self, worker_group: WorkerGroup,
                 backend_config: TensorflowConfig):
        if len(worker_group) > 1:
            # Compute URL for initializing distributed setup.
            def get_url():
                address, port = get_address_and_port()
                return f"{address}:{port}"

            urls = worker_group.execute(get_url)

            # Get setup tasks in order to throw errors on failure.
            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        setup_tensorflow_environment,
                        worker_addresses=urls,
                        index=i))
            ray.get(setup_futures)

        else:
            logger.info("Distributed Tensorflow is not being used.")

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: BackendConfig):
        # Currently no additional steps are needed to shut down Tensorflow.
        pass
