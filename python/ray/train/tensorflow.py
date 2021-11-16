import json
import logging
import os
from dataclasses import dataclass
from typing import List

import ray
from ray.train.backend import BackendConfig, Backend
from ray.train.session import shutdown_session
from ray.train.utils import get_address_and_port
from ray.train.worker_group import WorkerGroup
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
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


class TensorflowBackend(Backend):
    def on_start(self, worker_group: WorkerGroup,
                 backend_config: TensorflowConfig):
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

    def handle_failure(self, worker_group: WorkerGroup,
                       failed_worker_indexes: List[int],
                       backend_config: BackendConfig):
        """Failure handling for Tensorflow.

        Instead of restarting all workers, the failed workers are
        removed from the ``WorkerGroup``. The backend and session are
        shutdown on the remaining workers. Then new workers are added back in.
        """
        worker_group.remove_workers(failed_worker_indexes)
        if len(worker_group) > 0:
            self.on_shutdown(worker_group, backend_config)
            worker_group.execute(shutdown_session)
        worker_group.add_workers(len(failed_worker_indexes))
        self.on_start(worker_group, backend_config)
