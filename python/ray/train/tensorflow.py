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

import tensorflow as tf

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
        "cluster": {"worker": worker_addresses},
        "task": {"type": "worker", "index": index},
    }
    os.environ["TF_CONFIG"] = json.dumps(tf_config)


class TensorflowBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TensorflowConfig):
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
                    i, setup_tensorflow_environment, worker_addresses=urls, index=i
                )
            )
        ray.get(setup_futures)


@PublicAPI(stability="beta")
def prepare_dataset_shard(tf_dataset_shard: tf.data.Dataset):
    """A utility function that disables Tensorflow autosharding.

    This should be used on a  TensorFlow ``Dataset`` created by calling ``to_tf()``
    on a ``ray.data.Dataset`` returned by ``ray.train.get_dataset_shard()`` since
    the dataset has already been sharded across the workers.

    Args:
        tf_dataset_shard (tf.data.Dataset): A TensorFlow Dataset.

    Returns:
        A TensorFlow Dataset with autosharding turned off.
    """
    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = (
        tf.data.experimental.AutoShardPolicy.OFF
    )
    return tf_dataset_shard.with_options(options)
