from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import json
import tensorflow as tf

from ray.experimental.sgd.tensorflow.tensorflow_runner import TensorFlowRunner

logger = logging.getLogger(__name__)


class DistributedTensorFlowRunner(TensorFlowRunner):
    """Manages a distributed TensorFlow model replica."""
    def setup(self, urls, world_rank, world_size):
        """Connects to the distributed PyTorch backend and initializes the model.

        Args:
            url (str): the URL used to connect to distributed PyTorch.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        assert len(urls) == world_size
        tf_config = {
            "worker": urls,
            "task": {
                "index": world_rank,
                "type": "worker"
            }
        }
        os.environ["TF_CONFIG"] = json.dumps(tf_config)

        self.strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()

        with self.strategy.scope():
            self.model = self.model_creator()
            self.train_dataset, self.test_dataset = self.data_creator(self.batch_size)

        # for use in model.evaluate()
        self.local_model = self.model_creator()

    def step(self):

        """Runs a training epoch and updates the model parameters."""
        history = self.model.fit(self.train_dataset, verbose=0)

        if history == None:
            # model.fit() returns None for MultiWorkerMirroredStrategy
            stats = {}
        else:
            stats = {
                "train_loss" : history.history['loss'][-1]
            }

        self.epoch += 1

        return stats

    def validate(self):
        """Evaluates the model on the validation data set."""

        stats = {}

        results = self.model.evaluate(self.test_dataset, verbose=0)

        if results == None:
            # Using local Model since model.evaluate() returns None for MultiWorkerMirroredStrategy
            self.local_model.set_weights(self.model.get_weights())
            results = self.local_model.evaluate(self.test_dataset, verbose=0)
            stats["validation_loss"] = results[0]
            results = self.local_model.evaluate(self.train_dataset, verbose=0)
            stats["train_loss"] = results[0]
        else:
            stats["validation_loss"] = results[0]
            results = self.local_model.evaluate(self.train_dataset, verbose=0)
            stats["train_loss"] = results[0]

        return stats

