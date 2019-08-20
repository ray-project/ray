from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import json
import os
import tensorflow as tf

import ray
from ray.experimental.sgd import utils

logger = logging.getLogger(__name__)


class TensorFlowRunner(object):
    """Manages a TensorFlow model for training."""

    def __init__(self, model_creator, data_creator, config=None,
                 batch_size=16):
        """Initializes the runner.

        Args:
            model_creator (dict -> Model): see tensorflow_trainer.py.
            data_creator (dict -> BatchDataset, BatchDataset):
                see tensorflow_trainer.py.
            config (dict): see tensorflow_trainer.py.
            batch_size (int): see tensorflow_trainer.py.
        """

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.config = {} if config is None else config
        self.batch_size = batch_size
        self.verbose = True

        self.epoch = 0

    def setup(self):
        """Initializes the model."""
        logger.debug("Creating model")
        self.model = self.model_creator()

        logger.debug("Creating dataset")
        self.train_dataset, self.test_dataset = self.data_creator(
            self.batch_size)

    def setup_distributed(self, urls, world_rank, world_size):
        """Sets up TensorFLow distributed environment and initializes the model.

        Args:
            urls (str): the URLs that each node uses to connect.
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

        self.strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy(
        )

        logger.debug("Creating model with MultiWorkerMirroredStrategy")
        with self.strategy.scope():
            self.model = self.model_creator()
            self.train_dataset, self.test_dataset = self.data_creator(
                self.batch_size)

        # for use in model.evaluate()
        self.local_model = self.model_creator()

    def step(self):
        """Runs a training epoch and updates the model parameters."""

        history = self.model.fit(self.train_dataset, verbose=0)

        if history is None:
            # model.fit() returns None for MultiWorkerMirroredStrategy
            stats = {}
        else:
            stats = {"train_loss": history.history["loss"][-1]}

        self.epoch += 1
        return stats

    def validate(self):
        """Evaluates the model on the validation data set."""
        stats = {}

        results = self.model.evaluate(self.test_dataset, verbose=0)

        if results is None:
            # Using local Model since model.evaluate() returns None
            # for MultiWorkerMirroredStrategy
            self.local_model.set_weights(self.model.get_weights())
            results = self.local_model.evaluate(self.test_dataset, verbose=0)
            stats["validation_loss"] = results[0]
        else:
            stats["validation_loss"] = results[0]

        return stats

    def get_state(self):

        return {
            "epoch": self.epoch,
            "weights": self.model.get_weights(),
            "optimizer_weights": self.model.optimizer.get_weights()
        }

    def set_state(self, state):
        self.epoch = state["epoch"]
        if self.model.optimizer.weights == []:
            # NOTE: This is a hack; optimizer.weights are initially [] and are
            # generated at first run of fit(). need help getting around this
            self.model.fit(self.test_dataset)
        self.model.set_weights(state["weights"])

        import numpy as np
        state["optimizer_weights"][0] = np.array(
            state["optimizer_weights"][0], dtype=np.int64)
        # this part is due to ray.get() changing scalar np.int64 to int

        self.model.optimizer.set_weights(state["optimizer_weights"])

    def shutdown(self):
        del self.model
        del self.train_dataset
        del self.test_dataset

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        return utils.find_free_port()
