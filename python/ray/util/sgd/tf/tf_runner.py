import logging
import json
import os
import numpy as np

import ray
import ray.services
from ray.util.sgd import utils

logger = logging.getLogger(__name__)


def _try_import_strategy():
    """Late import for Tesnorflow"""
    import tensorflow as tf
    return tf.distribute.experimental.MultiWorkerMirroredStrategy


class TFRunner:
    """Manages a TensorFlow model for training."""

    def __init__(self, model_creator, data_creator, config=None,
                 verbose=False):
        """Initializes the runner.

        Args:
            model_creator (dict -> Model): see tf_trainer.py.
            data_creator (dict -> tf.Dataset, tf.Dataset): see tf_trainer.py.
            config (dict): see tf_trainer.py.
            verbose (bool): Outputs training data if true.
        """

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.config = {} if config is None else config
        self.epoch = 0
        self.verbose = verbose

    def setup(self):
        """Initializes the model."""
        logger.debug("Creating dataset")
        self.train_dataset, self.test_dataset = self.data_creator(self.config)

        logger.debug("Creating model")
        self.model = self.model_creator(self.config)

    def setup_distributed(self, urls, world_rank, world_size):
        """Sets up TensorFLow distributed environment and initializes the model.

        Args:
            urls (str): the URLs that each node uses to connect.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        assert len(urls) == world_size
        tf_config = {
            "cluster": {
                "worker": urls
            },
            "task": {
                "index": world_rank,
                "type": "worker"
            }
        }
        os.environ["TF_CONFIG"] = json.dumps(tf_config)

        MultiWorkerMirroredStrategy = _try_import_strategy()

        # MultiWorkerMirroredStrategy handles everything for us, from
        # sharding the dataset (or even sharding the data itself if the loader
        # reads files from disk) to merging the metrics and weight updates
        #
        # worker 0 is the "chief" worker and will handle the map-reduce
        # every worker ends up with the exact same metrics and model
        # after model.fit
        #
        # because of this, we only really ever need to query its state
        self.strategy = MultiWorkerMirroredStrategy()

        self.train_dataset, self.test_dataset = self.data_creator(self.config)

        logger.debug("Creating model with MultiWorkerMirroredStrategy")
        with self.strategy.scope():
            self.model = self.model_creator(self.config)

        # For use in model.evaluate()
        self.local_model = None

    def step(self):
        """Runs a training epoch and updates the model parameters."""
        fit_default_config = {"verbose": self.verbose}
        fit_default_config.update(self.config.get("fit_config", {}))

        history = self.model.fit(self.train_dataset, **fit_default_config)
        if history is None:
            stats = {}
        else:
            stats = {"train_" + k: v[-1] for k, v in history.history.items()}

        self.epoch += 1
        return stats

    def validate(self):
        """Evaluates the model on the validation data set."""
        stats = {}
        evaluate_config = {"verbose": self.verbose}
        evaluate_config.update(self.config.get("evaluate_config", {}))

        results = self.model.evaluate(self.test_dataset, **evaluate_config)
        if results is None:
            # Using local Model since model.evaluate() returns None
            # for MultiWorkerMirroredStrategy
            logger.warning("Running a local model to get validation score.")
            self.local_model = self.model_creator(self.config)
            self.local_model.set_weights(self.model.get_weights())
            results = self.local_model.evaluate(self.test_dataset,
                                                **evaluate_config)

        if isinstance(results, list):
            stats = {
                "validation_" + k: v
                for k, v in zip(self.model.metrics_names, results)
            }
        else:
            stats = {"loss": results}

        return stats

    def get_state(self):
        """Returns the state of the runner."""
        return {
            "epoch": self.epoch,
            "weights": self.model.get_weights(),
            "optimizer_weights": self.model.optimizer.get_weights()
        }

    def set_state(self, state):
        """Sets the state of the model."""

        self.model = self.model_creator(self.config)
        self.epoch = state["epoch"]
        self.model.set_weights(state["weights"])
        # This part is due to ray.get() changing scalar np.int64 object to int
        state["optimizer_weights"][0] = np.array(
            state["optimizer_weights"][0], dtype=np.int64)

        if self.model.optimizer.weights == []:
            self.model._make_train_function()
        self.model.optimizer.set_weights(state["optimizer_weights"])

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.model
        del self.train_dataset
        del self.test_dataset

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        return utils.find_free_port()
