from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import logging

import ray

from ray.tune import Trainable
from ray.tune.resources import Resources
from ray.experimental.sgd.tensorflow.tensorflow_runner import TensorFlowRunner
from ray.experimental.sgd.tensorflow.distributed_tensorflow_runner import (
    DistributedTensorFlowRunner)
from ray.experimental.sgd import utils

logger = logging.getLogger(__name__)


class TensorFlowTrainer(object):
    def __init__(self,
        model_creator,
        data_creator,
        config=None,
        num_replicas=1,
        use_gpu=False,
        batch_size=512
        ):
        """Sets up the TensorFlow trainer.

        Args:
            model_creator (dict -> Model): creates the model
                using the config.
            data_creator (dict -> BatchDataset, BatchDataset): creates the training
                and validation data sets using the config.
            config (dict): configuration passed to 'model_creator',
                'data_creator', and 'optimizer_creator'.
            num_replicas (int): the number of workers used in distributed
                training.
            use_gpu (bool): Sets resource allocation for workers to 1 GPU
                if true.
            batch_size (int): batch size for an update.
            backend (string): backend used by distributed PyTorch.
        """
        self.model_creator = model_creator
        self.data_creator = data_creator
        self.config = {} if config is None else config
        self.batch_size = batch_size
        self.use_gpu = use_gpu
        self.num_replicas = num_replicas
        self.verbose = True


        # Generate actor class
        if num_replicas == 1:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(use_gpu))(TensorFlowRunner)
            # Start workers
            self.workers = [
                Runner.remote(model_creator, data_creator,
                              self.config, batch_size)
            ]
            # Get setup tasks in order to throw errors on failure
            ray.get(self.workers[0].setup.remote())
        else:
            # Geneate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(use_gpu))(DistributedTensorFlowRunner)

            # Start workers
            self.workers = [
                Runner.remote(model_creator, data_creator,
                              self.config, batch_size)
                for i in range(num_replicas)
            ]

            # Compute URL for initializing distributed setup
            ips = ray.get([
                worker.get_node_ip.remote()
                for worker in self.workers
            ])
            ports = ray.get([
                worker.find_free_port.remote()
                for worker in self.workers
            ])

            urls = [
                "{ip}:{port}".format(ip=ips[i], port=ports[i])
                for i in range(len(self.workers))
            ]

            # Get setup tasks in order to throw errors on failure
            ray.get([
                worker.setup.remote(urls, i, len(self.workers))
                for i, worker in enumerate(self.workers)
            ])

    def train(self):
        """Runs a training epoch."""
        worker_stats = ray.get([w.step.remote() for w in self.workers])
        stats = worker_stats[0].copy()
        return stats

    def validate(self):
        """Evaluates the model on the validation data set."""
        stats = ray.get(self.workers[0].validate.remote())
        return stats

    def get_state(self):
        # NOTE: at first I tried to implement get_model(),
        # but was not possible since we cannot restore optimizer weights in the trainer.
        # see TensorFlowRunner.set_state()
        state = ray.get(self.workers[0].get_state.remote())
        return state

    def save(self, checkpoint):
        """Saves the model at the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """

        state = self.get_state()
        state["optimizer_weights"][0] = np.array(state["optimizer_weights"][0], dtype=np.int64)
        # this fix is needed due to ray.get() changing scalar np.int64 to int, causing error at optimizer.set_weights()

        # NOTE: this is a HACK, help needed
        # because newly created model does not have variables initialized, set_weights does not work
        self.model = self.model_creator()
        _, self.test_dataset = self.data_creator(self.batch_size)
        self.model.fit(self.test_dataset)
        self.model.set_weights(state["weights"])
        self.model.optimizer.set_weights(state["optimizer_weights"])

        self.model.save(checkpoint+'.h5')

        del state["weights"]
        del state["optimizer_weights"]

        with open(checkpoint+'_state.json', 'w') as f:
            state = json.dump(state,f)

        return checkpoint

    def restore(self, checkpoint):
        """Restores the model from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """
        model = tf.keras.models.load_model(checkpoint+'.h5')
        with open(checkpoint+'_state.json') as f:
            state = json.load(f)

        state["weights"] = model.get_weights()
        state["optimizer_weights"] = model.optimizer.get_weights()

        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self):
        """Shuts down workers and releases resources."""
        for worker in self.workers:
            worker.shutdown.remote()
            worker.__ray_terminate__.remote()


class TensorFlowTrainable(Trainable):
    @classmethod
    def default_resource_request(cls, config):
        return Resources(
            cpu=0,
            gpu=0,
            extra_cpu=config["num_replicas"],
            extra_gpu=int(config["use_gpu"]) * config["num_replicas"])

    def _setup(self, config):
        self._trainer = TensorFlowTrainer(
            model_creator=config["model_creator"],
            data_creator=config["data_creator"],
            config=config,
            num_replicas=config["num_replicas"],
            use_gpu=config["use_gpu"],
            batch_size=config["batch_size"])

    def _train(self):

        train_stats = self._trainer.train()
        validation_stats = self._trainer.validate()

        train_stats.update(validation_stats)

        return train_stats

    def _save(self, checkpoint_dir):
        return self._trainer.save(os.path.join(checkpoint_dir, "model"))

    def _restore(self, checkpoint_path):
        return self._trainer.restore(checkpoint_path)

    def _stop(self):
        self._trainer.shutdown()

