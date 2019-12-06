from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import torch
import torch.distributed as dist
import logging

import ray

from ray.tune import Trainable
from ray.tune.trial import Resources
from ray.experimental.sgd.pytorch.distributed_pytorch_runner import (
    DistributedPyTorchRunner)
from ray.experimental.sgd import utils
from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner

logger = logging.getLogger(__name__)


class PyTorchTrainer(object):
    """Train a PyTorch model using distributed PyTorch.

    Launches a set of actors which connect via distributed PyTorch and
    coordinate gradient updates to train the provided model.
    """

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 loss_creator,
                 train_function=None,
                 validation_function=None,
                 initialization_hook=None,
                 config=None,
                 num_replicas=1,
                 use_gpu=False,
                 batch_size=16,
                 backend="auto"):
        """Sets up the PyTorch trainer.

        Args:
            model_creator (dict -> torch.nn.Module): creates the model
                using the config.
            data_creator (int, dict -> DataLoader, DataLoader): Function that
                takes in (batch_size, config) and returns two Torch DataLoader
                objects.
            optimizer_creator (torch.nn.Module, dict -> optimizer):
                creates the loss and optimizer using the model and the config.
            loss_creator (dict -> loss): Creates the loss function/criterion
                using the config.
            train_function: Trains a model for a epoch. This takes in (
                model, train_dataloader, criterion, optimizer, config), and
                returns a dict of training stats.
            validation_function: Runs validation. This takes in (
                model, val_dataloader, criterion, config) and returns a dict of
                validation stats.
            config (dict): configuration passed to "model_creator",
                "data_creator", "optimizer_creator", and "loss_creator".
            num_replicas (int): the number of workers used in distributed
                training.
            use_gpu (bool): Sets resource allocation for workers to 1 GPU
                if true.
            batch_size (int): batch size for an update.
            backend (string): backend used by distributed PyTorch.
        """
        # TODO: add support for mixed precision
        # TODO: add support for callbacks
        if num_replicas > 1 and not dist.is_available():
            raise ValueError(
                ("Distributed PyTorch is not supported on macOS. "
                 "To run without distributed PyTorch, set 'num_replicas=1'. "
                 "For more information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        self.model_creator = model_creator
        self.config = {} if config is None else config
        self.optimizer_timer = utils.TimerStat(window_size=1)

        if backend == "auto":
            backend = "nccl" if use_gpu else "gloo"

        logger.info("Using {} as backend.".format(backend))

        if num_replicas == 1:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(use_gpu))(PyTorchRunner)
            # Start workers
            self.workers = [
                Runner.remote(
                    model_creator,
                    data_creator,
                    optimizer_creator,
                    loss_creator,
                    train_function=train_function,
                    validation_function=validation_function,
                    config=self.config,
                    batch_size=batch_size)
            ]
            if initialization_hook:
                self.apply_all_workers(initialization_hook)
            # Get setup tasks in order to throw errors on failure
            ray.get(self.workers[0].setup.remote())
        else:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(use_gpu))(DistributedPyTorchRunner)
            # Compute batch size per replica
            batch_size_per_replica = batch_size // num_replicas
            if batch_size % num_replicas > 0:
                new_batch_size = batch_size_per_replica * num_replicas
                logger.warning(
                    ("Changing batch size from {old_batch_size} to "
                     "{new_batch_size} to evenly distribute batches across "
                     "{num_replicas} replicas.").format(
                         old_batch_size=batch_size,
                         new_batch_size=new_batch_size,
                         num_replicas=num_replicas))
            # Start workers
            self.workers = [
                Runner.remote(
                    model_creator,
                    data_creator,
                    optimizer_creator,
                    loss_creator,
                    backend=backend,
                    train_function=train_function,
                    validation_function=validation_function,
                    config=self.config,
                    batch_size=batch_size_per_replica)
                for i in range(num_replicas)
            ]
            if initialization_hook:
                self.apply_all_workers(initialization_hook)

            # Compute URL for initializing distributed PyTorch
            ip = ray.get(self.workers[0].get_node_ip.remote())
            port = ray.get(self.workers[0].find_free_port.remote())
            address = "tcp://{ip}:{port}".format(ip=ip, port=port)
            # Get setup tasks in order to throw errors on failure
            ray.get([
                worker.setup.remote(address, i, len(self.workers))
                for i, worker in enumerate(self.workers)
            ])

    def train(self):
        """Runs a training epoch."""
        with self.optimizer_timer:
            worker_stats = ray.get([w.step.remote() for w in self.workers])

        train_stats = worker_stats[0].copy()
        train_stats["train_loss"] = np.mean(
            [s["train_loss"] for s in worker_stats])
        return train_stats

    def apply_all_workers(self, fn):
        return ray.get([w.apply_fn.remote(fn) for w in self.workers])

    def validate(self):
        """Evaluates the model on the validation data set."""
        worker_stats = ray.get([w.validate.remote() for w in self.workers])
        validation_stats = worker_stats[0].copy()
        if "validation_loss" in validation_stats:
            validation_stats["validation_loss"] = np.nanmean(
                [s.get("validation_loss", np.nan) for s in worker_stats])
        return validation_stats

    def get_model(self):
        """Returns the learned model."""
        model = self.model_creator(self.config)
        state = ray.get(self.workers[0].get_state.remote())
        model.load_state_dict(state["model"])
        return model

    def save(self, checkpoint):
        """Saves the model at the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """
        state = ray.get(self.workers[0].get_state.remote())
        torch.save(state, checkpoint)
        return checkpoint

    def restore(self, checkpoint):
        """Restores the model from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """
        state = torch.load(checkpoint)
        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self):
        """Shuts down workers and releases resources."""
        for worker in self.workers:
            worker.shutdown.remote()
            worker.__ray_terminate__.remote()


class PyTorchTrainable(Trainable):
    @classmethod
    def default_resource_request(cls, config):
        return Resources(
            cpu=0,
            gpu=0,
            extra_cpu=config["num_replicas"],
            extra_gpu=int(config["use_gpu"]) * config["num_replicas"])

    def _setup(self, config):
        self._trainer = PyTorchTrainer(**config)

    def _train(self):
        train_stats = self._trainer.train()
        validation_stats = self._trainer.validate()

        train_stats.update(validation_stats)

        # output {"mean_loss": test_loss, "mean_accuracy": accuracy}
        return train_stats

    def _save(self, checkpoint_dir):
        return self._trainer.save(os.path.join(checkpoint_dir, "model.pth"))

    def _restore(self, checkpoint_path):
        return self._trainer.restore(checkpoint_path)

    def _stop(self):
        self._trainer.shutdown()
