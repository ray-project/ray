from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
from filelock import FileLock
import logging
import os
import torch.nn as nn
import torch.distributed as dist
import torch.utils.data
from torch.nn.parallel import DistributedDataParallel

from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner

logger = logging.getLogger(__name__)


class DistributedPyTorchRunner(PyTorchRunner):
    """Manages a distributed PyTorch model replica."""

    def __init__(self, *args, backend="gloo", **kwargs):
        """Initializes the runner.

        Args:
            args: Arguments for the PyTorchRunner.
            kwargs: Keyword arguments for the PyTorchRunner.
            backend (string): backend used by distributed PyTorch.
        """
        super(DistributedPyTorchRunner, self).__init__(*args, **kwargs)
        self.backend = backend

    def setup(self, url, world_rank, world_size):
        """Connects to the distributed PyTorch backend and initializes the model.

        Args:
            url (str): the URL used to connect to distributed PyTorch.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        self._setup_distributed_pytorch(url, world_rank, world_size)
        self._setup_training()

    def _setup_distributed_pytorch(self, url, world_rank, world_size):
        with self._timers["setup_proc"]:
            self.world_rank = world_rank
            logger.debug(
                "Connecting to {} world_rank: {} world_size: {}".format(
                    url, world_rank, world_size))
            logger.debug("using {}".format(self.backend))
            dist.init_process_group(
                backend=self.backend,
                init_method=url,
                rank=world_rank,
                world_size=world_size)

    def _setup_training(self):
        logger.debug("Creating model")
        self.models = self.model_creator(self.config)
        if not isinstance(self.models, collections.Iterable):
            self.models = [self.models]
        assert all(isinstance(model, nn.Module) for model in self.models), (
            "All models must be PyTorch models: {}.".format(self.models))
        if torch.cuda.is_available():
            self.models = [model.cuda() for model in self.models]
        self.models = [DistributedDataParallel(model) for model in self.models]

        logger.debug("Creating optimizer.")
        self.optimizers = self.optimizer_creator(self.given_models,
                                                 self.config)
        if not isinstance(self.optimizers, collections.Iterable):
            self.optimizers = [self.optimizers]
        self.criterion = self.loss_creator(self.config)
        if torch.cuda.is_available():
            self.criterion = self.criterion.cuda()

        logger.debug("Creating dataset.")
        with FileLock(os.path.expanduser("~/.ray_data.lock")):
            data_loaders = self.data_creator(self.batch_size, self.config)
        self.train_loader, self.validation_loader = self._validate_loaders(
            data_loaders)

    def step(self):
        """Runs a training epoch and updates the model parameters.

        Automatically sets epoch of sampler if possible.
        """
        logger.debug("Starting step")
        if hasattr(self.train_loader.sampler, "set_epoch"):
            self.train_loader.sampler.set_epoch(self.epoch)
        return super(DistributedPyTorchRunner, self).step()

    def get_state(self):
        """Returns the state of the runner."""
        return {
            "epoch": self.epoch,
            "models": [
                model.module.cpu().state_dict() for model in self.models
            ],
            "optimizers": [opt.state_dict() for opt in self.optimizers],
            "stats": self.stats()
        }

    def set_state(self, state):
        """Sets the state of the model."""
        # TODO: restore timer stats
        for model, model_state_dict in zip(self.models, state["models"]):
            model.module.load_state_dict(model_state_dict)
        for optimizer, opt_state_dict in zip(self.optimizers,
                                             state["optimizers"]):
            optimizer.load_state_dict(opt_state_dict)
        self.epoch = state["stats"]["epoch"]

    def shutdown(self):
        """Attempts to shut down the worker."""
        super(DistributedPyTorchRunner, self).shutdown()
        dist.destroy_process_group()
