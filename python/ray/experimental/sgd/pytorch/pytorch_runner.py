from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
from filelock import FileLock
import logging
import os
import torch
import torch.utils.data
from torch.utils.data import DataLoader

import ray
from ray.experimental.sgd.pytorch import utils as pytorch_utils
from ray.experimental.sgd import utils

logger = logging.getLogger(__name__)


class PyTorchRunner(object):
    """Manages a PyTorch model for training."""

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 loss_creator,
                 train_function=None,
                 validation_function=None,
                 config=None,
                 batch_size=16):
        """Initializes the runner.

        Args:
            model_creator (dict -> torch.nn.Module): see pytorch_trainer.py
            data_creator (int, dict -> DataLoader, DataLoader): see
                pytorch_trainer.py.
            optimizer_creator (torch.nn.Module, dict -> loss, optimizer):
                see pytorch_trainer.py.
            loss_creator (dict -> loss): see pytorch_trainer.py.
            train_function: see pytorch_trainer.py
            validation_function: see pytorch_trainer.py
            config (dict): see pytorch_trainer.py.
            batch_size (int): see pytorch_trainer.py.
        """
        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.config = {} if config is None else config
        self.train_function = train_function or pytorch_utils.train
        self.validation_function = (validation_function
                                    or pytorch_utils.validate)
        self.batch_size = batch_size
        self.verbose = True

        self.epoch = 0
        self._timers = {
            k: utils.TimerStat(window_size=1)
            for k in [
                "setup_proc", "setup_model", "get_state", "set_state",
                "validation", "training"
            ]
        }

        self.models = None
        self.optimizers = None
        self.criterion = None
        self.train_loader = None
        self.validation_loader = None

    def _validate_loaders(self, data_loaders):
        assert data_loaders, "Dataloaders need to be returned in data_creator."
        if isinstance(data_loaders, DataLoader):
            return data_loaders, None
        elif len(data_loaders) == 2 and isinstance(data_loaders[0],
                                                   DataLoader):
            return data_loaders
        else:
            raise ValueError(
                "Dataloaders must be <= 2. Got {}".format(data_loaders))

    def setup(self):
        """Initializes the model."""
        logger.debug("Creating model")
        self.models = self.model_creator(self.config)
        if not isinstance(self.models, collections.Iterable):
            self.models = [self.models]
        if torch.cuda.is_available():
            self.models = [model.cuda() for model in self.models]

        logger.debug("Creating optimizer")
        self.optimizers = self.optimizer_creator(self.given_models,
                                                 self.config)
        if not isinstance(self.optimizers, collections.Iterable):
            self.optimizers = [self.optimizers]
        self.criterion = self.loss_creator(self.config)
        if torch.cuda.is_available():
            self.criterion = self.criterion.cuda()

        logger.debug("Creating dataset")
        with FileLock(os.path.expanduser("~/.ray_data.lock")):
            dataloaders = self.data_creator(self.batch_size, self.config)
            self.train_loader, self.validation_loader = self._validate_loaders(
                dataloaders)

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        return utils.find_free_port()

    def step(self):
        """Runs a training epoch and updates the model parameters."""
        logger.debug("Begin Training Epoch {}".format(self.epoch + 1))
        with self._timers["training"]:
            train_stats = self.train_function(
                self.given_models, self.train_loader, self.criterion,
                self.given_optimizers, self.config)
            train_stats["epoch"] = self.epoch

        self.epoch += 1

        train_stats.update(self.stats())
        return train_stats

    def validate(self):
        """Evaluates the model on the validation data set."""
        if self.validation_loader is None:
            raise ValueError("No validation dataloader provided.")
        with self._timers["validation"]:
            validation_stats = self.validation_function(
                self.given_models, self.validation_loader, self.criterion,
                self.config)

        validation_stats.update(self.stats())
        return validation_stats

    def stats(self):
        """Returns a dictionary of statistics collected."""
        stats = {"epoch": self.epoch}
        for k, t in self._timers.items():
            stats[k + "_time_mean"] = t.mean
            stats[k + "_time_total"] = t.sum
            t.reset()
        return stats

    def get_state(self):
        """Returns the state of the runner."""
        return {
            "epoch": self.epoch,
            "models": [model.cpu().state_dict() for model in self.models],
            "optimizers": [opt.state_dict() for opt in self.optimizers],
            "stats": self.stats()
        }

    def set_state(self, state):
        """Sets the state of the model."""
        # TODO: restore timer stats
        for model, state_dict in zip(self.models, state["models"]):
            model.load_state_dict(state_dict)
        for optimizer, state_dict in zip(self.optimizers, state["optimizers"]):
            optimizer.load_state_dict(state_dict)
        self.epoch = state["stats"]["epoch"]

    def apply_fn(self, fn):
        return fn(self)

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.validation_loader
        del self.train_loader
        del self.criterion
        del self.optimizers
        del self.models
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    @property
    def given_optimizers(self):
        if len(self.optimizers) > 1:
            return self.optimizers
        else:
            return self.optimizers[0]

    @property
    def given_models(self):
        if len(self.models) > 1:
            return self.models
        else:
            return self.models[0]
