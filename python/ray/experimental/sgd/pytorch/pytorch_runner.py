from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import torch
import torch.utils.data

import ray
from ray.experimental.sgd.pytorch import pytorch_utils
from ray.experimental.sgd import utils

logger = logging.getLogger(__name__)


class PyTorchRunner(object):
    """Manages a PyTorch model for training."""

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 config=None,
                 batch_size=16):
        """Initializes the runner.

        Args:
            model_creator (dict -> torch.nn.Module): see pytorch_trainer.py.
            data_creator (dict -> Dataset, Dataset): see pytorch_trainer.py.
            optimizer_creator (torch.nn.Module, dict -> loss, optimizer):
                see pytorch_trainer.py.
            config (dict): see pytorch_trainer.py.
            batch_size (int): see pytorch_trainer.py.
        """

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.config = {} if config is None else config
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

    def setup(self):
        """Initializes the model."""
        logger.debug("Creating model")
        self.model = self.model_creator(self.config)
        if torch.cuda.is_available():
            self.model = self.model.cuda()

        logger.debug("Creating optimizer")
        self.criterion, self.optimizer = self.optimizer_creator(
            self.model, self.config)
        if torch.cuda.is_available():
            self.criterion = self.criterion.cuda()

        logger.debug("Creating dataset")
        self.training_set, self.validation_set = self.data_creator(self.config)
        self.train_loader = torch.utils.data.DataLoader(
            self.training_set,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=2,
            pin_memory=False)

        self.validation_loader = torch.utils.data.DataLoader(
            self.validation_set,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=2,
            pin_memory=False)

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
            train_stats = pytorch_utils.train(self.train_loader, self.model,
                                              self.criterion, self.optimizer)
            train_stats["epoch"] = self.epoch

        self.epoch += 1

        train_stats.update(self.stats())
        return train_stats

    def validate(self):
        """Evaluates the model on the validation data set."""
        with self._timers["validation"]:
            validation_stats = pytorch_utils.validate(
                self.validation_loader, self.model, self.criterion)

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
            "model": self.model.state_dict(),
            "optimizer": self.optimizer.state_dict(),
            "stats": self.stats()
        }

    def set_state(self, state):
        """Sets the state of the model."""
        # TODO: restore timer stats
        self.model.load_state_dict(state["model"])
        self.optimizer.load_state_dict(state["optimizer"])
        self.epoch = state["stats"]["epoch"]

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.validation_loader
        del self.validation_set
        del self.train_loader
        del self.training_set
        del self.criterion
        del self.optimizer
        del self.model
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
