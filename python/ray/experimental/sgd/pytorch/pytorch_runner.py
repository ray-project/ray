import collections
from filelock import FileLock
import logging
import inspect
import os
import torch
import torch.utils.data
from torch.utils.data import Dataset

import ray
from ray.experimental.sgd.pytorch import utils as pytorch_utils
from ray.experimental.sgd import utils

logger = logging.getLogger(__name__)


class PyTorchRunner:
    """Manages a PyTorch model for training."""

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 loss_creator,
                 train_function=None,
                 validation_function=None,
                 config=None,
                 dataloader_config=None,
                 batch_size=16):
        """Initializes the runner.

        Args:
            model_creator (dict -> torch.nn.Module): see pytorch_trainer.py
            data_creator (int, dict -> Dataset, Dataset): see
                pytorch_trainer.py.
            optimizer_creator (torch.nn.Module, dict -> loss, optimizer):
                see pytorch_trainer.py.
            loss_creator (dict -> loss | Loss class): see pytorch_trainer.py.
            train_function: see pytorch_trainer.py
            validation_function: see pytorch_trainer.py
            config (dict): see pytorch_trainer.py.
            dataloader_config (dict): See pytorch_trainer.py.
            batch_size (int): see pytorch_trainer.py.
        """
        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.config = {} if config is None else config
        self.dataloader_config = {
            "num_workers": 2,
            "pin_memory": True
        } if dataloader_config is None else dataloader_config
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

    def _validate_datasets(self, dataset):
        assert dataset, "Datasets need to be returned in data_creator."
        if issubclass(type(dataset), Dataset):
            return dataset, None
        elif len(dataset) == 2 and issubclass(type(dataset[0]), Dataset):
            return dataset
        else:
            raise ValueError("Datasets must be <= 2. Got {}".format(dataset))

    def _create_loss(self):
        if inspect.isclass(self.loss_creator) and issubclass(
                self.loss_creator, torch.nn.modules.loss._Loss):
            self.criterion = self.loss_creator()
        else:
            self.criterion = self.loss_creator(self.config)

        if torch.cuda.is_available():
            self.criterion = self.criterion.cuda()

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

        self._create_loss()

        logger.debug("Creating dataset")
        # When creating datasets, a filelock will be used to ensure no
        # race conditions in data downloading among different workers.
        with FileLock(os.path.expanduser("~/.ray_data.lock")):
            datasets = self.data_creator(self.config)
            train_set, val_set = self._validate_datasets(datasets)

        self.train_loader = torch.utils.data.DataLoader(
            train_set, batch_size=self.batch_size, **self.dataloader_config)

        self.validation_loader = None
        if val_set:
            self.validation_loader = torch.utils.data.DataLoader(
                val_set, batch_size=self.batch_size, **self.dataloader_config)

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
        # This is so that we create a duplicate of weights into CPU rather than
        # move the model weights entirely out of the GPU, so that we can
        # resume training while saving intermediate checkpoints.
        cpu_state_dicts = []
        for model in self.models:
            state_dict = model.state_dict()
            for k, v in state_dict.items():
                state_dict[k] = v.cpu()
            cpu_state_dicts += [state_dict]
        return {
            "epoch": self.epoch,
            "models": cpu_state_dicts,
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
