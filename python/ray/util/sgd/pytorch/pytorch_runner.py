import collections
from filelock import FileLock
import logging
import inspect
import itertools
import os
import torch
import torch.utils.data
from torch.utils.data import Dataset

import ray
from ray.util.sgd.pytorch.constants import USE_FP16, SCHEDULER_STEP
from ray.util.sgd.pytorch.training_operator import TrainingOperator
from ray.util.sgd import utils

logger = logging.getLogger(__name__)
amp = None

try:
    from apex import amp
except ImportError:
    logger.debug("apex is not installed.")
    pass


class PyTorchRunner:
    """Manages a PyTorch model for training.

    Args:
        model_creator (dict -> *): see pytorch_trainer.py
        data_creator (dict -> Dataset, Dataset): see pytorch_trainer.py.
        optimizer_creator (models, dict -> optimizers): see pytorch_trainer.py.
        loss_creator (dict -> loss | Loss class): see pytorch_trainer.py.
        scheduler_creator (optimizers, dict -> schedulers): see
            pytorch_trainer.py.
        training_operator_cls: see pytorch_trainer.py
        config (dict): see pytorch_trainer.py.
        dataloader_config (dict): See pytorch_trainer.py.
        batch_size (int): see pytorch_trainer.py.
        use_fp16 (bool): see pytorch_trainer.py.
        apex_args (dict|None): see pytorch_trainer.py.
        scheduler_step_freq (str): see pytorch_trainer.py.
    """

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 loss_creator,
                 scheduler_creator=None,
                 training_operator_cls=None,
                 config=None,
                 dataloader_config=None,
                 batch_size=16,
                 use_fp16=False,
                 apex_args=None,
                 scheduler_step_freq="batch"):
        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.scheduler_creator = scheduler_creator
        self.training_operator_cls = training_operator_cls or TrainingOperator
        self.config = {} if config is None else config
        self.dataloader_config = {
            "num_workers": 2
        } if dataloader_config is None else dataloader_config
        self.batch_size = batch_size
        self.verbose = True

        self.epochs = 0
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
        self.schedulers = None
        self.train_loader = None
        self.validation_loader = None
        self.use_fp16 = use_fp16
        self.apex_args = apex_args or {}
        if use_fp16 and not amp:
            raise ImportError(
                "Please install apex from "
                "https://www.github.com/nvidia/apex to use fp16 training.")
        self.scheduler_step_freq = scheduler_step_freq

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

    def _create_schedulers_if_available(self):
        # Learning rate schedules are optional.
        if not self.scheduler_creator:
            return
        self.schedulers = self.scheduler_creator(self.given_optimizers,
                                                 self.config)

        if not isinstance(self.schedulers, collections.Iterable):
            self.schedulers = [self.schedulers]

    def _try_setup_apex(self):
        """Sets up the model for fp16 training via apex if available."""
        if self.use_fp16 and amp:
            self.models, self.optimizers = amp.initialize(
                self.models, self.optimizers, **self.apex_args)

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
        self._create_schedulers_if_available()
        self._try_setup_apex()
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

        self.training_operator = self.training_operator_cls(
            self.config,
            models=self.models,
            optimizers=self.optimizers,
            criterion=self.criterion,
            train_loader=self.train_loader,
            validation_loader=self.validation_loader,
            world_rank=0,
            schedulers=self.schedulers,
            use_fp16=self.use_fp16)

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        return utils.find_free_port()

    def train_epoch(self, num_steps=None, info=None):
        """Runs a training epoch and updates the model parameters."""
        logger.debug("Begin Training Step {}".format(self.epochs + 1))
        info = info or {}
        info.update({
            USE_FP16: self.use_fp16,
            SCHEDULER_STEP: self.scheduler_step_freq
        })
        with self._timers["training"]:
            iterator = self.train_loader
            if num_steps:
                iterator = itertools.islice(iter(self.train_loader), num_steps)
            train_stats = self.training_operator.train_epoch(iterator, info)

        self.epochs += 1
        train_stats.update(self.stats())
        return train_stats

    def validate(self, num_steps=None, info=None):
        """Evaluates the model on the validation data set."""
        if self.validation_loader is None:
            raise ValueError("No validation dataloader provided.")
        info = info or {}
        with self._timers["validation"]:
            iterator = self.validation_loader
            if num_steps:
                iterator = itertools.islice(
                    iter(self.validation_loader), num_steps)
            validation_stats = self.training_operator.validate(iterator, info)

        validation_stats.update(self.stats())
        return validation_stats

    def stats(self):
        """Returns a dictionary of statistics collected."""
        stats = {"epoch": self.epochs}
        for k, t in self._timers.items():
            stats[k + "_time_mean"] = t.mean
            stats[k + "_time_total"] = t.sum
            t.reset()
        return stats

    def _get_model_state_dicts(self):
        # This is so that we create a duplicate of weights into CPU rather than
        # move the model weights entirely out of the GPU, so that we can
        # resume training while saving intermediate checkpoints.
        cpu_state_dicts = []
        for model in self.models:
            state_dict = model.state_dict()
            cpu_state_dicts += [{k: v.cpu() for k, v in state_dict.items()}]
        return cpu_state_dicts

    def _set_model_state_dicts(self, models_state_dicts):
        for model, state_dict in zip(self.models, models_state_dicts):
            model.load_state_dict(state_dict)

    def get_state(self):
        """Returns the state of the runner."""

        state = {
            "epoch": self.epochs,
            "operator": self.training_operator.state_dict(),
            "models": self._get_model_state_dicts(),
            "optimizers": [opt.state_dict() for opt in self.optimizers],
            "stats": self.stats()
        }
        if self.schedulers:
            state.update({
                "schedulers": [
                    scheduler.state_dict() for scheduler in self.schedulers
                ]
            })
        # Check if fp16 is True and if NVIDIA Apex is imported.
        if self.use_fp16 and amp:
            state.update({"amp": amp.state_dict()})
        return state

    def set_state(self, state):
        """Sets the state of the model."""
        # TODO: restore timer stats
        self._set_model_state_dicts(state["models"])
        for optimizer, state_dict in zip(self.optimizers, state["optimizers"]):
            optimizer.load_state_dict(state_dict)
        if self.schedulers:
            for scheduler, state_dict in zip(self.schedulers,
                                             state["schedulers"]):
                scheduler.load_state_dict(state_dict)

        if self.use_fp16 and "amp" in state and amp:
            amp.load_state_dict(state["amp"])
        self.epochs = state["stats"]["epoch"]
        self.training_operator.load_state_dict(state_dict)

    def apply(self, fn):
        return fn()

    def apply_operator(self, fn):
        return fn(self.training_operator)

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.training_operator
        del self.validation_loader
        del self.train_loader
        del self.criterion
        del self.optimizers
        del self.models
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    @property
    def given_models(self):
        if len(self.models) > 1:
            return self.models
        else:
            return self.models[0]

    @property
    def given_optimizers(self):
        if len(self.optimizers) > 1:
            return self.optimizers
        else:
            return self.optimizers[0]

    @property
    def given_schedulers(self):
        if not self.schedulers:
            return self.schedulers
        if len(self.schedulers) > 1:
            return self.schedulers
        else:
            return self.schedulers[0]
