from filelock import FileLock
import logging
import inspect
import io
import itertools
import os
import tempfile
import torch
import torch.nn as nn

import ray
from ray.util.sgd.torch.constants import USE_FP16, SCHEDULER_STEP, NUM_STEPS
from ray.util.sgd.torch.training_operator import TrainingOperator
from ray.util.sgd import utils

logger = logging.getLogger(__name__)
amp = None

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

try:
    from apex import amp
except ImportError:
    logger.debug("apex is not installed.")
    pass


class TorchRunner:
    """Manages a PyTorch model for training."""

    def __init__(self,
                 training_operator_cls,
                 config=None,
                 use_gpu=False,
                 serialize_data_creation=True,
                 use_fp16=False,
                 use_tqdm=False,
                 apex_args=None,
                 scheduler_step_freq=None):
        self.training_operator_cls = training_operator_cls
        self.config = {} if config is None else config

        self.timers = utils.TimerCollection()
        self.epochs = 0
        self.training_operator = None
        self.serialize_data_creation = serialize_data_creation
        self.use_gpu = use_gpu
        self.use_fp16 = use_fp16
        self.use_tqdm = use_tqdm
        self.apex_args = apex_args or {}
        if use_fp16 and not amp:
            raise ImportError(
                "Please install apex from "
                "https://www.github.com/nvidia/apex to use fp16 training.")
        self.scheduler_step_freq = scheduler_step_freq

    def _validate_loaders(self, loaders):
        assert loaders, "Loaders need to be returned in data_creator."
        if isinstance(loaders, (tuple, list)):
            if len(loaders) == 1:
                return loaders, None
            elif len(loaders) == 2:
                return loaders
            else:
                raise ValueError(
                    f"Number of loaders must be <= 2. Got {loaders}")
        # No great way of checking type otherwise
        return loaders, None

    def _initialize_dataloaders(self):
        logger.debug("Instantiating dataloaders.")
        loaders = None
        if self.serialize_data_creation:
            logger.debug("Serializing the dataloading process.")
            with FileLock(
                    os.path.join(tempfile.gettempdir(), ".raydata.lock")):
                loaders = self.data_creator(self.config)
        else:
            loaders = self.data_creator(self.config)
        train_loader, val_loader = self._validate_loaders(loaders)

        self.train_loader, self.validation_loader = train_loader, val_loader

    def _create_loss(self):
        if not self.loss_creator:
            return
        logger.debug("Creating loss.")
        if inspect.isclass(self.loss_creator) and issubclass(
                self.loss_creator, torch.nn.modules.loss._Loss):
            self.criterion = self.loss_creator()
        else:
            self.criterion = self.loss_creator(self.config)

        if self.use_gpu and torch.cuda.is_available():
            if hasattr(self.criterion, "cuda"):
                self.criterion = self.criterion.cuda()

    def _create_schedulers_if_available(self):
        # Learning rate schedules are optional.
        if not self.scheduler_creator:
            return
        self.schedulers = self.scheduler_creator(self.given_optimizers,
                                                 self.config)

        if not isinstance(self.schedulers, Iterable):
            self.schedulers = [self.schedulers]

    def setup(self):
        """Merges setup_components and setup_operator in one call."""
        self.setup_operator()

    def setup_operator(self):
        """Create the training operator."""
        self.training_operator = self.training_operator_cls(self.config,
                                                            world_rank=0,
                                                            use_gpu=self.use_gpu,
                                                            use_fp16=self.use_fp16,
                                                            use_tqdm=self.use_tqdm,
                                                            apex_args=self.apex_args)

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        return utils.find_free_port()

    def train_epoch(self,
                    num_steps=None,
                    profile=False,
                    info=None,
                    iterator=None):
        """Runs a training epoch and updates the model parameters."""
        logger.debug(f"Begin Training Step {self.epochs + 1}")
        info = info or {}
        self._toggle_profiling(profile=profile)

        info.update({
            NUM_STEPS: num_steps,
            USE_FP16: self.use_fp16,
            SCHEDULER_STEP: self.scheduler_step_freq
        })
        with self.timers.record("train_epoch"):
            if iterator is None:
                iterator = iter(self.train_loader)
            else:
                # Dataset will provide us with a list of tuples but we
                # need two lists.
                def format_batch(batch):
                    features, targets = zip(*batch)
                    return torch.cat(features), torch.cat(targets)

                iterator = map(format_batch, iterator)
            if num_steps:
                iterator = itertools.islice(iterator, num_steps)
            train_stats = self.training_operator.train_epoch(iterator, info)

        self.epochs += 1
        # This is so that `epochs` is first in ordering.
        stats = dict(epoch=self.epochs, **train_stats)
        if profile:
            stats.update(profile=self.timers.stats())
        return stats

    def validate(self, num_steps=None, profile=False, info=None):
        """Evaluates the model on the validation data set."""
        if self.validation_loader is None:
            raise ValueError("No validation dataloader provided. Make sure "
                             "you pass in a validation_loader to "
                             "TrainingOperator.register.")
        info = info or {}
        self._toggle_profiling(profile=profile)
        validation_loader = self.validation_loader

        with self.timers.record("validation"):
            iterator = validation_loader
            if num_steps:
                iterator = itertools.islice(iterator, num_steps)
            validation_stats = self.training_operator.validate(
                iterator, info=info)
        if profile:
            validation_stats.update(profile=self.timers.stats())
        return validation_stats

    def _toggle_profiling(self, profile=False):
        """Enables/Disables and resets timing profiles."""
        if profile:
            self.timers.enable()
            self.timers.reset()
        else:
            self.timers.disable()
        self.training_operator._set_timers(self.timers)

    def state_dict(self):
        """Returns the state of the runner."""
        state = {
            "epoch": self.epochs,
            "operator": self.training_operator.state_dict(),
            "models": [model.state_dict() for model in self.models],
            "optimizers": [opt.state_dict() for opt in self.optimizers]
        }
        schedulers = self.schedulers
        if schedulers:
            state.update({
                "schedulers": [
                    scheduler.state_dict() for scheduler in schedulers
                ]
            })
        # Check if fp16 is True and if NVIDIA Apex is imported.
        if self.use_fp16 and self.training_operator._amp:
            state.update({"amp": self.training_operator._amp.state_dict()})
        return state

    def load_state_dict(self, state):
        """Sets the state of the model."""
        models = self.models
        for model, state_dict in zip(models, state["models"]):
            model.load_state_dict(state_dict)
        optimizers = self.optimizers
        for optimizer, state_dict in zip(optimizers, state["optimizers"]):
            optimizer.load_state_dict(state_dict)
        schedulers = self.schedulers
        if schedulers:
            for scheduler, state_dict in zip(schedulers,
                                             state["schedulers"]):
                scheduler.load_state_dict(state_dict)

        if self.use_fp16 and "amp" in state and self.training_operator._amp:
            self.training_operator._amp.load_state_dict(state["amp"])
        self.epochs = state["epoch"]
        self.training_operator.load_state_dict(state["operator"])

    def state_stream(self):
        """Returns a bytes object for the state dict."""
        state_dict = self.state_dict()
        _buffer = io.BytesIO()
        torch.save(state_dict, _buffer)
        return _buffer.getvalue()

    def load_state_stream(self, byte_obj):
        """Loads a bytes object the training state dict."""
        _buffer = io.BytesIO(byte_obj)
        state_dict = torch.load(_buffer)
        return self.load_state_dict(state_dict)

    def apply(self, fn):
        return fn()

    def apply_operator(self, fn):
        return fn(self.training_operator)

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.training_operator
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    @property
    def models(self):
        return self.training_operator._original_models

    @property
    def optimizers(self):
        return self.training_operator._optimizers

    @property
    def schedulers(self):
        return self.training_operator._schedulers

    @property
    def train_loader(self):
        return self.training_operator._train_loader

    @property
    def validation_loader(self):
        return self.training_operator._validation_loader

    @property
    def criterion(self):
        return self.training_operator._criterion

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