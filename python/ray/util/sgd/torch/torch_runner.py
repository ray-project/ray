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
from ray.util.sgd.data import Dataset
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
        self.models = None
        self.optimizers = None
        self.criterion = None
        self.schedulers = None
        self.train_loader = None
        self.validation_loader = None
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

    def _try_setup_apex(self):
        """Sets up the model for fp16 training via apex if available."""
        if self.use_fp16 and amp:
            self.training_operator._original_models, \
            self.training_operator._optimizers = amp.initialize(
                self.training_operator._original_models,
                self.training_operator._optimizers, **self.apex_args)

    def setup(self):
        """Merges setup_components and setup_operator in one call."""
        self.setup_operator()
        self.setup_components()

    def setup_operator(self):
        """Create the training operator."""
        self.training_operator = self.training_operator_cls(self.config,
                                                            world_rank=0,
                                                            use_gpu=self.use_gpu,
                                                            use_fp16=self.use_fp16,
                                                            use_tqdm=self.use_tqdm,)
    def setup_components(self):
        self._try_setup_apex()
        self.training_operator._models = \
            self.training_operator._original_models

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
            if iterator is None and self.training_operator.train_loader is \
                None:
                raise RuntimeError("Either train_loader must be registered "
                                   "in your Training Operator, or an iterator "
                                   "must be passed into TorchTrainer.train")
            if iterator is None:
                iterator = iter(self.training_operator.train_loader)
            elif isinstance(iterator, Dataset):
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

    def validate(self, num_steps=None, profile=False, info=None,
                 iterator=None):
        """Evaluates the model on the validation data set."""
        if iterator is None and self.training_operator.validation_loader is \
            None:
            raise RuntimeError("Either validation_loader must be registered "
                               "in your Training Operator, or an iterator "
                               "must be passed into TorchTrainer.validate")
        if not iterator:
            iterator = iter(self.training_operator.validation_loader)
        info = info or {}
        self._toggle_profiling(profile=profile)

        with self.timers.record("validation"):
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
            "models": [model.state_dict() for model in
                       self.training_operator._original_models],
            "optimizers": [opt.state_dict() for opt in
                           self.training_operator.optimizers],
            "operator": self.training_operator.state_dict(),
        }
        if self.schedulers:
            state.update({
                "schedulers": [
                    scheduler.state_dict() for scheduler in
                    self.training_operator.schedulers
                ]
            })
        # Check if fp16 is True and if NVIDIA Apex is imported.
        if self.use_fp16 and amp:
            state.update({"amp": amp.state_dict()})
        return state

    def load_state_dict(self, state):
        """Sets the state of the model."""
        for model, state_dict in zip(self.training_operator._original_models,
                                     state["models"]):
            model.load_state_dict(state_dict)
        for optimizer, state_dict in zip(self.training_operator._optimizers,
                                         state[
            "optimizers"]):
            optimizer.load_state_dict(state_dict)
        if self.schedulers:
            for scheduler, state_dict in zip(
                self.training_operator._schedulers,
                                             state["schedulers"]):
                scheduler.load_state_dict(state_dict)

        if self.use_fp16 and "amp" in state and amp:
            amp.load_state_dict(state["amp"])
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
    def given_models(self):
        if len(self.training_operator._original_models) > 1:
            return self.training_operator._original_models
        else:
            return self.training_operator._original_models[0]

    @property
    def given_optimizers(self):
        if len(self.training_operator.optimizers) > 1:
            return self.training_operator.optimizers
        else:
            return self.training_operator.optimizers[0]

    @property
    def given_schedulers(self):
        if not self.training_operator.schedulers:
            return self.training_operator.schedulers
        if len(self.training_operator.schedulers) > 1:
            return self.training_operator.schedulers
        else:
            return self.training_operator.schedulers[0]
