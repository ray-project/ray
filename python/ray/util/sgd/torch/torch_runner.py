import logging
import io
import itertools

import ray
import torch

from ray.util.sgd.torch.constants import USE_FP16, NUM_STEPS
from ray.util.sgd import utils

logger = logging.getLogger(__name__)
amp = None

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
        if use_fp16 and not amp:
            raise ImportError(
                "Please install apex from "
                "https://www.github.com/nvidia/apex to use fp16 training.")
        self.scheduler_step_freq = scheduler_step_freq

        # Training and Validation iterators
        self.train_iterator = None
        self._should_reset_train_loader = True

        self.val_iterator = None
        self._should_reset_val_loader = True

    def setup_operator(self):
        """Create the training operator."""
        self.training_operator = self.training_operator_cls(
            self.config,
            world_rank=0,
            local_rank=0,
            is_distributed=False,
            use_gpu=self.use_gpu,
            use_fp16=self.use_fp16,
            use_tqdm=self.use_tqdm,
            scheduler_step_freq=self.scheduler_step_freq)

    def get_iterator(self, training=True):
        if training:
            # In training.
            if self._should_reset_train_loader:
                self.epochs += 1
                self.train_iterator = iter(self.train_loader)
                self._should_reset_train_loader = False
            return self.train_iterator
        else:
            # In validation.
            if self._should_reset_val_loader:
                self.val_iterator = iter(self.validation_loader)
                self._should_reset_val_loader = False
            return self.val_iterator

    def make_iterator(self, training=True, num_steps=None):
        steps = 0
        # Needed to make sure we don't loop forever if iterator is empty
        has_at_least_one = False
        while True:
            iterator = self.get_iterator(training=training)
            if num_steps is not None and steps >= num_steps:
                # Stop iterating after reaching num_steps.
                break
            try:
                item = next(iterator)
                steps += 1
                if not has_at_least_one:
                    has_at_least_one = True
                yield item
            except StopIteration:
                # Set should reset iterator on next cycle to True.
                if training:
                    self._should_reset_train_loader = True
                else:
                    self._should_reset_val_loader = True
                if num_steps is None or not has_at_least_one:
                    # End after current epoch or if iterator has no elements.
                    break
                else:
                    # Else, start cycling through the iterator again.
                    pass

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
            "epoch_idx": self.epochs,
        })
        with self.timers.record("train_epoch"):
            if iterator is not None:
                # Dataset will provide us with a list of tuples but we
                # need two lists.
                def format_batch(batch):
                    features, targets = zip(*batch)
                    return torch.cat(features), torch.cat(targets)

                iterator = map(format_batch, iterator)
                if num_steps:
                    iterator = itertools.islice(iterator, num_steps)
                self.epochs += 1
            else:
                iterator = self.make_iterator(
                    training=True, num_steps=num_steps)
            train_stats = self.training_operator.train_epoch(iterator, info)

        # This is so that `epochs` is first in ordering.
        stats = dict(epoch=self.epochs, **train_stats)
        if profile:
            stats.update(profile=self.timers.stats())
        return stats

    def validate(self, num_steps=None, profile=False, info=None):
        """Evaluates the model on the validation data set."""
        info = info or {}
        self._toggle_profiling(profile=profile)

        with self.timers.record("validation"):
            iterator = self.make_iterator(training=False, num_steps=num_steps)
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
        model_states = [model.state_dict() for model in self.models]
        optimizer_states = [
            optimizer.state_dict() for optimizer in self.optimizers
        ]
        state = {
            "epoch": self.epochs,
            "operator": self.training_operator.state_dict(),
            "models": model_states,
            "optimizers": optimizer_states
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
            for scheduler, state_dict in zip(schedulers, state["schedulers"]):
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
        """Loads a bytes object the training state dict.

        This is needed because we don't want to deserialize the tensor
        onto the same device (which is from the driver process). We want to
        map it onto the actor's specific device.

        From: github.com/pytorch/pytorch/issues/10622#issuecomment-474733769
        """
        _buffer = io.BytesIO(byte_obj)
        to_gpu = self.use_gpu and torch.cuda.is_available()
        state_dict = torch.load(
            _buffer,
            map_location=("cpu" if not to_gpu else
                          lambda storage, loc: storage.cuda()))
        return self.load_state_dict(state_dict)

    def apply(self, fn):
        return fn()

    def apply_operator(self, fn):
        return fn(self.training_operator)

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.train_iterator
        del self.val_iterator
        del self.training_operator
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    def get_models(self):
        """Getter method. Needed for remote actor calls."""
        return self.models

    def get_node_ip(self):
        return ray.util.get_node_ip_address()

    @property
    def models(self):
        return self.training_operator._get_original_models()

    @property
    def optimizers(self):
        return self.training_operator._get_optimizers()

    @property
    def schedulers(self):
        return self.training_operator._get_schedulers()

    @property
    def train_loader(self):
        return self.training_operator._get_train_loader()

    @property
    def validation_loader(self):
        return self.training_operator._get_validation_loader()

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
