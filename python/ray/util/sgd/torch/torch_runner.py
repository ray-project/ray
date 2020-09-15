import logging
import io
import itertools
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

    def setup_operator(self):
        """Create the training operator."""
        self.training_operator = self.training_operator_cls(
            self.config,
            world_rank=0,
            use_gpu=self.use_gpu,
            use_fp16=self.use_fp16,
            use_tqdm=self.use_tqdm,
            apex_args=self.apex_args,
            scheduler_step_freq=self.scheduler_step_freq)

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
            raise ValueError("No validation dataloader provided. Make sure"
                             "you pass in a validation_loader to "
                             "TrainingOperator.register_data.")
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
        del self.training_operator
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    def get_models(self):
        """Getter method. Needed for remote actor calls."""
        return self.models

    @property
    def models(self):
        if not hasattr(self.training_operator, "_original_models"):
            raise RuntimeError("Training Operator does not have any "
                               "registered models. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self.training_operator._original_models

    @property
    def optimizers(self):
        if not hasattr(self.training_operator, "_optimizers"):
            raise RuntimeError("Training Operator does not have any "
                               "registered optimizers. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self.training_operator._optimizers

    @property
    def schedulers(self):
        if not hasattr(self.training_operator, "_schedulers"):
            raise RuntimeError("Training Operator does not have any "
                               "registered schedulers. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self.training_operator._schedulers

    @property
    def train_loader(self):
        if not hasattr(self.training_operator, "_train_loader"):
            logger.warning("Training Operator does not have any "
                           "registered train loader. If this is "
                           "unexepected, make sure to call "
                           "self.register_data(...) inside the setup method "
                           "of your Training Operator.")
            return None
        return self.training_operator._train_loader

    @property
    def validation_loader(self):
        if not hasattr(self.training_operator, "_validation_loader"):
            logger.warning("Training Operator does not have any "
                           "registered validation loader. If this is "
                           "unexepected, make sure to call "
                           "self.register_data(...) inside the setup method "
                           "of your Training Operator.")
            return None
        return self.training_operator._validation_loader

    @property
    def criterion(self):
        if not hasattr(self.training_operator, "_criterion"):
            raise RuntimeError("Training Operator does not have any "
                               "registered criterion. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
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
