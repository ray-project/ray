import itertools
import logging
from typing import Any, Dict, Union, Optional, \
    Callable, TypeVar

import torch
import torch.nn as nn
import typing

from ray.util.sgd.utils import (TimerCollection, AverageMeterCollection,
                                NUM_SAMPLES)
from ray.util.sgd.torch.constants import (SCHEDULER_STEP_EPOCH, NUM_STEPS,
                                          SCHEDULER_STEP_BATCH, SCHEDULER_STEP)

logger = logging.getLogger(__name__)
amp = None

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

try:
    from apex import amp
except ImportError:
    # Apex library is not installed, so we cannot enable mixed precision.
    # We don't log here because logging happens in the torch_runner,
    # where amp is initialized.
    logger.debug("apex is not installed.")
    pass

tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass

S = TypeVar('S')

class TrainingOperator:
    """Abstract class for custom training or validation loops.

    The scheduler will only be called at a batch or epoch frequency, depending
    on the user parameter. Be sure to set ``scheduler_step_freq`` in
    ``TorchTrainer`` to either "batch" or "epoch" to increment the scheduler
    correctly during training. If using a learning rate scheduler
    that depends on validation loss, you can use ``trainer.update_scheduler``.

    For both training and validation, there are two granularities that
    you can provide customization: per epoch or per batch.
    You do not need to override both.

    .. image:: raysgd-custom.jpg
        :scale: 80%
        :align: center

    Raises:
        ValueError if multiple models/optimizers/schedulers are provided.
            You are expected to subclass this class if you wish
            to train over multiple models/optimizers/schedulers.
    """

    def __init__(self,
                 config: Dict[str, Any],
                 world_rank: int,
                 use_gpu: bool = False,
                 use_fp16: bool = False,
                 use_tqdm: bool = False,) -> None:
        # You are not expected to override this method.
        self._world_rank = world_rank
        self._config = config
        self._use_fp16 = use_fp16
        self._use_gpu = use_gpu and torch.cuda.is_available()
        if tqdm is None and use_tqdm:
            raise ValueError("tqdm must be installed to use tqdm in training.")
        self._use_tqdm = use_tqdm
        self.global_step = 0

        self.timers = TimerCollection()
        self.setup(config)

    def _set_timers(self, timers):
        """Passes in the timers from the Runner."""
        self.timers = timers

    def setup(self, config: Dict[str, Any]) -> None:
        """Override this method to implement operator setup.
        You must call self.register in this method to register training state
        to Ray SGD.

        Args:
            config (dict): Custom configuration value to be passed to
                all creator and operator constructors. Same as ``self.config``.
        """
        raise NotImplementedError

    def register(self, *, models: Union[torch.nn.Module, typing.Iterable[
        torch.nn.Module]],
                 optimizers: Union[torch.optim.Optimizer, typing.Iterable[
                     torch.optim.Optimizer]],
                 train_loader: Optional[typing.Iterable[S]] = None,
                 validation_loader: Optional[typing.Iterable[S]] = None,
                 criterion: Optional[Callable[[torch.tensor, torch.tensor],
                                              torch.tensor]]
                 = None,
                 schedulers: Optional[Union[torch.optim.lr_scheduler.LambdaLR,
                                            typing.Iterable[
                                                torch.optim.lr_scheduler.LambdaLR]]]
                 = None) -> None:
        """Registers parameters with Ray SGD. Arguments can later be
        accessed with the explicit properties listed below.

        Args:
            models (torch.nn.Module or Iterable[nn.Module]): Pytorch model or
                multiple Pytorch models to use for training. If `use_gpu` is
                True and Cuda is available, models will use GPU.
            optimizers (torch.optim.Optimizer or Iterable[
                torch.optim.Optimizer]): Pytorch optimizer or multiple Pytorch
                optimizers to use for training.
            train_loader (Iterator, optional): An iterator for training
                data. If none is provided, training data must be passed in
                through TorchTrainer.train
            validation_loader (Iterator, optional): An iterator for validation
                data. If none is provided, validation data must be passed in
                through TorchTrainer.validate
            criterion (Callable, optional): Function to return loss
                metric given features and target. If not provided,
                must implement a custom training loop.
            schedulers (torch.optim.lr_scheduler or Iterable[
                torch.optim.lr_scheduler], optional): A learning rate
                scheduler or multiple learning rate schedulers.

        """
        logger.info("Registering models...")
        self._original_models = models
        if not isinstance(self._original_models, Iterable):
            self._original_models = [self._original_models]
        assert all(isinstance(model, nn.Module) for model in
                   self._original_models), (
                f"All models must be PyTorch models: {self._original_models}.")
        if self.use_gpu and torch.cuda.is_available():
            self._original_models = [model.cuda() for model in self._original_models]
        if self.use_gpu and torch.cuda.is_available():
            self._original_models = [model.cuda() for model in self._original_models]

        logger.info("Registering optimizers...")
        self._optimizers = optimizers
        if not isinstance(self._optimizers, Iterable):
            self._optimizers = [self._optimizers]

        if train_loader:
            logger.info("Registering train data loader...")
            self._train_loader = train_loader
        else:
            self._train_loader = None

        if validation_loader:
            logger.info("Registering validation data loader...")
            self._validation_loader = validation_loader
        else:
            self._validation_loader = None

        if schedulers:
            logger.info("Registering scheduler...")
            self._schedulers = schedulers
            if not isinstance(self._schedulers, Iterable):
                self._schedulers = [self._schedulers]
        else:
            self._schedulers = [None]

        if criterion:
            logger.info("Registering criterion...")
            self._criterion = criterion
            if self.use_gpu and torch.cuda.is_available():
                if hasattr(self._criterion, "cuda"):
                    self._criterion = self._criterion.cuda()
        else:
            self._criterion = None


    def train_epoch(self, iterator: typing.Iterable[S], info: Dict[str,
                                                                 Any]) -> Dict[
        str, Any]:
        """Runs one standard training pass over the training dataloader.

        By default, this method will iterate over the given iterator and
        call ``self.train_batch`` over each batch. If ``scheduler_step_freq``
        is set, this default method will also step the scheduler accordingly.

        You do not need to call ``train_batch`` in this method if you plan
        to implement a custom optimization/training routine here.

        You may find ``ray.util.sgd.utils.AverageMeterCollection`` useful
        when overriding this method. See example below:

        .. code-block:: python

            def train_epoch(self, ...):
                meter_collection = AverageMeterCollection()
                self.model.train()
                for batch in iterator:
                    # do some processing
                    metrics = {"metric_1": 1, "metric_2": 3} # dict of metrics

                    # This keeps track of all metrics across multiple batches
                    meter_collection.update(metrics, n=len(batch))

                # Returns stats of the meters.
                stats = meter_collection.summary()
                return stats


        Args:
            iterator (iter): Iterator over the training data for the entire
                epoch. This iterator is expected to be entirely consumed.
            info (dict): Dictionary for information to be used for custom
                training operations.

        Returns:
            A dict of metrics from training.
        """
        if self.use_tqdm and self.world_rank == 0:
            desc = ""
            if info is not None and "epoch_idx" in info:
                if "num_epochs" in info:
                    desc = f"{info['epoch_idx'] + 1}/{info['num_epochs']}e"
                else:
                    desc = f"{info['epoch_idx'] + 1}e"
            _progress_bar = tqdm(
                total=info[NUM_STEPS] or len(self.train_loader),
                desc=desc,
                unit="batch",
                leave=False)

        metric_meters = AverageMeterCollection()

        self.model.train()
        for batch_idx, batch in enumerate(iterator):
            batch_info = {
                "batch_idx": batch_idx,
                "global_step": self.global_step
            }
            batch_info.update(info)
            metrics = self.train_batch(batch, batch_info=batch_info)

            if self.use_tqdm and self.world_rank == 0:
                _progress_bar.n = batch_idx + 1
                postfix = {}
                if "train_loss" in metrics:
                    postfix.update(loss=metrics["train_loss"])
                _progress_bar.set_postfix(postfix)

            if self.scheduler and batch_info.get(
                    SCHEDULER_STEP) == SCHEDULER_STEP_BATCH:
                self.scheduler.step()

            metric_meters.update(metrics, n=metrics.pop(NUM_SAMPLES, 1))
            self.global_step += 1

        if self.scheduler and info.get(SCHEDULER_STEP) == SCHEDULER_STEP_EPOCH:
            self.scheduler.step()

        return metric_meters.summary()

    def train_batch(self, batch: S, batch_info: Dict[str, Any]) -> Dict[str,
                                                                      Any]:
        """Computes loss and updates the model over one batch.

        This method is responsible for computing the loss and gradient and
        updating the model.

        By default, this method implementation assumes that batches
        are in (\\*features, labels) format. So we also support multiple inputs
        model. If using amp/fp16 training, it will also scale the loss
        automatically.

        You can provide custom loss metrics and training operations if you
        override this method. If overriding this method, you can access model,
        optimizer, criterion via ``self.model``, ``self.optimizer``,
        and ``self.criterion``.

        You do not need to override this method if you plan to
        override ``train_epoch``.

        Args:
            batch: One item of the training iterator.
            batch_info (dict): Information dict passed in from ``train_epoch``.

        Returns:
            A dictionary of metrics.
                By default, this dictionary contains "loss" and "num_samples".
                "num_samples" corresponds to number of datapoints in the batch.
                However, you can provide any number of other values.
                Consider returning "num_samples" in the metrics because
                by default, ``train_epoch`` uses "num_samples" to
                calculate averages.

        """
        # unpack features into list to support multiple inputs model
        *features, target = batch
        # Create non_blocking tensors for distributed training
        if self.use_gpu:
            features = [
                feature.cuda(non_blocking=True) for feature in features
            ]
            target = target.cuda(non_blocking=True)

        # Compute output.
        with self.timers.record("fwd"):
            output = self.model(*features)
            if not self.criterion:
                raise RuntimeError("No criterion function registered. Either register a criterion/loss function, or implement custom trianing by overriding this method.")
            if not callable(self.criterion):
                raise RuntimeError("Criterion {} is not callable. Either "
                                   "register a callable criterion function "
                                   "or implement custom training by "
                                   "overriding this method.".format(self.criterion))
            loss = self.criterion(output, target)

        # Compute gradients in a backward pass.
        with self.timers.record("grad"):
            self.optimizer.zero_grad()
            if self.use_fp16:
                with amp.scale_loss(loss, self.optimizer) as scaled_loss:
                    scaled_loss.backward()
            else:
                loss.backward()

        # Call step of optimizer to update model params.
        with self.timers.record("apply"):
            self.optimizer.step()

        return {"train_loss": loss.item(), NUM_SAMPLES: features[0].size(0)}

    def validate(self, val_iterator: typing.Iterable[S], info: Dict[str,
                                                                 Any]) -> \
        Dict[str, Any]:
        """Runs one standard validation pass over the val_iterator.

        This will call ``model.eval()`` and ``torch.no_grad`` when iterating
        over the validation dataloader.

        If overriding this method, you can access model, criterion via
        ``self.model`` and ``self.criterion``. You also do not need to call
        ``validate_batch`` if overriding this method.

        Args:
            val_iterator (iter): Iterable constructed from the
                validation dataloader.
            info: (dict): Dictionary for information to be used for custom
                validation operations.

        Returns:
            A dict of metrics from the evaluation.
                By default, returns "val_accuracy" and "val_loss"
                which is computed by aggregating "loss" and "correct" values
                from ``validate_batch`` and dividing it by the sum of
                ``num_samples`` from all calls to ``self.validate_batch``.
        """
        metric_meters = AverageMeterCollection()

        # switch to evaluate mode
        self.model.eval()
        with torch.no_grad():
            for batch_idx, batch in enumerate(val_iterator):
                batch_info = {"batch_idx": batch_idx}
                batch_info.update(info)
                metrics = self.validate_batch(batch, batch_info)
                metric_meters.update(metrics, n=metrics.pop(NUM_SAMPLES, 1))

        return metric_meters.summary()

    def validate_batch(self, batch: S, batch_info: Dict[str, Any]) -> Dict[
        str, Any]:
        """Calculates the loss and accuracy over a given batch.

        You can override this method to provide arbitrary metrics.

        Same as ``train_batch``, this method implementation assumes that
        batches are in (\\*features, labels) format by default. So we also
        support multiple inputs model.

        Args:
            batch: One item of the validation iterator.
            batch_info (dict): Contains information per batch from
                ``validate()``.

        Returns:
            A dict of metrics.
                By default, returns "val_loss", "val_accuracy", and
                "num_samples". When overriding, consider returning
                "num_samples" in the metrics because
                by default, ``validate`` uses "num_samples" to
                calculate averages.
        """
        # unpack features into list to support multiple inputs model
        *features, target = batch
        if self.use_gpu:
            features = [
                feature.cuda(non_blocking=True) for feature in features
            ]
            target = target.cuda(non_blocking=True)

        # compute output

        with self.timers.record("eval_fwd"):
            output = self.model(*features)
            loss = self.criterion(output, target)
            _, predicted = torch.max(output.data, 1)

        num_correct = (predicted == target).sum().item()
        num_samples = target.size(0)
        return {
            "val_loss": loss.item(),
            "val_accuracy": num_correct / num_samples,
            NUM_SAMPLES: num_samples
        }

    def state_dict(self) -> Dict[str, Any]:
        """Override this to return a representation of the operator state.
        Any information that is passed into self.register will automatically
        be saved by Ray SGD. Use this method to save any additional state.

        Returns:
            dict: The state dict of the operator."""
        pass

    def load_state_dict(self, state_dict: Dict[str, Any]) -> None:
        """Override this to load the representation of the operator state.
        Any information passed into self.register will automatically be loaded
        by Ray SGD. Use this method to load any additional state.

        Args:
            state_dict (dict): State dict as returned by the operator. """
        pass

    @property
    def config(self):
        """dict: Provided into TorchTrainer."""
        return self._config

    @property
    def model(self):
        """First or only model created by the provided ``model_creator``."""
        return self._models[0]

    @property
    def models(self):
        """List of models created by the provided ``model_creator``."""
        return self._models

    @property
    def optimizer(self):
        """First or only optimizer(s) created by the ``optimizer_creator``."""
        return self._optimizers[0]

    @property
    def optimizers(self):
        """List of optimizers created by the ``optimizer_creator``."""
        return self._optimizers

    @property
    def train_loader(self):
        """Iterable: 1st Dataloader from ``data_creator``.
        """
        return self._train_loader

    @property
    def validation_loader(self):
        """Iterable: 2nd Dataloader from ``data_creator``."""
        return self._validation_loader

    @property
    def world_rank(self):
        """int: The rank of the parent runner. Always 0 if not distributed."""
        return self._world_rank

    @property
    def criterion(self):
        """Criterion created by the provided ``loss_creator``."""
        return self._criterion

    @property
    def scheduler(self):
        """First or only scheduler(s) created by the ``scheduler_creator``."""
        if self._schedulers:
            return self._schedulers[0]

    @property
    def schedulers(self):
        """List of schedulers created by the ``scheduler_creator``."""
        return self._schedulers

    @property
    def use_gpu(self):
        """Returns True if cuda is available and use_gpu is True."""
        return self._use_gpu

    @property
    def use_fp16(self):
        """bool: Whether the model and optimizer have been FP16 enabled."""
        return self._use_fp16

    @property
    def use_tqdm(self):
        """bool: Whether tqdm progress bars are enabled."""
        return self._use_tqdm


class _TestingOperator(TrainingOperator):
    def train_epoch(self, iterator, info):
        func = self.config.get("custom_func")
        if callable(func):
            return func(self, iterator, info)
        return {"done": 1}


class _TestMetricsOperator(TrainingOperator):
    def setup(self, config):
        self._train_scores = config["scores"].copy()
        self._val_scores = config["val_scores"].copy()
        self.key = config["key"]

    def train_batch(self, batch, batch_info=None):
        metrics = super(_TestMetricsOperator, self).train_batch(
            batch, batch_info)
        num_samples = metrics[NUM_SAMPLES]
        metrics.update({self.key: self._train_scores.pop(0) / num_samples})
        return metrics

    def validate_batch(self, batch, batch_info=None):
        metrics = super(_TestMetricsOperator, self).validate_batch(
            batch, batch_info)
        num_samples = metrics[NUM_SAMPLES]
        metrics.update({self.key: self._val_scores.pop(0) / num_samples})
        return metrics
