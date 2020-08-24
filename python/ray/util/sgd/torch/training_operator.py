import itertools
import logging
import torch
import torch.nn as nn

from ray.util.sgd.utils import (TimerCollection, AverageMeterCollection,
                                NUM_SAMPLES)
from ray.util.sgd.torch.constants import (SCHEDULER_STEP_EPOCH, NUM_STEPS,
                                          SCHEDULER_STEP_BATCH, SCHEDULER_STEP)
from torch.nn.parallel.distributed import DistributedDataParallel
from torch.utils.data import DistributedSampler, DataLoader, IterableDataset

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


def _is_multiple(component):
    """Checks if a component (optimizer, model, etc) is not singular."""
    return isinstance(component, Iterable) and len(component) > 1


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
                 config,
                 # models,
                 # optimizers,
                 # train_loader,
                 # validation_loader,
                 world_rank,
                 # criterion=None,
                 # schedulers=None,
                 device_ids=None,
                 use_gpu=False,
                 use_fp16=False,
                 use_tqdm=False,
                 apex_args=None,
                 wrap_ddp=False,
                 wrap_distributed_sampler=False,
                 add_dist_sampler=False):
        # You are not expected to override this method.
        # self._models = models  # List of models
        # assert isinstance(
        #     models,
        #     Iterable), (f"Components need to be iterable. Got: {type(models)}")
        # self._optimizers = optimizers  # List of optimizers
        # assert isinstance(optimizers, Iterable), (
        #     f"Components need to be iterable. Got: {type(optimizers)}")
        # self._train_loader = train_loader
        # self._validation_loader = validation_loader
        self._world_rank = world_rank
        # self._criterion = criterion
        # self._schedulers = schedulers
        # if schedulers:
        #     assert isinstance(schedulers, Iterable), (
        #         f"Components need to be iterable. Got: {type(schedulers)}")
        self._config = config
        self._use_fp16 = use_fp16
        self._device_ids = device_ids
        self._use_gpu = use_gpu and torch.cuda.is_available()
        self._device = torch.device("cuda" if self._use_gpu else "cpu")
        if tqdm is None and use_tqdm:
            raise ValueError("tqdm must be installed to use tqdm in training.")
        self._use_tqdm = use_tqdm
        self.global_step = 0
        self._apex_args = apex_args if apex_args else {}
        self._wrap_ddp = wrap_ddp
        self._wrap_distributed_sampler = wrap_distributed_sampler
        self._add_dist_sampler = add_dist_sampler

        # if type(self) is TrainingOperator:
        #     for component in (models, schedulers, optimizers):
        #         if _is_multiple(component):
        #             raise ValueError(
        #                 "Need to provide a custom operator subclassing "
        #                 "TrainingOperator if using multi-scheduler, "
        #                 "multi-model or multi-optimizer training/validation.")
        self.timers = TimerCollection()
        self.setup(config)

    def _set_timers(self, timers):
        """Passes in the timers from the Runner."""
        self.timers = timers

    def setup(self, config):
        """Override this method to implement custom operator setup.

        Args:
            config (dict): Custom configuration value to be passed to
                all creator and operator constructors. Same as ``self.config``.
        """
        raise NotImplementedError

    def register(self, *, models, optimizers, train_loader, validation_loader,
                 loss=None, schedulers=None):
        logger.debug("Registering models.")
        self._models = models
        if not isinstance(self._models, Iterable):
            self._models = [self._models]
        assert all(isinstance(model, nn.Module) for model in self._models), (
            f"All models must be PyTorch models: {self._models}.")
        if self.use_gpu and torch.cuda.is_available():
            self._models = [model.cuda() for model in self._models]

        logger.debug("Registering optimizers.")
        self._optimizers = optimizers
        if not isinstance(self._optimizers, Iterable):
            self._optimizers = [self._optimizers]


        logger.debug("Registering data loaders..")
        self._train_loader = train_loader
        self._validation_loader = validation_loader

        if self._wrap_distributed_sampler:
            logging.debug("Wrapping data loaders with DistributedSampler.")
            def with_sampler(loader):
                # Automatically set the DistributedSampler
                data_loader_args = {
                    "dataset": loader.dataset,
                    "batch_size": loader.batch_size,
                    "shuffle": False,
                    "num_workers": loader.num_workers,
                    "collate_fn": loader.collate_fn,
                    "pin_memory": loader.pin_memory,
                    "drop_last": loader.drop_last,
                    "timeout": loader.timeout,
                    "worker_init_fn": loader.worker_init_fn,
                    "sampler": DistributedSampler(loader.dataset)
                }
                return DataLoader(**data_loader_args)

            def should_wrap_dataloader(loader):
                return (isinstance(loader, DataLoader)
                        and not isinstance(loader.dataset, IterableDataset))

            if should_wrap_dataloader(self.train_loader):
                if self._add_dist_sampler:
                    self._train_loader = with_sampler(self._train_loader)

            if self._validation_loader is not None and should_wrap_dataloader(
                self._validation_loader):
                if self._add_dist_sampler:
                    self._validation_loader = with_sampler(
                        self._validation_loader)


        if schedulers:
            logger.debug("Registering scheduler.")
            self._schedulers = schedulers
            if not isinstance(self._schedulers, Iterable):
                self._schedulers = [self._schedulers]
        else:
            self._schedulers = [None]

        if loss:
            logger.debug("Registering loss.")
            self._criterion = loss
            if self.use_gpu and torch.cuda.is_available():
                if hasattr(self._criterion, "cuda"):
                    self._criterion = self._criterion.cuda()
        else:
            self._criterion = None

        logger.debug("Setting up Apex.")
        if self.use_fp16 and amp:
            self._models, self._optimizers = amp.initialize(
                self._models, self._optimizers, **self._apex_args)


        if self._wrap_ddp:
            logging.debug("Setting up DDP for models.")
            self._models = [DistributedDataParallel(model,
                                                    device_ids=self.device_ids) for model in self._models]

    def train_epoch(self, info, num_steps=None):
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
        iterator = iter(self.train_loader)
        if num_steps:
            iterator = itertools.islice(iterator, num_steps)
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

    def train_batch(self, batch, batch_info):
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
            batch: One item of the validation iterator.
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

    def validate(self, info, num_steps=None):
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
        if self._validation_loader is None:
            raise ValueError("No validation dataloader provided.")
        val_iterator = iter(self._validation_loader)
        if num_steps:
            val_iterator = itertools.islice(val_iterator, num_steps)
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

    def validate_batch(self, batch, batch_info):
        """Calcuates the loss and accuracy over a given batch.

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

    def state_dict(self):
        """Override this to return a representation of the operator state.

        Returns:
            dict: The state dict of the operator."""
        state = {
            "models": [model.state_dict() for model in self._models],
            "optimizers": [opt.state_dict() for opt in self._optimizers],
        }

        if self._schedulers:
            state.update({
                "schedulers": [scheduler.state_dict() for scheduler in
                               self._schedulers]
            })

        # Check if fp16 is True and if NVIDIA Apex is imported.
        if self.use_fp16 and amp:
            state.update({"amp": amp.state_dict()})

        return state

    def load_state_dict(self, state_dict):
        """Override this to load the representation of the operator state.

        Args:
            state_dict (dict): State dict as returned by the operator. """
        for model, model_state_dict in zip(self._models, state_dict["models"]):
            model.load_state_dict(model_state_dict)
        for optimizer, opt_state_dict in zip(self._optimizers, state_dict[
            "optimizers"]):
            optimizer.load_state_dict(opt_state_dict)
        if self._schedulers:
            for scheduler, sched_state_dict in zip(self._schedulers,
                                                   state_dict["schedulers"]):
                scheduler.load_state_dict(sched_state_dict)
        if self.use_fp16 and "amp" in state_dict and amp:
            amp.load_state_dict(state_dict["amp"])

    @property
    def device(self):
        """torch.device: The appropriate torch device, at your convenience."""
        return self._device

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

    @property
    def device_ids(self):
        """List[int]: Device IDs for the model.

        This is useful for using batch norm with DistributedDataParallel.
        """
        return self._device_ids


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
