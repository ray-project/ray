import inspect
import itertools
import logging
import os
import tempfile

import torch
import torch.nn as nn
from filelock import FileLock

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
                 world_rank,
                 device_ids=None,
                 use_gpu=False,
                 use_fp16=False,
                 use_tqdm=False,
                 apex_args=None,
                 wrap_ddp=False,
                 wrap_distributed_sampler=False,
                 add_dist_sampler=False,
                 scheduler_step_freq=None):
        # You are not expected to override this method.
        self._world_rank = world_rank
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
        self._scheduler_step_freq = scheduler_step_freq

        self.timers = TimerCollection()
        self.setup(config)

    def _set_timers(self, timers):
        """Passes in the timers from the Runner."""
        self.timers = timers

    def setup(self, config):
        """Override this method to implement custom operator setup. You must
        self.register here to register training components with Ray SGD.

        Args:
            config (dict): Custom configuration value to be passed to
                all creator and operator constructors. Same as ``self.config``.
        """
        raise NotImplementedError

    def register(self, *, models, optimizers, train_loader,
                 validation_loader, criterion=None, schedulers=None):
        """Registers parameters with Ray SGD. Also sets up necessary
        training components (Cuda, DDP, Distributed Sampler, Fp16).

        If more than one model, optimizer, or scheduler is passed in,
        you should implement your own custom training loop.

        .. code-block:: python

            @override
            def setup(self, config):
                model = ...
                optimizer = ...
                train_loader = ...
                val_loader = ...
                loss = ...

                self.model, self.optimizer, self.train_loader,
                self.val_loader, self.loss = self.register(models=model,
                optimizers=optimizer, train_loader=train_loader,
                validation_loader=validation_loader, criterion=loss)

                # At this point DDP, Cuda, Distributed Sampling, and Fp16
                # are set up for all our components. We then use self.model,
                # self.optimizer, etc. in our training loop.


        Args:
            models (torch.nn.Module or Iterable[nn.Module]): Pytorch model or
                multiple Pytorch models to use for training. If `use_gpu` is
                True, Cuda is available, and TorchTrainer(..., wrap_ddp=True)
                models will be wrapped in DDP. If wrap_ddp is False,
                you should handle DDP for your models in setup.
            optimizers (torch.optim.Optimizer or Iterable[
                torch.optim.Optimizer]): Pytorch optimizer or multiple Pytorch
                optimizers to use for training.
            train_loader (Iterator): An iterator for training
                data. If None is explicitly passed in, a Ray SGD Dataset
                must be passed in through TorchTrainer.train. Ray SGD will
                automatically use a Distributed Sampler if TorchTrainer(...,
                add_dist_sampler=True).
            validation_loader (Iterator): An iterator for validation
                data. Ray SGD will automatically use a Distributed Sampler
                if TorchTrainer(..., add_dist_sampler=True).
            criterion (Callable, optional): Function to return loss
                metric given features and target. If not provided,
                must implement a custom training loop.
            schedulers (torch.optim.lr_scheduler or Iterable[
                torch.optim.lr_scheduler], optional): A learning rate
                scheduler or multiple learning rate schedulers.

        Returns:
            Tuple of model, optimizer, criterion if not None, and scheduler
            if not None. train_loader and validation_loader are not
            returned. You should use the iterators passed into train_epoch
            and validate instead.

        """
        return_vals = []
        logger.debug("Registering models.")
        self._original_models = models
        if not isinstance(self._original_models, Iterable):
            self._original_models = [self._original_models]
        assert all(isinstance(model, nn.Module) for model in
                   self._original_models), (
            f"All models must be PyTorch models: {self._original_models}.")
        if self.use_gpu and torch.cuda.is_available():
            self._original_models = [model.cuda() for model in
                                     self._original_models]

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

            if should_wrap_dataloader(self._train_loader):
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
            self._schedulers = None

        if criterion:
            logger.debug("Registering loss.")
            self._criterion = criterion
            if self.use_gpu and torch.cuda.is_available():
                if hasattr(self._criterion, "cuda"):
                    self._criterion = self._criterion.cuda()
        else:
            self._criterion = None

        logger.debug("Setting up Apex.")
        if self.use_fp16 and amp:
            self._models, self._optimizers = amp.initialize(
                self._models, self._optimizers, **self._apex_args)
            self._amp = amp


        if self._wrap_ddp:
            logging.debug("Setting up DDP for models.")
            self._models = [DistributedDataParallel(model,
                                                    device_ids=self.device_ids) for model in self._original_models]
        else:
            self._models = self._original_models

        if len(self._models) == 1:
            return_vals.append(self._models[0])
        else:
            return_vals.append(self._models)

        if len(self._optimizers) == 1:
            return_vals.append(self._optimizers[0])
        else:
            return_vals.append(self._optimizers)

        if self._criterion is not None:
            return_vals.append(self._criterion)

        if self._schedulers is not None:
            if self._scheduler_step_freq is None:
                raise ValueError("scheduler_step_freq passed into "
                                 "TorchTrainer cannot be None if you "
                                 "are registering schedulers. Set this to "
                                 "'manual' if you will be manually stepping "
                                 "the schedulers.")
            if len(self._schedulers) == 1:
                return_vals.append(self._schedulers[0])
            else:
                return_vals.append(self._schedulers)

        return tuple(return_vals)

    def train_epoch(self, iterator, info):
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
        if not hasattr(self, "model"):
            raise RuntimeError("Either set self.model in setup function or "
                               "override this method to implement a custom "
                               "training loop.")
        model = self.model
        scheduler = None
        if hasattr(self, "scheduler"):
            scheduler = self.scheduler

        if self.use_tqdm and self.world_rank == 0:
            desc = ""
            if info is not None and "epoch_idx" in info:
                if "num_epochs" in info:
                    desc = f"{info['epoch_idx'] + 1}/{info['num_epochs']}e"
                else:
                    desc = f"{info['epoch_idx'] + 1}e"

            # TODO: Implement len for Dataset?
            total = info[NUM_STEPS]
            if total is None:
                if hasattr(iterator, "__len__"):
                    total = len(iterator)

            _progress_bar = tqdm(
                total=total,
                desc=desc,
                unit="batch",
                leave=False)

        metric_meters = AverageMeterCollection()

        model.train()
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

            if scheduler and self._scheduler_step_freq == SCHEDULER_STEP_BATCH:
                scheduler.step()

            metric_meters.update(metrics, n=metrics.pop(NUM_SAMPLES, 1))
            self.global_step += 1

        if scheduler and self._scheduler_step_freq == SCHEDULER_STEP_EPOCH:
            scheduler.step()

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
        override this method.

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
        if not hasattr(self, "model"):
            raise RuntimeError("Either set self.model in setup function or "
                               "override this method to implement a custom "
                               "training loop.")
        if not hasattr(self, "optimizer"):
            raise RuntimeError("Either set self.optimizer in setup function "
                               "or override this method to implement a custom "
                               "training loop.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("Either set self.criterion in setup function "
                               "or override this method to implement a custom "
                               "training loop.")
        model = self.model
        optimizer = self.optimizer
        criterion = self.criterion
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
            output = model(*features)
            loss = criterion(output, target)

        # Compute gradients in a backward pass.
        with self.timers.record("grad"):
            optimizer.zero_grad()
            if self.use_fp16:
                with amp.scale_loss(loss, optimizer) as scaled_loss:
                    scaled_loss.backward()
            else:
                loss.backward()

        # Call step of optimizer to update model params.
        with self.timers.record("apply"):
            optimizer.step()

        return {"train_loss": loss.item(), NUM_SAMPLES: features[0].size(0)}

    def validate(self, val_iterator, info):
        """Runs one standard validation pass over the val_iterator.

        This will call ``model.eval()`` and ``torch.no_grad`` when iterating
        over the validation dataloader.

        You also do not need to call ``validate_batch`` if overriding this
        method.

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
        if not hasattr(self, "model"):
            raise RuntimeError("Either set self.model in setup function or "
                               "override this method to implement a custom "
                               "validation loop.")
        model = self.model
        metric_meters = AverageMeterCollection()

        # switch to evaluate mode
        model.eval()
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
        if not hasattr(self, "model"):
            raise RuntimeError("Either set self.model in setup function or "
                               "override this method to implement a custom "
                               "training loop.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("Either set self.criterion in setup function "
                               "or override this method to implement a custom "
                               "training loop.")
        model = self.model
        criterion = self.criterion
        # unpack features into list to support multiple inputs model
        *features, target = batch
        if self.use_gpu:
            features = [
                feature.cuda(non_blocking=True) for feature in features
            ]
            target = target.cuda(non_blocking=True)

        # compute output

        with self.timers.record("eval_fwd"):
            output = model(*features)
            loss = criterion(output, target)
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
        Any argument passed into self.register will automatically be saved.
        Use this method to save any additional state.

        Returns:
            dict: The state dict of the operator."""
        pass

    def load_state_dict(self, state_dict):
        """Override this to load the representation of the operator state.
        Anything passed into self.register will automatically be loaded. Use
        this method to load any additional state.

        Args:
            state_dict (dict): State dict as returned by the operator. """
        pass

    @staticmethod
    def from_creators(model_creator, optimizer_creator, data_creator=None,
                      scheduler_creator=None, loss_creator=None,
                      serialize_data_creation=True):

        if not (callable(model_creator) and callable(optimizer_creator)):
            raise ValueError(
                "Must provide a callable model_creator and optimizer_creator.")

        class CreatorOperator(TrainingOperator):
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

            def _initialize_dataloaders(self, config):
                logger.debug("Instantiating dataloaders.")
                loaders = None
                if serialize_data_creation:
                    logger.debug("Serializing the dataloading process.")
                    with FileLock(
                        os.path.join(tempfile.gettempdir(), ".raydata.lock")):
                        loaders = data_creator(config)
                else:
                    loaders = data_creator(config)
                train_loader, val_loader = self._validate_loaders(loaders)

                return train_loader, val_loader

            def setup(self, config):
                kwargs = {}
                logger.debug("Loading data.")
                train_loader = None
                validation_loader = None
                if data_creator and callable(data_creator):
                    train_loader, validation_loader = \
                        self._initialize_dataloaders(config)

                kwargs["train_loader"] = train_loader
                kwargs["validation_loader"] = validation_loader

                logger.debug("Creating model")
                models = model_creator(config)

                kwargs["models"] = models

                logger.debug("Creating optimizer.")
                optimizers = optimizer_creator(models, config)

                kwargs["optimizers"] = optimizers

                if scheduler_creator:
                    logger.debug("Creating scheduler.")
                    schedulers = scheduler_creator(optimizers, config)
                    kwargs["schedulers"] = schedulers

                if loss_creator:
                    logger.debug("Creating loss.")
                    if inspect.isclass(loss_creator) and issubclass(
                        loss_creator, torch.nn.modules.loss._Loss):
                        criterion = loss_creator()
                    else:
                        criterion = loss_creator(config)
                    kwargs["criterion"] = criterion

                state = self.register(**kwargs)
                self.model, self.optimizer = state[:2]
                if len(state) == 3:
                    self.criterion = state[2]
                if len(state) == 4:
                    self.scheduler = state[3]

        return CreatorOperator

    @property
    def device(self):
        """torch.device: The appropriate torch device, at your convenience."""
        return self._device

    @property
    def config(self):
        """dict: Provided into TorchTrainer."""
        return self._config

    @property
    def world_rank(self):
        """int: The rank of the parent runner. Always 0 if not distributed."""
        return self._world_rank

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


def get_test_operator(operator_cls):
    class _TestingOperator(operator_cls):
        def train_epoch(self, iterator, info):
            func = self.config.get("custom_func")
            if callable(func):
                return func(self, iterator, info)
            return {"done": 1}
    return _TestingOperator

def get_test_metrics_operator(operator_cls):
    class _TestMetricsOperator(operator_cls):
        def setup(self, config):
            super(_TestMetricsOperator, self).setup(config)
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

    return _TestMetricsOperator
