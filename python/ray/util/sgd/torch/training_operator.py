import inspect
import logging
import os
import tempfile

import torch
import torch.nn as nn
from filelock import FileLock

from ray.util.sgd.utils import (TimerCollection, AverageMeterCollection,
                                NUM_SAMPLES)
from ray.util.sgd.torch.constants import (
    SCHEDULER_STEP_EPOCH,
    NUM_STEPS,
    SCHEDULER_STEP_BATCH,
)

from torch.nn.parallel import DistributedDataParallel
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
    """Abstract class to define training and validation state and logic.

    You must subclass this class and override the ``setup`` method to define
    your training components such as the model, optimizer, data, loss,
    and scheduler. When you pass this class to ``TorchTrainer``, a copy of
    this class will be made on each worker.

    .. code-block:: python

        class MyTrainingOperator(TrainingOperator):

            def setup(self, config):
                model = nn.Linear(1, 1)
                optimizer = torch.optim.SGD(
                    model.parameters(), lr=config.get("lr", 1e-4))
                loss = torch.nn.MSELoss()

                batch_size = config["batch_size"]
                train_data, val_data = LinearDataset(2, 5), LinearDataset(2, 5)
                train_loader = DataLoader(train_data, batch_size=batch_size)
                val_loader = DataLoader(val_data, batch_size=batch_size)

                self.model, self.optimizer = self.register(
                    models=model,
                    optimizers=optimizer,
                    criterion=loss)

                self.register_data(
                    train_loader=train_loader,
                    validation_loader=val_loader)

        trainer = TorchTrainer(
            training_operator_cls=MyTrainingOperator,
            config={"batch_size": 32},
            use_gpu=True
        )
        for i in range(4):
            trainer.train()

    This class provides default implementations for training and validation.
    Set ``self.model``, ``self.optimizer``, and
    ``self.criterion`` to leverage the default training and validation loops.
    If ``self.scheduler`` is set, it will only be called at a batch or epoch
    frequency, depending on the user parameter. Set
    ``scheduler_step_freq`` in ``TorchTrainer`` to either "batch" or "epoch"
    to increment the scheduler correctly during training. If using a
    learning rate scheduler that depends on validation loss, you can use
    ``trainer.update_scheduler``.

    If you want to provide custom training and validation loops, you can do
    so using this class as well. There are two granularities that
    you can provide customization: per epoch or per batch.
    You do not need to override both.

    .. image:: raysgd-custom.jpg
        :scale: 80%
        :align: center

    If you are using multiple models, optimizers, or schedulers, you must
    implement custom training and validation.

    Raises:
        ValueError
            You are expected to either set ``self.model``,
            ``self.optimizer``, and ``self.criterion`` instance attributes in
            setup or implement custom training & validation.
    """

    def __init__(self,
                 config,
                 world_rank,
                 local_rank,
                 is_distributed=False,
                 device=None,
                 use_gpu=False,
                 use_fp16=False,
                 use_tqdm=False,
                 wrap_ddp=False,
                 add_dist_sampler=False,
                 scheduler_step_freq=None):
        # You are not expected to override this method.
        self._world_rank = world_rank
        self._local_rank = local_rank
        self._config = config
        self._is_distributed = is_distributed
        self._use_fp16 = use_fp16
        self._device = device
        self._use_gpu = use_gpu and torch.cuda.is_available()
        if tqdm is None and use_tqdm:
            raise ValueError("tqdm must be installed to use tqdm in training.")
        self._use_tqdm = use_tqdm
        self.global_step = 0
        self._wrap_ddp = wrap_ddp
        self._add_dist_sampler = add_dist_sampler
        self._scheduler_step_freq = scheduler_step_freq

        self.timers = TimerCollection()
        self.setup(config)

    def _set_timers(self, timers):
        """Passes in the timers from the Runner."""
        self.timers = timers

    def _configure_amp(self, amp, models, optimizers, apex_args):
        models, optimizers = amp.initialize(models, optimizers, **apex_args)
        return models, optimizers

    def _configure_ddp(self, models, device_ids, ddp_args):
        return [
            DistributedDataParallel(model, device_ids=device_ids, **ddp_args)
            for model in models
        ]

    def _return_items(self, items, original_items):
        """Helper method to return items in same format as original_items."""
        if isinstance(original_items, tuple):
            return tuple(items)
        elif isinstance(original_items, Iterable):
            # Items is already a list.
            return items
        else:
            assert len(items) == 1
            return items[0]

    def setup(self, config):
        """Override this method to implement operator setup.

        You should call self.register and self.register_data here to
        register training components and data loaders with Ray SGD.

        Args:
            config (dict): Custom configuration value to be passed to
                all creator and operator constructors. Same as ``self.config``.
        """
        raise NotImplementedError

    def register(self,
                 *,
                 models,
                 optimizers,
                 criterion=None,
                 schedulers=None,
                 ddp_args=None,
                 apex_args=None):
        """Registers parameters with Ray SGD and sets up training components.

        By calling this method to register your models, optimizers,
        criterion, and schedulers, Ray SGD will automatically handle
        necessary setup such as GPU/devices, Distributed Data Parallel, and
        Fp16. The registered components are returned and should be set as
        instance attributes to access during training/validation.

        If more than one model, optimizer, or scheduler is passed in,
        you should implement your own custom training loop.

        Calling register will perform the following steps in this order:
            1. If using GPU, Move model(s) and criterion to the corresponding
                Cuda device.
            2. If using fp16, initializes amp with model(s), optimizer(s),
                and apex_args.
            3. If using distributed training and wrap_ddp is True,
                wraps model(s) with DistributedDataParallel.

        .. code-block:: python

            class MyTrainingOperator(TrainingOperator):
                def setup(self, config):
                    model = ...
                    optimizer = ...
                    train_loader = ...
                    val_loader = ...
                    loss = ...

                    self.model, self.optimizer, self.criterion = self.register(
                    models=model, optimizers=optimizer, criterion=loss)

                    # At this point DDP, Cuda, and Fp16
                    # are set up for all our components. We then use
                    # self.model, self.optimizer, etc. in our training loop.

                    self.register_data(train_loader=train_loader,
                    validation_loader=val_loader)


        Args:
            models (torch.nn.Module or Iterable[nn.Module]): Pytorch model or
                multiple Pytorch models to use for training. If
                `use_gpu=True` is passed into ``TorchTrainer``, and Cuda is
                available, models will automatically be placed on GPU.
                If ``wrap_ddp=True`` is passed into ``TorchTrainer``,
                models will be wrapped in DDP. If wrap_ddp is False,
                you should handle DDP for your models in setup.
            optimizers (torch.optim.Optimizer or Iterable[
                torch.optim.Optimizer]): Pytorch optimizer or multiple Pytorch
                optimizers to use for training.
            criterion (Callable, optional): Function to return loss
                metric given features and target. If not provided,
                must implement a custom training loop.
            schedulers (torch.optim.lr_scheduler or Iterable[
                torch.optim.lr_scheduler], optional): A learning rate
                scheduler or multiple learning rate schedulers.
            ddp_args (dict|None): Dict containing keyword args for
                DistributedDataParallel if distributed training is being
                used. `module` and `device_ids` are automatically passed in,
                but this dict is useful for passing in other args such as
                `find_unused_parameters=True`.
            apex_args (dict|None): Dict containing keyword args for
                amp.initialize if fp16 is being used. See
                https://nvidia.github.io/apex/amp.html#module-apex.amp.
                By default, the models and optimizers are passed in.
                Consider using "num_losses" if operating over multiple
                models and optimizers.

        Returns:
            Tuple of model, optimizer, criterion if not None, and scheduler
            if not None.
        """
        if ddp_args and not isinstance(ddp_args, dict):
            raise ValueError("ddp_args needs to be a dict object.")
        ddp_args = ddp_args if ddp_args else {}

        if apex_args and not isinstance(apex_args, dict):
            raise ValueError("apex_args needs to be a dict object.")
        apex_args = apex_args if apex_args else {}

        return_vals = []
        logger.debug("Registering models.")
        self._original_models = models
        if not isinstance(self._original_models, Iterable):
            self._original_models = [self._original_models]
        assert all(
            isinstance(model, nn.Module) for model in self._original_models), (
                f"All models must be PyTorch models: {self._original_models}.")
        if self.use_gpu and torch.cuda.is_available():
            self._original_models = [
                model.cuda() for model in self._original_models
            ]

        logger.debug("Registering optimizers.")
        self._optimizers = optimizers
        if not isinstance(self._optimizers, Iterable):
            self._optimizers = [self._optimizers]

        if schedulers:
            logger.debug("Registering scheduler.")
            self._schedulers = schedulers
            if not isinstance(self._schedulers, Iterable):
                self._schedulers = [self._schedulers]
        else:
            if isinstance(schedulers, Iterable):
                self._schedulers = []
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

        if self.use_fp16 and amp:
            logger.debug("Setting up Apex.")
            self._amp = amp
            self._original_models, self._optimizers = self._configure_amp(
                self._amp,
                self._original_models,
                self._optimizers,
                apex_args=apex_args)

        if self._wrap_ddp:
            logging.debug("Setting up DDP for models.")
            self._models = self._configure_ddp(
                models=self._original_models,
                device_ids=self.device_ids,
                ddp_args=ddp_args)
        else:
            self._models = self._original_models

        return_vals.append(self._return_items(self._models, models))
        return_vals.append(self._return_items(self._optimizers, optimizers))

        if self._criterion is not None:
            return_vals.append(self._criterion)

        if self._schedulers is not None:
            if self.scheduler_step_freq is None:
                raise ValueError("scheduler_step_freq passed into "
                                 "TorchTrainer cannot be None if you "
                                 "are registering schedulers. Set this to "
                                 "'manual' if you will be manually stepping "
                                 "the schedulers.")
            return_vals.append(
                self._return_items(self._schedulers, schedulers))

        return tuple(return_vals)

    def register_data(self, *, train_loader=None, validation_loader=None):
        """Registers data loaders with Ray SGD.

        Calling this method will automatically setup Distributed Sampler for
        these data loaders if add_dist_sampler=True is passed into the
        TorchTrainer. This method does not return the wrapped data loaders.
        You should use the iterators passed into train_epoch and validate
        instead.

        .. code-block:: python

            class MyTrainingOperator(TrainingOperator):
                def setup(self, config):
                    model = ...
                    optimizer = ...
                    train_loader = ...
                    val_loader = ...
                    loss = ...

                    self.model, self.optimizer, self.criterion = self.register(
                    models=model, optimizers=optimizer, criterion=loss)

                    self.register_data(train_loader=train_loader,
                    validation_loader=val_loader)

                    # At this point the data loaders are registered with
                    # Ray SGD and are wrapped with Distributed Samplers if
                    # applicable.


                def train_epoch(self, iterator, info):
                    # If providing custom training or validation methods,
                    # the registered data loaders are passed in through the
                    # iterator parameter.
                    ...

        Args:
            train_loader (Iterator): An iterator for training
                data. If None is explicitly passed in, a Ray SGD Dataset
                must be passed in through TorchTrainer.train. Ray SGD will
                automatically use a Distributed Sampler if TorchTrainer(...,
                add_dist_sampler=True).
            validation_loader (Iterator): An iterator for validation
                data. Ray SGD will automatically use a Distributed Sampler
                if TorchTrainer(..., add_dist_sampler=True).
        """

        logger.debug("Registering data loaders..")
        self._train_loader = train_loader
        self._validation_loader = validation_loader

        if self._is_distributed:

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
                    logging.debug("Wrapping train data loader with "
                                  "DistributedSampler.")
                    self._train_loader = with_sampler(self._train_loader)

            if self._validation_loader is not None and should_wrap_dataloader(
                    self._validation_loader):
                if self._add_dist_sampler:
                    logging.debug("Wrapping validation data loader with "
                                  "DistributedSampler.")
                    self._validation_loader = with_sampler(
                        self._validation_loader)

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
                total=total, desc=desc, unit="batch", leave=False)

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

            if scheduler and self.scheduler_step_freq == SCHEDULER_STEP_BATCH:
                scheduler.step()

            metric_meters.update(metrics, n=metrics.pop(NUM_SAMPLES, 1))
            self.global_step += 1

        if scheduler and self.scheduler_step_freq == SCHEDULER_STEP_EPOCH:
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
        Any argument passed into self.register and self.register_data will
        automatically be saved.
        Use this method to save any additional state. If your TorchTrainer
        is on a CPU-only machine, make sure this method converts all state
        to be CPU-compatible.

        Returns:
            dict: The state dict of the operator."""
        pass

    def load_state_dict(self, state_dict):
        """Override this to load the representation of the operator state.
        Anything passed into self.register and self.register_data will
        automatically be loaded. Use this method to load any additional state.
        Args:
            state_dict (dict): State dict as returned by the operator. """
        pass

    def _get_original_models(self):
        if not hasattr(self, "_original_models"):
            raise RuntimeError("Training Operator does not have any "
                               "registered models. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self._original_models

    def _get_optimizers(self):
        if not hasattr(self, "_optimizers"):
            raise RuntimeError("Training Operator does not have any "
                               "registered optimizers. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self._optimizers

    def _get_schedulers(self):
        if not hasattr(self, "_schedulers"):
            raise RuntimeError("Training Operator does not have any "
                               "registered schedulers. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self._schedulers

    def _get_train_loader(self):
        if not hasattr(self, "_train_loader") or \
                self._train_loader is None:
            raise RuntimeError(
                "Training Operator does not have any "
                "registered train loader. If this is "
                "unexepected, make sure to call "
                "self.register_data(...) inside the setup method "
                "of your Training Operator.")
        return self._train_loader

    def _get_validation_loader(self):
        if not hasattr(self, "_validation_loader") or \
                self._validation_loader is None:
            raise RuntimeError(
                "Training Operator does not have any "
                "registered validation loader. If this is "
                "unexepected, make sure to call "
                "self.register_data(...) inside the setup method "
                "of your Training Operator.")
        return self._validation_loader

    def _get_criterion(self):
        if not hasattr(self, "_criterion"):
            raise RuntimeError("Training Operator does not have any "
                               "registered criterion. Are you calling "
                               "self.register(...) inside the setup method "
                               "of your Training Operator?")
        return self._criterion

    @classmethod
    def from_ptl(cls,
                 lightning_module_cls,
                 train_dataloader=None,
                 val_dataloader=None):
        """Create a custom TrainingOperator class from a LightningModule.

        .. code-block:: python

            MyLightningOperator = TrainingOperator.from_ptl(
                MyLightningModule)
            trainer = TorchTrainer(training_operator_cls=MyLightningOperator,
                ...)

        Args:
            lightning_module_cls: Your LightningModule class. An object of
                this class will get instantiated on each worker.
            train_dataloader: The data loader to use for training. If None
                is provided, LightningModule.train_dataloader will be used
                instead.
            val_dataloader: The data loader to use for validation. If None
                is provided, LightningModule.val_dataloader will be used
                instead.

        Returns:
            A TrainingOperator class properly configured given the
            LightningModule.
        """
        from ray.util.sgd.torch.lightning_operator import LightningOperator

        class CustomLightningOperator(LightningOperator):
            _lightning_module_cls = lightning_module_cls
            _train_dataloader = train_dataloader
            _val_dataloader = val_dataloader

        return CustomLightningOperator

    @classmethod
    def from_creators(cls,
                      model_creator,
                      optimizer_creator,
                      data_creator=None,
                      loss_creator=None,
                      scheduler_creator=None,
                      serialize_data_creation=True):
        """Create a custom TrainingOperator class from creator functions.

        This method is useful for backwards compatibility with
        previous versions of Ray. To provide custom training and validation,
        you should subclass the class that is returned by this method instead
        of ``TrainingOperator``.

        .. code-block:: python

            MyCreatorOperator = TrainingOperator.from_creators(
                model_creator, optimizer_creator)
            trainer = TorchTrainer(training_operator_cls=MyCreatorOperator,
                ...)

        Args:
            model_creator (dict -> Model(s)): Constructor function that takes
                in config and returns the model(s) to be optimized. These
                must be ``torch.nn.Module`` objects. If multiple models are
                returned, a ``training_operator_cls`` must be specified.
                You do not need to handle GPU/devices in this function;
                RaySGD will do that under the hood.
            data_creator (dict -> Iterable(s)): Constructor function
                that takes in the passed config and returns one or
                two Iterable objects. Note that even though two Iterable
                objects can be returned, only one will be used for training,
                and the other will be used for validation. If not provided,
                you must pass in a Dataset to ``TorchTrainer.train``.
            optimizer_creator ((models, dict) -> optimizers): Constructor
                function that takes in the return values from
                ``model_creator`` and the passed config and returns One or
                more Torch optimizer objects. You do not need to handle
                GPU/devices in this function; ``RaySGD`` will do that for you.
            loss_creator (torch.nn.*Loss class | dict -> loss): A constructor
                function for the training loss. This can be either a function
                that takes in the provided config for customization or a
                subclass of ``torch.nn.modules.loss._Loss``, which is most
                Pytorch loss classes. For example,
                ``loss_creator=torch.nn.BCELoss``. If not provided, you must
                provide a custom TrainingOperator.
            scheduler_creator ((optimizers, dict) -> scheduler):
                A constructor function for the torch scheduler. This is
                a function that takes in the generated optimizers (from
                ``optimizer_creator``) provided config for customization.
                Be sure to set ``scheduler_step_freq`` to increment the
                scheduler correctly.
            serialize_data_creation (bool): A filelock will be used
                to ensure no race conditions in data downloading among
                different workers on the same node (using the local file
                system). Defaults to True.

        Returns:
            A CreatorOperator class- a subclass of TrainingOperator with a
            ``setup`` method that utilizes the passed in creator functions.
        """

        if not (callable(model_creator) and callable(optimizer_creator)):
            raise ValueError(
                "Must provide a callable model_creator and optimizer_creator.")

        class CustomCreatorOperator(CreatorOperator):
            _model_creator = model_creator
            _optimizer_creator = optimizer_creator
            _data_creator = data_creator
            _loss_creator = loss_creator
            _scheduler_creator = scheduler_creator
            _serialize_data_creation = serialize_data_creation

        return CustomCreatorOperator

    @property
    def device(self):
        """torch.device: The appropriate torch device, at your
        convenience."""
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
    def local_rank(self):
        """int: Local rank of parent runner. Always 0 if not distributed."""
        return self._local_rank

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
        """Optional[List[int]]: Device IDs for the model.

        This is useful for using batch norm with DistributedDataParallel.
        Not applicable if not using GPU.
        """
        if not self.use_gpu:
            return None
        return [self.device.index]

    @property
    def scheduler_step_freq(self):
        """Optional[str]: The ``scheduler_step_freq`` passed into
        ``TorchTrainer``

        This is useful to determine when to call scheduler.step.
        """
        return self._scheduler_step_freq


class CreatorOperator(TrainingOperator):
    """A subclass of TrainingOperator with training defined by creator funcs.

    This class allows for backwards compatibility with pre Ray 1.0 versions.

    This class is returned by `TrainingOperator.from_creators(...)`. If you
    need to add custom functionality, you should subclass this class,
    implement the appropriate methods and pass the subclass into
    `TorchTrainer`.

    .. code-block:: python

        MyCreatorOperator = TrainingOperator.from_creators(
            model_creator, optimizer_creator)
        trainer = TorchTrainer(training_operator_cls=MyCreatorOperator,
            ...)
    """

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
        if self._serialize_data_creation:
            logger.debug("Serializing the dataloading process.")
            with FileLock(
                    os.path.join(tempfile.gettempdir(), ".raydata.lock")):
                loaders = self.__class__._data_creator(config)
        else:
            loaders = self.__class__._data_creator(config)
        train_loader, val_loader = self._validate_loaders(loaders)

        return train_loader, val_loader

    def setup(self, config):
        kwargs = {}
        logger.debug("Loading data.")
        train_loader = None
        validation_loader = None
        if self.__class__._data_creator and callable(
                self.__class__._data_creator):
            train_loader, validation_loader = self._initialize_dataloaders(
                config)

        logger.debug("Creating model")
        models = self.__class__._model_creator(config)

        kwargs["models"] = models

        logger.debug("Creating optimizer.")
        optimizers = self.__class__._optimizer_creator(models, config)

        kwargs["optimizers"] = optimizers

        if self.__class__._scheduler_creator:
            logger.debug("Creating scheduler.")
            schedulers = self.__class__._scheduler_creator(optimizers, config)
            kwargs["schedulers"] = schedulers

        if self.__class__._loss_creator:
            logger.debug("Creating loss.")
            if inspect.isclass(self.__class__._loss_creator) and issubclass(
                    self.__class__._loss_creator, torch.nn.modules.loss._Loss):
                criterion = self.__class__._loss_creator()
            else:
                criterion = self.__class__._loss_creator(config)
            kwargs["criterion"] = criterion

        state = self.register(**kwargs)
        self._registered_models, self._registered_optimizers = state[:2]
        if isinstance(self.models, (list, tuple)):
            logger.info("Multiple models have been registered. If custom "
                        "training methods are not provided, only the first "
                        "model will be used.")
            self._registered_model = self.models[0]
        else:
            self._registered_model = self.models

        if isinstance(self.optimizers, (list, tuple)):
            logger.info("Multiple optimizers have been registered. If custom "
                        "training methods are not provided, only the first "
                        "optimizer will be used.")
            self._reigstered_optimizer = self.optimizers[0]
        else:
            self._registered_optimizer = self.optimizers

        if len(state) >= 3:
            self._registered_criterion = state[2]
        if len(state) == 4:
            self._registered_schedulers = state[3]
            if isinstance(self.schedulers, (list, tuple)):
                logger.info("Multiple schedulers have been registered. If "
                            "custom training methods are not provided, "
                            "only the first scheduler will be used.")
                self._registered_scheduler = self.schedulers[0]
            else:
                self._registered_scheduler = self.schedulers

        self.register_data(
            train_loader=train_loader, validation_loader=validation_loader)

    @property
    def model(self):
        """First or only model created by the provided ``model_creator``."""
        return self._registered_model

    @property
    def optimizer(self):
        """First or only optimizer(s) created by the ``optimizer_creator``."""
        return self._registered_optimizer

    @property
    def scheduler(self):
        """First or only scheduler(s) created by the ``scheduler_creator``."""
        return self._registered_scheduler

    @property
    def criterion(self):
        """Criterion created by the provided ``loss_creator``."""
        return self._registered_criterion

    @property
    def models(self):
        """List of models created by the provided ``model_creator``."""
        return self._registered_models

    @property
    def optimizers(self):
        """List of optimizers created by the ``optimizer_creator``."""
        return self._registered_optimizers

    @property
    def schedulers(self):
        """List of schedulers created by the ``scheduler_creator``."""
        return self._registered_schedulers


def get_test_operator(operator_cls):
    class _TestingOperator(operator_cls):
        def train_epoch(self, iterator, info):
            func = self.config.get("custom_func")
            if callable(func):
                return func(self, iterator, info)
            return {"done": 1}

        def validate(self, iterator, info):
            return self.train_epoch(iterator, info)

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
