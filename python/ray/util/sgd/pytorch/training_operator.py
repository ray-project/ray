import collections
import torch

from ray.util.sgd.utils import TimerStat, AverageMeter
from ray.util.sgd.pytorch.constants import (
    SCHEDULER_STEP_EPOCH, SCHEDULER_STEP_BATCH, SCHEDULER_STEP, BATCH_COUNT)

amp = None

try:
    from apex import amp
except ImportError:
    # Apex library is not installed, so we cannot enable mixed precision.
    # We don't log here because logging happens in the pytorch_runner,
    # where amp is initialized.
    pass


def _is_multiple(component):
    """Checks if a component (optimizer, model, etc) is not singular."""
    return isinstance(component, collections.Iterable) and len(component) > 1


class TrainingOperator:
    """Abstract class for custom training or validation loops.

    The scheduler will only be called at a batch or epoch frequency, depending
    on the user parameter. Be sure to set ``scheduler_step_freq`` in
    ``PyTorchTrainer`` to either "batch" or "epoch" to increment the scheduler
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
                 models,
                 optimizers,
                 criterion,
                 train_loader,
                 validation_loader,
                 world_rank,
                 schedulers=None,
                 use_fp16=False):
        # You are not expected to override this method.
        self.timers = {
            k: TimerStat()
            for k in ["fwd", "grad", "apply", "epoch_time"]
        }
        self._validated_customization = False
        self._models = models  # List of models
        assert isinstance(models, collections.Iterable), (
            "Components need to be iterable. Got: {}".format(type(models)))
        self._optimizers = optimizers  # List of optimizers
        assert isinstance(optimizers, collections.Iterable), (
            "Components need to be iterable. Got: {}".format(type(optimizers)))
        self._train_loader = train_loader
        self._validation_loader = validation_loader
        self._world_rank = world_rank
        self._criterion = criterion
        self._schedulers = schedulers
        if schedulers:
            assert isinstance(schedulers, collections.Iterable), (
                "Components need to be iterable. Got: {}".format(
                    type(schedulers)))
        self._config = config
        self._use_fp16 = use_fp16
        self.global_step = 0

        if type(self) is TrainingOperator:
            for component in (models, schedulers, optimizers):
                if _is_multiple(component):
                    raise ValueError(
                        "Need to provide a custom operator subclassing "
                        "TrainingOperator if using multi-scheduler, "
                        "multi-model or multi-optimizer training/validation.")

        self.setup(config)

    def setup(self, config):
        """Override this method to implement custom operator setup.

        Args:
            config (dict): Custom configuration value to be passed to
                all creator and operator constructors. Same as ``self.config``.
        """
        pass

    def train_epoch(self, iterator, info):
        """Runs one standard training pass over the train_iterator.

        By default, this method will iterate over the given iterator and
        call ``self.train_batch`` over each batch.

        If ``scheduler_step_freq`` is set, this class will also step the
        scheduler accordingly.

        You do not need to call ``train_batch`` in this method if you plan
        to implement a custom optimization/training routine here.

        Args:
            iterator (iter): Iterator over the training data for the entire
                epoch. This iterator is expected to be entirely consumed.
            info (dict): Dictionary for information to be used for custom
                training operations.

        Returns:
            A dict of metrics from training.
        """
        self._losses = AverageMeter()

        self.model.train()
        with self.timers["epoch_time"]:
            for batch_idx, batch in enumerate(iterator):
                batch_info = {
                    "batch_idx": batch_idx,
                    "global_step": self.global_step
                }
                batch_info.update(info)
                metrics = self.train_batch(batch, batch_info=batch_info)

                if self.scheduler and batch_info.get(
                        SCHEDULER_STEP) == SCHEDULER_STEP_BATCH:
                    self.scheduler.step()

                if "loss" in metrics:
                    self._losses.update(
                        metrics["loss"], n=metrics.get("num_samples", 1))
                self.global_step += 1

        if self.scheduler and info.get(SCHEDULER_STEP) == SCHEDULER_STEP_EPOCH:
            self.scheduler.step()

        stats = {
            BATCH_COUNT: batch_idx + 1,
            "mean_train_loss": self._losses.avg,
            "last_train_loss": self._losses.val,
            "epoch_time": self.timers["epoch_time"].last
        }
        stats.update({
            timer_tag: timer.mean
            for timer_tag, timer in self.timers.items()
        })
        return stats

    def forward(self, features, target):
        output = self.model(features)
        loss = self.criterion(output, target)

    def train_batch(self, batch, batch_info):
        """Computes loss and updates the model over one batch.

        This method is responsible for computing the loss and gradient and
        updating the model.

        By default, this method implementation assumes that batches
        are in (features, labels) format. If using amp/fp16
        training, it will also scale the loss automatically.

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

        """
        features, target = batch
        # Create non_blocking tensors for distributed training
        if torch.cuda.is_available():
            features = features.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

        # Compute output.
        with self.timers["fwd"]:
            loss = self.forward(features, target)

        # Compute gradients in a backward pass.
        with self.timers["grad"]:
            self.optimizer.zero_grad()
            if self.use_fp16:
                with amp.scale_loss(loss, self.optimizer) as scaled_loss:
                    scaled_loss.backward()
            else:
                loss.backward()

        # Call step of optimizer to update model params.
        with self.timers["apply"]:
            self.optimizer.step()
        return {"loss": loss.item(), "num_samples": features.size(0)}

    def validate(self, val_iterator, info):
        """Runs one standard validation pass over the val_iterator.

        This will call ``model.eval()`` and ``torch.no_grad`` when iterating
        over the validation dataset.

        If overriding this method, you can access model, criterion via
        ``self.model`` and ``self.criterion``. You also do not need to call
        ``validate_batch`` if overriding this method.

        Args:
            val_iterator (iter): Iterable constructed over the
                validation dataset.
            info: (dict): Dictionary for information to be used for custom
                validation operations.

        Returns:
            A dict of metrics from the evaluation.
                By default, returns "mean_accuracy" and "mean_validation_loss"
                which is computed by aggregating "loss" and "correct" values
                from ``validate_batch`` and dividing it by the sum of
                ``num_samples`` from all calls to ``self.validate_batch``.
        """
        losses = AverageMeter()
        total_correct = 0

        # switch to evaluate mode
        self.model.eval()
        with torch.no_grad():
            for batch_idx, batch in enumerate(val_iterator):
                batch_info = {"batch_idx": batch_idx}
                batch_info.update(info)
                metrics = self.validate_batch(batch, batch_info)
                if "loss" in metrics:
                    losses.update(
                        metrics["loss"], n=metrics.get("num_samples", 1))

                if "num_correct" in metrics:
                    total_correct += metrics["num_correct"]

        stats = {
            "batch_count": batch_idx + 1,
            "mean_validation_loss": losses.avg,
            "mean_accuracy": total_correct / losses.count
        }
        return stats

    def validate_batch(self, batch, batch_info):
        """Calcuates the loss and accuracy over a given batch.

        You can override this method to provide arbitrary metrics.

        Args:
            batch: One item of the validation iterator.
            batch_info (dict): Contains information per batch from
                ``validate()``.

        Returns:
            A dict of metrics.
                By default, returns "loss", "num_correct", and "num_samples".
        """
        features, target = batch
        if torch.cuda.is_available():
            features = features.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

        # compute output
        output = self.model(features)
        loss = self.criterion(output, target)
        _, predicted = torch.max(output.data, 1)

        return {
            "loss": loss.item(),
            "num_correct": (predicted == target).sum().item(),
            "num_samples": target.size(0)
        }

    def state_dict(self):
        """Returns a serializable representation of the operator state."""
        pass

    def load_state_dict(self, state_dict):
        """Loads a serializable representation of the operator state."""
        pass

    @property
    def config(self):
        """Dictionary as provided into PyTorchTrainer."""
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
        """
        Data loader for the validation dataset created by the data_creator.
        """
        return self._train_loader

    @property
    def validation_loader(self):
        """
        Data loader for the train dataset created by the data_creator.
        """
        return self._validation_loader

    @property
    def world_rank(self):
        """The rank of the parent runner. Always 0 if not distributed."""
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
    def use_fp16(self):
        """Whether the model and optimizer have been FP16 enabled."""
        return self._use_fp16


class _TestingOperator(TrainingOperator):
    def train_epoch(self, iterator, info):
        func = self.config.get("custom_func")
        if callable(func):
            return func(self, iterator, info)
        return {"done": 1}
