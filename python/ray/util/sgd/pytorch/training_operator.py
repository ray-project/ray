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
    return isinstance(component, list) and len(component) > 1


class TrainingOperator:
    """Abstract class for custom training or validation loops.

    The scheduler will only be called at a batch or epoch frequency, depending
    on the user parameter. Be sure to set ``scheduler_step_freq`` in
    ``PyTorchTrainer`` to either "batch" or "epoch" to increment the scheduler
    correctly during training. If using a learning rate scheduler
    that depends on validation loss, you can use ``trainer.update_scheduler``.


    Raises:
        ValueError if multiple models/optimizers/schedulers are provided. You
            are expected to subclass this class if you wish
            to train over multiple models/optimizers/schedulers.
    """

    @property
    def config(self):
        """Dictionary as provided into PyTorchTrainer."""
        return self._config

    @property
    def model(self):
        """First or only model created by the provided model_creator."""
        return self._models[0]

    @property
    def models(self):
        """List of models created by the provided model_creator."""
        return self._models

    @property
    def optimizer(self):
        """First or only optimizer(s) created by the optimizer_creator."""
        return self._optimizers[0]

    @property
    def optimizers(self):
        """List of optimizers created by the optimizer_creator."""
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
        """Criterion created by the provided loss_creator."""
        return self._criterion

    @property
    def scheduler(self):
        """First or only scheduler(s) created by the scheduler_creator."""
        if self._schedulers:
            return self._schedulers[0]

    @property
    def schedulers(self):
        """List of schedulers created by the scheduler_creator."""
        return self._schedulers

    @property
    def use_fp16(self):
        """Whether the model and optimizer have been FP16 enabled."""
        return self._use_fp16

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
            for k in ["fwd", "grad", "apply", "train_step"]
        }
        self._validated_customization = False
        self._models = models  # List of models
        assert isinstance(models, list), "Components need to be in a list."
        self._optimizers = optimizers  # List of optimizers
        assert isinstance(optimizers, list), "Components need to be in a list."
        self._train_loader = train_loader
        self._validation_loader = validation_loader
        self._world_rank = world_rank
        self._criterion = criterion
        self._schedulers = schedulers
        if schedulers:
            assert isinstance(schedulers,
                              list), "Components need to be in a list."
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
        """Override this method to implement custom operator setup."""
        pass

    def train_epoch(self, iterator, info):
        """Runs one standard training pass over the train_iterator.

        This function automatically measures timing for various operations such
        as host to device transfer, and gradient calculation.

        It also automatically detects and places the data on the GPU
        if available.

        Returns:
            A dict of metrics from training.
        """
        self._losses = AverageMeter()

        self.model.train()
        with self.timers["train_step"]:
            for batch_idx, batch in enumerate(iterator):
                batch_info = {
                    "batch_idx": batch_idx,
                    "global_step": self.global_step
                }
                batch_info.update(info)
                metrics = self.train_batch(batch, batch_info=batch_info)
                if "loss" in metrics:
                    self._losses.update(
                        metrics["loss"], n=metrics.get("num_samples", 1))
                self.global_step += 1

        if self.scheduler and self.config.get(
                SCHEDULER_STEP) == SCHEDULER_STEP_EPOCH:
            self.scheduler.step()

        stats = {
            BATCH_COUNT: batch_idx + 1,
            "mean_train_loss": self._losses.avg,
            "last_train_loss": self._losses.val,
            "epoch_time": self.timers["train_step"].last
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

        if self.scheduler and batch_info.get(
                SCHEDULER_STEP) == SCHEDULER_STEP_BATCH:
            self.scheduler.step()
        return {"loss": loss.item(), "num_samples": features.size(0)}

    def validate(self, val_iterator, info):
        """Runs one standard validation pass over the val_iterator.

        This function automatically measures timing for various operations such
        as host to device transfer and processing time for the batch.

        It also automatically detects and places the data on GPU device
        if available.

        Args:
            config: (dict): A user configuration provided into the Trainer
                constructor.
            model: The model as created by the model_creator.
            train_iterator: An iterator created from the DataLoader which
                wraps the provided Dataset.
            criterion: The loss object created by the loss_creator.
            scheduler (optional): The torch.optim.lr_scheduler object
                as created by the scheduler_creator. By default,
                this is not used in this function.

        Returns:
            A dict of metrics from the evaluation.
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

                if "correct" in metrics:
                    total_correct += metrics["correct"]

        stats = {
            "batch_count": batch_idx + 1,
            "mean_validation_loss": losses.avg,
            "mean_accuracy": total_correct / losses.count
        }
        return stats

    def validate_batch(self, batch, batch_info):
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
            "correct": (predicted == target).sum().item(),
            "num_samples": target.size(0)
        }

    def state_dict(self):
        pass

    def load_state_dict(self, state_dict):
        pass
