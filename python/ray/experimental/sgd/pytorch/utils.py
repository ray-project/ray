import collections
import time
import torch

from ray.experimental.sgd.utils import TimerStat

amp = None

try:
    from apex import amp
except ImportError:
    # Apex library is not installed, so we cannot enable mixed precision.
    # We don't log here because logging happens in the pytorch_runner,
    # where amp is initialized.
    pass

USE_FP16 = "__use_fp16__"
TEST_MODE = "__test_mode__"
SCHEDULER_STEP = "scheduler_step"
SCHEDULER_STEP_BATCH = "batch"
SCHEDULER_STEP_EPOCH = "epoch"

VALID_SCHEDULER_STEP = {SCHEDULER_STEP_BATCH, SCHEDULER_STEP_EPOCH}

def validate_multiple_components(model, optimizer, scheduler):
    if isinstance(model, collections.Iterable) or isinstance(
            optimizer, collections.Iterable) or isinstance(
                scheduler, collections.Iterable):
        raise ValueError(
            "Need to provide a custom operator if using multi-model "
            "or multi-scheduler or multi-optimizer training/validation.")

class BaseOperator:
    """Abstract class for custom training or validation loops.

    The scheduler will only be called at a batch or epoch frequency, depending
    on the user parameter. Be sure to set ``scheduler_step_freq`` in
    ``PyTorchTrainer`` to either "batch" or "epoch" to increment the scheduler
    correctly during training. If using a learning rate scheduler
    that depends on validation loss, you can use ``trainer.update_scheduler``.
    """
    @property
    def config(self):
        return self._config

    @property
    def model(self):
        return self._model

    @property
    def optimizer(self):
        return self._optimizer

    @property
    def criterion(self):
        return self._criterion

    @property
    def scheduler(self):
        return self._scheduler

    def __init__(self):
        self.timers = {
            k: TimerStat() for k in ["fwd", "grad", "apply", "train_epoch"]}
        self._validated_customization = False
        self.setup(self, self.config)

    def setup(self, config):
        pass

    def train_epoch(self, iterator, num_steps=None):
        """Runs one standard training pass over the train_iterator.

        This function automatically measures timing for various operations such
        as host to device transfer, gradient calculation, and gradient application.

        It also automatically detects and places the data on the given GPU device
        if available.

        Raises:
            ValueError if multiple models/optimizers/schedulers are provided. You
                are expected to have a custom training function if you wish
                to use multiple models/optimizers/schedulers.

        Returns:
            A dict of metrics from training.
        """
        self._losses = AverageMeter()

        self.model.train()
        with self.timers["train_epoch"]:
            for batch_idx, batch in enumerate(iterator):
                metrics = self.train_batch(batch, batch_idx)
                if "loss" in metrics:
                    self._losses.update(
                        metrics["loss"], n=metrics.get("num_samples", 1))
                if batch_idx + 1 == num_steps:
                    break

        if scheduler and config.get(SCHEDULER_STEP) == SCHEDULER_STEP_EPOCH:
            scheduler.step()

        stats = {
            "batch_count": batch_idx + 1,
            "mean_train_loss": self._losses.avg,
            "last_train_loss": self._losses.val,
            "epoch_time": self.timers["train_epoch"].last
        }
        stats.update({
            timer_tag: timer.mean for timer_tag, timer in timers.items()})
        return stats

    def train_batch(self, batch, batch_idx):
        if not self._validated_customization:
            validate_multiple_components(model, optimizer, scheduler)
            self._validated_customization = True
        features, target = batch
        # Create non_blocking tensors for distributed training
        if torch.cuda.is_available():
            features = features.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

        # Compute output.
        with self.timers["fwd"]:
            output = self.model(features)
            loss = self.criterion(output, target)

        # Compute gradients in a backward pass.
        with self.timers["grad"]:
            optimizer.zero_grad()
            if config.get(USE_FP16):
                with amp.scale_loss(loss, optimizer) as scaled_loss:
                    scaled_loss.backward()
            else:
                loss.backward()

        # Call step of optimizer to update model params.
        with self.timers["apply"]:
            optimizer.step()

        if scheduler and config.get(SCHEDULER_STEP) == SCHEDULER_STEP_BATCH:
            scheduler.step()
        return {"loss": loss.item(), "num_samples": features.size(0)}

    def evaluation_epoch(self, val_iterator, num_steps=None):
        """Runs one standard validation pass over the val_iterator.

        This function automatically measures timing for various operations such
        as host to device transfer and processing time for the batch.

        It also automatically detects and places the data on the given GPU device
        if available.

        Raises:
            ValueError if multiple models/schedulers are provided. You
                are expected to have a custom validation function if you wish
                to use multiple models/schedulers.

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

        # switch to evaluate mode
        model.eval()
        with torch.no_grad():
            for batch_idx, batch in enumerate(val_iterator):
                metrics = self.evaluation_batch(batch, batch_idx)
                if "loss" in metrics:
                    self._losses.update(
                        metrics["loss"], n=metrics.get("num_samples", 1))
                if batch_idx + 1 == num_steps:
                    break

        stats = {
            "batch_count": batch_idx + 1,
            "batch_time": batch_time.avg,
            "evaluation_loss": losses.avg,
            "mean_accuracy": correct / total
        }
        return stats


    def evaluation_batch(self, batch, batch_idx):
        if not self._validated_customization:
            validate_multiple_components(model, optimizer, scheduler)
            self._validated_customization = True

        features, target = batch
        if torch.cuda.is_available():
            features = features.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

        # compute output
        output = model(features)
        loss = criterion(output, target)
        _, predicted = torch.max(output.data, 1)

        return {
            "loss": loss.item(),
            "correct": (predicted == target).sum().item(),
            "num_samples": target.size(0)
        }

    def save(self, checkpoint_path):
        pass

    def restore(self, checkpoint_path):
        pass


class AverageMeter:
    """Computes and stores the average and current value."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count
