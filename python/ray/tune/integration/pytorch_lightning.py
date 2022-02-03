import logging
from typing import Dict, List, Optional, Union

from pytorch_lightning import Callback, Trainer, LightningModule
from ray import tune

import os

logger = logging.getLogger(__name__)


class TuneCallback(Callback):
    """Base class for Tune's PyTorch Lightning callbacks."""

    _allowed = [
        "init_start",
        "init_end",
        "fit_start",
        "fit_end",
        "sanity_check_start",
        "sanity_check_end",
        "epoch_start",
        "epoch_end",
        "batch_start",
        "validation_batch_start",
        "validation_batch_end",
        "test_batch_start",
        "test_batch_end",
        "batch_end",
        "train_start",
        "train_end",
        "validation_start",
        "validation_end",
        "test_start",
        "test_end",
        "keyboard_interrupt",
    ]

    def __init__(self, on: Union[str, List[str]] = "validation_end"):
        if not isinstance(on, list):
            on = [on]
        if any(w not in self._allowed for w in on):
            raise ValueError(
                "Invalid trigger time selected: {}. Must be one of {}".format(
                    on, self._allowed
                )
            )
        self._on = on

    def _handle(self, trainer: Trainer, pl_module: Optional[LightningModule]):
        raise NotImplementedError

    def on_init_start(self, trainer: Trainer):
        if "init_start" in self._on:
            self._handle(trainer, None)

    def on_init_end(self, trainer: Trainer):
        if "init_end" in self._on:
            self._handle(trainer, None)

    def on_fit_start(
        self, trainer: Trainer, pl_module: Optional[LightningModule] = None
    ):
        if "fit_start" in self._on:
            self._handle(trainer, None)

    def on_fit_end(self, trainer: Trainer, pl_module: Optional[LightningModule] = None):
        if "fit_end" in self._on:
            self._handle(trainer, None)

    def on_sanity_check_start(self, trainer: Trainer, pl_module: LightningModule):
        if "sanity_check_start" in self._on:
            self._handle(trainer, pl_module)

    def on_sanity_check_end(self, trainer: Trainer, pl_module: LightningModule):
        if "sanity_check_end" in self._on:
            self._handle(trainer, pl_module)

    def on_epoch_start(self, trainer: Trainer, pl_module: LightningModule):
        if "epoch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_epoch_end(self, trainer: Trainer, pl_module: LightningModule):
        if "epoch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_batch_start(self, trainer: Trainer, pl_module: LightningModule):
        if "batch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_batch_start(
        self,
        trainer: Trainer,
        pl_module: LightningModule,
        batch,
        batch_idx,
        dataloader_idx,
    ):
        if "validation_batch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_batch_end(
        self,
        trainer: Trainer,
        pl_module: LightningModule,
        outputs,
        batch,
        batch_idx,
        dataloader_idx,
    ):
        if "validation_batch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_test_batch_start(
        self,
        trainer: Trainer,
        pl_module: LightningModule,
        batch,
        batch_idx,
        dataloader_idx,
    ):
        if "test_batch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_test_batch_end(
        self,
        trainer: Trainer,
        pl_module: LightningModule,
        outputs,
        batch,
        batch_idx,
        dataloader_idx,
    ):
        if "test_batch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_batch_end(self, trainer: Trainer, pl_module: LightningModule):
        if "batch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_train_start(self, trainer: Trainer, pl_module: LightningModule):
        if "train_start" in self._on:
            self._handle(trainer, pl_module)

    def on_train_end(self, trainer: Trainer, pl_module: LightningModule):
        if "train_end" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_start(self, trainer: Trainer, pl_module: LightningModule):
        if "validation_start" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_end(self, trainer: Trainer, pl_module: LightningModule):
        if "validation_end" in self._on:
            self._handle(trainer, pl_module)

    def on_test_start(self, trainer: Trainer, pl_module: LightningModule):
        if "test_start" in self._on:
            self._handle(trainer, pl_module)

    def on_test_end(self, trainer: Trainer, pl_module: LightningModule):
        if "test_end" in self._on:
            self._handle(trainer, pl_module)

    def on_keyboard_interrupt(self, trainer: Trainer, pl_module: LightningModule):
        if "keyboard_interrupt" in self._on:
            self._handle(trainer, pl_module)


class TuneReportCallback(TuneCallback):
    """PyTorch Lightning to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to PyTorch Lightning,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to PyTorch Lightning.
        on (str|list): When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the ``on_``), e.g.
            "batch_start", or "train_end". Defaults to "validation_end".

    Example:

    .. code-block:: python

        import pytorch_lightning as pl
        from ray.tune.integration.pytorch_lightning import TuneReportCallback

        # Report loss and accuracy to Tune after each validation epoch:
        trainer = pl.Trainer(callbacks=[TuneReportCallback(
                ["val_loss", "val_acc"], on="validation_end")])

        # Same as above, but report as `loss` and `mean_accuracy`:
        trainer = pl.Trainer(callbacks=[TuneReportCallback(
                {"loss": "val_loss", "mean_accuracy": "val_acc"},
                on="validation_end")])

    """

    def __init__(
        self,
        metrics: Union[None, str, List[str], Dict[str, str]] = None,
        on: Union[str, List[str]] = "validation_end",
    ):
        super(TuneReportCallback, self).__init__(on)
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

    def _get_report_dict(self, trainer: Trainer, pl_module: LightningModule):
        # Don't report if just doing initial validation sanity checks.
        if trainer.sanity_checking:
            return
        if not self._metrics:
            report_dict = {k: v.item() for k, v in trainer.callback_metrics.items()}
        else:
            report_dict = {}
            for key in self._metrics:
                if isinstance(self._metrics, dict):
                    metric = self._metrics[key]
                else:
                    metric = key
                if metric in trainer.callback_metrics:
                    report_dict[key] = trainer.callback_metrics[metric].item()
                else:
                    logger.warning(
                        f"Metric {metric} does not exist in "
                        "`trainer.callback_metrics."
                    )

        return report_dict

    def _handle(self, trainer: Trainer, pl_module: LightningModule):
        report_dict = self._get_report_dict(trainer, pl_module)
        if report_dict is not None:
            tune.report(**report_dict)


class _TuneCheckpointCallback(TuneCallback):
    """PyTorch Lightning checkpoint callback

    Saves checkpoints after each validation step.

    Checkpoint are currently not registered if no ``tune.report()`` call
    is made afterwards. Consider using ``TuneReportCheckpointCallback``
    instead.

    Args:
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on (str|list): When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the ``on_``), e.g.
            "batch_start", or "train_end". Defaults to "validation_end".


    """

    def __init__(
        self, filename: str = "checkpoint", on: Union[str, List[str]] = "validation_end"
    ):
        super(_TuneCheckpointCallback, self).__init__(on)
        self._filename = filename

    def _handle(self, trainer: Trainer, pl_module: LightningModule):
        if trainer.sanity_checking:
            return
        step = f"epoch={trainer.current_epoch}-step={trainer.global_step}"
        with tune.checkpoint_dir(step=step) as checkpoint_dir:
            trainer.save_checkpoint(os.path.join(checkpoint_dir, self._filename))


class TuneReportCheckpointCallback(TuneCallback):
    """PyTorch Lightning report and checkpoint callback

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to PyTorch Lightning,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to PyTorch Lightning.
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on (str|list): When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the ``on_``), e.g.
            "batch_start", or "train_end". Defaults to "validation_end".


    Example:

    .. code-block:: python

        import pytorch_lightning as pl
        from ray.tune.integration.pytorch_lightning import (
            TuneReportCheckpointCallback)

        # Save checkpoint after each training batch and after each
        # validation epoch.
        trainer = pl.Trainer(callbacks=[TuneReportCheckpointCallback(
            metrics={"loss": "val_loss", "mean_accuracy": "val_acc"},
            filename="trainer.ckpt", on="validation_end")])


    """

    _checkpoint_callback_cls = _TuneCheckpointCallback
    _report_callbacks_cls = TuneReportCallback

    def __init__(
        self,
        metrics: Union[None, str, List[str], Dict[str, str]] = None,
        filename: str = "checkpoint",
        on: Union[str, List[str]] = "validation_end",
    ):
        super(TuneReportCheckpointCallback, self).__init__(on)
        self._checkpoint = self._checkpoint_callback_cls(filename, on)
        self._report = self._report_callbacks_cls(metrics, on)

    def _handle(self, trainer: Trainer, pl_module: LightningModule):
        self._checkpoint._handle(trainer, pl_module)
        self._report._handle(trainer, pl_module)
