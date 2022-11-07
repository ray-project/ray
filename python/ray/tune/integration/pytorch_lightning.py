import inspect
import logging
from typing import Dict, List, Optional, Type, Union

from pytorch_lightning import Callback, Trainer, LightningModule
from ray import tune
from ray.util import PublicAPI

import os

logger = logging.getLogger(__name__)

# Get all Pytorch Lightning Callback hooks based on whatever PTL version is being used.
_allowed_hooks = {
    name
    for name, fn in inspect.getmembers(Callback, predicate=inspect.isfunction)
    if name.startswith("on_")
}


def _override_ptl_hooks(callback_cls: Type["TuneCallback"]) -> Type["TuneCallback"]:
    """Overrides all allowed PTL Callback hooks with our custom handle logic."""

    def generate_overridden_hook(fn_name):
        def overridden_hook(
            self,
            trainer: Trainer,
            *args,
            pl_module: Optional[LightningModule] = None,
            **kwargs,
        ):
            if fn_name in self._on:
                self._handle(trainer=trainer, pl_module=pl_module)

        return overridden_hook

    # Set the overridden hook to all the allowed hooks in TuneCallback.
    for fn_name in _allowed_hooks:
        setattr(callback_cls, fn_name, generate_overridden_hook(fn_name))

    return callback_cls


@_override_ptl_hooks
class TuneCallback(Callback):
    """Base class for Tune's PyTorch Lightning callbacks.

    Args:
        When to trigger checkpoint creations. Must be one of
        the PyTorch Lightning event hooks (less the ``on_``), e.g.
        "train_batch_start", or "train_end". Defaults to "validation_end"
    """

    def __init__(self, on: Union[str, List[str]] = "validation_end"):
        if not isinstance(on, list):
            on = [on]

        for hook in on:
            if f"on_{hook}" not in _allowed_hooks:
                raise ValueError(
                    f"Invalid hook selected: {hook}. Must be one of "
                    f"{_allowed_hooks}"
                )

        # Add back the "on_" prefix for internal consistency.
        on = [f"on_{hook}" for hook in on]

        self._on = on

    def _handle(self, trainer: Trainer, pl_module: Optional[LightningModule]):
        raise NotImplementedError


@PublicAPI
class TuneReportCallback(TuneCallback):
    """PyTorch Lightning to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to PyTorch Lightning,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to PyTorch Lightning.
        on: When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the ``on_``), e.g.
            "train_batch_start", or "train_end". Defaults to "validation_end".

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
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        on: Union[str, List[str]] = "validation_end",
    ):
        super(TuneReportCallback, self).__init__(on=on)
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
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on: When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the ``on_``), e.g.
            "train_batch_start", or "train_end". Defaults to "validation_end".


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


@PublicAPI
class TuneReportCheckpointCallback(TuneCallback):
    """PyTorch Lightning report and checkpoint callback

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to PyTorch Lightning,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to PyTorch Lightning.
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on: When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the ``on_``), e.g.
            "train_batch_start", or "train_end". Defaults to "validation_end".


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
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = "checkpoint",
        on: Union[str, List[str]] = "validation_end",
    ):
        super(TuneReportCheckpointCallback, self).__init__(on)
        self._checkpoint = self._checkpoint_callback_cls(filename, on)
        self._report = self._report_callbacks_cls(metrics, on)

    def _handle(self, trainer: Trainer, pl_module: LightningModule):
        self._checkpoint._handle(trainer, pl_module)
        self._report._handle(trainer, pl_module)
