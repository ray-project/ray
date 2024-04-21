import inspect
import logging
import os
import tempfile
import warnings
from contextlib import contextmanager
from typing import Dict, List, Optional, Type, Union

from ray import train
from ray.train import Checkpoint
from ray.util import log_once
from ray.util.annotations import Deprecated, PublicAPI

try:
    from lightning import Callback, LightningModule, Trainer
except ModuleNotFoundError:
    from pytorch_lightning import Callback, LightningModule, Trainer


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
        save_checkpoints: If True (default), checkpoints will be saved and
            reported to Ray. If False, only metrics will be reported.
        on: When to trigger checkpoint creations and metric reports. Must be one of
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

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = "checkpoint",
        save_checkpoints: bool = True,
        on: Union[str, List[str]] = "validation_end",
    ):
        super(TuneReportCheckpointCallback, self).__init__(on=on)
        if isinstance(metrics, str):
            metrics = [metrics]
        self._save_checkpoints = save_checkpoints
        self._filename = filename
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

    @contextmanager
    def _get_checkpoint(self, trainer: Trainer) -> Optional[Checkpoint]:
        if not self._save_checkpoints:
            yield None
            return

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            trainer.save_checkpoint(os.path.join(checkpoint_dir, self._filename))
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            yield checkpoint

    def _handle(self, trainer: Trainer, pl_module: LightningModule):
        if trainer.sanity_checking:
            return

        report_dict = self._get_report_dict(trainer, pl_module)
        if not report_dict:
            return

        with self._get_checkpoint(trainer) as checkpoint:
            train.report(report_dict, checkpoint=checkpoint)


class _TuneCheckpointCallback(TuneCallback):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "`ray.tune.integration.pytorch_lightning._TuneCheckpointCallback` "
            "is deprecated."
        )


@Deprecated
class TuneReportCallback(TuneReportCheckpointCallback):
    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        on: Union[str, List[str]] = "validation_end",
    ):
        if log_once("tune_ptl_report_deprecated"):
            warnings.warn(
                "`ray.tune.integration.pytorch_lightning.TuneReportCallback` "
                "is deprecated. Use "
                "`ray.tune.integration.pytorch_lightning.TuneReportCheckpointCallback`"
                " instead."
            )
        super(TuneReportCallback, self).__init__(
            metrics=metrics, save_checkpoints=False, on=on
        )
