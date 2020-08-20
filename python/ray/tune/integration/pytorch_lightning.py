from pytorch_lightning import Callback
from ray import tune

import os


class TuneCallback(Callback):
    """Base class for Tune's PyTorch Lightning callbacks."""
    _allowed = [
        "init_start", "init_end", "fit_start", "fit_end", "sanity_check_start",
        "sanity_check_end", "epoch_start", "epoch_end", "batch_start",
        "validation_batch_start", "validation_batch_end", "test_batch_start",
        "test_batch_end", "batch_end", "train_start", "train_end",
        "validation_start", "validation_end", "test_start", "test_end",
        "keyboard_interrupt"
    ]

    def __init__(self, on="validation"):
        if not isinstance(on, list):
            on = [on]
        if any(w not in self._allowed for w in on):
            raise ValueError(
                "Invalid trigger time selected: {}. Must be one of {}".format(
                    on, self._allowed))
        self._on = on

    def _handle(self, trainer, pl_module):
        raise NotImplementedError

    def on_init_start(self, trainer):
        if "init_start" in self._on:
            self._handle(trainer, None)

    def on_init_end(self, trainer):
        if "init_end" in self._on:
            self._handle(trainer, None)

    def on_fit_start(self, trainer):
        if "fit_start" in self._on:
            self._handle(trainer, None)

    def on_fit_end(self, trainer):
        if "fit_end" in self._on:
            self._handle(trainer, None)

    def on_sanity_check_start(self, trainer, pl_module):
        if "sanity_check_start" in self._on:
            self._handle(trainer, pl_module)

    def on_sanity_check_end(self, trainer, pl_module):
        if "sanity_check_end" in self._on:
            self._handle(trainer, pl_module)

    def on_epoch_start(self, trainer, pl_module):
        if "epoch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_epoch_end(self, trainer, pl_module):
        if "epoch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_batch_start(self, trainer, pl_module):
        if "batch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_batch_start(self, trainer, pl_module):
        if "validation_batch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_batch_end(self, trainer, pl_module):
        if "validation_batch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_test_batch_start(self, trainer, pl_module):
        if "test_batch_start" in self._on:
            self._handle(trainer, pl_module)

    def on_test_batch_end(self, trainer, pl_module):
        if "test_batch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_batch_end(self, trainer, pl_module):
        if "batch_end" in self._on:
            self._handle(trainer, pl_module)

    def on_train_start(self, trainer, pl_module):
        if "train_start" in self._on:
            self._handle(trainer, pl_module)

    def on_train_end(self, trainer, pl_module):
        if "train_end" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_start(self, trainer, pl_module):
        if "validation_start" in self._on:
            self._handle(trainer, pl_module)

    def on_validation_end(self, trainer, pl_module):
        if "validation_end" in self._on:
            self._handle(trainer, pl_module)

    def on_test_start(self, trainer, pl_module):
        if "test_start" in self._on:
            self._handle(trainer, pl_module)

    def on_test_end(self, trainer, pl_module):
        if "test_end" in self._on:
            self._handle(trainer, pl_module)

    def on_keyboard_interrupt(self, trainer, pl_module):
        if "keyboard_interrupt" in self._on:
            self._handle(trainer, pl_module)


class ReportCallback(TuneCallback):
    """PyTorch Lightning to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to PyTorch Lightning,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to PyTorch Lightning.
        on (str|list): When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the "on_"), e.g.
            "batch_start", or "train_end". Defaults to "validation_end".

    Example:

    .. code-block:: python

        import pytorch_lightning as pl
        from ray.tune.integration.pytorch_lightning import ReportCallback

        trainer = pl.Trainer(
            callbacks=[TuneReportCallback(["loss", "accuracy"])])


    """

    def __init__(self, metrics, on="validation_end"):
        super(ReportCallback, self).__init__(on)
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

    def _handle(self, trainer, pl_module):
        report_dict = {}
        for key in self._metrics:
            if isinstance(self._metrics, dict):
                metric = self._metrics[key]
            else:
                metric = key
            report_dict[key] = trainer.callback_metrics[metric].item()
        tune.report(**report_dict)


class CheckpointCallback(TuneCallback):
    """PyTorch Lightning checkpoint callback

    Saves checkpoints after each validation step.

    Args:
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on (str|list): When to trigger checkpoint creations. Must be one of
            the PyTorch Lightning event hooks (less the "on_"), e.g.
            "batch_start", or "train_end". Defaults to "validation_end".


    Example:

    .. code-block:: python

        import pytorch_lightning as pl
        from ray.tune.integration.pytorch_lightning import CheckpointCallback

        trainer = pl.Trainer(
            callbacks=[CheckpointCallback("trainer.ckpt", "validation_end")])


    """

    def __init__(self, filename="checkpoint", on="validation_end"):
        super(CheckpointCallback, self).__init__(on)
        self._filename = filename

    def _handle(self, trainer, pl_module):
        with tune.checkpoint_dir(step=trainer.global_step) as checkpoint_dir:
            trainer.save_checkpoint(
                os.path.join(checkpoint_dir, self._filename))
