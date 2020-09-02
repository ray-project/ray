from typing import Dict, List, Union

from tensorflow.keras.callbacks import Callback
from ray import tune

import os


class TuneCallback(Callback):
    """Base class for Tune's Keras callbacks."""
    _allowed = [
        "batch_begin",
        "batch_end",
        "epoch_begin",
        "epoch_end",
        "train_batch_begin",
        "train_batch_end",
        "test_batch_begin",
        "test_batch_end",
        "predict_batch_begin",
        "predict_batch_end",
        "train_begin",
        "train_end",
        "test_begin",
        "test_end",
        "predict_begin",
        "predict_end",
    ]

    def __init__(self, on: Union[str, List[str]] = "validation_end"):
        super(TuneCallback, self).__init__()

        if not isinstance(on, list):
            on = [on]
        if any(w not in self._allowed for w in on):
            raise ValueError(
                "Invalid trigger time selected: {}. Must be one of {}".format(
                    on, self._allowed))
        self._on = on

    def _handle(self, logs: Dict):
        raise NotImplementedError

    def on_batch_begin(self, batch, logs=None):
        if "batch_begin" in self._on:
            self._handle(logs)

    def on_batch_end(self, batch, logs=None):
        if "batch_end" in self._on:
            self._handle(logs)

    def on_epoch_begin(self, epoch, logs=None):
        if "epoch_begin" in self._on:
            self._handle(logs)

    def on_epoch_end(self, epoch, logs=None):
        if "epoch_end" in self._on:
            self._handle(logs)

    def on_train_batch_begin(self, batch, logs=None):
        if "train_batch_begin" in self._on:
            self._handle(logs)

    def on_train_batch_end(self, batch, logs=None):
        if "train_batch_end" in self._on:
            self._handle(logs)

    def on_test_batch_begin(self, batch, logs=None):
        if "test_batch_begin" in self._on:
            self._handle(logs)

    def on_test_batch_end(self, batch, logs=None):
        if "test_batch_end" in self._on:
            self._handle(logs)

    def on_predict_batch_begin(self, batch, logs=None):
        if "predict_batch_begin" in self._on:
            self._handle(logs)

    def on_predict_batch_end(self, batch, logs=None):
        if "predict_batch_end" in self._on:
            self._handle(logs)

    def on_train_begin(self, logs=None):
        if "train_begin" in self._on:
            self._handle(logs)

    def on_train_end(self, logs=None):
        if "train_end" in self._on:
            self._handle(logs)

    def on_test_begin(self, logs=None):
        if "test_begin" in self._on:
            self._handle(logs)

    def on_test_end(self, logs=None):
        if "test_end" in self._on:
            self._handle(logs)

    def on_predict_begin(self, logs=None):
        if "predict_begin" in self._on:
            self._handle(logs)

    def on_predict_end(self, logs=None):
        if "predict_end" in self._on:
            self._handle(logs)


class TuneReportCallback(TuneCallback):
    """Keras to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to Keras,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to Keras. If this is None,
            all Keras logs will be reported.
        on (str|list): When to trigger checkpoint creations. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start", or "predict_end". Defaults to "epoch_end".

    Example:

    .. code-block:: python

        from ray.tune.integration.keras import TuneReportCallback

        # Report accuracy to Tune after each epoch:
        model.fit(
            x_train,
            y_train,
            batch_size=batch_size,
            epochs=epochs,
            verbose=0,
            validation_data=(x_test, y_test),
            callbacks=[TuneReportCallback(
                {"mean_accuracy": "accuracy"}, on="epoch_end")])

    """

    def __init__(self,
                 metrics: Union[None, str, List[str], Dict[str, str]] = None,
                 on: Union[str, List[str]] = "epoch_end"):
        super(TuneReportCallback, self).__init__(on)
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

    def _handle(self, logs: Dict):
        if not self._metrics:
            report_dict = logs
        else:
            report_dict = {}
            for key in self._metrics:
                if isinstance(self._metrics, dict):
                    metric = self._metrics[key]
                else:
                    metric = key
                report_dict[key] = logs[metric]
        tune.report(**report_dict)


class _TuneCheckpointCallback(TuneCallback):
    """Keras checkpoint callback

    Saves checkpoints after each validation step.

    Checkpoint are currently not registered if no ``tune.report()`` call
    is made afterwards. Consider using ``TuneReportCheckpointCallback``
    instead.

    Args:
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on (str|list): When to trigger checkpoint creations. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start", or "predict_end". Defaults to "epoch_end".


    """

    def __init__(self,
                 filename: str = "checkpoint",
                 on: Union[str, List[str]] = "epoch_end"):
        super(_TuneCheckpointCallback, self).__init__(on)
        self._filename = filename
        self._epoch = 0

    def _handle(self, logs: Dict):
        with tune.checkpoint_dir(step=self._epoch) as checkpoint_dir:
            self.model.save(
                os.path.join(checkpoint_dir, self._filename), overwrite=True)

    def on_epoch_begin(self, epoch, logs=None):
        self._epoch = epoch
        super(_TuneCheckpointCallback, self).on_epoch_begin(epoch, logs)


class TuneReportCheckpointCallback(TuneCallback):
    """Keras report and checkpoint callback

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to Keras,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to Keras. If this is None,
            all Keras logs will be reported.
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        on (str|list): When to trigger checkpoint creations. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start", or "predict_end". Defaults to "epoch_end".


    Example:

    .. code-block:: python

        from ray.tune.integration.keras import TuneReportCheckpointCallback

        # Save checkpoint and report accuracy to Tune after each epoch:
        model.fit(
            x_train,
            y_train,
            batch_size=batch_size,
            epochs=epochs,
            verbose=0,
            validation_data=(x_test, y_test),
            callbacks=[TuneReportCheckpointCallback(
                metrics={"mean_accuracy": "accuracy"},
                filename="model",
                on="epoch_end")])


    """

    def __init__(self,
                 metrics: Union[None, str, List[str], Dict[str, str]] = None,
                 filename: str = "checkpoint",
                 on: Union[str, List[str]] = "epoch_end"):
        super(TuneReportCheckpointCallback, self).__init__(on)
        self._checkpoint = _TuneCheckpointCallback(filename, on)
        self._report = TuneReportCallback(metrics, on)

    def _handle(self, logs: Dict):
        self._checkpoint._handle(logs)
        self._report._handle(logs)

    def on_epoch_begin(self, epoch, logs=None):
        # Pass through for the checkpoint callback to register epoch
        self._checkpoint.on_epoch_begin(epoch, logs)
        self._report.on_epoch_begin(epoch, logs)

    def set_model(self, model):
        # Pass through for the checkpoint callback to set model
        self._checkpoint.set_model(model)
        self._report.set_model(model)
