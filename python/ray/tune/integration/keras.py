from collections import Counter
from typing import Dict, List, Union, Optional

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
                    on, self._allowed
                )
            )
        self._on = on

    def _handle(self, logs: Dict, when: str):
        raise NotImplementedError

    def on_batch_begin(self, batch, logs=None):
        if "batch_begin" in self._on:
            self._handle(logs, "batch_begin")

    def on_batch_end(self, batch, logs=None):
        if "batch_end" in self._on:
            self._handle(logs, "batch_end")

    def on_epoch_begin(self, epoch, logs=None):
        if "epoch_begin" in self._on:
            self._handle(logs, "epoch_begin")

    def on_epoch_end(self, epoch, logs=None):
        if "epoch_end" in self._on:
            self._handle(logs, "epoch_end")

    def on_train_batch_begin(self, batch, logs=None):
        if "train_batch_begin" in self._on:
            self._handle(logs, "train_batch_begin")

    def on_train_batch_end(self, batch, logs=None):
        if "train_batch_end" in self._on:
            self._handle(logs, "train_batch_end")

    def on_test_batch_begin(self, batch, logs=None):
        if "test_batch_begin" in self._on:
            self._handle(logs, "test_batch_begin")

    def on_test_batch_end(self, batch, logs=None):
        if "test_batch_end" in self._on:
            self._handle(logs, "test_batch_end")

    def on_predict_batch_begin(self, batch, logs=None):
        if "predict_batch_begin" in self._on:
            self._handle(logs, "predict_batch_begin")

    def on_predict_batch_end(self, batch, logs=None):
        if "predict_batch_end" in self._on:
            self._handle(logs, "predict_batch_end")

    def on_train_begin(self, logs=None):
        if "train_begin" in self._on:
            self._handle(logs, "train_begin")

    def on_train_end(self, logs=None):
        if "train_end" in self._on:
            self._handle(logs, "train_end")

    def on_test_begin(self, logs=None):
        if "test_begin" in self._on:
            self._handle(logs, "test_begin")

    def on_test_end(self, logs=None):
        if "test_end" in self._on:
            self._handle(logs, "test_end")

    def on_predict_begin(self, logs=None):
        if "predict_begin" in self._on:
            self._handle(logs, "predict_begin")

    def on_predict_end(self, logs=None):
        if "predict_end" in self._on:
            self._handle(logs, "predict_end")


class TuneReportCallback(TuneCallback):
    """Keras to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to Keras,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to Keras. If this is None,
            all Keras logs will be reported.
        on: When to trigger checkpoint creations. Must be one of
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

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        on: Union[str, List[str]] = "epoch_end",
    ):
        super(TuneReportCallback, self).__init__(on)
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

    def _handle(self, logs: Dict, when: str = None):
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
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        frequency: Checkpoint frequency. If this is an integer `n`,
            checkpoints are saved every `n` times each hook was called. If
            this is a list, it specifies the checkpoint frequencies for each
            hook individually.
        on: When to trigger checkpoint creations. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start", or "predict_end". Defaults to "epoch_end".


    """

    def __init__(
        self,
        filename: str = "checkpoint",
        frequency: Union[int, List[int]] = 1,
        on: Union[str, List[str]] = "epoch_end",
    ):

        if isinstance(frequency, list):
            if not isinstance(on, list) or len(frequency) != len(on):
                raise ValueError(
                    "If you pass a list for checkpoint frequencies, the `on` "
                    "parameter has to be a list with the same length."
                )

        self._frequency = frequency

        super(_TuneCheckpointCallback, self).__init__(on)

        self._filename = filename
        self._counter = Counter()
        self._cp_count = 0  # Has to be monotonically increasing

    def _handle(self, logs: Dict, when: str = None):
        self._counter[when] += 1

        if isinstance(self._frequency, list):
            index = self._on.index(when)
            freq = self._frequency[index]
        else:
            freq = self._frequency

        if self._counter[when] % freq == 0:
            with tune.checkpoint_dir(step=self._cp_count) as checkpoint_dir:
                self.model.save(
                    os.path.join(checkpoint_dir, self._filename), overwrite=True
                )
                self._cp_count += 1


class TuneReportCheckpointCallback(TuneCallback):
    """Keras report and checkpoint callback

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Use this callback to register saved checkpoints with Ray Tune. This means
    that checkpoints will be manages by the `CheckpointManager` and can be
    used for advanced scheduling and search  algorithms, like
    Population Based Training.

    The ``tf.keras.callbacks.ModelCheckpoint`` callback also saves checkpoints,
    but doesn't register them with Ray Tune.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to Keras,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to Keras. If this is None,
            all Keras logs will be reported.
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        frequency: Checkpoint frequency. If this is an integer `n`,
            checkpoints are saved every `n` times each hook was called. If
            this is a list, it specifies the checkpoint frequencies for each
            hook individually.
        on: When to trigger checkpoint creations. Must be one of
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

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = "checkpoint",
        frequency: Union[int, List[int]] = 1,
        on: Union[str, List[str]] = "epoch_end",
    ):
        super(TuneReportCheckpointCallback, self).__init__(on)
        self._checkpoint = _TuneCheckpointCallback(filename, frequency, on)
        self._report = TuneReportCallback(metrics, on)

    def _handle(self, logs: Dict, when: str = None):
        self._checkpoint._handle(logs, when)
        self._report._handle(logs, when)

    def set_model(self, model):
        # Pass through for the checkpoint callback to set model
        self._checkpoint.set_model(model)
        self._report.set_model(model)
