import os
from collections import Counter
from typing import Dict, List, Optional, Union

from ray import tune
from ray.util.annotations import Deprecated
from ray.air.integrations.keras import _Callback as TuneCallback

_DEPRECATION_MESSAGE = (
    "The `ray.tune.integration.keras` module is deprecated in favor of "
    "`ray.air.integrations.keras.ReportCheckpointCallback` and will be removed "
    "in Ray 2.7."
)


@Deprecated(message=_DEPRECATION_MESSAGE)
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


@Deprecated(message=_DEPRECATION_MESSAGE)
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


@Deprecated(message=_DEPRECATION_MESSAGE)
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
