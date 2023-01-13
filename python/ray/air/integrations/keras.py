from collections import Counter
from typing import Dict, List, Optional, Union
import warnings

from tensorflow.keras.callbacks import Callback as KerasCallback

from ray.air import session
from ray.train.tensorflow import TensorflowCheckpoint
from ray.util.annotations import PublicAPI, Deprecated


class _Callback(KerasCallback):
    """Base class for Air's Keras callbacks."""

    _allowed = [
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
        super(_Callback, self).__init__()

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


@PublicAPI(stability="alpha")
class ReportCheckpointCallback(_Callback):
    """Keras callback for Ray AIR reporting and checkpointing.

    .. note::
        Metrics are always reported with checkpoints, even if the event isn't specified
        in ``report_metrics_on``.

    Example:
        .. code-block: python

            ############# Using it in TrainSession ###############
            from ray.air.integrations.keras import Callback
            def train_loop_per_worker():
                strategy = tf.distribute.MultiWorkerMirroredStrategy()
                with strategy.scope():
                    model = build_model()
                    #model.compile(...)
                model.fit(dataset_shard, callbacks=[Callback()])

    Args:
        metrics: Metrics to report. If this is a list, each item describes
            the metric key reported to Keras, and it's reported under the
            same name. If this is a dict, each key is the name reported
            and the respective value is the metric key reported to Keras.
            If this is None, all Keras logs are reported.
        report_metrics_on: When to report metrics. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start" or "predict_end". Defaults to "epoch_end".
        checkpoint_on: When to save checkpoints. Must be one of the Keras event hooks
            (less the ``on_``), e.g. "train_start" or "predict_end". Defaults to
            "epoch_end".
    """

    def __init__(
        self,
        checkpoint_on: Union[str, List[str]] = "epoch_end",
        report_metrics_on: Union[str, List[str]] = "epoch_end",
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
    ):
        if isinstance(checkpoint_on, str):
            checkpoint_on = [checkpoint_on]
        if isinstance(report_metrics_on, str):
            report_metrics_on = [report_metrics_on]

        on = list(set(checkpoint_on + report_metrics_on))
        super().__init__(on=on)

        self._checkpoint_on: List[str] = checkpoint_on
        self._report_metrics_on: List[str] = report_metrics_on
        self._metrics = metrics

    def _handle(self, logs: Dict, when: str):
        assert when in self._checkpoint_on or when in self._report_metrics_on

        metrics = self._get_reported_metrics(logs)

        if when in self._checkpoint_on:
            checkpoint = TensorflowCheckpoint.from_model(self.model)
        else:
            checkpoint = None

        session.report(metrics, checkpoint=checkpoint)

    def _get_reported_metrics(self, logs: Dict) -> Dict:
        assert isinstance(self._metrics, (type(None), str, list, dict))

        if self._metrics is None:
            reported_metrics = logs
        elif isinstance(self._metrics, str):
            reported_metrics = {self._metrics: logs[self._metrics]}
        elif isinstance(self._metrics, list):
            reported_metrics = {metric: logs[metric] for metric in self._metrics}
        elif isinstance(self._metrics, dict):
            reported_metrics = {
                key: logs[metric] for key, metric in self._metrics.items()
            }

        assert isinstance(reported_metrics, dict)
        return reported_metrics


@Deprecated
class Callback(_Callback):
    """
    Keras callback for Ray AIR reporting and checkpointing.

    You can use this in both TuneSession and TrainSession.

    Example:
        .. code-block: python

            ############# Using it in TrainSession ###############
            from ray.air.integrations.keras import Callback
            def train_loop_per_worker():
                strategy = tf.distribute.MultiWorkerMirroredStrategy()
                with strategy.scope():
                    model = build_model()
                    #model.compile(...)
                model.fit(dataset_shard, callbacks=[Callback()])

    Args:
        metrics: Metrics to report. If this is a list, each item describes
            the metric key reported to Keras, and it will reported under the
            same name. If this is a dict, each key will be the name reported
            and the respective value will be the metric key reported to Keras.
            If this is None, all Keras logs will be reported.
        on: When to report metrics. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start", or "predict_end". Defaults to "epoch_end".
        frequency: Checkpoint frequency. If this is an integer `n`,
            checkpoints are saved every `n` times each hook was called. If
            this is a list, it specifies the checkpoint frequencies for each
            hook individually.

    """

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        on: Union[str, List[str]] = "epoch_end",
        frequency: Union[int, List[int]] = 1,
    ):
        warnings.warn(
            "`ray.air.integrations.keras.Callback` is deprecated. Use "
            "`ray.air.integrations.keras.ReportCheckpointCallback` instead.",
            DeprecationWarning,
        )

        if isinstance(frequency, list):
            if not isinstance(on, list) or len(frequency) != len(on):
                raise ValueError(
                    "If you pass a list for checkpoint frequencies, the `on` "
                    "parameter has to be a list with the same length."
                )

        self._frequency = frequency
        super(Callback, self).__init__(on)
        self._metrics = metrics
        self._counter = Counter()

    def _handle(self, logs: Dict, when: str = None):
        self._counter[when] += 1

        if isinstance(self._frequency, list):
            index = self._on.index(when)
            freq = self._frequency[index]
        else:
            freq = self._frequency

        checkpoint = None
        if freq > 0 and self._counter[when] % freq == 0:
            checkpoint = TensorflowCheckpoint.from_model(self.model)

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

        session.report(report_dict, checkpoint=checkpoint)
