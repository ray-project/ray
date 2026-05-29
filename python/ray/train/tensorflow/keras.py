import shutil
from abc import abstractmethod
from typing import Dict, List, Optional, Union

from tensorflow.keras.callbacks import Callback as KerasCallback

import ray
from ray.train.tensorflow import TensorflowCheckpoint
from ray.util.annotations import PublicAPI


class _Callback(KerasCallback):
    """Base class for Ray Train's Keras callbacks."""

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


class RayReportCallback(_Callback):
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

    @abstractmethod
    def _save_and_report_checkpoint(
        self, metrics: Dict, checkpoint: TensorflowCheckpoint
    ):
        """Save checkpoint and report metrics corresonding to this checkpoint."""
        raise NotImplementedError

    @abstractmethod
    def _report_metrics(self, metrics: Dict):
        """Report metrics."""
        raise NotImplementedError

    def _handle(self, logs: Dict, when: str):
        assert when in self._checkpoint_on or when in self._report_metrics_on

        metrics = self._get_reported_metrics(logs)

        should_checkpoint = when in self._checkpoint_on
        if should_checkpoint:
            checkpoint = TensorflowCheckpoint.from_model(self.model)
            self._save_and_report_checkpoint(metrics, checkpoint)
            # Clean up temporary checkpoint
            shutil.rmtree(checkpoint.path, ignore_errors=True)
        else:
            self._report_metrics(metrics)


@PublicAPI(stability="alpha")
class ReportCheckpointCallback(RayReportCallback):
    """Keras callback for Ray Train reporting and checkpointing.

    .. note::
        Metrics are always reported with checkpoints, even if the event isn't specified
        in ``report_metrics_on``.

    Example:
        .. testcode:: python

            ############# Using it in TrainSession ###############
            from ray.air.integrations.keras import ReportCheckpointCallback
            def train_loop_per_worker():
                strategy = tf.distribute.MultiWorkerMirroredStrategy()
                with strategy.scope():
                    model = build_model()

                model.fit(dataset_shard, callbacks=[ReportCheckpointCallback()])

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

    def _save_and_report_checkpoint(
        self, metrics: Dict, checkpoint: TensorflowCheckpoint
    ):
        """Save checkpoint and report metrics corresonding to this checkpoint."""
        ray.train.report(metrics, checkpoint=checkpoint)

    def _report_metrics(self, metrics: Dict):
        """Report metrics."""
        ray.train.report(metrics, checkpoint=None)
