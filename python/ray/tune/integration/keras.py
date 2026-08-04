from typing import Dict

import ray.tune
from ray.train.tensorflow import TensorflowCheckpoint
from ray.train.tensorflow.keras import RayReportCallback
from ray.util.annotations import PublicAPI

_DEPRECATION_MESSAGE = (
    "The `ray.tune.integration.keras` module is deprecated in favor of "
    "`ray.train.tensorflow.keras.ReportCheckpointCallback`."
)


class TuneReportCallback:
    """Deprecated.
    Use :class:`ray.train.tensorflow.keras.ReportCheckpointCallback` instead."""

    def __new__(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)


class _TuneCheckpointCallback:
    """Deprecated.
    Use :class:`ray.train.tensorflow.keras.ReportCheckpointCallback` instead."""

    def __new__(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)


@PublicAPI(stability="alpha")
class TuneReportCheckpointCallback(RayReportCallback):
    """Keras callback for Ray Tune reporting and checkpointing.

    .. note::
        Metrics are always reported with checkpoints, even if the event isn't specified
        in ``report_metrics_on``.

    Example:
        .. code-block:: python

            ############# Using it in Ray Tune ###############
            from ray.tune.integrations.keras import TuneReportCheckpointCallback

            def train_fn():
                model = build_model()
                model.fit(dataset_shard, callbacks=[TuneReportCheckpointCallback()])

            tuner = tune.Tuner(train_fn)
            results = tuner.fit()

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
        ray.tune.report(metrics, checkpoint=checkpoint)

    def _report_metrics(self, metrics: Dict):
        ray.tune.report(metrics)
