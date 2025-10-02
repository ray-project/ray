from typing import Dict

import ray.tune
from ray.air.integrations.keras import RayReportCallback
from ray.train.tensorflow import TensorflowCheckpoint
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


@PublicAPI(stability="beta")
class TuneReportCheckpointCallback(RayReportCallback):
    """Keras callback to save checkpoints and report metrics for Ray Tune.

    Args:
        metrics: Metrics to report. If this is a list,
            each item describes the metric key reported to XGBoost,
            and it will be reported under the same name.
            This can also be a dict of {<key-to-report>: <keras-metric-key>},
            which can be used to rename xgboost default metrics.
        filename: Customize the saved checkpoint file type by passing
            a filename. Defaults to "model.ubj".
        frequency: How often to save checkpoints, in terms of iterations.
            Defaults to 0 (no checkpoints are saved during training).
        checkpoint_at_end: Whether or not to save a checkpoint at the end of training.
        results_postprocessing_fn: An optional Callable that takes in
            the metrics dict that will be reported (after it has been flattened)
            and returns a modified dict. For example, this can be used to
            average results across CV fold when using ``xgboost.cv``.

    Examples
    --------

    Reporting checkpoints and metrics to Ray Tune when running many
    independent xgboost trials (without data parallelism within a trial).

    .. testcode::
        :skipif: True

        import xgboost

        from ray.tune import Tuner
        from ray.tune.integration.xgboost import TuneReportCheckpointCallback

        def train_fn(config):
            # Report log loss to Ray Tune after each validation epoch.
            bst = xgboost.train(
                ...,
                callbacks=[
                    TuneReportCheckpointCallback(
                        metrics={"loss": "eval-logloss"}, frequency=1
                    )
                ],
            )

        tuner = Tuner(train_fn)
        results = tuner.fit()
    """


    # @contextmanager
    # def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
    #     with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
    #         model.save_model(Path(temp_checkpoint_dir, self._filename).as_posix())
    #         yield Checkpoint(temp_checkpoint_dir)

    def _save_and_report_checkpoint(self, report_dict: Dict, model: TensorflowCheckpoint):
        ray.tune.report(report_dict, checkpoint=model)

    def _report_metrics(self, report_dict: Dict):
        ray.tune.report(report_dict)
