import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

from lightgbm import Booster

import ray.tune
from ray.train.lightgbm._lightgbm_utils import RayReportCallback
from ray.tune import Checkpoint
from ray.util.annotations import Deprecated, PublicAPI


@PublicAPI(stability="beta")
class TuneReportCheckpointCallback(RayReportCallback):
    """Creates a callback that reports metrics and checkpoints model.

    Args:
        metrics: Metrics to report. If this is a list,
            each item should be a metric key reported by LightGBM,
            and it will be reported to Ray Train/Tune under the same name.
            This can also be a dict of {<key-to-report>: <lightgbm-metric-key>},
            which can be used to rename LightGBM default metrics.
        filename: Customize the saved checkpoint file type by passing
            a filename. Defaults to "model.txt".
        frequency: How often to save checkpoints, in terms of iterations.
            Defaults to 0 (no checkpoints are saved during training).
        checkpoint_at_end: Whether or not to save a checkpoint at the end of training.
        results_postprocessing_fn: An optional Callable that takes in
            the metrics dict that will be reported (after it has been flattened)
            and returns a modified dict.

    Examples
    --------

    Reporting checkpoints and metrics to Ray Tune when running many
    independent LightGBM trials (without data parallelism within a trial).

    .. testcode::
        :skipif: True

        import lightgbm

        from ray.tune.integration.lightgbm import TuneReportCheckpointCallback

        config = {
            # ...
            "metric": ["binary_logloss", "binary_error"],
        }

        # Report only log loss to Tune after each validation epoch.
        bst = lightgbm.train(
            ...,
            callbacks=[
                TuneReportCheckpointCallback(
                    metrics={"loss": "eval-binary_logloss"}, frequency=1
                )
            ],
        )

    """

    @contextmanager
    def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            model.save_model(Path(temp_checkpoint_dir, self._filename).as_posix())
            yield Checkpoint.from_directory(temp_checkpoint_dir)

    def _save_and_report_checkpoint(self, report_dict: Dict, model: Booster):
        with self._get_checkpoint(model=model) as checkpoint:
            ray.tune.report(report_dict, checkpoint=checkpoint)

    def _report_metrics(self, report_dict: Dict):
        ray.tune.report(report_dict)


@Deprecated
class TuneReportCallback:
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove in 2.11.
        raise DeprecationWarning(
            "`TuneReportCallback` is deprecated. "
            "Use `ray.tune.integration.lightgbm.TuneReportCheckpointCallback` instead."
        )
