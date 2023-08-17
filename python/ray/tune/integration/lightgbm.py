from contextlib import contextmanager
from typing import Callable, Dict, List, Union, Optional

import os
import tempfile
import warnings

from ray import train

from ray.air.checkpoint import Checkpoint as LegacyCheckpoint
from ray.train._checkpoint import Checkpoint
from ray.train._internal.storage import _use_storage_context
from ray.tune.utils import flatten_dict
from ray.util import log_once

from lightgbm.callback import CallbackEnv
from lightgbm.basic import Booster


class TuneCallback:
    """Base class for Tune's LightGBM callbacks."""

    pass


class TuneReportCheckpointCallback(TuneCallback):
    """Creates a callback that reports metrics and checkpoints model.

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to LightGBM,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to LightGBM.
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint". If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from LightGBM.
        frequency: How often to save checkpoints. Per default, a
            checkpoint is saved every five iterations.
        results_postprocessing_fn: An optional Callable that takes in
            the dict that will be reported to Tune (after it has been flattened)
            and returns a modified dict that will be reported instead.

    Example:

    .. code-block:: python

        import lightgbm
        from ray.tune.integration.lightgbm import (
            TuneReportCheckpointCallback
        )

        config = {
            # ...
            "metric": ["binary_logloss", "binary_error"],
        }

        # Report only log loss to Tune after each validation epoch.
        # Save model as `lightgbm.mdl`.
        bst = lightgbm.train(
            config,
            train_set,
            valid_sets=[test_set],
            valid_names=["eval"],
            verbose_eval=False,
            callbacks=[TuneReportCheckpointCallback(
                {"loss": "eval-binary_logloss"}, "lightgbm.mdl)])

    """

    order = 20

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = "checkpoint",
        frequency: int = 5,
        results_postprocessing_fn: Optional[
            Callable[[Dict[str, Union[float, List[float]]]], Dict[str, float]]
        ] = None,
    ):
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics
        self._filename = filename
        self._frequency = frequency
        self._results_postprocessing_fn = results_postprocessing_fn

    def _get_report_dict(self, evals_log: Dict[str, Dict[str, list]]) -> dict:
        result_dict = flatten_dict(evals_log, delimiter="-")
        if not self._metrics:
            report_dict = result_dict
        else:
            report_dict = {}
            for key in self._metrics:
                if isinstance(self._metrics, dict):
                    metric = self._metrics[key]
                else:
                    metric = key
                report_dict[key] = result_dict[metric]
        if self._results_postprocessing_fn:
            report_dict = self._results_postprocessing_fn(report_dict)
        return report_dict

    def _get_eval_result(self, env: CallbackEnv) -> dict:
        eval_result = {}
        for entry in env.evaluation_result_list:
            data_name, eval_name, result = entry[0:3]
            if len(entry) > 4:
                stdv = entry[4]
                suffix = "-mean"
            else:
                stdv = None
                suffix = ""
            if data_name not in eval_result:
                eval_result[data_name] = {}
            eval_result[data_name][eval_name + suffix] = result
            if stdv is not None:
                eval_result[data_name][eval_name + "-stdv"] = stdv
        return eval_result

    @contextmanager
    def _get_checkpoint(
        self, model: Booster, epoch: int, filename: str, frequency: int
    ) -> Optional[Union[Checkpoint, LegacyCheckpoint]]:
        if epoch % frequency > 0 or (not epoch and frequency > 1):
            # Skip 0th checkpoint if frequency > 1
            yield None
            return

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            model.save_model(os.path.join(checkpoint_dir, filename))

            if _use_storage_context():
                checkpoint = Checkpoint.from_directory(checkpoint_dir)
            else:
                checkpoint = LegacyCheckpoint.from_directory(checkpoint_dir)

            yield checkpoint

    def __call__(self, env: CallbackEnv) -> None:
        with self._get_checkpoint(
            model=env.model,
            epoch=env.iteration,
            filename=self._filename,
            frequency=self._frequency,
        ) as checkpoint:
            eval_result = self._get_eval_result(env)
            report_dict = self._get_report_dict(eval_result)
            train.report(report_dict, checkpoint=checkpoint)


class _TuneCheckpointCallback(TuneCallback):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "`ray.tune.integration.lightgbm._TuneCheckpointCallback` is deprecated."
        )


class TuneReportCallback(TuneReportCheckpointCallback):
    def __init__(self, *args, **kwargs):
        with log_once("tune_report_deprecated"):
            warnings.warn(
                "`ray.tune.integration.lightgbm.TuneReportCallback` is deprecated. "
                "Use `ray.tune.integration.lightgbm.TuneCheckpointReportCallback` "
                "instead."
            )
        super().__init__(*args, **kwargs)
