from typing import Dict, List, Union
from ray import tune

import os

from ray.tune.utils import flatten_dict
from lightgbm.callback import CallbackEnv
from lightgbm.basic import Booster


class TuneCallback:
    """Base class for Tune's LightGBM callbacks."""

    pass


class TuneReportCallback(TuneCallback):
    """Create a callback that reports metrics to Ray Tune.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to LightGBM,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to LightGBM. If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from LightGBM.

    Example:

    .. code-block:: python

        import lightgbm
        from ray.tune.integration.lightgbm import TuneReportCallback

        config = {
            # ...
            "metric": ["binary_logloss", "binary_error"],
        }

        # Report only log loss to Tune after each validation epoch:
        bst = lightgbm.train(
            config,
            train_set,
            valid_sets=[test_set],
            valid_names=["eval"],
            verbose_eval=False,
            callbacks=[TuneReportCallback({"loss": "eval-binary_logloss"})])
    """

    order = 20

    def __init__(self, metrics: Union[None, str, List[str], Dict[str, str]] = None):
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

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
        return report_dict

    def _get_eval_result(self, env: CallbackEnv) -> dict:
        eval_result = {}
        for data_name, eval_name, result, _ in env.evaluation_result_list:
            if data_name not in eval_result:
                eval_result[data_name] = {}
            eval_result[data_name][eval_name] = result
        return eval_result

    def __call__(self, env: CallbackEnv) -> None:
        eval_result = self._get_eval_result(env)
        report_dict = self._get_report_dict(eval_result)
        tune.report(**report_dict)


class _TuneCheckpointCallback(TuneCallback):
    """LightGBM checkpoint callback

    Saves checkpoints after each validation step.

    Checkpoint are currently not registered if no ``tune.report()`` call
    is made afterwards. Consider using ``TuneReportCheckpointCallback``
    instead.

    Args:
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        frequency (int): How often to save checkpoints. Per default, a
            checkpoint is saved every five iterations.

    """

    order = 19

    def __init__(self, filename: str = "checkpoint", frequency: int = 5):
        self._filename = filename
        self._frequency = frequency

    @staticmethod
    def _create_checkpoint(model: Booster, epoch: int, filename: str, frequency: int):
        if epoch % frequency > 0:
            return
        with tune.checkpoint_dir(step=epoch) as checkpoint_dir:
            model.save_model(os.path.join(checkpoint_dir, filename))

    def __call__(self, env: CallbackEnv) -> None:
        self._create_checkpoint(
            env.model, env.iteration, self._filename, self._frequency
        )


class TuneReportCheckpointCallback(TuneCallback):
    """Creates a callback that reports metrics and checkpoints model.

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to LightGBM,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to LightGBM.
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint". If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from LightGBM.
        frequency (int): How often to save checkpoints. Per default, a
            checkpoint is saved every five iterations.

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

    order = 21

    _checkpoint_callback_cls = _TuneCheckpointCallback
    _report_callback_cls = TuneReportCallback

    def __init__(
        self,
        metrics: Union[None, str, List[str], Dict[str, str]] = None,
        filename: str = "checkpoint",
        frequency: int = 5,
    ):
        self._checkpoint = self._checkpoint_callback_cls(filename, frequency)
        self._report = self._report_callback_cls(metrics)

    def __call__(self, env: CallbackEnv) -> None:
        self._checkpoint(env)
        self._report(env)
