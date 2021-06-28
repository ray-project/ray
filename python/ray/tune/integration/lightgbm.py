from typing import Dict, List, Union, Any, Callable
from ray import tune

import os

from ray.tune.utils import flatten_dict
from lightgbm.callback import CallbackEnv


def tune_report_callback(
        metrics: Union[None, str, List[str], Dict[str,
                                                  str]] = None) -> Callable:
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
        from ray.tune.integration.lightgbm import tune_report_callback

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
            callbacks=[tune_report_callback({"loss": "eval-binary_logloss"})])
    """

    if isinstance(metrics, str):
        metrics = [metrics]

    def _get_report_dict(evals_log: Dict[str, Dict[str, list]]) -> dict:
        result_dict = flatten_dict(evals_log, delimiter="-")
        if not metrics:
            report_dict = result_dict
        else:
            report_dict = {}
            for key in metrics:
                if isinstance(metrics, dict):
                    metric = metrics[key]
                else:
                    metric = key
                report_dict[key] = result_dict[metric]
        return report_dict

    def _callback(env: CallbackEnv) -> None:
        eval_result = {}
        for data_name, eval_name, result, _ in env.evaluation_result_list:
            if data_name not in eval_result:
                eval_result[data_name] = {}
            eval_result[data_name][eval_name] = result

        report_dict = _get_report_dict(eval_result)
        tune.report(**report_dict)

    _callback.order = 20  # type: ignore
    return _callback


def tune_report_checkpoint_callback(metrics: Union[None, str, List[str],
                                                   Dict[str, str]] = None,
                                    filename: str = "checkpoint",
                                    frequency: int = 5):
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
            tune_report_checkpoint_callback
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
            callbacks=[tune_report_checkpoint_callback(
                {"loss": "eval-binary_logloss"}, "lightgbm.mdl)])

    """

    report_callback = tune_report_callback(metrics)

    def _callback(env: CallbackEnv) -> None:
        if env.iteration % frequency <= 0:
            with tune.checkpoint_dir(step=env.iteration) as checkpoint_dir:
                env.model.save_model(os.path.join(checkpoint_dir, filename))
        report_callback(env)

    _callback.order = 21  # type: ignore
    return _callback
