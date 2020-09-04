from typing import Dict, List, Union
from ray import tune

import os


class TuneCallback:
    """Base class for Tune's XGBoost callbacks."""
    pass

    def __call__(self, env):
        raise NotImplementedError


class TuneReportCallback(TuneCallback):
    """XGBoost to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to XGBoost,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to XGBoost. If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from XGBoost.

    Example:

    .. code-block:: python

        import xgboost
        from ray.tune.integration.xgboost import TuneReportCallback

        config = {
            # ...
            "eval_metric": ["auc", "logloss"]
        }

        # Report only log loss to Tune after each validation epoch:
        bst = xgb.train(
            config,
            train_set,
            evals=[(test_set, "eval")],
            verbose_eval=False,
            callbacks=[TuneReportCallback({"loss": "eval-logloss"})])

    """

    def __init__(self,
                 metrics: Union[None, str, List[str], Dict[str, str]] = None):
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

    def __call__(self, env):
        result_dict = dict(env.evaluation_result_list)
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
        tune.report(**report_dict)


class _TuneCheckpointCallback(TuneCallback):
    """XGBoost checkpoint callback

    Saves checkpoints after each validation step.

    Checkpoint are currently not registered if no ``tune.report()`` call
    is made afterwards. Consider using ``TuneReportCheckpointCallback``
    instead.

    Args:
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".

    """

    def __init__(self, filename: str = "checkpoint"):
        self._filename = filename

    def __call__(self, env):
        with tune.checkpoint_dir(step=env.iteration) as checkpoint_dir:
            env.model.save_model(os.path.join(checkpoint_dir, self._filename))


class TuneReportCheckpointCallback(TuneCallback):
    """XGBoost report and checkpoint callback

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to XGBoost,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to XGBoost.
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint". If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from XGBoost.

    Example:

    .. code-block:: python

        import xgboost
        from ray.tune.integration.xgboost import TuneReportCheckpointCallback

        config = {
            # ...
            "eval_metric": ["auc", "logloss"]
        }

        # Report only log loss to Tune after each validation epoch.
        # Save model as `xgboost.mdl`.
        bst = xgb.train(
            config,
            train_set,
            evals=[(test_set, "eval")],
            verbose_eval=False,
            callbacks=[TuneReportCheckpointCallback(
                {"loss": "eval-logloss"}, "xgboost.mdl)])

    """

    def __init__(self,
                 metrics: Union[None, str, List[str], Dict[str, str]] = None,
                 filename: str = "checkpoint"):
        self._checkpoint = _TuneCheckpointCallback(filename)
        self._report = TuneReportCallback(metrics)

    def __call__(self, env):
        self._checkpoint(env)
        self._report(env)
