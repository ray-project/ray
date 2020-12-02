from typing import Dict, List, Union
from ray import tune

import os

try:
    from xgboost_ray.session import is_xgboost_ray_actor, get_actor_rank, \
        put_queue
except ImportError:

    def is_xgboost_ray_actor():
        return False

    def get_actor_rank():
        return 0

    def put_queue(_):
        return False


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
        # Only one worker should report to Tune
        if is_xgboost_ray_actor() and get_actor_rank() != 0:
            return

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

        if is_xgboost_ray_actor():
            put_queue(lambda: tune.report(**report_dict))
        else:
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
        def _create_checkpoint():
            with tune.checkpoint_dir(step=env.iteration) as checkpoint_dir:
                env.model.save_model(
                    os.path.join(checkpoint_dir, self._filename))

        if not is_xgboost_ray_actor():
            _create_checkpoint()
        elif get_actor_rank() == 0:
            put_queue(lambda: _create_checkpoint())


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
