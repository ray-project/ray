from typing import Dict, List, Union
from collections import OrderedDict
from ray import tune

import os

from ray.tune.utils import flatten_dict
from xgboost.core import Booster

try:
    from xgboost.callback import TrainingCallback
except ImportError:

    class TrainingCallback:
        pass


class TuneCallback(TrainingCallback):
    """Base class for Tune's XGBoost callbacks."""

    def __call__(self, env):
        """Compatibility with xgboost<1.3"""
        return self.after_iteration(env.model, env.iteration,
                                    env.evaluation_result_list)

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
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

    def _get_report_dict(self, evals_log):
        if isinstance(evals_log, OrderedDict):
            # xgboost>=1.3
            result_dict = flatten_dict(evals_log, delimiter="-")
            for k in list(result_dict):
                result_dict[k] = result_dict[k][-1]
        else:
            # xgboost<1.3
            result_dict = dict(evals_log)
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

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):

        report_dict = self._get_report_dict(evals_log)
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
        frequency (int): How often to save checkpoints. Per default, a
            checkpoint is saved every five iterations.

    """

    def __init__(self, filename: str = "checkpoint", frequency: int = 5):
        self._filename = filename
        self._frequency = frequency

    @staticmethod
    def _create_checkpoint(model: Booster, epoch: int, filename: str,
                           frequency: int):
        if epoch % frequency > 0:
            return
        with tune.checkpoint_dir(step=epoch) as checkpoint_dir:
            model.save_model(os.path.join(checkpoint_dir, filename))

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        self._create_checkpoint(model, epoch, self._filename, self._frequency)


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
        frequency (int): How often to save checkpoints. Per default, a
            checkpoint is saved every five iterations.

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
    _checkpoint_callback_cls = _TuneCheckpointCallback
    _report_callbacks_cls = TuneReportCallback

    def __init__(self,
                 metrics: Union[None, str, List[str], Dict[str, str]] = None,
                 filename: str = "checkpoint",
                 frequency: int = 5):
        self._checkpoint = self._checkpoint_callback_cls(filename, frequency)
        self._report = self._report_callbacks_cls(metrics)

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        self._checkpoint.after_iteration(model, epoch, evals_log)
        self._report.after_iteration(model, epoch, evals_log)
