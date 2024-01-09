from collections import OrderedDict
from contextlib import contextmanager
from typing import Callable, Dict, List, Union, Optional

import os
import tempfile
import warnings

from ray import train, tune

from ray.train import Checkpoint
from ray.tune.utils import flatten_dict
from ray.util import log_once
from ray.util.annotations import Deprecated
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
        return self.after_iteration(
            env.model, env.iteration, env.evaluation_result_list
        )

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        raise NotImplementedError


class TuneReportCheckpointCallback(TuneCallback):
    """XGBoost callback to save checkpoints and report metrics.

    Saves checkpoints after each validation step. Also reports metrics to Ray Train
    or Ray Tune.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to XGBoost,
            and it will be reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to XGBoost.
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint". If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from XGBoost.
        frequency: How often to save checkpoints. Defaults to 0 (no checkpoints
            are saved during training). A checkpoint is always saved at the end
            of training.
        results_postprocessing_fn: An optional Callable that takes in
            the dict that will be reported to Tune (after it has been flattened)
            and returns a modified dict that will be reported instead. Can be used
            to eg. average results across CV fold when using ``xgboost.cv``.

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

    _checkpoint_callback_cls = None
    # ATTN: There's a typo here (callback_s_) compared to lightgbm.
    # The property is used in e.g. XGBoost-Ray, so we can't just rename it.
    # Just be aware of it when changing logic in both xgboost + lightgbm
    _report_callbacks_cls = None

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = "checkpoint",
        frequency: int = 1,
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

        if self._results_postprocessing_fn:
            report_dict = self._results_postprocessing_fn(report_dict)

        return report_dict

    @staticmethod
    def _create_checkpoint(model: Booster, epoch: int, filename: str, frequency: int):
        # Deprecate: Remove in Ray 2.8
        if not frequency or epoch % frequency > 0 or (not epoch and frequency > 1):
            # Skip 0th checkpoint if frequency > 1
            return
        with tune.checkpoint_dir(step=epoch) as checkpoint_dir:
            model.save_model(os.path.join(checkpoint_dir, filename))

    @contextmanager
    def _get_checkpoint(
        self, model: Booster, epoch: int, filename: str, frequency: int
    ) -> Optional[Checkpoint]:
        if not frequency or epoch % frequency > 0 or (not epoch and frequency > 1):
            # Skip 0th checkpoint if frequency > 1
            yield None
            return

        with tempfile.TemporaryDirectory() as checkpoint_dir:
            model.save_model(os.path.join(checkpoint_dir, filename))
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            yield checkpoint

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        if self._frequency > 0 and self._checkpoint_callback_cls:
            self._checkpoint_callback_cls.after_iteration(self, model, epoch, evals_log)
        if self._report_callbacks_cls:
            # Deprecate: Raise error in Ray 2.8
            if log_once("xgboost_ray_legacy"):
                warnings.warn(
                    "You are using an outdated version of XGBoost-Ray that won't be "
                    "compatible with future releases of Ray. Please update XGBoost-Ray "
                    "with `pip install -U xgboost_ray`."
                )

            self._report_callbacks_cls.after_iteration(self, model, epoch, evals_log)
            return

        with self._get_checkpoint(
            model=model, epoch=epoch, filename=self._filename, frequency=self._frequency
        ) as checkpoint:
            report_dict = self._get_report_dict(evals_log)
            train.report(report_dict, checkpoint=checkpoint)


class _TuneCheckpointCallback(TuneCallback):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "`ray.tune.integration.xgboost._TuneCheckpointCallback` is deprecated."
        )


@Deprecated
class TuneReportCallback(TuneReportCheckpointCallback):
    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        results_postprocessing_fn: Optional[
            Callable[[Dict[str, Union[float, List[float]]]], Dict[str, float]]
        ] = None,
    ):
        if log_once("tune_xgboost_report_deprecated"):
            warnings.warn(
                "`ray.tune.integration.xgboost.TuneReportCallback` is deprecated. "
                "Use `ray.tune.integration.xgboost.TuneCheckpointReportCallback` "
                "instead."
            )
        super().__init__(
            metrics=metrics,
            results_postprocessing_fn=results_postprocessing_fn,
            frequency=0,
        )
