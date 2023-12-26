from collections import OrderedDict
from contextlib import contextmanager
import os
import tempfile
from typing import Callable, Dict, List, Union, Optional, Type
import warnings

from xgboost.core import Booster

from ray import train, tune
from ray.train import Checkpoint
from ray.train.xgboost import XGBoostCheckpoint
from ray.train.constants import _DEPRECATED_VALUE
from ray.tune.utils import flatten_dict
from ray.util import log_once
from ray.util.annotations import Deprecated

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
        frequency: int = 1,
        checkpoint_at_end: bool = True,
        results_postprocessing_fn: Optional[
            Callable[[Dict[str, Union[float, List[float]]]], Dict[str, float]]
        ] = None,
        checkpoint_cls: Type[XGBoostCheckpoint] = XGBoostCheckpoint,
        filename: str = _DEPRECATED_VALUE,
    ):
        if filename != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "`filename` is deprecated. Supply a custom `checkpoint_cls` "
                "subclassing `ray.train.xgboost.XGBoostCheckpoint` instead."
            )

        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics
        self._frequency = frequency
        self._checkpoint_at_end = checkpoint_at_end

        if not issubclass(checkpoint_cls, XGBoostCheckpoint):
            raise ValueError(
                "`checkpoint_cls` must subclass `ray.train.xgboost.XGBoostCheckpoint`"
            )
        self._checkpoint_cls = checkpoint_cls

        self._results_postprocessing_fn = results_postprocessing_fn

        # Keeps track of the eval metrics from the last iteration,
        # so that the latest metrics can be reported at the end of training.
        self._evals_log = None

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
    def _get_checkpoint(self, model: Booster, epoch: int) -> Optional[Checkpoint]:
        checkpointing_disabled = self._frequency == 0
        # Ex: if frequency=2, checkpoint at epoch 1, 3, 5, ... (counting from 0)
        should_checkpoint = (epoch + 1) % self._frequency == 0
        if checkpointing_disabled or not should_checkpoint:
            yield None
            return

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            checkpoint = self._checkpoint_cls.from_model(
                model, path=temp_checkpoint_dir
            )
            yield checkpoint

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        self._evals_log = evals_log

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

        with self._get_checkpoint(model=model, epoch=epoch) as checkpoint:
            report_dict = self._get_report_dict(evals_log)
            train.report(report_dict, checkpoint=checkpoint)

    def after_training(self, model: Booster):
        if not self._checkpoint_at_end:
            return model

        assert self._evals_log is not None

        total_epochs = model.num_boosted_rounds()
        with self._get_checkpoint(model=model, epoch=total_epochs) as checkpoint:
            final_metrics = self._get_report_dict(self._evals_log)
            train.report(final_metrics, checkpoint=checkpoint)

        return model


@Deprecated
class TuneReportCallback(TuneReportCheckpointCallback):
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove in 2.11.
        raise DeprecationWarning(
            "`TuneReportCallback` is deprecated. "
            "Use `ray.tune.integration.xgboost.TuneReportCheckpointCallback` instead."
        )
