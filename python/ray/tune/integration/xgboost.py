from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
import tempfile
from typing import Callable, Dict, List, Union, Optional

from xgboost.core import Booster

from ray import train
from ray.train import Checkpoint
from ray.tune.utils import flatten_dict
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
        metrics: Metrics to report. If this is a list,
            each item describes the metric key reported to XGBoost,
            and it will be reported under the same name.
            This can also be a dict of {<key-to-report>: <xgboost-metric-key>},
            which can be used to rename xgboost default metrics.
        filename: Customize the saved checkpoint file type by passing
            a filename. Defaults to "model.json".
        frequency: How often to save checkpoints. Defaults to 0 (no checkpoints
            are saved during training).
        checkpoint_at_end: Whether or not to save a checkpoint at the end of training.
        results_postprocessing_fn: An optional Callable that takes in
            the metrics dict that will be reported (after it has been flattened)
            and returns a modified dict. For example, this can be used to
            average results across CV fold when using ``xgboost.cv``.

    Example:

    .. code-block:: python

        import xgboost
        from ray.tune.integration.xgboost import TuneReportCheckpointCallback

        config = {
            # ...
            "eval_metric": ["auc", "logloss"]
        }

        # Report log loss to Tune after each validation epoch.
        bst = xgb.train(
            config,
            train_set,
            evals=[(test_set, "eval")],
            verbose_eval=False,
            callbacks=[
                TuneReportCheckpointCallback(
                    metrics={"loss": "eval-logloss"}, frequency=1
                )
            ],
        )

    """

    CHECKPOINT_NAME = "model.json"

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = CHECKPOINT_NAME,
        frequency: int = 0,
        checkpoint_at_end: bool = True,
        results_postprocessing_fn: Optional[
            Callable[[Dict[str, Union[float, List[float]]]], Dict[str, float]]
        ] = None,
    ):
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics
        self._filename = filename
        self._frequency = frequency
        self._checkpoint_at_end = checkpoint_at_end
        self._results_postprocessing_fn = results_postprocessing_fn

        # Keeps track of the eval metrics from the last iteration,
        # so that the latest metrics can be reported with the checkpoint
        # at the end of training.
        self._evals_log = None

    @classmethod
    def get_model(
        cls, checkpoint: Checkpoint, filename: str = CHECKPOINT_NAME
    ) -> Booster:
        """Retrieve the model stored in a checkpoint reported by this callback."""
        with checkpoint.as_directory() as checkpoint_path:
            booster = Booster()
            booster.load_model(Path(checkpoint_path, filename).as_posix())
            return booster

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

    @contextmanager
    def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            model.save_model(Path(temp_checkpoint_dir, self._filename).as_posix())
            yield Checkpoint(temp_checkpoint_dir)

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        self._evals_log = evals_log

        checkpointing_disabled = self._frequency == 0
        # Ex: if frequency=2, checkpoint at epoch 1, 3, 5, ... (counting from 0)
        should_checkpoint = (
            not checkpointing_disabled and (epoch + 1) % self._frequency == 0
        )

        report_dict = self._get_report_dict(evals_log)
        if should_checkpoint:
            with self._get_checkpoint(model=model) as checkpoint:
                train.report(report_dict, checkpoint=checkpoint)
        else:
            train.report(report_dict)

    def after_training(self, model: Booster):
        if not self._checkpoint_at_end:
            return model

        report_dict = self._get_report_dict(self._evals_log) if self._evals_log else {}
        with self._get_checkpoint(model=model) as checkpoint:
            train.report(report_dict, checkpoint=checkpoint)

        return model


@Deprecated
class TuneReportCallback(TuneReportCheckpointCallback):
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove in 2.11.
        raise DeprecationWarning(
            "`TuneReportCallback` is deprecated. "
            "Use `ray.tune.integration.xgboost.TuneReportCheckpointCallback` instead."
        )


class _TuneCheckpointCallback(TuneCallback):
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove ASAP after merging in xgboost_ray.
        raise DeprecationWarning(
            "Use `ray.tune.integration.xgboost.TuneReportCheckpointCallback` instead."
        )
